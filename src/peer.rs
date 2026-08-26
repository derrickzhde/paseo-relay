use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message, Utf8Bytes, WebSocket};
use futures_util::stream::SplitSink;
use futures_util::SinkExt;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub type SocketId = u64;

const GRACEFUL_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

pub enum Outbound {
    Frame { msg: Message, permit: OwnedSemaphorePermit, ack: oneshot::Sender<bool> },
}

/// A handle to one connection's writer task. Cloning is cheap and every clone addresses the
/// same underlying socket.
#[derive(Clone)]
pub struct Peer {
    pub id: SocketId,
    /// One permit total. It travels with the message and is released by the writer once the
    /// frame has actually been handed to the socket, so a cancelled source cannot free the
    /// slot early.
    inflight: Arc<Semaphore>,
    data_tx: mpsc::Sender<Outbound>,
    control_tx: mpsc::UnboundedSender<Utf8Bytes>,
    control_bytes: Arc<AtomicUsize>,
    control_limit: usize,
    close_tx: mpsc::Sender<(u16, String)>,
}

impl Peer {
    /// Enqueues one payload and waits until the writer has pushed it into the socket.
    /// Returns false on timeout, writer death, or write failure; the caller is expected to
    /// close and detach the peer in that case.
    pub async fn deliver(&self, msg: Message, deadline: Instant) -> bool {
        let attempt = async {
            let permit = Arc::clone(&self.inflight).acquire_owned().await.ok()?;
            let (ack_tx, ack_rx) = oneshot::channel();
            self.data_tx.send(Outbound::Frame { msg, permit, ack: ack_tx }).await.ok()?;
            ack_rx.await.ok()
        };
        matches!(tokio::time::timeout_at(deadline, attempt).await, Ok(Some(true)))
    }

    /// Queues a relay-generated control notification. Returns false once the queue exceeds its
    /// byte budget, which means the destination is not draining and should be shed.
    pub fn control(&self, text: String) -> bool {
        let bytes = text.len();
        if self.control_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes > self.control_limit {
            self.control_bytes.fetch_sub(bytes, Ordering::Relaxed);
            return false;
        }
        if self.control_tx.send(Utf8Bytes::from(text)).is_err() {
            self.control_bytes.fetch_sub(bytes, Ordering::Relaxed);
            return false;
        }
        true
    }

    /// Never blocks and never competes with data for queue space. A peer that already has a
    /// close queued keeps the first reason.
    pub fn close(&self, code: u16, reason: impl Into<String>) {
        let _ = self.close_tx.try_send((code, reason.into()));
    }
}

pub fn spawn_writer(
    id: SocketId,
    sink: SplitSink<WebSocket, Message>,
    control_limit: usize,
    write_timeout: Duration,
) -> (Peer, JoinHandle<()>) {
    let (data_tx, data_rx) = mpsc::channel(1);
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let (close_tx, close_rx) = mpsc::channel(1);
    let control_bytes = Arc::new(AtomicUsize::new(0));

    let writer = tokio::spawn(writer_task(
        sink,
        data_rx,
        control_rx,
        close_rx,
        Arc::clone(&control_bytes),
        write_timeout,
    ));

    (
        Peer {
            id,
            inflight: Arc::new(Semaphore::new(1)),
            data_tx,
            control_tx,
            control_bytes,
            control_limit,
            close_tx,
        },
        writer,
    )
}

async fn writer_task(
    mut sink: SplitSink<WebSocket, Message>,
    mut data_rx: mpsc::Receiver<Outbound>,
    mut control_rx: mpsc::UnboundedReceiver<Utf8Bytes>,
    mut close_rx: mpsc::Receiver<(u16, String)>,
    control_bytes: Arc<AtomicUsize>,
    write_timeout: Duration,
) {
    loop {
        tokio::select! {
            biased;

            Some((code, reason)) = close_rx.recv() => {
                graceful(&mut sink, code, reason).await;
                break;
            }

            Some(text) = control_rx.recv() => {
                control_bytes.fetch_sub(text.len(), Ordering::Relaxed);
                if !write_guarded(&mut sink, Message::Text(text), &mut close_rx, write_timeout).await {
                    break;
                }
            }

            Some(Outbound::Frame { msg, permit, ack }) = data_rx.recv() => {
                let written = write_guarded(&mut sink, msg, &mut close_rx, write_timeout).await;
                // Release the in-flight slot only after the socket is done with the frame.
                drop(permit);
                let _ = ack.send(written);
                if !written {
                    break;
                }
            }

            else => break,
        }
    }
}

/// One socket write, racing against a close request and the write deadline.
///
/// `SinkExt::send` has no published cancel-safety guarantee on tokio-tungstenite, so once the
/// write has begun the sink is never reused: both the close and timeout arms drop the socket
/// instead of attempting a close handshake.
async fn write_guarded(
    sink: &mut SplitSink<WebSocket, Message>,
    msg: Message,
    close_rx: &mut mpsc::Receiver<(u16, String)>,
    write_timeout: Duration,
) -> bool {
    if let Ok((code, reason)) = close_rx.try_recv() {
        graceful(sink, code, reason).await;
        return false;
    }

    tokio::select! {
        biased;
        result = sink.send(msg) => result.is_ok(),
        Some(_) = close_rx.recv() => false,
        _ = tokio::time::sleep(write_timeout) => false,
    }
}

async fn graceful(sink: &mut SplitSink<WebSocket, Message>, code: u16, reason: String) {
    let frame = Message::Close(Some(CloseFrame { code, reason: Utf8Bytes::from(reason) }));
    let _ = tokio::time::timeout(GRACEFUL_CLOSE_TIMEOUT, sink.send(frame)).await;
}
