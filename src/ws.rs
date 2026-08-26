use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::StreamExt;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::Instant;
use tungstenite::error::CapacityError;

use crate::handshake::{self, Check};
use crate::metrics::Metrics;
use crate::peer::{spawn_writer, Peer};
use crate::protocol::{Connection, Role, Version};
use crate::room::{run_all, Destinations};
use crate::state::AppState;

/// The daemon expects a control notification within 8 seconds and re-opens data sockets from
/// the inventory we push, so these two steps mirror `ownership.ex` exactly.
const NUDGE_AFTER: Duration = Duration::from_secs(10);
const CONTROL_GRACE: Duration = Duration::from_secs(5);
const CLOSE_DRAIN: Duration = Duration::from_secs(1);

pub async fn upgrade(
    State(state): State<Arc<AppState>>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
    upgrade: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> Response {
    // Order matches socket.ex: upgrade check, then parameters, then admission.
    let Ok(upgrade) = upgrade else {
        return text(StatusCode::UPGRADE_REQUIRED, "Expected WebSocket upgrade");
    };

    let query = query.map(|Query(query)| query).unwrap_or_default();
    let connection = match Connection::from_query(&query) {
        Ok(connection) => connection,
        Err(message) => return text(StatusCode::BAD_REQUEST, message),
    };

    if !state.config.allows(&connection.server_id) {
        Metrics::inc(&state.metrics.connection_rejections);
        return text(StatusCode::FORBIDDEN, "Relay serverId not allowed");
    }

    let permit = match state.connection_slots.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            Metrics::inc(&state.metrics.connection_rejections);
            return text(StatusCode::SERVICE_UNAVAILABLE, "Relay connection capacity");
        }
    };

    // Draining only blocks brand new routes; existing sessions keep reconnecting.
    if state.draining() && !state.rooms.contains(&connection.server_id) {
        return text(StatusCode::SERVICE_UNAVAILABLE, "draining");
    }

    let limit = connection.max_payload_bytes();
    upgrade
        .max_frame_size(limit)
        .max_message_size(limit)
        .on_upgrade(move |socket| serve(state, connection, socket, permit))
}

fn text(status: StatusCode, body: &'static str) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain")], body).into_response()
}

async fn serve(
    state: Arc<AppState>,
    connection: Connection,
    socket: WebSocket,
    _permit: OwnedSemaphorePermit,
) {
    let id = state.next_socket_id();
    let (sink, mut stream) = socket.split();
    let (peer, writer) = spawn_writer(
        id,
        sink,
        state.config.control_queue_bytes,
        state.config.delivery_timeout,
    );

    state.metrics.active_websockets.fetch_add(1, Ordering::Relaxed);
    run_all(state.rooms.attach(&connection, peer.clone()));

    if connection.version == Version::V2 && connection.role == Role::Client {
        spawn_control_watchdog(
            Arc::clone(&state),
            connection.server_id.clone(),
            connection.connection_id.clone(),
            id,
        );
    }

    let writer_ended = tokio::select! {
        _ = read_loop(&state, &connection, &peer, &mut stream) => false,
        _ = writer => true,
    };

    run_all(state.rooms.detach(&connection, id));

    if writer_ended {
        // tungstenite only flushes queued close frames while the stream is still polled;
        // dropping read_loop here would leave the peer seeing 1006 instead of our close code.
        let _ = tokio::time::timeout(
            CLOSE_DRAIN,
            async { while stream.next().await.is_some() {} },
        )
        .await;
    }

    state.metrics.active_websockets.fetch_sub(1, Ordering::Relaxed);
}

async fn read_loop(
    state: &Arc<AppState>,
    connection: &Connection,
    peer: &Peer,
    stream: &mut futures_util::stream::SplitStream<WebSocket>,
) {
    while let Some(next) = stream.next().await {
        let message = match next {
            Ok(message) => message,
            Err(error) => {
                // tungstenite ends the stream on an oversized message without emitting a close
                // frame, so the 1009 the protocol requires has to be sent explicitly.
                if is_capacity_error(&error) {
                    peer.close(1009, "Message too large");
                }
                return;
            }
        };

        let payload: &[u8] = match &message {
            Message::Text(text) => text.as_bytes(),
            Message::Binary(bytes) => bytes,
            // Ping and Pong are answered by the protocol layer as long as we keep polling.
            Message::Ping(_) | Message::Pong(_) => continue,
            // tungstenite queues the RFC 6455 close echo itself and rejects any close we
            // try to send afterwards (`SendAfterClosing`). It can only flush that echo while
            // the stream is still being polled, so keep polling: the next `next()` completes
            // the handshake and then ends the stream. Returning here instead would leave the
            // peer with a truncated connection reported as 1006.
            Message::Close(_) => continue,
        };

        if !forward(state, connection, peer, &message, payload).await {
            return;
        }
    }
}

/// Returns false when the source connection must stop reading.
async fn forward(
    state: &Arc<AppState>,
    connection: &Connection,
    peer: &Peer,
    message: &Message,
    payload: &[u8],
) -> bool {
    if connection.is_control() {
        answer_control_ping(peer, payload);
        return true;
    }

    if connection.role == Role::Client {
        if let Check::Reject(_) = handshake::check(payload) {
            Metrics::inc(&state.metrics.handshake_rejections);
            peer.close(1008, "Invalid handshake key");
            return false;
        }
    }

    let deadline = Instant::now() + state.config.delivery_timeout;
    match state.rooms.destinations(connection, peer.id) {
        Destinations::Control => true,
        Destinations::Detached => false,
        Destinations::Peers(peers) => {
            if !deliver(state, peers, message, deadline).await {
                peer.close(1013, "Delivery unavailable");
                return false;
            }
            true
        }
        Destinations::Wait(rx) => {
            let wait_duration = deadline
                .saturating_duration_since(Instant::now())
                .min(state.config.data_attach_timeout);
            match tokio::time::timeout(wait_duration, rx).await {
                Ok(Ok(destination)) => {
                    if !deliver(state, vec![destination], message, deadline).await {
                        peer.close(1013, "Delivery unavailable");
                        return false;
                    }
                    true
                }
                _ => {
                    state.rooms.drop_waiter(
                        &connection.server_id,
                        &connection.connection_id,
                        peer.id,
                    );
                    peer.close(1013, "Data route unavailable");
                    false
                }
            }
        }
    }
}

/// Fans out to every destination and waits for all of them. Mirrors `delivery.ex:26-31`:
/// the source resumes when at least one write succeeded, and each failed destination is shed.
/// Returns false when every destination failed (empty list counts as success).
async fn deliver(state: &Arc<AppState>, peers: Vec<Peer>, message: &Message, deadline: Instant) -> bool {
    if peers.is_empty() {
        return true;
    }

    let bytes = payload_len(message) as u64;
    let results = futures_util::future::join_all(
        peers.iter().map(|peer| peer.deliver(message.clone(), deadline)),
    )
    .await;

    let mut any_success = false;
    for (peer, written) in peers.iter().zip(results) {
        if written {
            any_success = true;
            Metrics::inc(&state.metrics.frames_forwarded);
            Metrics::add(&state.metrics.bytes_forwarded, bytes);
        } else {
            Metrics::inc(&state.metrics.slow_consumer_closes);
            peer.close(1013, "Slow consumer");
        }
    }
    any_success
}

/// Answers on the very socket that sent the ping, mirroring `socket.ex:376-380`. Looking the
/// control peer up in the routing table instead would misdeliver the pong whenever the control
/// slot has just been taken over by a reconnecting daemon.
fn answer_control_ping(peer: &Peer, payload: &[u8]) {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return;
    };
    if value.get("type").and_then(serde_json::Value::as_str) != Some("ping") {
        return;
    }
    let pong = serde_json::json!({ "type": "pong", "ts": now_millis() }).to_string();
    if !peer.control(pong) {
        peer.close(1013, "Delivery unavailable");
    }
}

fn spawn_control_watchdog(
    state: Arc<AppState>,
    server_id: String,
    connection_id: String,
    source: crate::peer::SocketId,
) {
    tokio::spawn(async move {
        tokio::time::sleep(NUDGE_AFTER).await;
        let Some(action) = state.rooms.nudge(&server_id, &connection_id, source) else {
            return;
        };
        action.run();

        tokio::time::sleep(CONTROL_GRACE).await;
        if let Some(control) =
            state.rooms.control_if_awaiting(&server_id, &connection_id, source)
        {
            control.close(1011, "Control unresponsive");
        }
    });
}

fn payload_len(message: &Message) -> usize {
    match message {
        Message::Text(text) => text.len(),
        Message::Binary(bytes) => bytes.len(),
        _ => 0,
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn is_capacity_error(error: &axum::Error) -> bool {
    use std::error::Error as _;

    let mut source = error.source();
    while let Some(err) = source {
        if let Some(tungstenite_err) = err.downcast_ref::<tungstenite::Error>() {
            return matches!(
                tungstenite_err,
                tungstenite::Error::Capacity(CapacityError::MessageTooLong { .. })
            );
        }
        source = err.source();
    }
    false
}
