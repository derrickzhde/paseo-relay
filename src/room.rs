use std::collections::HashMap;
use std::sync::Mutex;

use tokio::sync::oneshot;

use crate::peer::{Peer, SocketId};
use crate::protocol::{Connection, Role, Version};

/// Work produced while the routing table is locked and executed after the guard is dropped.
/// Cascading a detach can touch many peers; sending inside the lock would let one slow
/// destination stall the whole table.
pub enum Action {
    Close { peer: Peer, code: u16, reason: &'static str },
    Control { peer: Peer, text: String },
    Wake { tx: oneshot::Sender<Peer>, peer: Peer },
}

impl Action {
    pub fn run(self) {
        match self {
            Action::Close { peer, code, reason } => peer.close(code, reason),
            Action::Control { peer, text } => {
                if !peer.control(text) {
                    peer.close(1013, "Slow consumer");
                }
            }
            Action::Wake { tx, peer } => {
                let _ = tx.send(peer);
            }
        }
    }
}

pub fn run_all(actions: Vec<Action>) {
    for action in actions {
        action.run();
    }
}

pub enum Destinations {
    /// Forward to these peers. Empty means "no counterpart"; the frame is dropped.
    Peers(Vec<Peer>),
    /// A v2 client frame that arrived before the daemon opened its data channel.
    Wait(oneshot::Receiver<Peer>),
    /// The control channel never forwards; its inbound frames are handled locally.
    Control,
    /// The socket is no longer attached.
    Detached,
}

struct Waiter {
    source: SocketId,
    tx: oneshot::Sender<Peer>,
}

#[derive(Default)]
struct Room {
    v1_server: Option<Peer>,
    v1_client: Option<Peer>,
    control: Option<Peer>,
    data: HashMap<String, Peer>,
    clients: HashMap<String, HashMap<SocketId, Peer>>,
    waiters: HashMap<String, Vec<Waiter>>,
}

impl Room {
    fn is_empty(&self) -> bool {
        self.v1_server.is_none()
            && self.v1_client.is_none()
            && self.control.is_none()
            && self.data.is_empty()
            && self.clients.is_empty()
            && self.waiters.is_empty()
    }

    fn connection_ids(&self) -> Vec<&str> {
        self.clients.keys().map(String::as_str).collect()
    }

    fn sync_text(&self) -> String {
        serde_json::json!({ "type": "sync", "connectionIds": self.connection_ids() }).to_string()
    }

    fn awaiting_data(&self, connection_id: &str) -> bool {
        self.clients.contains_key(connection_id) && !self.data.contains_key(connection_id)
    }
}

#[derive(Default)]
pub struct Rooms {
    inner: Mutex<HashMap<String, Room>>,
}

impl Rooms {
    pub fn new() -> Self {
        Self::default()
    }

    /// Installs a peer into its slot, evicting whatever held it before. Returns the follow-up
    /// sends (eviction closes, control notifications, woken waiters).
    pub fn attach(&self, connection: &Connection, peer: Peer) -> Vec<Action> {
        let mut rooms = self.inner.lock().unwrap();
        let room = rooms.entry(connection.server_id.clone()).or_default();
        let mut actions = Vec::new();

        match (connection.version, connection.role) {
            (Version::V1, Role::Server) => {
                replace(&mut room.v1_server, peer, &mut actions);
            }
            (Version::V1, Role::Client) => {
                replace(&mut room.v1_client, peer, &mut actions);
            }
            (Version::V2, Role::Server) if connection.connection_id.is_empty() => {
                replace(&mut room.control, peer.clone(), &mut actions);
                // The daemon terminates a control socket that stays silent for 8 seconds
                // (relay-transport.ts CONTROL_READY_TIMEOUT_MS), so greet it immediately.
                actions.push(Action::Control { peer, text: room.sync_text() });
            }
            (Version::V2, Role::Server) => {
                if let Some(previous) =
                    room.data.insert(connection.connection_id.clone(), peer.clone())
                {
                    actions.push(Action::Close {
                        peer: previous,
                        code: 1008,
                        reason: "Replaced by new connection",
                    });
                }
                for waiter in room.waiters.remove(&connection.connection_id).unwrap_or_default() {
                    actions.push(Action::Wake { tx: waiter.tx, peer: peer.clone() });
                }
            }
            (Version::V2, Role::Client) => {
                room.clients
                    .entry(connection.connection_id.clone())
                    .or_default()
                    .insert(peer.id, peer);
                if let Some(control) = room.control.clone() {
                    let text = serde_json::json!({
                        "type": "connected",
                        "connectionId": connection.connection_id,
                    })
                    .to_string();
                    actions.push(Action::Control { peer: control, text });
                }
            }
        }

        actions
    }

    /// The single exit path for every socket, whatever the cause. Slots are compared by
    /// `SocketId` so a stale detach cannot evict the connection that replaced it.
    pub fn detach(&self, connection: &Connection, socket_id: SocketId) -> Vec<Action> {
        let mut rooms = self.inner.lock().unwrap();
        let Some(room) = rooms.get_mut(&connection.server_id) else {
            return Vec::new();
        };
        let mut actions = Vec::new();

        match (connection.version, connection.role) {
            (Version::V1, Role::Server) => clear(&mut room.v1_server, socket_id),
            (Version::V1, Role::Client) => clear(&mut room.v1_client, socket_id),
            (Version::V2, Role::Server) if connection.connection_id.is_empty() => {
                clear(&mut room.control, socket_id);
            }
            (Version::V2, Role::Server) => {
                let owned = room
                    .data
                    .get(&connection.connection_id)
                    .is_some_and(|peer| peer.id == socket_id);
                if owned {
                    room.data.remove(&connection.connection_id);
                    for peer in room
                        .clients
                        .get(&connection.connection_id)
                        .into_iter()
                        .flat_map(HashMap::values)
                    {
                        actions.push(Action::Close {
                            peer: peer.clone(),
                            code: 1012,
                            reason: "Server disconnected",
                        });
                    }
                }
            }
            (Version::V2, Role::Client) => {
                let emptied = match room.clients.get_mut(&connection.connection_id) {
                    Some(peers) => {
                        peers.remove(&socket_id);
                        peers.is_empty()
                    }
                    None => false,
                };
                if emptied {
                    room.clients.remove(&connection.connection_id);
                    if let Some(peer) = room.data.remove(&connection.connection_id) {
                        actions.push(Action::Close {
                            peer,
                            code: 1001,
                            reason: "Client disconnected",
                        });
                    }
                    if let Some(control) = room.control.clone() {
                        let text = serde_json::json!({
                            "type": "disconnected",
                            "connectionId": connection.connection_id,
                        })
                        .to_string();
                        actions.push(Action::Control { peer: control, text });
                    }
                }
            }
        }

        // A departing source must never leave a waiter behind, or the room can never be freed.
        room.waiters.retain(|_, waiters| {
            waiters.retain(|waiter| waiter.source != socket_id);
            !waiters.is_empty()
        });

        if room.is_empty() {
            rooms.remove(&connection.server_id);
        }

        actions
    }

    pub fn destinations(&self, connection: &Connection, socket_id: SocketId) -> Destinations {
        let mut rooms = self.inner.lock().unwrap();
        let Some(room) = rooms.get_mut(&connection.server_id) else {
            return Destinations::Detached;
        };

        match (connection.version, connection.role) {
            (Version::V1, Role::Server) => {
                Destinations::Peers(room.v1_client.clone().into_iter().collect())
            }
            (Version::V1, Role::Client) => {
                Destinations::Peers(room.v1_server.clone().into_iter().collect())
            }
            (Version::V2, Role::Server) if connection.connection_id.is_empty() => {
                Destinations::Control
            }
            (Version::V2, Role::Server) => Destinations::Peers(
                room.clients
                    .get(&connection.connection_id)
                    .map(|peers| peers.values().cloned().collect())
                    .unwrap_or_default(),
            ),
            (Version::V2, Role::Client) => match room.data.get(&connection.connection_id) {
                Some(peer) => Destinations::Peers(vec![peer.clone()]),
                None => {
                    let (tx, rx) = oneshot::channel();
                    room.waiters
                        .entry(connection.connection_id.clone())
                        .or_default()
                        .push(Waiter { source: socket_id, tx });
                    Destinations::Wait(rx)
                }
            },
        }
    }

    /// Re-sends the connection inventory when a client has been waiting too long for its data
    /// channel. Returns nothing once the data channel showed up.
    pub fn nudge(&self, server_id: &str, connection_id: &str, source: SocketId) -> Option<Action> {
        let rooms = self.inner.lock().unwrap();
        let room = rooms.get(server_id)?;
        if !room.awaiting_data(connection_id) {
            return None;
        }
        if !room
            .clients
            .get(connection_id)
            .is_some_and(|peers| peers.contains_key(&source))
        {
            return None;
        }
        let peer = room.control.clone()?;
        Some(Action::Control { peer, text: room.sync_text() })
    }

    pub fn control_if_awaiting(
        &self,
        server_id: &str,
        connection_id: &str,
        source: SocketId,
    ) -> Option<Peer> {
        let rooms = self.inner.lock().unwrap();
        let room = rooms.get(server_id)?;
        if !room.awaiting_data(connection_id) {
            return None;
        }
        if !room
            .clients
            .get(connection_id)
            .is_some_and(|peers| peers.contains_key(&source))
        {
            return None;
        }
        room.control.clone()
    }

    /// Drops a waiter that timed out so the room can eventually be reclaimed.
    pub fn drop_waiter(&self, server_id: &str, connection_id: &str, source: SocketId) {
        let mut rooms = self.inner.lock().unwrap();
        let Some(room) = rooms.get_mut(server_id) else { return };
        if let Some(waiters) = room.waiters.get_mut(connection_id) {
            waiters.retain(|waiter| waiter.source != source);
            if waiters.is_empty() {
                room.waiters.remove(connection_id);
            }
        }
        if room.is_empty() {
            rooms.remove(server_id);
        }
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn contains(&self, server_id: &str) -> bool {
        self.inner.lock().unwrap().contains_key(server_id)
    }
}

fn replace(slot: &mut Option<Peer>, peer: Peer, actions: &mut Vec<Action>) {
    if let Some(previous) = slot.replace(peer) {
        actions.push(Action::Close {
            peer: previous,
            code: 1008,
            reason: "Replaced by new connection",
        });
    }
}

fn clear(slot: &mut Option<Peer>, socket_id: SocketId) {
    if slot.as_ref().is_some_and(|peer| peer.id == socket_id) {
        *slot = None;
    }
}
