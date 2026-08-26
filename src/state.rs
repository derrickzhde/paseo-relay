use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::peer::SocketId;
use crate::room::Rooms;

pub struct AppState {
    pub config: Config,
    pub rooms: Rooms,
    pub metrics: Metrics,
    pub connection_slots: Arc<Semaphore>,
    draining: AtomicBool,
    next_socket_id: AtomicU64,
}

impl AppState {
    pub fn new(config: Config) -> Self {
        let draining = AtomicBool::new(config.drain);
        let connection_slots = Arc::new(Semaphore::new(config.max_sockets));
        AppState {
            config,
            rooms: Rooms::new(),
            metrics: Metrics::default(),
            connection_slots,
            draining,
            next_socket_id: AtomicU64::new(1),
        }
    }

    pub fn next_socket_id(&self) -> SocketId {
        self.next_socket_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn draining(&self) -> bool {
        self.draining.load(Ordering::Relaxed)
    }

    pub fn begin_drain(&self) {
        self.draining.store(true, Ordering::Relaxed);
    }

    pub fn at_capacity(&self) -> bool {
        self.connection_slots.available_permits() == 0
    }

    pub fn ready(&self) -> bool {
        !self.draining() && !self.at_capacity()
    }
}
