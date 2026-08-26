use std::fmt::Write;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Default)]
pub struct Metrics {
    pub active_websockets: AtomicUsize,
    pub connection_rejections: AtomicU64,
    pub frames_forwarded: AtomicU64,
    pub bytes_forwarded: AtomicU64,
    pub slow_consumer_closes: AtomicU64,
    pub handshake_rejections: AtomicU64,
}

impl Metrics {
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(counter: &AtomicU64, amount: u64) {
        counter.fetch_add(amount, Ordering::Relaxed);
    }

    pub fn render(&self, ready: bool, draining: bool, active_sessions: usize) -> String {
        let mut out = String::with_capacity(1024);
        gauge(&mut out, "paseo_relay_ready", "Whether this node admits new relay work.", u64::from(ready));
        gauge(&mut out, "paseo_relay_draining", "Whether this node is draining.", u64::from(draining));
        gauge(
            &mut out,
            "paseo_relay_active_websockets",
            "Currently attached WebSockets.",
            self.active_websockets.load(Ordering::Relaxed) as u64,
        );
        gauge(
            &mut out,
            "paseo_relay_active_sessions",
            "Currently routed serverIds.",
            active_sessions as u64,
        );
        counter(
            &mut out,
            "paseo_relay_connection_rejections_total",
            "Upgrade requests refused before attaching.",
            self.connection_rejections.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "paseo_relay_frames_forwarded_total",
            "Frames written to a destination.",
            self.frames_forwarded.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "paseo_relay_bytes_forwarded_total",
            "Payload bytes written to destinations.",
            self.bytes_forwarded.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "paseo_relay_slow_consumer_closes_total",
            "Destinations shed for failing to drain.",
            self.slow_consumer_closes.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "paseo_relay_handshake_rejections_total",
            "Client handshakes refused for an invalid key.",
            self.handshake_rejections.load(Ordering::Relaxed),
        );
        out
    }
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}");
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_prometheus_text() {
        let metrics = Metrics::default();
        metrics.active_websockets.store(3, Ordering::Relaxed);
        Metrics::inc(&metrics.frames_forwarded);

        let body = metrics.render(true, false, 2);
        assert!(body.contains("paseo_relay_ready 1"));
        assert!(body.contains("paseo_relay_draining 0"));
        assert!(body.contains("paseo_relay_active_websockets 3"));
        assert!(body.contains("paseo_relay_active_sessions 2"));
        assert!(body.contains("paseo_relay_frames_forwarded_total 1"));
        assert!(body.contains("# TYPE paseo_relay_ready gauge"));
        assert!(body.contains("# TYPE paseo_relay_frames_forwarded_total counter"));
        assert!(body.ends_with('\n'));
    }
}
