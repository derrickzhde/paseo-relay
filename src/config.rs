use std::collections::HashSet;
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    pub host: IpAddr,
    pub port: u16,
    /// Empty means every serverId is accepted.
    pub allowed_server_ids: HashSet<String>,
    pub max_sockets: usize,
    pub control_queue_bytes: usize,
    pub delivery_timeout: Duration,
    pub data_attach_timeout: Duration,
    pub drain: bool,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    host: Option<IpAddr>,
    port: Option<u16>,
    allowed_server_ids: Option<Vec<String>>,
    max_sockets: Option<usize>,
    control_queue_bytes: Option<usize>,
    delivery_timeout_ms: Option<u64>,
    data_attach_timeout_ms: Option<u64>,
    drain: Option<bool>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: IpAddr::from([127, 0, 0, 1]),
            port: 4000,
            allowed_server_ids: HashSet::new(),
            max_sockets: 20_000,
            control_queue_bytes: 1024 * 1024,
            delivery_timeout: Duration::from_millis(30_000),
            data_attach_timeout: Duration::from_millis(15_000),
            drain: false,
        }
    }
}

impl Config {
    /// Reads the config file at `path`, falling back to defaults when it is absent.
    /// `PASEO_RELAY_CONFIG` may override the default path `config.toml` in the
    /// working directory.
    pub fn load(path: Option<&str>) -> Result<Self, String> {
        let path = match path {
            Some(path) => Path::new(path),
            None => Path::new("config.toml"),
        };

        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(format!("cannot read {}: {error}", path.display())),
        };

        let file: FileConfig = toml::from_str(&raw)
            .map_err(|error| format!("invalid config file {}: {error}", path.display()))?;

        Ok(Config {
            host: file.host.unwrap_or(IpAddr::from([127, 0, 0, 1])),
            port: file.port.unwrap_or(4000),
            allowed_server_ids: file
                .allowed_server_ids
                .map(|ids| ids.into_iter().collect())
                .unwrap_or_default(),
            max_sockets: file.max_sockets.unwrap_or(20_000),
            control_queue_bytes: file.control_queue_bytes.unwrap_or(1024 * 1024),
            delivery_timeout: Duration::from_millis(file.delivery_timeout_ms.unwrap_or(30_000)),
            data_attach_timeout: Duration::from_millis(
                file.data_attach_timeout_ms.unwrap_or(15_000),
            ),
            drain: file.drain.unwrap_or(false),
        })
    }

    pub fn allows(&self, server_id: &str) -> bool {
        self.allowed_server_ids.is_empty() || self.allowed_server_ids.contains(server_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_allowlist_accepts_everything() {
        let config = Config::default();
        assert!(config.allows("anything"));
    }

    #[test]
    fn populated_allowlist_rejects_unknown_ids() {
        let config = Config {
            allowed_server_ids: ["srv_a".to_string(), "srv_b".to_string()].into_iter().collect(),
            ..Config::default()
        };
        assert!(config.allows("srv_a"));
        assert!(config.allows("srv_c") == false);
    }

    #[test]
    fn missing_file_falls_back_to_defaults() {
        let config = Config::load(Some("no-such-file.toml")).unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn full_file_overrides_every_field() {
        let raw = r#"
            host = "0.0.0.0"
            port = 9000
            allowed_server_ids = ["srv_a", "srv_b"]
            max_sockets = 100
            control_queue_bytes = 2048
            delivery_timeout_ms = 500
            data_attach_timeout_ms = 250
            drain = true
        "#;
        let dir = std::env::temp_dir().join("paseo-relay-config-test-full");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, raw).unwrap();

        let config = Config::load(Some(path.to_str().unwrap())).unwrap();
        assert_eq!(config.host, IpAddr::from([0, 0, 0, 0]));
        assert_eq!(config.port, 9000);
        assert!(config.allows("srv_a"));
        assert!(!config.allows("srv_c"));
        assert_eq!(config.max_sockets, 100);
        assert_eq!(config.control_queue_bytes, 2048);
        assert_eq!(config.delivery_timeout, Duration::from_millis(500));
        assert_eq!(config.data_attach_timeout, Duration::from_millis(250));
        assert!(config.drain);
    }

    #[test]
    fn unknown_field_is_rejected() {
        let dir = std::env::temp_dir().join("paseo-relay-config-test-unknown");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(&path, "nonsense_key = 1\n").unwrap();

        assert!(Config::load(Some(path.to_str().unwrap())).is_err());
    }
}
