use std::collections::HashSet;
use std::env;
use std::net::IpAddr;
use std::time::Duration;

#[derive(Clone, Debug)]
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
    pub fn from_env() -> Result<Self, String> {
        let defaults = Config::default();

        Ok(Config {
            host: parse("PASEO_RELAY_HOST", defaults.host)?,
            port: parse("PASEO_RELAY_PORT", defaults.port)?,
            allowed_server_ids: parse_server_ids("PASEO_RELAY_ALLOWED_SERVER_IDS"),
            max_sockets: parse("PASEO_RELAY_MAX_SOCKETS", defaults.max_sockets)?,
            control_queue_bytes: parse(
                "PASEO_RELAY_CONTROL_QUEUE_BYTES",
                defaults.control_queue_bytes,
            )?,
            delivery_timeout: parse_millis(
                "PASEO_RELAY_DELIVERY_TIMEOUT_MS",
                defaults.delivery_timeout,
            )?,
            data_attach_timeout: parse_millis(
                "PASEO_RELAY_DATA_ATTACH_TIMEOUT_MS",
                defaults.data_attach_timeout,
            )?,
            drain: parse_bool("PASEO_RELAY_DRAIN", defaults.drain)?,
        })
    }

    pub fn allows(&self, server_id: &str) -> bool {
        self.allowed_server_ids.is_empty() || self.allowed_server_ids.contains(server_id)
    }
}

fn read(name: &str) -> Option<String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Some(value.trim().to_string()),
        _ => None,
    }
}

fn parse<T: std::str::FromStr>(name: &str, fallback: T) -> Result<T, String> {
    match read(name) {
        None => Ok(fallback),
        Some(value) => value.parse().map_err(|_| format!("{name}: cannot parse {value:?}")),
    }
}

fn parse_millis(name: &str, fallback: Duration) -> Result<Duration, String> {
    match read(name) {
        None => Ok(fallback),
        Some(value) => value
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|_| format!("{name}: cannot parse {value:?} as milliseconds")),
    }
}

fn parse_bool(name: &str, fallback: bool) -> Result<bool, String> {
    match read(name).as_deref() {
        None => Ok(fallback),
        Some("true" | "1" | "yes") => Ok(true),
        Some("false" | "0" | "no") => Ok(false),
        Some(value) => Err(format!("{name}: cannot parse {value:?} as boolean")),
    }
}

fn parse_server_ids(name: &str) -> HashSet<String> {
    read(name)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
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
        assert!(!config.allows("srv_c"));
    }

    #[test]
    fn server_ids_split_on_commas_and_ignore_blanks() {
        let parsed: HashSet<String> = " a , ,b,, c "
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect();
        assert_eq!(parsed, ["a".to_string(), "b".to_string(), "c".to_string()].into_iter().collect());
    }

    #[test]
    fn booleans_accept_common_spellings() {
        assert!(matches!(parse_bool("PASEO_RELAY_TEST_MISSING_BOOL", true), Ok(true)));
    }
}
