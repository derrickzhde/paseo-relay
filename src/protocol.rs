use std::collections::HashMap;

use rand::RngCore;

pub const MAX_FRAME_WIRE_BYTES: usize = 32 * 1024 * 1024;
const MAX_CLIENT_FRAME_HEADER_BYTES: usize = 14;
pub const MAX_MESSAGE_PAYLOAD_BYTES: usize = MAX_FRAME_WIRE_BYTES - MAX_CLIENT_FRAME_HEADER_BYTES;
pub const MAX_CONTROL_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_ROUTE_ID_BYTES: usize = 256;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Server,
    Client,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Version {
    V1,
    V2,
}

#[derive(Clone, Debug)]
pub struct Connection {
    pub server_id: String,
    pub role: Role,
    pub version: Version,
    /// Always empty for V1. For V2 an empty value on a server role means the control channel.
    pub connection_id: String,
}

impl Connection {
    pub fn is_control(&self) -> bool {
        self.version == Version::V2 && self.role == Role::Server && self.connection_id.is_empty()
    }

    pub fn max_payload_bytes(&self) -> usize {
        if self.is_control() {
            MAX_CONTROL_PAYLOAD_BYTES
        } else {
            MAX_MESSAGE_PAYLOAD_BYTES
        }
    }

    /// Field order and trimming match `lib/paseo_relay/connection.ex` exactly: `role` and
    /// `serverId` are compared verbatim while `v` and `connectionId` are trimmed first.
    pub fn from_query(query: &HashMap<String, String>) -> Result<Self, &'static str> {
        let role = match query.get("role").map(String::as_str) {
            Some("server") => Role::Server,
            Some("client") => Role::Client,
            _ => return Err("Missing or invalid role parameter"),
        };

        let server_id = match query.get("serverId") {
            Some(value) if (1..=MAX_ROUTE_ID_BYTES).contains(&value.len()) => value.clone(),
            Some(value) if value.len() > MAX_ROUTE_ID_BYTES => return Err("serverId is too long"),
            _ => return Err("Missing serverId parameter"),
        };

        let version = match query.get("v").map(|value| value.trim()) {
            None | Some("") | Some("1") => Version::V1,
            Some("2") => Version::V2,
            Some(_) => return Err("Invalid v parameter (expected 1 or 2)"),
        };

        let connection_id = match version {
            Version::V1 => String::new(),
            Version::V2 => {
                let value = query.get("connectionId").map(|v| v.trim()).unwrap_or("");
                if value.len() > MAX_ROUTE_ID_BYTES {
                    return Err("connectionId is too long");
                }
                if role == Role::Client && value.is_empty() {
                    generated_connection_id()
                } else {
                    value.to_string()
                }
            }
        };

        Ok(Connection { server_id, role, version, connection_id })
    }
}

fn generated_connection_id() -> String {
    let mut bytes = [0u8; 8];
    rand::rng().fill_bytes(&mut bytes);
    let mut out = String::with_capacity(5 + 16);
    out.push_str("conn_");
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn rejects_missing_or_unknown_role() {
        assert_eq!(
            Connection::from_query(&query(&[("serverId", "s")])).unwrap_err(),
            "Missing or invalid role parameter"
        );
        assert_eq!(
            Connection::from_query(&query(&[("role", "peer"), ("serverId", "s")])).unwrap_err(),
            "Missing or invalid role parameter"
        );
    }

    #[test]
    fn role_is_not_trimmed() {
        assert_eq!(
            Connection::from_query(&query(&[("role", " server"), ("serverId", "s")])).unwrap_err(),
            "Missing or invalid role parameter"
        );
    }

    #[test]
    fn server_id_is_required_and_bounded() {
        assert_eq!(
            Connection::from_query(&query(&[("role", "client")])).unwrap_err(),
            "Missing serverId parameter"
        );
        assert_eq!(
            Connection::from_query(&query(&[("role", "client"), ("serverId", "")])).unwrap_err(),
            "Missing serverId parameter"
        );
        let long = "x".repeat(MAX_ROUTE_ID_BYTES + 1);
        assert_eq!(
            Connection::from_query(&query(&[("role", "client"), ("serverId", &long)])).unwrap_err(),
            "serverId is too long"
        );
        let exact = "x".repeat(MAX_ROUTE_ID_BYTES);
        assert!(Connection::from_query(&query(&[("role", "client"), ("serverId", &exact)])).is_ok());
    }

    #[test]
    fn server_id_is_not_trimmed() {
        let connection =
            Connection::from_query(&query(&[("role", "client"), ("serverId", " s ")])).unwrap();
        assert_eq!(connection.server_id, " s ");
    }

    #[test]
    fn version_defaults_to_one_and_is_trimmed() {
        let base = [("role", "client"), ("serverId", "s")];
        for value in ["", " ", "1", " 1 "] {
            let mut pairs = base.to_vec();
            pairs.push(("v", value));
            assert_eq!(Connection::from_query(&query(&pairs)).unwrap().version, Version::V1);
        }
        assert_eq!(Connection::from_query(&query(&base)).unwrap().version, Version::V1);

        let mut pairs = base.to_vec();
        pairs.push(("v", " 2 "));
        assert_eq!(Connection::from_query(&query(&pairs)).unwrap().version, Version::V2);

        let mut pairs = base.to_vec();
        pairs.push(("v", "3"));
        assert_eq!(
            Connection::from_query(&query(&pairs)).unwrap_err(),
            "Invalid v parameter (expected 1 or 2)"
        );
    }

    #[test]
    fn v1_ignores_connection_id() {
        let connection = Connection::from_query(&query(&[
            ("role", "client"),
            ("serverId", "s"),
            ("connectionId", "ignored"),
        ]))
        .unwrap();
        assert_eq!(connection.connection_id, "");
        assert!(!connection.is_control());
    }

    #[test]
    fn v2_generates_client_connection_id_when_blank() {
        let connection = Connection::from_query(&query(&[
            ("role", "client"),
            ("serverId", "s"),
            ("v", "2"),
            ("connectionId", "   "),
        ]))
        .unwrap();
        assert!(connection.connection_id.starts_with("conn_"));
        assert_eq!(connection.connection_id.len(), 5 + 16);
        assert!(connection.connection_id[5..].chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn v2_blank_server_connection_id_is_the_control_channel() {
        let connection =
            Connection::from_query(&query(&[("role", "server"), ("serverId", "s"), ("v", "2")]))
                .unwrap();
        assert!(connection.is_control());
        assert_eq!(connection.max_payload_bytes(), MAX_CONTROL_PAYLOAD_BYTES);
    }

    #[test]
    fn v2_rejects_oversized_connection_id() {
        let long = "x".repeat(MAX_ROUTE_ID_BYTES + 1);
        assert_eq!(
            Connection::from_query(&query(&[
                ("role", "client"),
                ("serverId", "s"),
                ("v", "2"),
                ("connectionId", &long),
            ]))
            .unwrap_err(),
            "connectionId is too long"
        );
    }

    #[test]
    fn role_is_checked_before_server_id() {
        assert_eq!(
            Connection::from_query(&query(&[("serverId", &"x".repeat(300))])).unwrap_err(),
            "Missing or invalid role parameter"
        );
    }
}
