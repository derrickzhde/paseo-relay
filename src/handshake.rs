use base64::engine::general_purpose::STANDARD;
use base64::Engine;

/// Curve25519 field prime 2^255 - 19, little-endian.
const FIELD_PRIME: [u8; 32] = [
    0xED, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F,
];

/// X25519 public keys that produce a low-order or otherwise degenerate shared secret.
/// Mirrors `lib/paseo_relay/handshake_validation.ex:14-22`.
const UNSUPPORTED_PUBLIC_KEYS: [[u8; 32]; 7] = [
    hex("0000000000000000000000000000000000000000000000000000000000000000"),
    hex("0100000000000000000000000000000000000000000000000000000000000000"),
    hex("E0EB7A7C3B41B8AE1656E3FAF19FC46ADA098DEB9C32B1FD866205165F49B800"),
    hex("5F9C95BCA3508C24B1D0B1559C83EF5B04445CC4581C8E86D8224EDDD09F1157"),
    hex("ECFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F"),
    hex("EDFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F"),
    hex("EEFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F"),
];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HandshakeType {
    Hello,
    E2eeHello,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Check {
    NotHandshake,
    Accept(HandshakeType),
    Reject(HandshakeType),
}

/// Anything that does not parse as a handshake envelope stays opaque and is forwarded as-is.
pub fn check(payload: &[u8]) -> Check {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return Check::NotHandshake;
    };

    let handshake_type = match value.get("type").and_then(serde_json::Value::as_str) {
        Some("hello") => HandshakeType::Hello,
        Some("e2ee_hello") => HandshakeType::E2eeHello,
        _ => return Check::NotHandshake,
    };

    let accepted = value
        .get("key")
        .and_then(serde_json::Value::as_str)
        .is_some_and(is_valid_public_key);

    if accepted {
        Check::Accept(handshake_type)
    } else {
        Check::Reject(handshake_type)
    }
}

fn is_valid_public_key(encoded: &str) -> bool {
    let Ok(decoded) = STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(key) = <[u8; 32]>::try_from(decoded.as_slice()) else {
        return false;
    };
    // Reject any non-canonical spelling of the same 32 bytes.
    if STANDARD.encode(key) != encoded {
        return false;
    }
    is_canonical_coordinate(&key) && !UNSUPPORTED_PUBLIC_KEYS.contains(&key)
}

/// Little-endian comparison against the field prime.
fn is_canonical_coordinate(key: &[u8; 32]) -> bool {
    for index in (0..32).rev() {
        if key[index] != FIELD_PRIME[index] {
            return key[index] < FIELD_PRIME[index];
        }
    }
    false
}

const fn hex(input: &str) -> [u8; 32] {
    let bytes = input.as_bytes();
    let mut out = [0u8; 32];
    let mut index = 0;
    while index < 32 {
        out[index] = nibble(bytes[index * 2]) << 4 | nibble(bytes[index * 2 + 1]);
        index += 1;
    }
    out
}

const fn nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'A'..=b'F' => byte - b'A' + 10,
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("invalid hex digit"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(kind: &str, key: &str) -> Vec<u8> {
        format!(r#"{{"type":"{kind}","key":"{key}"}}"#).into_bytes()
    }

    fn valid_key() -> String {
        // A canonical coordinate below the field prime and outside the blocklist.
        let mut key = [0u8; 32];
        key[0] = 9;
        STANDARD.encode(key)
    }

    #[test]
    fn non_json_is_opaque() {
        assert_eq!(check(b"not json at all"), Check::NotHandshake);
        assert_eq!(check(&[0xff, 0xfe, 0x00]), Check::NotHandshake);
    }

    #[test]
    fn other_message_types_are_opaque() {
        assert_eq!(check(br#"{"type":"ping"}"#), Check::NotHandshake);
        assert_eq!(check(br#"{"payload":"x"}"#), Check::NotHandshake);
    }

    #[test]
    fn accepts_canonical_key_for_both_handshake_types() {
        let key = valid_key();
        assert_eq!(check(&envelope("hello", &key)), Check::Accept(HandshakeType::Hello));
        assert_eq!(check(&envelope("e2ee_hello", &key)), Check::Accept(HandshakeType::E2eeHello));
    }

    #[test]
    fn rejects_every_blocklisted_key() {
        for key in UNSUPPORTED_PUBLIC_KEYS {
            let encoded = STANDARD.encode(key);
            assert_eq!(
                check(&envelope("hello", &encoded)),
                Check::Reject(HandshakeType::Hello),
                "expected rejection for {encoded}"
            );
        }
    }

    #[test]
    fn rejects_missing_or_non_string_key() {
        assert_eq!(check(br#"{"type":"hello"}"#), Check::Reject(HandshakeType::Hello));
        assert_eq!(check(br#"{"type":"hello","key":42}"#), Check::Reject(HandshakeType::Hello));
    }

    #[test]
    fn rejects_wrong_length() {
        assert_eq!(
            check(&envelope("hello", &STANDARD.encode([7u8; 31]))),
            Check::Reject(HandshakeType::Hello)
        );
        assert_eq!(
            check(&envelope("hello", &STANDARD.encode([7u8; 33]))),
            Check::Reject(HandshakeType::Hello)
        );
    }

    #[test]
    fn rejects_unpadded_base64() {
        let key = valid_key();
        let stripped = key.trim_end_matches('=').to_string();
        assert_ne!(stripped, key);
        assert_eq!(check(&envelope("hello", &stripped)), Check::Reject(HandshakeType::Hello));
    }

    #[test]
    fn rejects_coordinate_at_or_above_field_prime() {
        // p itself, p + 1 and the all-ones value are all non-canonical.
        for key in [FIELD_PRIME, hex("EEFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF7F"), [0xFF; 32]] {
            assert!(!is_canonical_coordinate(&key));
        }
    }

    #[test]
    fn accepts_coordinate_just_below_field_prime() {
        let mut key = FIELD_PRIME;
        key[0] -= 1; // p - 1, canonical but blocklisted separately
        assert!(is_canonical_coordinate(&key));
    }

    #[test]
    fn high_bit_forms_are_rejected_by_the_field_check() {
        let mut key = [0u8; 32];
        key[0] = 9;
        key[31] = 0x80; // sets bit 255
        assert!(!is_canonical_coordinate(&key));
    }
}
