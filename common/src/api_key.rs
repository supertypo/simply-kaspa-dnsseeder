use rand::RngExt;

const KEY_BYTES: usize = 32;

/// Generate a cryptographically random API key as a 64-char hex string.
#[must_use]
pub fn generate_api_key() -> String {
    let bytes: [u8; KEY_BYTES] = rand::rng().random();
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_64_hex_chars() {
        let k = generate_api_key();
        assert_eq!(k.len(), KEY_BYTES * 2);
        assert!(k.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn successive_calls_differ() {
        assert_ne!(generate_api_key(), generate_api_key());
    }
}
