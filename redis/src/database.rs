use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Default)]
pub(crate) struct Database {
    values: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
}

impl Database {
    pub(crate) fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let values = self
            .values
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        values.get(key).cloned()
    }

    pub(crate) fn set(&self, key: Vec<u8>, value: Vec<u8>) {
        let mut values = self
            .values
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        values.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::Database;

    #[test]
    fn stores_retrieves_and_overwrites_values() {
        let database = Database::default();

        assert_eq!(database.get(b"key"), None);
        database.set(b"key".to_vec(), b"first".to_vec());
        assert_eq!(database.get(b"key"), Some(b"first".to_vec()));

        database.set(b"key".to_vec(), b"second".to_vec());
        assert_eq!(database.get(b"key"), Some(b"second".to_vec()));
    }

    #[test]
    fn preserves_empty_and_non_utf8_bytes() {
        let database = Database::default();
        let key = b"\0\xff".to_vec();
        let value = b"\xff\0\r\n".to_vec();

        database.set(key.clone(), value.clone());

        assert_eq!(database.get(&key), Some(value));
        database.set(Vec::new(), Vec::new());
        assert_eq!(database.get(b""), Some(Vec::new()));
    }
}
