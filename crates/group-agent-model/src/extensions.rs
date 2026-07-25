use std::collections::BTreeMap;
use std::fmt;

use serde_json::Value;
use thiserror::Error;

/// Ordered provider-neutral extension data.
///
/// Keys are trimmed before insertion, must not be empty, and are kept in
/// lexical order. Insertion rejects duplicate keys so values are never
/// overwritten accidentally. Values are available through explicit accessors,
/// while `Debug` reveals only keys.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct Extensions(BTreeMap<String, Value>);

impl Extensions {
    /// Creates an empty extension collection.
    #[must_use]
    pub const fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Builds extensions from key-value pairs.
    pub fn try_from_iter<I, K>(entries: I) -> Result<Self, ExtensionError>
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let mut extensions = Self::new();
        for (key, value) in entries {
            extensions.insert(key, value)?;
        }
        Ok(extensions)
    }

    /// Inserts one value, rejecting an empty or duplicate normalized key.
    pub fn insert(&mut self, key: impl Into<String>, value: Value) -> Result<(), ExtensionError> {
        let key = key.into();
        let key = key.trim();
        if key.is_empty() {
            return Err(ExtensionError::EmptyKey);
        }
        if self.0.contains_key(key) {
            return Err(ExtensionError::DuplicateKey {
                key: key.to_owned(),
            });
        }
        self.0.insert(key.to_owned(), value);
        Ok(())
    }

    /// Adds one value using the same duplicate-rejecting semantics as
    /// [`Self::insert`].
    pub fn with(mut self, key: impl Into<String>, value: Value) -> Result<Self, ExtensionError> {
        self.insert(key, value)?;
        Ok(self)
    }

    /// Returns a value by its normalized key.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key.trim())
    }

    /// Returns entries in stable lexical key order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &Value)> {
        self.0.iter().map(|(key, value)| (key.as_str(), value))
    }

    /// Returns keys in stable lexical order.
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &str> {
        self.0.keys().map(String::as_str)
    }

    /// Returns the number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no entries are present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Atomically merges fragmented or cumulative extension data.
    ///
    /// An existing key with the same value is idempotent. A different value
    /// returns an error without inserting any key from `other`. Existing
    /// values are inspected in place and are never cloned.
    pub fn merge_idempotent(&mut self, other: Self) -> Result<(), ExtensionMergeError> {
        self.validate_idempotent_merge(&other)?;
        self.commit_idempotent_merge(other);
        Ok(())
    }

    pub(crate) fn validate_idempotent_merge(
        &self,
        other: &Self,
    ) -> Result<usize, ExtensionMergeError> {
        let mut additional_keys = 0;
        for (key, value) in &other.0 {
            match self.0.get(key) {
                Some(existing) if existing == value => {}
                Some(_) => {
                    return Err(ExtensionMergeError::ConflictingValue { key: key.clone() });
                }
                None => additional_keys += 1,
            }
        }
        Ok(additional_keys)
    }

    pub(crate) fn commit_idempotent_merge(&mut self, other: Self) {
        for (key, value) in other.0 {
            self.0.entry(key).or_insert(value);
        }
    }
}

impl fmt::Debug for Extensions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Extensions")
            .field("keys", &self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Invalid extension construction.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ExtensionError {
    /// A key was empty after trimming.
    #[error("extension key must not be empty")]
    EmptyKey,
    /// A normalized key was inserted more than once.
    #[error("extension key `{key}` is already present")]
    DuplicateKey { key: String },
}

/// Conflicting cumulative or fragmented extension data.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ExtensionMergeError {
    /// One stable key was observed with two different values.
    #[error("extension key `{key}` has conflicting values")]
    ConflictingValue { key: String },
}
