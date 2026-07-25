//! Lock-free event schema-upgrade registration and replay.

use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;

use crate::{CatgaError, CatgaResult, Envelope, ErrorCode};

const MAX_UPGRADE_STEPS: usize = 100;

/// Upgrades one event payload schema to its next compatible schema.
pub trait EventUpgrader: Send + Sync {
    /// Returns the serialized type accepted by this upgrader.
    fn source_type(&self) -> &str;

    /// Returns the serialized type emitted by this upgrader.
    fn target_type(&self) -> &str;

    /// Returns the schema version accepted by this upgrader.
    fn source_version(&self) -> u32;

    /// Returns the schema version emitted by this upgrader.
    fn target_version(&self) -> u32;

    /// Transforms an owned envelope into the declared target schema.
    fn upgrade(&self, source: Envelope) -> CatgaResult<Envelope>;
}

#[derive(Clone, Default)]
struct VersionRules {
    upgraders: HashMap<Box<str>, Vec<Arc<dyn EventUpgrader>>>,
    current_versions: HashMap<Box<str>, u32>,
}

/// A copy-on-write schema-upgrade registry with lock-free upgrade reads.
pub struct EventVersionRegistry {
    rules: ArcSwap<VersionRules>,
}

impl Default for EventVersionRegistry {
    fn default() -> Self {
        Self {
            rules: ArcSwap::from_pointee(VersionRules::default()),
        }
    }
}

impl EventVersionRegistry {
    /// Registers one unique source-type and source-version upgrade step.
    pub fn register(&self, upgrader: Arc<dyn EventUpgrader>) -> CatgaResult<()> {
        validate_upgrader(upgrader.as_ref())?;
        loop {
            let current = self.rules.load_full();
            let mut next = (*current).clone();
            let source_type: Box<str> = upgrader.source_type().into();
            let upgrades = next.upgraders.entry(source_type).or_default();
            if upgrades
                .iter()
                .any(|registered| registered.source_version() == upgrader.source_version())
            {
                return Err(CatgaError::new(
                    ErrorCode::Conflict,
                    "an event upgrader is already registered for this source type and version",
                ));
            }
            upgrades.push(Arc::clone(&upgrader));
            upgrades.sort_unstable_by_key(|registered| registered.source_version());
            next.current_versions
                .entry(upgrader.target_type().into())
                .and_modify(|version| *version = (*version).max(upgrader.target_version()))
                .or_insert_with(|| upgrader.target_version());
            let previous = self.rules.compare_and_swap(&current, Arc::new(next));
            if Arc::ptr_eq(&*previous, &current) {
                return Ok(());
            }
        }
    }

    /// Returns the highest registered schema version for an event type, defaulting to `1`.
    pub fn current_version(&self, event_type: &str) -> u32 {
        self.rules
            .load()
            .current_versions
            .get(event_type)
            .copied()
            .unwrap_or(1)
    }

    /// Upgrades an envelope through every registered matching schema step.
    pub fn upgrade_to_latest(&self, mut event: Envelope) -> CatgaResult<Envelope> {
        let rules = self.rules.load();
        for _ in 0..MAX_UPGRADE_STEPS {
            let Some(upgraders) = rules.upgraders.get(event.message_type()) else {
                return Ok(event);
            };
            let Some(upgrader) = upgraders
                .iter()
                .find(|upgrader| upgrader.source_version() == event.schema_version())
            else {
                return Ok(event);
            };
            let upgraded = upgrader.upgrade(event)?;
            if upgraded.message_type() != upgrader.target_type()
                || upgraded.schema_version() != upgrader.target_version()
            {
                return Err(CatgaError::new(
                    ErrorCode::Validation,
                    "event upgrader output does not match its declared target schema",
                ));
            }
            event = upgraded;
        }
        Err(CatgaError::new(
            ErrorCode::Validation,
            "event schema upgrade exceeded the maximum chain length",
        ))
    }

    /// Returns whether at least one upgrade starts from this serialized event type.
    pub fn has_upgraders(&self, event_type: &str) -> bool {
        self.rules.load().upgraders.contains_key(event_type)
    }
}

fn validate_upgrader(upgrader: &dyn EventUpgrader) -> CatgaResult<()> {
    if upgrader.source_type().is_empty() || upgrader.target_type().is_empty() {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "event upgrader source and target types must not be empty",
        ));
    }
    if upgrader.target_version() <= upgrader.source_version() {
        return Err(CatgaError::new(
            ErrorCode::Validation,
            "event upgrader target version must exceed its source version",
        ));
    }
    Ok(())
}
