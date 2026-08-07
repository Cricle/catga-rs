//! Conditional branch builders for DSL flows.
//!
//! This module provides free functions for building conditional branches that can be
//! used directly or delegated from `DslFlow` builder methods.

use std::collections::{HashMap, hash_map::Entry};

use crate::flow::dsl_step::{BranchSelector, Condition};

/// Result type for building a conditional step variant.
pub enum ConditionalStep<S> {
    /// An if-else branch.
    If {
        condition: Condition<S>,
        then_branch: crate::flow::DslFlow<S>,
        else_branch: crate::flow::DslFlow<S>,
    },
    /// A match branch.
    Match {
        select_branch: BranchSelector<S>,
        branches: Vec<crate::flow::DslFlow<S>>,
        default_branch: crate::flow::DslFlow<S>,
    },
}

/// Builds an if-else conditional step.
pub fn build_if_else<S>(
    condition: Condition<S>,
    then_branch: crate::flow::DslFlow<S>,
    else_branch: crate::flow::DslFlow<S>,
) -> ConditionalStep<S> {
    ConditionalStep::If {
        condition,
        then_branch,
        else_branch,
    }
}

/// Builds a match conditional step with equality-based branch selection.
///
/// Duplicate case values keep the last registered branch, matching map insertion
/// semantics while retaining O(1) branch lookup during execution.
pub fn build_match<S, V, I>(selector: BranchSelector<S>, cases: I, default_branch: crate::flow::DslFlow<S>) -> ConditionalStep<S>
where
    V: Eq + std::hash::Hash,
    I: IntoIterator<Item = (V, crate::flow::DslFlow<S>)>,
{
    let mut positions = HashMap::new();
    let mut branches = Vec::new();
    for (value, branch) in cases {
        match positions.entry(value) {
            Entry::Occupied(position) => branches[*position.get()] = branch,
            Entry::Vacant(position) => {
                let index = branches.len();
                branches.push(branch);
                position.insert(index);
            }
        }
    }
    ConditionalStep::Match {
        select_branch: selector,
        branches,
        default_branch,
    }
}
