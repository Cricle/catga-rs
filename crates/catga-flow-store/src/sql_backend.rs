//! SQLx server-dialect placeholder rendering.

use std::borrow::Cow;

pub(crate) use crate::sql_common::{
    MAX_CAS_RETRIES, cas_error, deadline_millis, is_stale, stale_before_unix_millis, status_code,
    status_from_code, system_time_from_unix_millis_and_subsec_nanos, unix_millis,
    unix_millis_and_subsec_nanos,
};

/// Converts the canonical `?` bind template into the selected server dialect.
pub(crate) fn statement<'a>(
    template: &'a str,
    postgres: bool,
) -> sqlx::AssertSqlSafe<Cow<'a, str>> {
    if !postgres {
        return sqlx::AssertSqlSafe(Cow::Borrowed(template));
    }
    let mut parts = template.split('?');
    let mut rendered = String::with_capacity(template.len().saturating_add(16));
    if let Some(first) = parts.next() {
        rendered.push_str(first);
    }
    for (index, part) in parts.enumerate() {
        rendered.push('$');
        rendered.push_str(&index.saturating_add(1).to_string());
        rendered.push_str(part);
    }
    sqlx::AssertSqlSafe(Cow::Owned(rendered))
}
