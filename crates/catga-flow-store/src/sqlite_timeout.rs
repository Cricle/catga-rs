//! SQLite timeout receipt leasing.

use crate::server_timeout::define_server_timeout;
define_server_timeout!(sqlx::SqlitePool, sqlx::Sqlite, false, true, "SQLite");
