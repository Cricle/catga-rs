//! PostgreSQL timeout receipt leasing.

use crate::server_timeout::define_server_timeout;
define_server_timeout!(sqlx::PgPool, sqlx::Postgres, true, "PostgreSQL");
