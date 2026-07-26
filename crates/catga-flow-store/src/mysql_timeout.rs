//! MySQL timeout receipt leasing.

use crate::server_timeout::define_server_timeout;
define_server_timeout!(sqlx::MySqlPool, sqlx::MySql, false, "MySQL");
