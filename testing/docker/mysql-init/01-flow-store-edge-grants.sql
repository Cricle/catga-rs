-- The catga-flow-store dialect edge suites (tests/mysql_edges.rs) provision an
-- isolated database per test named `catga_edge_<uuid>` using the e2e `catga`
-- user. The image entrypoint only grants that user on `catga.*` (from
-- MYSQL_DATABASE), so extend the grant to the edge database namespace.
-- Runs on fresh data volumes only (standard /docker-entrypoint-initdb.d behavior);
-- the e2e runner tears down with --volumes, so CI always initializes cleanly.
GRANT ALL PRIVILEGES ON `catga\_edge\_%`.* TO 'catga'@'%';
