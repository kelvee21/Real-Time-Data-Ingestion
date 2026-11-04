-- postgres_setup.sql: database, app login and tables for the event pipeline.
--
-- run it once with psql as a superuser (it uses psql things: -v, \gexec, \connect):
--   psql -h localhost -p 5432 -U postgres -v app_password='pick-a-password' -f postgres_setup.sql
-- the same password goes into .env as PGPASSWORD. re-running it is safe, every
-- step checks first whether it already happened.

\set ON_ERROR_STOP on

-- 1. the database. CREATE DATABASE has no IF NOT EXISTS, so the command is
--    only built when ecommerce is not there yet
SELECT 'CREATE DATABASE ecommerce'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'ecommerce')\gexec

-- 2. a login just for the pipeline, so spark never runs as the superuser
SELECT format('CREATE ROLE spark_app LOGIN PASSWORD %L', :'app_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'spark_app')\gexec

\connect ecommerce

-- 3. the final table, one row per event
CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,           -- uuid from the generator, a re-delivered row just gets skipped
    event_time   TIMESTAMPTZ NOT NULL,       -- when the user acted. plain TIMESTAMP bit me once: the jdbc driver wrote my laptop's zone
    user_id      INT NOT NULL CHECK (user_id > 0),
    session_id   TEXT,
    event_type   TEXT NOT NULL CHECK (event_type IN ('view', 'add_to_cart', 'purchase')),
    product_id   INT NOT NULL CHECK (product_id > 0),
    category     TEXT,
    price        NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    quantity     INT CHECK (quantity >= 0),
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()   -- when the row landed, latency = ingested_at - event_time
);

-- 4. landing table for each micro-batch. same columns but no constraints, so the
--    bulk jdbc insert can not fail half way, the merge into events applies the rules
CREATE TABLE IF NOT EXISTS events_staging (LIKE events INCLUDING DEFAULTS EXCLUDING CONSTRAINTS);

-- 5. indexes for the time + type filters the reports run
CREATE INDEX IF NOT EXISTS idx_events_time ON events (event_time);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);

-- 6. the app login gets the least it needs: read and add events, empty its own staging
GRANT CONNECT ON DATABASE ecommerce TO spark_app;
GRANT USAGE ON SCHEMA public TO spark_app;
GRANT SELECT, INSERT ON events TO spark_app;
GRANT SELECT, INSERT, TRUNCATE ON events_staging TO spark_app;
