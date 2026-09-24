import os

import psycopg
from psycopg.rows import dict_row


def connect():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def init():
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(340029)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS definitions (
                name text NOT NULL, version integer NOT NULL, graph jsonb NOT NULL,
                daily boolean NOT NULL DEFAULT false, PRIMARY KEY(name,version));
            CREATE TABLE IF NOT EXISTS runs (
                id uuid PRIMARY KEY, name text NOT NULL, version integer NOT NULL,
                logical_date date NOT NULL, kind text NOT NULL, status text NOT NULL DEFAULT 'running',
                parallelism integer NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
                UNIQUE(name,version,logical_date,kind), FOREIGN KEY(name,version) REFERENCES definitions(name,version));
            CREATE TABLE IF NOT EXISTS nodes (
                run_id uuid REFERENCES runs(id), name text NOT NULL, task text NOT NULL,
                value integer NOT NULL, delay double precision NOT NULL, dependencies text[] NOT NULL,
                status text NOT NULL DEFAULT 'pending', attempts integer NOT NULL DEFAULT 0,
                token uuid, lease_until timestamptz, available_at timestamptz NOT NULL DEFAULT now(),
                output bigint, error text, PRIMARY KEY(run_id,name));
            CREATE TABLE IF NOT EXISTS effects (
                run_id uuid NOT NULL, node text NOT NULL, value bigint NOT NULL,
                PRIMARY KEY(run_id,node));
            CREATE TABLE IF NOT EXISTS queue_state (
                id integer PRIMARY KEY CHECK(id=1), turn bigint NOT NULL);
            INSERT INTO queue_state VALUES (1,0) ON CONFLICT DO NOTHING;
            ALTER TABLE nodes ADD COLUMN IF NOT EXISTS started_at timestamptz;
            ALTER TABLE runs ADD COLUMN IF NOT EXISTS generation integer NOT NULL DEFAULT 1;
            CREATE TABLE IF NOT EXISTS attempts (
                token uuid PRIMARY KEY,run_id uuid NOT NULL,name text NOT NULL,generation integer NOT NULL,
                started_at timestamptz NOT NULL DEFAULT clock_timestamp(),finished_at timestamptz,
                status text NOT NULL DEFAULT 'running',output bigint,error text);
            CREATE TABLE IF NOT EXISTS run_events (
                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,run_id uuid NOT NULL,
                generation integer NOT NULL,roots text[] NOT NULL,previous jsonb NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now());
        """)
