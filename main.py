import os
import psycopg2
from fastapi import FastAPI

DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI()


def get_connection():
    return psycopg2.connect(DATABASE_URL)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/db-check")
def db_check():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT NOW()")
        now = cur.fetchone()[0]
        cur.close()
        conn.close()
        return {"db_status": "ok", "time": str(now)}
    except Exception as e:
        return {"db_status": "error", "detail": str(e)}


@app.post("/init-db")
def init_db():
    """
    One-time (but safe to re-run) endpoint to create core tables:
    orders, events, tasks, notes.
    """
    schema_sql = """
    CREATE TABLE IF NOT EXISTS orders (
        id SERIAL PRIMARY KEY,
        order_number VARCHAR(64) UNIQUE NOT NULL,
        customer_name TEXT,
        customer_email TEXT,
        status VARCHAR(32) DEFAULT 'new',
        total_amount NUMERIC(10, 2),
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
        event_type VARCHAR(64) NOT NULL,
        payload JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS tasks (
        id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(id) ON DELETE SET NULL,
        title TEXT NOT NULL,
        description TEXT,
        status VARCHAR(32) DEFAULT 'open',
        source VARCHAR(32),
        due_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS notes (
        id SERIAL PRIMARY KEY,
        order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
        body TEXT NOT NULL,
        author TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );
    """
    try:
        conn = get_connection()
        conn.autocommit = True  # allow multiple CREATEs in one go
        cur = conn.cursor()
        cur.execute(schema_sql)
        cur.close()
        conn.close()
        return {"status": "ok", "detail": "Tables created (or already existed)."}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

