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
