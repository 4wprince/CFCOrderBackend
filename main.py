"""
CFC Order Workflow Backend - v5.8.4
All parsing/logic server-side. B2BWave API integration for clean order data.
Auto-sync every 15 minutes. Supplier sheet support with line items.
AI Summary with Anthropic Claude API. RL Carriers quote helper.
"""

import os
import re
import json
import base64
import urllib.request
import urllib.error
import threading
import time
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# =============================================================================
# CONFIG
# =============================================================================

DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

B2BWAVE_API_KEY = os.environ.get("B2BWAVE_API_KEY", "").strip()
B2BWAVE_BASE_URL = "https://cabinetsforcontractors.b2bwave.com"

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
CLAUDE_MODEL = "claude-3-5-sonnet-20241022"

ENABLE_SYNC_THREAD = os.environ.get("ENABLE_SYNC_THREAD", "true").lower() == "true"

# =============================================================================
# DB HELPER
# =============================================================================

@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()

# =============================================================================
# FASTAPI INIT
# =============================================================================

app = FastAPI(
    title="CFC Order Workflow Backend",
    version="5.8.4",
    description="Handles order ingestion, status, AI summary, RL quotes, alerts."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # sandbox
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# Pydantic Models
# =============================================================================

class OrderStatusUpdate(BaseModel):
    status: str
    note: Optional[str] = None

class EmailLog(BaseModel):
    order_id: str
    timestamp: str
    subject: str
    body_preview: str
    full_body: Optional[str] = None
    direction: str  # "inbound" or "outbound"
    source: str     # e.g. "gmail", "outbound_system"
    message_id: Optional[str] = None

class RLQuoteRequest(BaseModel):
    order_id: str
    pickup_zip: str
    delivery_zip: str
    weight_lbs: float
    class_code: str
    pallets: int
    liftgate: bool = False
    residential: bool = False

class RLCostUpdate(BaseModel):
    order_id: str
    rl_quote_no: str
    rl_cost: float
    notes: Optional[str] = None

class CopilotContextRequest(BaseModel):
    order_id: str

class CopilotActionRequest(BaseModel):
    order_id: str
    user_prompt: str

class ShippingEmailParseRequest(BaseModel):
    order_id: str
    carrier: str
    subject: str
    body: str

class PaymentLinkRecord(BaseModel):
    order_id: str
    invoice_no: str
    amount: float
    created_at: str
    link_url: str

class EmailLogUpsertRequest(BaseModel):
    logs: List[EmailLog]

# =============================================================================
# DB SCHEMA HELPERS (COMMENTS ONLY)
# =============================================================================
"""
orders:
  id SERIAL PRIMARY KEY
  order_id TEXT UNIQUE
  customer_name TEXT
  company_name TEXT
  status TEXT
  created_at TIMESTAMP
  updated_at TIMESTAMP
  sent_to_warehouse BOOLEAN DEFAULT FALSE
  sent_to_warehouse_at TIMESTAMP
  payment_received BOOLEAN DEFAULT FALSE
  payment_received_at TIMESTAMP
  is_trusted_customer BOOLEAN DEFAULT FALSE
  order_total NUMERIC
  rl_quote_no TEXT
  rl_quote_cost NUMERIC
  pro_number TEXT
  tracking TEXT

order_events:
  id SERIAL PRIMARY KEY
  order_id TEXT
  event_type TEXT
  event_data JSONB
  created_at TIMESTAMP DEFAULT NOW()
  source TEXT

email_logs:
  id SERIAL PRIMARY KEY
  order_id TEXT
  timestamp TIMESTAMP
  subject TEXT
  body_preview TEXT
  full_body TEXT
  direction TEXT
  source TEXT
  message_id TEXT

order_alerts:
  id SERIAL PRIMARY KEY
  order_id TEXT
  alert_type TEXT
  alert_message TEXT
  is_resolved BOOLEAN DEFAULT FALSE
  created_at TIMESTAMP DEFAULT NOW()
  resolved_at TIMESTAMP
"""

# =============================================================================
# B2BWAVE HELPERS
# =============================================================================

def b2bwave_request(path: str) -> Dict[str, Any]:
    if not B2BWAVE_API_KEY:
        raise RuntimeError("Missing B2BWAVE_API_KEY")

    url = f"{B2BWAVE_BASE_URL}{path}"
    req = urllib.request.Request(url)
    req.add_header("X-Api-Key", B2BWAVE_API_KEY)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"B2BWave HTTPError: {e.code} {e.reason}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"B2BWave URLError: {e.reason}")


def fetch_b2bwave_order(order_id: str) -> Dict[str, Any]:
    return b2bwave_request(f"/api/orders/{order_id}.json")


def poll_b2bwave_new_orders(since_minutes: int = 15) -> List[Dict[str, Any]]:
    since = datetime.utcnow() - timedelta(minutes=since_minutes)
    since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    data = b2bwave_request(f"/api/orders.json?updated_at_min={since_str}")
    if isinstance(data, list):
        return data
    return data.get("orders", [])

# =============================================================================
# AI SUMMARY (CLAUDE)
# =============================================================================

def call_claude_for_order_summary(order_payload: Dict[str, Any]) -> str:
    """
    Call Anthropic Claude to generate an order summary.
    """
    if not ANTHROPIC_API_KEY:
        return "AI summary unavailable: missing ANTHROPIC_API_KEY."

    prompt = (
        "You are an assistant summarizing cabinet orders for a busy operations manager.\n"
        "Return a concise, helpful summary of this order, including:\n"
        "- Customer name & company\n"
        "- High-level items (no line-by-line)\n"
        "- Important shipping notes or special instructions\n\n"
        f"Order JSON:\n{json.dumps(order_payload, indent=2)}"
    )

    body = {
        "model": CLAUDE_MODEL,
        "max_tokens": 350,
        "temperature": 0.3,
        "messages": [
            {"role": "user", "content": prompt}
        ]
    }

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        }
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            content = data.get("content", [])
            if content and isinstance(content, list):
                return content[0].get("text", "").strip()
            return "No summary returned from Claude."
    except Exception as e:
        return f"Error calling Claude: {e}"

# =============================================================================
# BACKGROUND SYNC THREAD
# =============================================================================

def sync_loop():
    """
    Periodically sync new/updated orders from B2BWave into our DB.
    """
    while True:
        try:
            new_orders = poll_b2bwave_new_orders(15)
            if new_orders:
                print(f"[SYNC] Found {len(new_orders)} updated orders")
                with get_db() as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        for ord_payload in new_orders:
                            order_id = str(ord_payload.get("id"))
                            customer_name = ord_payload.get("shipping_address", {}).get("full_name") or ""
                            company_name = ord_payload.get("shipping_address", {}).get("company") or ""
                            total = ord_payload.get("total") or 0

                            cur.execute("""
                                INSERT INTO orders (order_id, customer_name, company_name, order_total, created_at, updated_at)
                                VALUES (%s, %s, %s, %s, NOW(), NOW())
                                ON CONFLICT (order_id) DO UPDATE SET
                                    customer_name = EXCLUDED.customer_name,
                                    company_name = EXCLUDED.company_name,
                                    order_total = EXCLUDED.order_total,
                                    updated_at = NOW()
                            """, (order_id, customer_name, company_name, total))

                            ai_summary = call_claude_for_order_summary(ord_payload)
                            cur.execute("""
                                INSERT INTO order_events (order_id, event_type, event_data, source)
                                VALUES (%s, 'ai_summary', %s, 'claude')
                            """, (order_id, json.dumps({"summary": ai_summary})))

                        conn.commit()
        except Exception as e:
            print(f"[SYNC ERROR] {e}")

        time.sleep(60 * 15)


@app.on_event("startup")
def on_startup():
    if ENABLE_SYNC_THREAD:
        t = threading.Thread(target=sync_loop, daemon=True)
        t.start()
        print("[SYNC] Background sync thread started.")

# =============================================================================
# BASIC ROUTES
# =============================================================================

@app.get("/")
def root():
    return {"status": "ok", "service": "CFC Order Workflow Backend", "version": "5.8.4"}


@app.get("/orders")
def list_orders(limit: int = 200):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM orders
                ORDER BY created_at DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            return rows


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            if not order:
                raise HTTPException(status_code=404, detail="Order not found")

            cur.execute("""
                SELECT event_type, event_data, created_at, source
                FROM order_events
                WHERE order_id = %s
                ORDER BY created_at
            """, (order_id,))
            events = cur.fetchall()

            cur.execute("""
                SELECT id, timestamp, subject, body_preview, direction, source, message_id
                FROM email_logs
                WHERE order_id = %s
                ORDER BY timestamp
            """, (order_id,))
            email_logs = cur.fetchall()

            cur.execute("""
                SELECT id, alert_type, alert_message, is_resolved, created_at, resolved_at
                FROM order_alerts
                WHERE order_id = %s
                ORDER BY created_at
            """, (order_id,))
            alerts = cur.fetchall()

            return {
                "order": order,
                "events": events,
                "email_logs": email_logs,
                "alerts": alerts,
            }

# =============================================================================
# ORDER STATUS & EVENTS
# =============================================================================

@app.post("/orders/{order_id}/status")
def update_order_status(order_id: str, payload: OrderStatusUpdate):
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            if not order:
                raise HTTPException(status_code=404, detail="Order not found")

            cur.execute("""
                UPDATE orders
                SET status = %s, updated_at = NOW()
                WHERE order_id = %s
            """, (payload.status, order_id))

            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, 'status_change', %s, 'backend')
            """, (order_id, json.dumps({
                "from": order.get("status"),
                "to": payload.status,
                "note": payload.note or ""
            })))

            conn.commit()

            return {"status": "ok"}


@app.post("/orders/{order_id}/event")
def add_order_event(order_id: str, event_type: str, event_data: Dict[str, Any]):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, %s, %s, 'backend')
            """, (order_id, event_type, json.dumps(event_data)))
            conn.commit()
            return {"status": "ok"}

# =============================================================================
# EMAIL LOGGING / UPSERT
# =============================================================================

@app.post("/email-logs/upsert")
def upsert_email_logs(payload: EmailLogUpsertRequest):
    """
    Upsert email logs into DB. If message_id already exists for that order, update;
    otherwise insert a new record.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            for log in payload.logs:
                ts = datetime.fromisoformat(log.timestamp)
                cur.execute("""
                    SELECT id FROM email_logs
                    WHERE order_id = %s AND message_id = %s
                """, (log.order_id, log.message_id))
                existing = cur.fetchone()

                if existing:
                    cur.execute("""
                        UPDATE email_logs
                        SET timestamp = %s,
                            subject = %s,
                            body_preview = %s,
                            full_body = %s,
                            direction = %s,
                            source = %s
                        WHERE id = %s
                    """, (
                        ts,
                        log.subject,
                        log.body_preview,
                        log.full_body,
                        log.direction,
                        log.source,
                        existing[0],
                    ))
                else:
                    cur.execute("""
                        INSERT INTO email_logs (
                            order_id, timestamp, subject, body_preview, full_body,
                            direction, source, message_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        log.order_id,
                        ts,
                        log.subject,
                        log.body_preview,
                        log.full_body,
                        log.direction,
                        log.source,
                        log.message_id,
                    ))
            conn.commit()
            return {"status": "ok", "count": len(payload.logs)}

# =============================================================================
# PAYMENT LINKS
# =============================================================================

@app.post("/orders/{order_id}/payment-link")
def record_payment_link(order_id: str, payload: PaymentLinkRecord):
    """
    Record a payment link creation event + mark that a payment link exists.
    """
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, 'payment_link_created', %s, 'square')
            """, (order_id, json.dumps({
                "invoice_no": payload.invoice_no,
                "amount": payload.amount,
                "created_at": payload.created_at,
                "link_url": payload.link_url,
            })))

            cur.execute("""
                UPDATE orders
                SET updated_at = NOW()
                WHERE order_id = %s
            """, (order_id,))

            conn.commit()
            return {"status": "ok"}


@app.post("/orders/{order_id}/mark-paid")
def mark_order_paid(order_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            if not order:
                raise HTTPException(status_code=404, detail="Order not found")

            cur.execute("""
                UPDATE orders
                SET payment_received = TRUE,
                    payment_received_at = NOW(),
                    updated_at = NOW()
                WHERE order_id = %s
            """, (order_id,))

            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, 'payment_received', %s, 'backend')
            """, (order_id, json.dumps({})))

            conn.commit()
            return {"status": "ok"}

# =============================================================================
# RL QUOTE DETECTION
# =============================================================================

@app.post("/detect-rl-quote")
def detect_rl_quote(order_id: str, email_body: str):
    """Detect R+L quote number from email"""
    # Pattern: "RL Quote No: 9075654" or "Quote: 9075654" or "Quote #9075654"
    quote_match = re.search(r'(?:RL\s+)?Quote\s*(?:No|#)?[:\s]*(\d{6,10})', email_body, re.IGNORECASE)
    
    if quote_match:
        quote_no = quote_match.group(1)
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orders SET rl_quote_no = %s, updated_at = NOW()
                    WHERE order_id = %s
                """, (quote_no, order_id))
                
                cur.execute("""
                    INSERT INTO order_events (order_id, event_type, event_data, source)
                    VALUES (%s, 'rl_quote_captured', %s, 'email_detection')
                """, (order_id, json.dumps({'quote_no': quote_no})))
                
                return {"status": "ok", "quote_no": quote_no}
    
    return {"status": "ok", "quote_no": None, "message": "No quote number found"}


@app.post("/detect-pro-number")
def detect_pro_number(order_id: str, email_body: str):
    """Detect R+L PRO number from email"""
    # Pattern: "PRO 74408602-5" or "PRO# 74408602-5" or "Pro Number: 74408602-5"
    pro_match = re.search(r'PRO\s*(?:#|Number)?[:\s]*([A-Z]{0,2}\d{8,10}(?:-\d)?)', email_body, re.IGNORECASE)
    
    if pro_match:
        pro_no = pro_match.group(1).upper()
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orders SET pro_number = %s, tracking = %s, updated_at = NOW()
                    WHERE order_id = %s
                """, (pro_no, f"R+L PRO {pro_no}", order_id))
                
                cur.execute("""
                    INSERT INTO order_events (order_id, event_type, event_data, source)
                    VALUES (%s, 'pro_number_captured', %s, 'email_detection')
                """, (order_id, json.dumps({'pro_number': pro_no})))
                
                return {"status": "ok", "pro_number": pro_no}
    
    return {"status": "ok", "pro_number": None, "message": "No PRO number found"}

# =============================================================================
# TRUSTED CUSTOMER ALERT CHECK
# =============================================================================

@app.post("/check-payment-alerts")
def check_payment_alerts():
    """
    Check for trusted customers who shipped but haven't paid after 1 business day.
    Should be called periodically (e.g., daily at 9 AM).
    """
    alerts_created = 0
    
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Find orders: sent to warehouse, not paid, trusted customer, > 1 day old
            cur.execute("""
                SELECT o.order_id, o.customer_name, o.company_name, o.order_total,
                       o.sent_to_warehouse_at
                FROM orders o
                WHERE o.sent_to_warehouse = TRUE
                AND o.payment_received = FALSE
                AND o.is_trusted_customer = TRUE
                AND o.sent_to_warehouse_at < NOW() - INTERVAL '1 day'
                AND NOT EXISTS (
                    SELECT 1 FROM order_alerts a 
                    WHERE a.order_id = o.order_id 
                    AND a.alert_type = 'trusted_unpaid'
                    AND NOT a.is_resolved
                )
            """)
            
            orders = cur.fetchall()
            
            for order in orders:
                cur.execute("""
                    INSERT INTO order_alerts (order_id, alert_type, alert_message)
                    VALUES (%s, 'trusted_unpaid', %s)
                """, (
                    order['order_id'],
                    f"Trusted customer {order['customer_name']} ...- shipped but unpaid for 1+ day. Total: ${order['order_total']}"
                ))
                alerts_created += 1
    
    return {"status": "ok", "alerts_created": alerts_created}

# =============================================================================
# ENTRYPOINT (for local dev / Render)
# =============================================================================
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))  # Render provides PORT env var
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
    )
