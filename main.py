"""
CFC Order Workflow Backend - v4
Added: address, contact info, order total, tracking

"""

import os
import json
from datetime import datetime, timezone
from typing import Optional, List
from contextlib import contextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(
    title="CFC Order Workflow API",
    description="6-Checkpoint Order Tracking System",
    version="4.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def get_database_url():
    raw_url = os.environ.get("DATABASE_URL", "")
    url = raw_url.strip()
    if "sslmode=" in url:
        url = url.split("?")[0] + "?sslmode=require"
    else:
        url = url + "?sslmode=require"
    return url

@contextmanager
def get_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(get_database_url())
        yield conn
    finally:
        if conn:
            conn.close()

@contextmanager
def get_db_cursor(commit=True):
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()

# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class OrderCreate(BaseModel):
    order_id: str
    customer_name: Optional[str] = None
    order_date: Optional[datetime] = None
    email_thread_id: Optional[str] = None
    order_total: Optional[float] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    street: Optional[str] = None
    suite: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    comments: Optional[str] = None
    source: Optional[str] = "manual"

class CheckpointUpdate(BaseModel):
    checkpoint: str
    value: bool = True
    timestamp: Optional[datetime] = None
    source: Optional[str] = "manual"
    data: Optional[dict] = None

class OrderUpdate(BaseModel):
    customer_name: Optional[str] = None
    order_total: Optional[float] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    street: Optional[str] = None
    suite: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    comments: Optional[str] = None
    supplier: Optional[str] = None
    supplier_order_no: Optional[str] = None
    tracking: Optional[str] = None
    notes: Optional[str] = None

# =============================================================================
# HEALTH & DIAGNOSTICS
# =============================================================================

@app.get("/health")
def health_check():
    return {"status": "ok", "version": "4.0.0"}

@app.get("/db-check")
def db_check():
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT NOW()")
            result = cur.fetchone()
            return {"db_status": "ok", "time": str(result["now"])}
    except Exception as e:
        return {"db_status": "error", "detail": str(e)}

# =============================================================================
# SCHEMA
# =============================================================================

SCHEMA_SQL = """
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS orders CASCADE;

CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(255),
    order_date TIMESTAMP WITH TIME ZONE,
    email_thread_id VARCHAR(100),
    
    -- Order details
    order_total DECIMAL(10,2),
    
    -- Contact info
    email VARCHAR(255),
    phone VARCHAR(50),
    
    -- Address
    street VARCHAR(255),
    suite VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    
    -- Customer comments from order
    comments TEXT,
    
    -- Supplier info (can have multiple - stored as JSON for flexibility)
    supplier VARCHAR(50),
    supplier_order_no VARCHAR(50),
    suppliers JSONB,  -- [{name, order_no}, ...]
    
    -- Tracking
    tracking VARCHAR(255),
    
    -- 6 Checkpoints
    payment_link_sent BOOLEAN DEFAULT FALSE,
    payment_link_sent_at TIMESTAMP WITH TIME ZONE,
    
    payment_received BOOLEAN DEFAULT FALSE,
    payment_received_at TIMESTAMP WITH TIME ZONE,
    
    sent_to_warehouse BOOLEAN DEFAULT FALSE,
    sent_to_warehouse_at TIMESTAMP WITH TIME ZONE,
    
    warehouse_confirmed BOOLEAN DEFAULT FALSE,
    warehouse_confirmed_at TIMESTAMP WITH TIME ZONE,
    
    bol_sent BOOLEAN DEFAULT FALSE,
    bol_sent_at TIMESTAMP WITH TIME ZONE,
    
    is_complete BOOLEAN DEFAULT FALSE,
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Notes (internal)
    notes TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_orders_order_date ON orders(order_date DESC);
CREATE INDEX idx_orders_is_complete ON orders(is_complete);

CREATE TABLE events (
    event_id SERIAL PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    source VARCHAR(50) DEFAULT 'manual',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_events_order_id ON events(order_id);

-- Status view
CREATE OR REPLACE VIEW order_status_summary AS
SELECT 
    order_id,
    customer_name,
    order_date,
    order_total,
    email,
    phone,
    street,
    suite,
    city,
    state,
    zip_code,
    comments,
    supplier,
    supplier_order_no,
    suppliers,
    tracking,
    CASE 
        WHEN is_complete THEN 'complete'
        WHEN bol_sent THEN 'awaiting_shipment'
        WHEN warehouse_confirmed THEN 'needs_bol'
        WHEN sent_to_warehouse THEN 'awaiting_warehouse'
        WHEN payment_received THEN 'needs_warehouse_order'
        WHEN payment_link_sent THEN 'awaiting_payment'
        ELSE 'needs_payment_link'
    END AS current_status,
    EXTRACT(DAY FROM NOW() - order_date)::INT AS days_open,
    payment_link_sent,
    payment_received,
    sent_to_warehouse,
    warehouse_confirmed,
    bol_sent,
    is_complete,
    notes,
    created_at,
    updated_at
FROM orders
ORDER BY order_date DESC;
"""

@app.post("/init-db")
def init_database():
    try:
        with get_db_cursor() as cur:
            cur.execute(SCHEMA_SQL)
        return {"status": "ok", "detail": "Database schema v4 initialized."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# ORDER CRUD
# =============================================================================

@app.post("/orders")
def create_order(order: OrderCreate):
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (order.order_id,))
            if cur.fetchone():
                raise HTTPException(status_code=409, detail=f"Order {order.order_id} already exists")
            
            cur.execute("""
                INSERT INTO orders (
                    order_id, customer_name, order_date, email_thread_id,
                    order_total, email, phone,
                    street, suite, city, state, zip_code,
                    comments
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
            """, (
                order.order_id,
                order.customer_name,
                order.order_date or datetime.now(timezone.utc),
                order.email_thread_id,
                order.order_total,
                order.email,
                order.phone,
                order.street,
                order.suite,
                order.city,
                order.state,
                order.zip_code,
                order.comments
            ))
            new_order = cur.fetchone()
            
            cur.execute("""
                INSERT INTO events (order_id, event_type, event_data, source)
                VALUES (%s, %s, %s, %s)
            """, (
                order.order_id,
                "order_created",
                json.dumps({"customer_name": order.customer_name}),
                order.source
            ))
            
            return {"status": "ok", "order": dict(new_order)}
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders")
def list_orders(
    status: Optional[str] = Query(None),
    include_complete: bool = Query(False),
    limit: int = Query(200)
):
    try:
        with get_db_cursor() as cur:
            query = "SELECT * FROM order_status_summary WHERE 1=1"
            params = []
            
            if not include_complete:
                query += " AND is_complete = FALSE"
            
            if status:
                query += " AND current_status = %s"
                params.append(status)
            
            query += " ORDER BY order_date DESC LIMIT %s"
            params.append(limit)
            
            cur.execute(query, params)
            orders = cur.fetchall()
            
            return {
                "status": "ok",
                "count": len(orders),
                "orders": [dict(o) for o in orders]
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders/{order_id}")
def get_order(order_id: str):
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT * FROM order_status_summary WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            if not order:
                raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
            return {"status": "ok", "order": dict(order)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.patch("/orders/{order_id}")
def update_order(order_id: str, update: OrderUpdate):
    try:
        with get_db_cursor() as cur:
            updates = []
            params = []
            
            fields = [
                ('customer_name', update.customer_name),
                ('order_total', update.order_total),
                ('email', update.email),
                ('phone', update.phone),
                ('street', update.street),
                ('suite', update.suite),
                ('city', update.city),
                ('state', update.state),
                ('zip_code', update.zip_code),
                ('comments', update.comments),
                ('supplier', update.supplier),
                ('supplier_order_no', update.supplier_order_no),
                ('tracking', update.tracking),
                ('notes', update.notes)
            ]
            
            for field_name, field_value in fields:
                if field_value is not None:
                    updates.append(f"{field_name} = %s")
                    params.append(field_value)
            
            if not updates:
                raise HTTPException(status_code=400, detail="No fields to update")
            
            updates.append("updated_at = NOW()")
            params.append(order_id)
            
            query = f"UPDATE orders SET {', '.join(updates)} WHERE order_id = %s RETURNING *"
            cur.execute(query, params)
            
            order = cur.fetchone()
            if not order:
                raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
            
            return {"status": "ok", "order": dict(order)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# CHECKPOINTS
# =============================================================================

VALID_CHECKPOINTS = [
    "payment_link_sent",
    "payment_received", 
    "sent_to_warehouse",
    "warehouse_confirmed",
    "bol_sent",
    "is_complete"
]

@app.patch("/orders/{order_id}/checkpoint")
def update_checkpoint(order_id: str, update: CheckpointUpdate):
    if update.checkpoint not in VALID_CHECKPOINTS:
        raise HTTPException(status_code=400, detail=f"Invalid checkpoint: {update.checkpoint}")
    
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (order_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
            
            timestamp_col = f"{update.checkpoint}_at"
            if update.checkpoint == "is_complete":
                timestamp_col = "completed_at"
            
            timestamp = update.timestamp or datetime.now(timezone.utc)
            
            if update.value:
                cur.execute(f"""
                    UPDATE orders 
                    SET {update.checkpoint} = TRUE, {timestamp_col} = %s, updated_at = NOW()
                    WHERE order_id = %s
                """, (timestamp, order_id))
            else:
                cur.execute(f"""
                    UPDATE orders 
                    SET {update.checkpoint} = FALSE, {timestamp_col} = NULL, updated_at = NOW()
                    WHERE order_id = %s
                """, (order_id,))
            
            # Update supplier if provided
            if update.data:
                if update.data.get("supplier"):
                    cur.execute("UPDATE orders SET supplier = %s WHERE order_id = %s",
                        (update.data["supplier"], order_id))
                if update.data.get("supplier_order_no"):
                    cur.execute("UPDATE orders SET supplier_order_no = %s WHERE order_id = %s",
                        (update.data["supplier_order_no"], order_id))
                if update.data.get("tracking"):
                    cur.execute("UPDATE orders SET tracking = %s WHERE order_id = %s",
                        (update.data["tracking"], order_id))
            
            # Log event
            cur.execute("""
                INSERT INTO events (order_id, event_type, event_data, source)
                VALUES (%s, %s, %s, %s)
            """, (
                order_id,
                update.checkpoint if update.value else f"{update.checkpoint}_undone",
                json.dumps(update.data) if update.data else None,
                update.source
            ))
            
            cur.execute("SELECT * FROM order_status_summary WHERE order_id = %s", (order_id,))
            updated_order = cur.fetchone()
            
            return {"status": "ok", "order": dict(updated_order)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# EVENTS
# =============================================================================

@app.get("/orders/{order_id}/events")
def get_order_events(order_id: str):
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (order_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
            
            cur.execute("SELECT * FROM events WHERE order_id = %s ORDER BY created_at DESC", (order_id,))
            events = cur.fetchall()
            
            return {"status": "ok", "order_id": order_id, "events": [dict(e) for e in events]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# BULK IMPORT
# =============================================================================

class BulkOrderCreate(BaseModel):
    orders: List[OrderCreate]

@app.post("/orders/bulk")
def create_orders_bulk(bulk: BulkOrderCreate):
    created = []
    skipped = []
    
    try:
        with get_db_cursor() as cur:
            for order in bulk.orders:
                cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (order.order_id,))
                if cur.fetchone():
                    skipped.append(order.order_id)
                    continue
                
                cur.execute("""
                    INSERT INTO orders (
                        order_id, customer_name, order_date, email_thread_id,
                        order_total, email, phone,
                        street, suite, city, state, zip_code, comments
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    order.order_id, order.customer_name,
                    order.order_date or datetime.now(timezone.utc),
                    order.email_thread_id, order.order_total, order.email, order.phone,
                    order.street, order.suite, order.city, order.state, order.zip_code,
                    order.comments
                ))
                
                cur.execute("""
                    INSERT INTO events (order_id, event_type, source)
                    VALUES (%s, 'order_created', 'bulk_import')
                """, (order.order_id,))
                
                created.append(order.order_id)
        
        return {"status": "ok", "created": len(created), "skipped": len(skipped)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)
