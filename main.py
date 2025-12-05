"""
CFC Order Workflow Backend - v2
Simplified 6-Checkpoint System

Endpoints:
    GET  /health              - Health check
    GET  /db-check            - Database connectivity test
    POST /init-db             - Initialize database schema
    
    POST /orders              - Create new order
    GET  /orders              - List all orders with status
    GET  /orders/{order_id}   - Get single order details
    
    PATCH /orders/{order_id}/checkpoint  - Update a checkpoint
    GET   /orders/{order_id}/events      - Get event history for order
    
    GET  /orders/status/summary   - Get all orders with current status
    GET  /orders/status/stuck     - Get orders that are stuck
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
    version="2.0.0"
)

# Allow CORS for Google Sheets and other clients
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
    """Get and clean the DATABASE_URL from environment."""
    raw_url = os.environ.get("DATABASE_URL", "")
    
    # Clean up common issues
    url = raw_url.strip()
    
    # Ensure sslmode=require is present and clean
    if "sslmode=" in url:
        # Remove any corrupted sslmode and re-add clean one
        url = url.split("?")[0] + "?sslmode=require"
    else:
        url = url + "?sslmode=require"
    
    return url


@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    conn = None
    try:
        conn = psycopg2.connect(get_database_url())
        yield conn
    finally:
        if conn:
            conn.close()


@contextmanager
def get_db_cursor(commit=True):
    """Context manager for database cursor with auto-commit."""
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
    """Model for creating a new order."""
    order_id: str
    customer_name: Optional[str] = None
    order_date: Optional[datetime] = None
    email_thread_id: Optional[str] = None
    source: Optional[str] = "manual"  # Where did this order come from?


class CheckpointUpdate(BaseModel):
    """Model for updating a checkpoint."""
    checkpoint: str  # Which checkpoint: payment_link_sent, payment_received, etc.
    value: bool = True  # Set to True (or False to undo)
    timestamp: Optional[datetime] = None  # When it happened (defaults to now)
    source: Optional[str] = "manual"  # Where did this update come from?
    data: Optional[dict] = None  # Additional context (supplier info, etc.)


class OrderUpdate(BaseModel):
    """Model for updating order fields."""
    customer_name: Optional[str] = None
    supplier: Optional[str] = None
    supplier_order_no: Optional[str] = None
    notes: Optional[str] = None


# =============================================================================
# HEALTH & DIAGNOSTICS
# =============================================================================

@app.get("/health")
def health_check():
    """Basic health check."""
    return {"status": "ok", "version": "2.0.0"}


@app.get("/db-check")
def db_check():
    """Test database connectivity."""
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT NOW()")
            result = cur.fetchone()
            return {"db_status": "ok", "time": str(result["now"])}
    except Exception as e:
        return {"db_status": "error", "detail": str(e)}


# =============================================================================
# SCHEMA INITIALIZATION
# =============================================================================

SCHEMA_SQL = """
-- Drop existing tables if they exist (clean slate)
DROP TABLE IF EXISTS notes CASCADE;
DROP TABLE IF EXISTS tasks CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS orders CASCADE;

-- ORDERS TABLE
CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(255),
    order_date TIMESTAMP WITH TIME ZONE,
    email_thread_id VARCHAR(100),
    supplier VARCHAR(50),
    supplier_order_no VARCHAR(50),
    
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
    
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_orders_created_at ON orders(created_at DESC);
CREATE INDEX idx_orders_is_complete ON orders(is_complete);
CREATE INDEX idx_orders_supplier ON orders(supplier);

-- EVENTS TABLE
CREATE TABLE events (
    event_id SERIAL PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    source VARCHAR(50) DEFAULT 'manual',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_events_order_id ON events(order_id);
CREATE INDEX idx_events_created_at ON events(created_at DESC);
CREATE INDEX idx_events_type ON events(event_type);

-- ORDER STATUS VIEW
CREATE OR REPLACE VIEW order_status_summary AS
SELECT 
    order_id,
    customer_name,
    order_date,
    supplier,
    supplier_order_no,
    CASE 
        WHEN is_complete THEN 'complete'
        WHEN bol_sent THEN 'awaiting_shipment'
        WHEN warehouse_confirmed THEN 'needs_bol'
        WHEN sent_to_warehouse THEN 'awaiting_warehouse'
        WHEN payment_received THEN 'needs_warehouse_order'
        WHEN payment_link_sent THEN 'awaiting_payment'
        ELSE 'needs_payment_link'
    END AS current_status,
    EXTRACT(DAY FROM NOW() - created_at)::INT AS days_open,
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
ORDER BY created_at DESC;

-- STUCK ORDERS VIEW
CREATE OR REPLACE VIEW stuck_orders AS
SELECT 
    order_id,
    customer_name,
    supplier,
    CASE 
        WHEN NOT payment_link_sent THEN 'needs_payment_link'
        WHEN NOT payment_received THEN 'awaiting_payment'
        WHEN NOT sent_to_warehouse THEN 'needs_warehouse_order'
        WHEN NOT warehouse_confirmed THEN 'awaiting_warehouse'
        WHEN NOT bol_sent THEN 'needs_bol'
        ELSE 'ready_to_ship'
    END AS stuck_at,
    CASE 
        WHEN NOT payment_link_sent THEN created_at
        WHEN NOT payment_received THEN payment_link_sent_at
        WHEN NOT sent_to_warehouse THEN payment_received_at
        WHEN NOT warehouse_confirmed THEN sent_to_warehouse_at
        WHEN NOT bol_sent THEN warehouse_confirmed_at
        ELSE bol_sent_at
    END AS stuck_since,
    EXTRACT(DAY FROM NOW() - 
        CASE 
            WHEN NOT payment_link_sent THEN created_at
            WHEN NOT payment_received THEN payment_link_sent_at
            WHEN NOT sent_to_warehouse THEN payment_received_at
            WHEN NOT warehouse_confirmed THEN sent_to_warehouse_at
            WHEN NOT bol_sent THEN warehouse_confirmed_at
            ELSE bol_sent_at
        END
    )::INT AS days_stuck
FROM orders
WHERE is_complete = FALSE
ORDER BY days_stuck DESC;
"""


@app.post("/init-db")
def init_database():
    """Initialize (or reset) the database schema."""
    try:
        with get_db_cursor() as cur:
            cur.execute(SCHEMA_SQL)
        return {"status": "ok", "detail": "Database schema initialized successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ORDER CRUD
# =============================================================================

@app.post("/orders")
def create_order(order: OrderCreate):
    """
    Create a new order.
    This is Checkpoint 1 - Order Received.
    """
    try:
        with get_db_cursor() as cur:
            # Check if order already exists
            cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (order.order_id,))
            if cur.fetchone():
                raise HTTPException(status_code=409, detail=f"Order {order.order_id} already exists")
            
            # Insert new order
            cur.execute("""
                INSERT INTO orders (order_id, customer_name, order_date, email_thread_id)
                VALUES (%s, %s, %s, %s)
                RETURNING *
            """, (
                order.order_id,
                order.customer_name,
                order.order_date or datetime.now(timezone.utc),
                order.email_thread_id
            ))
            new_order = cur.fetchone()
            
            # Log the event
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
    status: Optional[str] = Query(None, description="Filter by current_status"),
    supplier: Optional[str] = Query(None, description="Filter by supplier"),
    include_complete: bool = Query(False, description="Include completed orders"),
    limit: int = Query(100, description="Max results to return")
):
    """List all orders with their current status."""
    try:
        with get_db_cursor() as cur:
            query = "SELECT * FROM order_status_summary WHERE 1=1"
            params = []
            
            if not include_complete:
                query += " AND is_complete = FALSE"
            
            if status:
                query += " AND current_status = %s"
                params.append(status)
            
            if supplier:
                query += " AND supplier = %s"
                params.append(supplier)
            
            query += " ORDER BY created_at DESC LIMIT %s"
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
    """Get a single order with full details."""
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
    """Update order fields (supplier, notes, etc.)."""
    try:
        with get_db_cursor() as cur:
            # Build dynamic update query
            updates = []
            params = []
            
            if update.customer_name is not None:
                updates.append("customer_name = %s")
                params.append(update.customer_name)
            
            if update.supplier is not None:
                updates.append("supplier = %s")
                params.append(update.supplier)
            
            if update.supplier_order_no is not None:
                updates.append("supplier_order_no = %s")
                params.append(update.supplier_order_no)
            
            if update.notes is not None:
                updates.append("notes = %s")
                params.append(update.notes)
            
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
# CHECKPOINT UPDATES
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
    """
    Update a specific checkpoint for an order.
    
    Valid checkpoints:
    - payment_link_sent
    - payment_received
    - sent_to_warehouse
    - warehouse_confirmed
    - bol_sent
    - is_complete
    """
    if update.checkpoint not in VALID_CHECKPOINTS:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid checkpoint. Must be one of: {VALID_CHECKPOINTS}"
        )
    
    try:
        with get_db_cursor() as cur:
            # Check order exists
            cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (order_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
            
            # Determine timestamp column
            timestamp_col = f"{update.checkpoint}_at"
            if update.checkpoint == "is_complete":
                timestamp_col = "completed_at"
            
            # Update the checkpoint
            timestamp = update.timestamp or datetime.now(timezone.utc)
            
            if update.value:
                # Setting checkpoint to True
                cur.execute(f"""
                    UPDATE orders 
                    SET {update.checkpoint} = TRUE, 
                        {timestamp_col} = %s,
                        updated_at = NOW()
                    WHERE order_id = %s
                    RETURNING *
                """, (timestamp, order_id))
            else:
                # Setting checkpoint to False (undo)
                cur.execute(f"""
                    UPDATE orders 
                    SET {update.checkpoint} = FALSE, 
                        {timestamp_col} = NULL,
                        updated_at = NOW()
                    WHERE order_id = %s
                    RETURNING *
                """, (order_id,))
            
            order = cur.fetchone()
            
            # Also update supplier info if provided
            if update.data:
                if update.data.get("supplier"):
                    cur.execute(
                        "UPDATE orders SET supplier = %s WHERE order_id = %s",
                        (update.data["supplier"], order_id)
                    )
                if update.data.get("supplier_order_no"):
                    cur.execute(
                        "UPDATE orders SET supplier_order_no = %s WHERE order_id = %s",
                        (update.data["supplier_order_no"], order_id)
                    )
            
            # Log the event
            event_type = update.checkpoint if update.value else f"{update.checkpoint}_undone"
            cur.execute("""
                INSERT INTO events (order_id, event_type, event_data, source)
                VALUES (%s, %s, %s, %s)
            """, (
                order_id,
                event_type,
                json.dumps(update.data) if update.data else None,
                update.source
            ))
            
            # Fetch updated order status
            cur.execute("SELECT * FROM order_status_summary WHERE order_id = %s", (order_id,))
            updated_order = cur.fetchone()
            
            return {
                "status": "ok",
                "checkpoint": update.checkpoint,
                "value": update.value,
                "order": dict(updated_order)
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# EVENTS / AUDIT LOG
# =============================================================================

@app.get("/orders/{order_id}/events")
def get_order_events(order_id: str):
    """Get the event history for an order."""
    try:
        with get_db_cursor() as cur:
            # Check order exists
            cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (order_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
            
            cur.execute("""
                SELECT * FROM events 
                WHERE order_id = %s 
                ORDER BY created_at DESC
            """, (order_id,))
            events = cur.fetchall()
            
            return {
                "status": "ok",
                "order_id": order_id,
                "count": len(events),
                "events": [dict(e) for e in events]
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# STATUS VIEWS
# =============================================================================

@app.get("/orders/status/summary")
def get_status_summary():
    """Get counts of orders by status."""
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT current_status, COUNT(*) as count
                FROM order_status_summary
                WHERE is_complete = FALSE
                GROUP BY current_status
                ORDER BY count DESC
            """)
            summary = cur.fetchall()
            
            # Also get total counts
            cur.execute("SELECT COUNT(*) as total FROM orders")
            total = cur.fetchone()["total"]
            
            cur.execute("SELECT COUNT(*) as complete FROM orders WHERE is_complete = TRUE")
            complete = cur.fetchone()["complete"]
            
            return {
                "status": "ok",
                "total_orders": total,
                "complete_orders": complete,
                "active_orders": total - complete,
                "by_status": [dict(s) for s in summary]
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/orders/status/stuck")
def get_stuck_orders(days: int = Query(3, description="Minimum days stuck")):
    """Get orders that have been stuck at a checkpoint for N days."""
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT * FROM stuck_orders
                WHERE days_stuck >= %s
                ORDER BY days_stuck DESC
            """, (days,))
            stuck = cur.fetchall()
            
            return {
                "status": "ok",
                "threshold_days": days,
                "count": len(stuck),
                "orders": [dict(s) for s in stuck]
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# BULK OPERATIONS (for Google Sheet sync)
# =============================================================================

class BulkOrderCreate(BaseModel):
    """Model for bulk order creation."""
    orders: List[OrderCreate]


@app.post("/orders/bulk")
def create_orders_bulk(bulk: BulkOrderCreate):
    """
    Create multiple orders at once.
    Useful for initial sync from Google Sheet.
    Skips orders that already exist.
    """
    try:
        created = []
        skipped = []
        
        with get_db_cursor() as cur:
            for order in bulk.orders:
                # Check if exists
                cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (order.order_id,))
                if cur.fetchone():
                    skipped.append(order.order_id)
                    continue
                
                # Insert
                cur.execute("""
                    INSERT INTO orders (order_id, customer_name, order_date, email_thread_id)
                    VALUES (%s, %s, %s, %s)
                """, (
                    order.order_id,
                    order.customer_name,
                    order.order_date or datetime.now(timezone.utc),
                    order.email_thread_id
                ))
                
                # Log event
                cur.execute("""
                    INSERT INTO events (order_id, event_type, event_data, source)
                    VALUES (%s, %s, %s, %s)
                """, (
                    order.order_id,
                    "order_created",
                    json.dumps({"customer_name": order.customer_name}),
                    order.source or "bulk_import"
                ))
                
                created.append(order.order_id)
        
        return {
            "status": "ok",
            "created_count": len(created),
            "skipped_count": len(skipped),
            "created": created,
            "skipped": skipped
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)