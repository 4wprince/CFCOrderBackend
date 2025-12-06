"""
CFC Order Workflow Backend - v5.2
All parsing/logic server-side. B2BWave API integration for clean order data.
"""

import os
import re
import json
import base64
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional, List
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
if DATABASE_URL and "sslmode" not in DATABASE_URL:
    DATABASE_URL += "?sslmode=require"

# B2BWave API Config
B2BWAVE_URL = os.environ.get("B2BWAVE_URL", "").strip().rstrip('/')
B2BWAVE_USERNAME = os.environ.get("B2BWAVE_USERNAME", "").strip()
B2BWAVE_API_KEY = os.environ.get("B2BWAVE_API_KEY", "").strip()

app = FastAPI(title="CFC Order Workflow", version="5.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# DATABASE
# =============================================================================

@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

SCHEMA_SQL = """
-- Drop view first (depends on orders)
DROP VIEW IF EXISTS order_status CASCADE;

-- Drop tables
DROP TABLE IF EXISTS order_line_items CASCADE;
DROP TABLE IF EXISTS order_events CASCADE;
DROP TABLE IF EXISTS order_alerts CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS warehouse_mapping CASCADE;
DROP TABLE IF EXISTS trusted_customers CASCADE;

CREATE TABLE warehouse_mapping (
    sku_prefix VARCHAR(20) PRIMARY KEY,
    warehouse_name VARCHAR(100) NOT NULL,
    warehouse_code VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Default warehouse mappings
INSERT INTO warehouse_mapping (sku_prefix, warehouse_name, warehouse_code) VALUES
('WSP', 'Cabinet & Stone', 'LI'),
('GSP', 'Cabinet & Stone', 'LI'),
('AKS', 'GHI', 'GHI'),
('DL', 'DL Cabinetry', 'DL'),
('DS', 'Durastone', 'DS'),
('ROC', 'ROC Cabinetry', 'ROC'),
('GHI', 'GHI', 'GHI'),
('LC', 'L&C Cabinetry', 'LC'),
('HSS', 'Cabinet & Stone', 'LI'),
('NSN', 'Cabinet & Stone', 'LI'),
('SHLS', 'Cabinet & Stone', 'LI'),
('WWW', 'LOVE', 'LOVE'),
('DERA', 'Cabinet & Stone', 'LI'),
('SAVNG', 'Cabinet & Stone', 'LI')
ON CONFLICT (sku_prefix) DO NOTHING;

-- Trusted customers (can ship before payment)
CREATE TABLE trusted_customers (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    company_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    payment_grace_days INTEGER DEFAULT 1,
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

INSERT INTO trusted_customers (customer_name, company_name, notes) VALUES
('Lou Palumbo', 'Louis And Clark Contracting', 'Long-time trusted customer'),
('Gerald Thomas', 'G & B Wood Creations', 'Trusted customer'),
('LD Stafford', 'Acute Custom Closets', 'Trusted customer'),
('James Marchant', NULL, 'Trusted customer')
ON CONFLICT DO NOTHING;

-- Alerts/flags table
CREATE TABLE order_alerts (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(20) REFERENCES orders(order_id) ON DELETE CASCADE,
    alert_type VARCHAR(50) NOT NULL,
    alert_message TEXT,
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_alerts_order ON order_alerts(order_id);
CREATE INDEX idx_alerts_unresolved ON order_alerts(is_resolved) WHERE NOT is_resolved;

CREATE TABLE orders (
    order_id VARCHAR(20) PRIMARY KEY,
    
    -- Customer info
    customer_name VARCHAR(255),
    company_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    
    -- Address
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    
    -- Order details
    order_date TIMESTAMP WITH TIME ZONE,
    order_total DECIMAL(10,2),
    comments TEXT,
    
    -- Warehouses (extracted from SKU prefixes)
    warehouse_1 VARCHAR(100),
    warehouse_2 VARCHAR(100),
    
    -- Payment
    payment_link_sent BOOLEAN DEFAULT FALSE,
    payment_link_sent_at TIMESTAMP WITH TIME ZONE,
    payment_received BOOLEAN DEFAULT FALSE,
    payment_received_at TIMESTAMP WITH TIME ZONE,
    payment_amount DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    
    -- Shipping quotes
    rl_quote_no VARCHAR(50),
    shipping_quote_amount DECIMAL(10,2),
    
    -- Warehouse processing
    sent_to_warehouse BOOLEAN DEFAULT FALSE,
    sent_to_warehouse_at TIMESTAMP WITH TIME ZONE,
    warehouse_confirmed BOOLEAN DEFAULT FALSE,
    warehouse_confirmed_at TIMESTAMP WITH TIME ZONE,
    supplier_order_no VARCHAR(100),
    
    -- Shipping
    bol_sent BOOLEAN DEFAULT FALSE,
    bol_sent_at TIMESTAMP WITH TIME ZONE,
    tracking VARCHAR(255),
    pro_number VARCHAR(50),
    
    -- Flags
    is_trusted_customer BOOLEAN DEFAULT FALSE,
    needs_review BOOLEAN DEFAULT FALSE,
    review_reason TEXT,
    
    -- Completion
    is_complete BOOLEAN DEFAULT FALSE,
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Meta
    email_thread_id VARCHAR(255),
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE order_line_items (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(20) REFERENCES orders(order_id) ON DELETE CASCADE,
    sku VARCHAR(100),
    sku_prefix VARCHAR(20),
    product_name TEXT,
    price DECIMAL(10,2),
    quantity INTEGER,
    line_total DECIMAL(10,2),
    warehouse VARCHAR(100)
);

CREATE TABLE order_events (
    event_id SERIAL PRIMARY KEY,
    order_id VARCHAR(20) REFERENCES orders(order_id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    source VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_orders_complete ON orders(is_complete);
CREATE INDEX idx_orders_date ON orders(order_date DESC);
CREATE INDEX idx_line_items_order ON order_line_items(order_id);
CREATE INDEX idx_events_order ON order_events(order_id);

-- View for current status
CREATE OR REPLACE VIEW order_status AS
SELECT 
    order_id,
    CASE
        WHEN is_complete THEN 'complete'
        WHEN bol_sent AND NOT is_complete THEN 'awaiting_shipment'
        WHEN warehouse_confirmed AND NOT bol_sent THEN 'needs_bol'
        WHEN sent_to_warehouse AND NOT warehouse_confirmed THEN 'awaiting_warehouse'
        WHEN payment_received AND NOT sent_to_warehouse THEN 'needs_warehouse_order'
        WHEN payment_link_sent AND NOT payment_received THEN 'awaiting_payment'
        ELSE 'needs_payment_link'
    END as current_status,
    EXTRACT(DAY FROM NOW() - order_date)::INTEGER as days_open
FROM orders;
"""

# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class ParseEmailRequest(BaseModel):
    email_body: str
    email_subject: str
    email_date: Optional[str] = None
    email_thread_id: Optional[str] = None

class ParseEmailResponse(BaseModel):
    status: str
    order_id: Optional[str]
    parsed_data: Optional[dict]
    warehouses: Optional[List[str]]
    message: Optional[str]

class OrderUpdate(BaseModel):
    customer_name: Optional[str] = None
    company_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    order_total: Optional[float] = None
    comments: Optional[str] = None
    notes: Optional[str] = None
    tracking: Optional[str] = None
    supplier_order_no: Optional[str] = None
    warehouse_1: Optional[str] = None
    warehouse_2: Optional[str] = None

class CheckpointUpdate(BaseModel):
    checkpoint: str  # payment_link_sent, payment_received, sent_to_warehouse, warehouse_confirmed, bol_sent, is_complete
    source: Optional[str] = "api"
    payment_amount: Optional[float] = None

class WarehouseMappingUpdate(BaseModel):
    sku_prefix: str
    warehouse_name: str
    warehouse_code: Optional[str] = None

# =============================================================================
# EMAIL PARSING (SERVER-SIDE)
# =============================================================================

def parse_b2bwave_email(body: str, subject: str) -> dict:
    """
    Parse B2BWave order email and extract all fields.
    Returns dict with: order_id, name, company, street, city, state, zip, phone, email, comments, total, line_items
    """
    result = {
        'order_id': None,
        'customer_name': None,
        'company_name': None,
        'street': None,
        'city': None,
        'state': None,
        'zip_code': None,
        'phone': None,
        'email': None,
        'comments': None,
        'order_total': None,
        'line_items': []
    }
    
    # Clean up body - normalize whitespace
    clean_body = body.replace('\r\n', '\n').replace('\r', '\n')
    
    # Extract order ID from subject: "Order Legendary Home Improvements-(#5261)"
    subject_match = re.search(r'\(#(\d{4,7})\)', subject)
    if subject_match:
        result['order_id'] = subject_match.group(1)
    
    # Also try from body
    if not result['order_id']:
        order_id_match = re.search(r'Order ID:\s*(\d{4,7})', clean_body)
        if order_id_match:
            result['order_id'] = order_id_match.group(1)
    
    # Extract Name
    name_match = re.search(r'Name:\s*(.+?)(?:\n|$)', clean_body)
    if name_match:
        result['customer_name'] = name_match.group(1).strip()
    
    # Extract Company
    company_match = re.search(r'Company:\s*(.+?)(?:\n|$)', clean_body)
    if company_match:
        result['company_name'] = company_match.group(1).strip()
    
    # Extract Phone (format: "Phone 352-665-0280" or "Phone: 352-665-0280")
    phone_match = re.search(r'Phone[:\s]+(\d{3}[-.\s]?\d{3}[-.\s]?\d{4})', clean_body)
    if phone_match:
        result['phone'] = phone_match.group(1).replace('.', '-').replace(' ', '-')
    
    # Extract Email
    email_match = re.search(r'Email:\s*([\w.-]+@[\w.-]+\.\w+)', clean_body)
    if email_match:
        result['email'] = email_match.group(1).lower()
    
    # Extract Comments
    comments_match = re.search(r'Comments:\s*(.+?)(?:\n\n|\nTotal:|\nGross|$)', clean_body, re.DOTALL)
    if comments_match:
        result['comments'] = comments_match.group(1).strip()
    
    # Extract Total
    total_match = re.search(r'(?:^|\n)Total:\s*\$?([\d,]+\.?\d*)', clean_body)
    if total_match:
        result['order_total'] = float(total_match.group(1).replace(',', ''))
    
    # =========================================================================
    # IMPROVED ADDRESS PARSING
    # B2BWave format variations:
    # 1. "4943 SE 10th Place\nKeystone Heights  FL  32656"
    # 2. "4943 SE 10th Place\n\nKeystone Heights  FL  32656" (blank line between)
    # 3. Multi-space separated: "City  State  Zip"
    # =========================================================================
    
    # First, find city/state/zip pattern anywhere in email
    # Pattern: City (words)  STATE (2 letters)  ZIP (5 digits)
    csz_patterns = [
        # Double-space separated: "Keystone Heights  FL  32656"
        r'([A-Za-z][A-Za-z\s]+?)\s{2,}([A-Z]{2})\s{2,}(\d{5}(?:-\d{4})?)',
        # Single space with comma: "Keystone Heights, FL 32656"
        r'([A-Za-z][A-Za-z\s]+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)',
        # Single space: "Keystone Heights FL 32656"
        r'([A-Za-z][A-Za-z\s]+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)',
    ]
    
    for pattern in csz_patterns:
        csz_match = re.search(pattern, clean_body)
        if csz_match:
            city = csz_match.group(1).strip()
            state = csz_match.group(2)
            zip_code = csz_match.group(3)
            
            # Validate - city should not contain certain keywords
            if not any(kw in city.lower() for kw in ['total', 'order', 'email', 'phone', 'comment', 'name', 'company']):
                result['city'] = city
                result['state'] = state
                result['zip_code'] = zip_code
                break
    
    # Now find street address - look for line starting with number before the city/state/zip
    if result['city']:
        # Find all lines that start with a number (potential street addresses)
        street_pattern = r'^(\d+[^\n]+?)(?:\n|$)'
        street_matches = re.findall(street_pattern, clean_body, re.MULTILINE)
        
        for street in street_matches:
            street = street.strip()
            # Skip if it's a phone number line or contains keywords
            if 'phone' in street.lower():
                continue
            if re.match(r'^\d{3}[-.\s]?\d{3}[-.\s]?\d{4}', street):
                continue  # This is a phone number
            if '$' in street:
                continue  # This is a price line
            
            result['street'] = street
            break
    
    # If we still don't have street, try alternative approach
    if not result['street']:
        # Look for common street patterns
        street_match = re.search(r'(\d+\s+(?:N\.?|S\.?|E\.?|W\.?|North|South|East|West)?\s*[A-Za-z0-9\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Way|Court|Ct|Place|Pl|Circle|Cir|Trail)[^\n]*)', clean_body, re.IGNORECASE)
        if street_match:
            result['street'] = street_match.group(1).strip()
    
    # Extract SKU codes for warehouse mapping
    # Look for patterns like HSS-3VDB15, NSN-SM8, SHLS-B09
    sku_pattern = re.findall(r'\b([A-Z]{2,5})-[A-Z0-9]+\b', clean_body)
    sku_prefixes = list(set(sku_pattern))
    result['sku_prefixes'] = sku_prefixes
    
    return result

def get_warehouses_for_skus(sku_prefixes: List[str]) -> List[str]:
    """Look up warehouse names for given SKU prefixes"""
    if not sku_prefixes:
        return []
    
    warehouses = []
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            placeholders = ','.join(['%s'] * len(sku_prefixes))
            cur.execute(f"""
                SELECT DISTINCT warehouse_name 
                FROM warehouse_mapping 
                WHERE sku_prefix IN ({placeholders})
            """, sku_prefixes)
            warehouses = [row['warehouse_name'] for row in cur.fetchall()]
    
    return warehouses

# =============================================================================
# B2BWAVE API INTEGRATION
# =============================================================================

def b2bwave_api_request(endpoint: str, params: dict = None) -> dict:
    """Make authenticated request to B2BWave API"""
    if not B2BWAVE_URL or not B2BWAVE_USERNAME or not B2BWAVE_API_KEY:
        raise HTTPException(status_code=500, detail="B2BWave API not configured")
    
    url = f"{B2BWAVE_URL}/api/{endpoint}.json"
    if params:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{query}"
    
    # HTTP Basic Auth
    credentials = base64.b64encode(f"{B2BWAVE_USERNAME}:{B2BWAVE_API_KEY}".encode()).decode()
    
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Basic {credentials}")
    req.add_header("Content-Type", "application/json")
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        raise HTTPException(status_code=e.code, detail=f"B2BWave API error: {e.reason}")
    except urllib.error.URLError as e:
        raise HTTPException(status_code=500, detail=f"B2BWave connection error: {str(e)}")

def sync_order_from_b2bwave(order_data: dict) -> dict:
    """
    Sync a single order from B2BWave API response to our database.
    Returns the order_id and status.
    """
    order = order_data.get('order', order_data)
    
    order_id = str(order.get('id'))
    
    # Extract customer info
    customer_name = order.get('customer_name', '')
    company_name = order.get('customer_company', '')
    email = order.get('customer_email', '')
    phone = order.get('customer_phone', '')
    
    # Extract address - B2BWave provides these as separate fields!
    street = order.get('address', '')
    street2 = order.get('address2', '')
    if street2:
        street = f"{street}, {street2}"
    city = order.get('city', '')
    state = order.get('province', '')  # B2BWave calls it 'province'
    zip_code = order.get('postal_code', '')
    
    # Comments
    comments = order.get('comments_customer', '')
    
    # Totals
    order_total = float(order.get('gross_total', 0) or 0)
    
    # Order date
    submitted_at = order.get('submitted_at')
    if submitted_at:
        try:
            order_date = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
        except:
            order_date = datetime.now(timezone.utc)
    else:
        order_date = datetime.now(timezone.utc)
    
    # Extract line items and SKU prefixes
    order_products = order.get('order_products', [])
    sku_prefixes = []
    line_items = []
    
    for op in order_products:
        product = op.get('order_product', op)
        product_code = product.get('product_code', '')
        product_name = product.get('product_name', '')
        quantity = float(product.get('quantity', 0) or 0)
        price = float(product.get('final_price', 0) or 0)
        
        # Extract SKU prefix
        if '-' in product_code:
            prefix = product_code.split('-')[0]
            if prefix and prefix not in sku_prefixes:
                sku_prefixes.append(prefix)
        
        line_items.append({
            'sku': product_code,
            'product_name': product_name,
            'quantity': quantity,
            'price': price
        })
    
    # Get warehouses for SKU prefixes
    warehouses = get_warehouses_for_skus(sku_prefixes)
    warehouse_1 = warehouses[0] if len(warehouses) > 0 else None
    warehouse_2 = warehouses[1] if len(warehouses) > 1 else None
    
    # Check if trusted customer
    is_trusted = False
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id FROM trusted_customers 
                WHERE LOWER(customer_name) = LOWER(%s) 
                   OR LOWER(company_name) = LOWER(%s)
                   OR LOWER(email) = LOWER(%s)
            """, (customer_name, company_name, email))
            if cur.fetchone():
                is_trusted = True
    
    # Upsert order
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO orders (
                    order_id, order_date, customer_name, company_name,
                    street, city, state, zip_code, phone, email,
                    comments, order_total, warehouse_1, warehouse_2,
                    is_trusted_customer
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    company_name = EXCLUDED.company_name,
                    street = EXCLUDED.street,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    phone = EXCLUDED.phone,
                    email = EXCLUDED.email,
                    comments = EXCLUDED.comments,
                    order_total = EXCLUDED.order_total,
                    warehouse_1 = COALESCE(orders.warehouse_1, EXCLUDED.warehouse_1),
                    warehouse_2 = COALESCE(orders.warehouse_2, EXCLUDED.warehouse_2),
                    is_trusted_customer = EXCLUDED.is_trusted_customer,
                    updated_at = NOW()
                RETURNING order_id
            """, (
                order_id, order_date, customer_name, company_name,
                street, city, state, zip_code, phone, email,
                comments, order_total, warehouse_1, warehouse_2,
                is_trusted
            ))
            result = cur.fetchone()
            
            # Log sync event
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, 'b2bwave_sync', %s, 'api')
            """, (order_id, json.dumps({'sku_prefixes': sku_prefixes})))
    
    return {
        'order_id': order_id,
        'customer_name': customer_name,
        'company_name': company_name,
        'city': city,
        'state': state,
        'zip_code': zip_code,
        'warehouse_1': warehouse_1,
        'warehouse_2': warehouse_2,
        'line_items_count': len(line_items)
    }

# =============================================================================
# ROUTES
# =============================================================================

@app.get("/")
def root():
    return {"status": "ok", "service": "CFC Order Workflow", "version": "5.2.0"}

@app.get("/health")
def health():
    return {"status": "ok", "version": "5.0.0"}

@app.post("/init-db")
def init_db():
    """Initialize database schema (destructive!)"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    return {"status": "ok", "message": "Database schema initialized", "version": "5.2.0"}

# =============================================================================
# B2BWAVE SYNC ENDPOINTS
# =============================================================================

@app.get("/b2bwave/test")
def test_b2bwave():
    """Test B2BWave API connection"""
    if not B2BWAVE_URL or not B2BWAVE_USERNAME or not B2BWAVE_API_KEY:
        return {
            "status": "error",
            "message": "B2BWave API not configured",
            "config": {
                "url_set": bool(B2BWAVE_URL),
                "username_set": bool(B2BWAVE_USERNAME),
                "api_key_set": bool(B2BWAVE_API_KEY)
            }
        }
    
    try:
        # Try to fetch one order to test connection
        data = b2bwave_api_request("orders", {"submitted_at_gteq": "2024-01-01"})
        order_count = len(data) if isinstance(data, list) else 1
        return {
            "status": "ok",
            "message": f"B2BWave API connected. Found {order_count} orders.",
            "url": B2BWAVE_URL
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

@app.post("/b2bwave/sync")
def sync_from_b2bwave(days_back: int = 14):
    """
    Sync orders from B2BWave API.
    Default: last 14 days of orders.
    """
    # Calculate date range
    since_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    
    try:
        data = b2bwave_api_request("orders", {"submitted_at_gteq": since_date})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"B2BWave API error: {str(e)}")
    
    # Handle response format
    orders_list = data if isinstance(data, list) else [data]
    
    synced = []
    errors = []
    
    for order_data in orders_list:
        try:
            result = sync_order_from_b2bwave(order_data)
            synced.append(result)
        except Exception as e:
            order_id = order_data.get('order', order_data).get('id', 'unknown')
            errors.append({"order_id": order_id, "error": str(e)})
    
    return {
        "status": "ok",
        "synced_count": len(synced),
        "error_count": len(errors),
        "synced_orders": synced,
        "errors": errors if errors else None
    }

@app.get("/b2bwave/order/{order_id}")
def get_b2bwave_order(order_id: str):
    """Fetch a specific order from B2BWave and sync it"""
    try:
        data = b2bwave_api_request("orders", {"id_eq": order_id})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"B2BWave API error: {str(e)}")
    
    if not data:
        raise HTTPException(status_code=404, detail="Order not found in B2BWave")
    
    # Handle response format
    order_data = data[0] if isinstance(data, list) else data
    
    result = sync_order_from_b2bwave(order_data)
    
    return {
        "status": "ok",
        "message": f"Order {order_id} synced from B2BWave",
        "order": result
    }

# =============================================================================
# EMAIL PARSING ENDPOINT
# =============================================================================

@app.post("/parse-email", response_model=ParseEmailResponse)
def parse_email(request: ParseEmailRequest):
    """
    Parse a B2BWave order email and create/update the order.
    This is the main entry point - Google Sheet just sends raw email here.
    """
    parsed = parse_b2bwave_email(request.email_body, request.email_subject)
    
    if not parsed['order_id']:
        return ParseEmailResponse(
            status="error",
            message="Could not extract order ID from email"
        )
    
    # Get warehouses from SKU prefixes
    warehouses = get_warehouses_for_skus(parsed.get('sku_prefixes', []))
    
    # Create or update order
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check if order exists
            cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (parsed['order_id'],))
            exists = cur.fetchone()
            
            if exists:
                # Update existing order (don't overwrite checkpoints)
                cur.execute("""
                    UPDATE orders SET
                        customer_name = COALESCE(%s, customer_name),
                        company_name = COALESCE(%s, company_name),
                        email = COALESCE(%s, email),
                        phone = COALESCE(%s, phone),
                        street = COALESCE(%s, street),
                        city = COALESCE(%s, city),
                        state = COALESCE(%s, state),
                        zip_code = COALESCE(%s, zip_code),
                        order_total = COALESCE(%s, order_total),
                        comments = COALESCE(%s, comments),
                        warehouse_1 = COALESCE(%s, warehouse_1),
                        warehouse_2 = COALESCE(%s, warehouse_2),
                        updated_at = NOW()
                    WHERE order_id = %s
                """, (
                    parsed['customer_name'],
                    parsed['company_name'],
                    parsed['email'],
                    parsed['phone'],
                    parsed['street'],
                    parsed['city'],
                    parsed['state'],
                    parsed['zip_code'],
                    parsed['order_total'],
                    parsed['comments'],
                    warehouses[0] if len(warehouses) > 0 else None,
                    warehouses[1] if len(warehouses) > 1 else None,
                    parsed['order_id']
                ))
                
                return ParseEmailResponse(
                    status="updated",
                    order_id=parsed['order_id'],
                    parsed_data=parsed,
                    warehouses=warehouses,
                    message="Order updated"
                )
            else:
                # Create new order
                order_date = request.email_date or datetime.now(timezone.utc).isoformat()
                
                # Check if trusted customer
                trusted = is_trusted_customer(conn, parsed['customer_name'] or '', parsed['company_name'] or '')
                
                cur.execute("""
                    INSERT INTO orders (
                        order_id, customer_name, company_name, email, phone,
                        street, city, state, zip_code,
                        order_date, order_total, comments,
                        warehouse_1, warehouse_2, email_thread_id,
                        is_trusted_customer
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    parsed['order_id'],
                    parsed['customer_name'],
                    parsed['company_name'],
                    parsed['email'],
                    parsed['phone'],
                    parsed['street'],
                    parsed['city'],
                    parsed['state'],
                    parsed['zip_code'],
                    order_date,
                    parsed['order_total'],
                    parsed['comments'],
                    warehouses[0] if len(warehouses) > 0 else None,
                    warehouses[1] if len(warehouses) > 1 else None,
                    request.email_thread_id,
                    trusted
                ))
                
                # Log event
                cur.execute("""
                    INSERT INTO order_events (order_id, event_type, event_data, source)
                    VALUES (%s, 'order_created', %s, 'email_parse')
                """, (parsed['order_id'], json.dumps(parsed)))
                
                return ParseEmailResponse(
                    status="created",
                    order_id=parsed['order_id'],
                    parsed_data=parsed,
                    warehouses=warehouses,
                    message="Order created"
                )

# =============================================================================
# PAYMENT DETECTION ENDPOINTS
# =============================================================================

@app.post("/detect-payment-link")
def detect_payment_link(order_id: str, email_body: str):
    """Detect if email contains Square payment link"""
    if 'square.link' in email_body.lower():
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orders SET 
                        payment_link_sent = TRUE,
                        payment_link_sent_at = NOW(),
                        updated_at = NOW()
                    WHERE order_id = %s AND NOT payment_link_sent
                """, (order_id,))
                
                if cur.rowcount > 0:
                    cur.execute("""
                        INSERT INTO order_events (order_id, event_type, source)
                        VALUES (%s, 'payment_link_sent', 'email_detection')
                    """, (order_id,))
                    return {"status": "ok", "updated": True}
        
        return {"status": "ok", "updated": False, "message": "Already marked"}
    
    return {"status": "ok", "updated": False, "message": "No square link found"}

@app.post("/detect-payment-received")
def detect_payment_received(email_subject: str, email_body: str):
    """
    Detect Square payment notification.
    Subject format: "$4,913.99 payment received from Dylan Gentry"
    """
    # Extract amount from subject
    amount_match = re.search(r'\$([\d,]+\.?\d*)\s+payment received', email_subject, re.IGNORECASE)
    if not amount_match:
        return {"status": "ok", "updated": False, "message": "Not a payment notification"}
    
    payment_amount = float(amount_match.group(1).replace(',', ''))
    
    # Extract customer name
    name_match = re.search(r'payment received from (.+)$', email_subject, re.IGNORECASE)
    customer_name = name_match.group(1).strip() if name_match else None
    
    # Try to match to an order
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # First try exact amount match on unpaid orders
            cur.execute("""
                SELECT order_id, order_total, customer_name 
                FROM orders 
                WHERE NOT payment_received 
                AND order_total IS NOT NULL
                ORDER BY order_date DESC
                LIMIT 50
            """)
            orders = cur.fetchall()
            
            matched_order = None
            
            # Try to match by amount (payment should be >= order total)
            for order in orders:
                if order['order_total'] and payment_amount >= float(order['order_total']):
                    # Could be this order - check name similarity if we have it
                    if customer_name and order['customer_name']:
                        # Simple check - first name match
                        pay_first = customer_name.split()[0].lower()
                        order_first = order['customer_name'].split()[0].lower()
                        if pay_first == order_first:
                            matched_order = order
                            break
                    elif not matched_order:
                        # Take first amount match if no name match
                        matched_order = order
            
            if matched_order:
                order_total = float(matched_order['order_total']) if matched_order['order_total'] else 0
                shipping_cost = payment_amount - order_total if order_total else None
                
                cur.execute("""
                    UPDATE orders SET 
                        payment_received = TRUE,
                        payment_received_at = NOW(),
                        payment_amount = %s,
                        shipping_cost = %s,
                        updated_at = NOW()
                    WHERE order_id = %s
                """, (payment_amount, shipping_cost, matched_order['order_id']))
                
                cur.execute("""
                    INSERT INTO order_events (order_id, event_type, event_data, source)
                    VALUES (%s, 'payment_received', %s, 'square_notification')
                """, (matched_order['order_id'], json.dumps({
                    'payment_amount': payment_amount,
                    'shipping_cost': shipping_cost,
                    'customer_name': customer_name
                })))
                
                return {
                    "status": "ok",
                    "updated": True,
                    "order_id": matched_order['order_id'],
                    "payment_amount": payment_amount,
                    "shipping_cost": shipping_cost
                }
            
            return {
                "status": "ok",
                "updated": False,
                "message": "Could not match payment to order",
                "payment_amount": payment_amount,
                "customer_name": customer_name
            }

# =============================================================================
# ORDER CRUD
# =============================================================================

@app.get("/orders")
def list_orders(
    status: Optional[str] = None,
    include_complete: bool = False,
    limit: int = 200
):
    """List orders with optional filters"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT o.*, s.current_status, s.days_open
                FROM orders o
                JOIN order_status s ON o.order_id = s.order_id
                WHERE 1=1
            """
            params = []
            
            if not include_complete:
                query += " AND NOT o.is_complete"
            
            if status:
                query += " AND s.current_status = %s"
                params.append(status)
            
            query += " ORDER BY o.order_date DESC LIMIT %s"
            params.append(limit)
            
            cur.execute(query, params)
            orders = cur.fetchall()
            
            # Convert decimals to floats for JSON
            for order in orders:
                for key in ['order_total', 'payment_amount', 'shipping_cost']:
                    if order.get(key):
                        order[key] = float(order[key])
            
            return {"status": "ok", "count": len(orders), "orders": orders}

@app.get("/orders/{order_id}")
def get_order(order_id: str):
    """Get single order details"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT o.*, s.current_status, s.days_open
                FROM orders o
                JOIN order_status s ON o.order_id = s.order_id
                WHERE o.order_id = %s
            """, (order_id,))
            order = cur.fetchone()
            
            if not order:
                raise HTTPException(status_code=404, detail="Order not found")
            
            # Convert decimals
            for key in ['order_total', 'payment_amount', 'shipping_cost']:
                if order.get(key):
                    order[key] = float(order[key])
            
            return {"status": "ok", "order": order}

@app.patch("/orders/{order_id}")
def update_order(order_id: str, update: OrderUpdate):
    """Update order fields"""
    with get_db() as conn:
        with conn.cursor() as cur:
            # Build dynamic update
            fields = []
            values = []
            
            for field, value in update.dict(exclude_unset=True).items():
                if value is not None:
                    fields.append(f"{field} = %s")
                    values.append(value)
            
            if not fields:
                raise HTTPException(status_code=400, detail="No fields to update")
            
            fields.append("updated_at = NOW()")
            values.append(order_id)
            
            query = f"UPDATE orders SET {', '.join(fields)} WHERE order_id = %s"
            cur.execute(query, values)
            
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Order not found")
            
            return {"status": "ok", "message": "Order updated"}

@app.patch("/orders/{order_id}/checkpoint")
def update_checkpoint(order_id: str, update: CheckpointUpdate):
    """Update order checkpoint"""
    valid_checkpoints = [
        'payment_link_sent', 'payment_received', 'sent_to_warehouse',
        'warehouse_confirmed', 'bol_sent', 'is_complete'
    ]
    
    if update.checkpoint not in valid_checkpoints:
        raise HTTPException(status_code=400, detail=f"Invalid checkpoint. Must be one of: {valid_checkpoints}")
    
    with get_db() as conn:
        with conn.cursor() as cur:
            timestamp_field = f"{update.checkpoint}_at" if update.checkpoint != 'is_complete' else 'completed_at'
            
            # Build update query
            set_parts = [f"{update.checkpoint} = TRUE", f"{timestamp_field} = NOW()", "updated_at = NOW()"]
            params = []
            
            # Handle payment amount if provided
            if update.checkpoint == 'payment_received' and update.payment_amount:
                set_parts.append("payment_amount = %s")
                params.append(update.payment_amount)
                
                # Calculate shipping cost
                cur.execute("SELECT order_total FROM orders WHERE order_id = %s", (order_id,))
                row = cur.fetchone()
                if row and row[0]:
                    shipping = update.payment_amount - float(row[0])
                    set_parts.append("shipping_cost = %s")
                    params.append(shipping)
            
            params.append(order_id)
            
            query = f"UPDATE orders SET {', '.join(set_parts)} WHERE order_id = %s"
            cur.execute(query, params)
            
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Order not found")
            
            # Log event
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, %s, %s, %s)
            """, (
                order_id,
                update.checkpoint,
                json.dumps({'payment_amount': update.payment_amount} if update.payment_amount else {}),
                update.source
            ))
            
            return {"status": "ok", "checkpoint": update.checkpoint}

# =============================================================================
# WAREHOUSE MAPPING
# =============================================================================

@app.get("/warehouse-mapping")
def get_warehouse_mapping():
    """Get all warehouse mappings"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM warehouse_mapping ORDER BY sku_prefix")
            mappings = cur.fetchall()
            return {"status": "ok", "mappings": mappings}

@app.post("/warehouse-mapping")
def add_warehouse_mapping(mapping: WarehouseMappingUpdate):
    """Add or update warehouse mapping"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO warehouse_mapping (sku_prefix, warehouse_name, warehouse_code)
                VALUES (%s, %s, %s)
                ON CONFLICT (sku_prefix) DO UPDATE SET
                    warehouse_name = EXCLUDED.warehouse_name,
                    warehouse_code = EXCLUDED.warehouse_code
            """, (mapping.sku_prefix.upper(), mapping.warehouse_name, mapping.warehouse_code))
            
            return {"status": "ok", "message": "Mapping saved"}

# =============================================================================
# STATUS SUMMARY
# =============================================================================

@app.get("/orders/status/summary")
def status_summary():
    """Get count of orders by status"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT current_status, COUNT(*) as count
                FROM order_status
                GROUP BY current_status
                ORDER BY 
                    CASE current_status
                        WHEN 'needs_payment_link' THEN 1
                        WHEN 'awaiting_payment' THEN 2
                        WHEN 'needs_warehouse_order' THEN 3
                        WHEN 'awaiting_warehouse' THEN 4
                        WHEN 'needs_bol' THEN 5
                        WHEN 'awaiting_shipment' THEN 6
                        WHEN 'complete' THEN 7
                    END
            """)
            summary = cur.fetchall()
            return {"status": "ok", "summary": summary}

@app.get("/orders/{order_id}/events")
def get_order_events(order_id: str):
    """Get event history for an order"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM order_events 
                WHERE order_id = %s 
                ORDER BY created_at DESC
            """, (order_id,))
            events = cur.fetchall()
            return {"status": "ok", "events": events}

# =============================================================================
# TRUSTED CUSTOMERS
# =============================================================================

@app.get("/trusted-customers")
def list_trusted_customers():
    """List all trusted customers"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM trusted_customers ORDER BY customer_name")
            customers = cur.fetchall()
            return {"status": "ok", "customers": customers}

@app.post("/trusted-customers")
def add_trusted_customer(customer_name: str, company_name: Optional[str] = None, notes: Optional[str] = None):
    """Add a trusted customer"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trusted_customers (customer_name, company_name, notes)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (customer_name, company_name, notes))
            new_id = cur.fetchone()[0]
            return {"status": "ok", "id": new_id}

@app.delete("/trusted-customers/{customer_id}")
def remove_trusted_customer(customer_id: int):
    """Remove a trusted customer"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM trusted_customers WHERE id = %s", (customer_id,))
            return {"status": "ok"}

def is_trusted_customer(conn, customer_name: str, company_name: str = None) -> bool:
    """Check if customer is in trusted list"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM trusted_customers 
            WHERE LOWER(customer_name) = LOWER(%s)
            OR (company_name IS NOT NULL AND LOWER(company_name) = LOWER(%s))
        """, (customer_name, company_name or ''))
        return cur.fetchone() is not None

# =============================================================================
# ALERTS
# =============================================================================

@app.get("/alerts")
def list_alerts(include_resolved: bool = False):
    """List order alerts"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT a.*, o.customer_name, o.company_name, o.order_total
                FROM order_alerts a
                JOIN orders o ON a.order_id = o.order_id
            """
            if not include_resolved:
                query += " WHERE NOT a.is_resolved"
            query += " ORDER BY a.created_at DESC"
            
            cur.execute(query)
            alerts = cur.fetchall()
            return {"status": "ok", "alerts": alerts}

@app.post("/alerts")
def create_alert(order_id: str, alert_type: str, alert_message: str):
    """Create an alert for an order"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO order_alerts (order_id, alert_type, alert_message)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (order_id, alert_type, alert_message))
            new_id = cur.fetchone()[0]
            return {"status": "ok", "id": new_id}

@app.patch("/alerts/{alert_id}/resolve")
def resolve_alert(alert_id: int):
    """Resolve an alert"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE order_alerts 
                SET is_resolved = TRUE, resolved_at = NOW()
                WHERE id = %s
            """, (alert_id,))
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
                    f"Trusted customer {order['customer_name']} - shipped but unpaid for 1+ day. Total: ${order['order_total']}"
                ))
                alerts_created += 1
    
    return {"status": "ok", "alerts_created": alerts_created}
