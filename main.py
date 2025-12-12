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

# Anthropic API Config
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()

# Auto-sync config
AUTO_SYNC_INTERVAL_MINUTES = 15
AUTO_SYNC_DAYS_BACK = 7

# Supplier contact info
SUPPLIER_INFO = {
    'LI': {
        'name': 'Li',
        'address': '561 Keuka Rd, Interlachen FL 32148',
        'contact': 'Li Yang (615) 410-6775',
        'email': 'cabinetrydistribution@gmail.com'
    },
    'DL': {
        'name': 'DL Cabinetry',
        'address': '8145 Baymeadows Way W, Jacksonville FL 32256',
        'contact': 'Lily Chen (904) 723-1061',
        'email': 'ecomm@dlcabinetry.com'
    },
    'ROC': {
        'name': 'ROC Cabinetry',
        'address': '505 Best Friend Court Suite 580, Norcross GA 30071',
        'contact': 'Franklin Velasquez (770) 847-8222',
        'email': 'weborders01@roccabinetry.com'
    },
    'Go Bravura': {
        'name': 'Go Bravura',
        'address': '14200 Hollister Street Suite 200, Houston TX 77066',
        'contact': 'Vincent Pan (832) 756-2768',
        'email': 'vpan@gobravura.com'
    },
    'Love-Milestone': {
        'name': 'Love-Milestone',
        'address': '10963 Florida Crown Dr STE 100, Orlando FL 32824',
        'contact': 'Ireen',
        'email': 'lovetoucheskitchen@gmail.com'
    },
    'Cabinet & Stone': {
        'name': 'Cabinet & Stone',
        'address': '1760 Stebbins Dr, Houston TX 77043',
        'contact': 'Amy Cao (281) 833-0980',
        'email': 'amy@cabinetstonellc.com'
    },
    'DuraStone': {
        'name': 'DuraStone',
        'address': '9815 North Fwy, Houston TX 77037',
        'contact': 'Ranjith Venugopalan / Rachel Guo (832) 228-7866',
        'email': 'ranji@durastoneusa.com'
    },
    'L&C Cabinetry': {
        'name': 'L&C Cabinetry',
        'address': '2028 Virginia Beach Blvd, Virginia Beach VA 23454',
        'contact': 'Rey Allison (757) 917-5619',
        'email': 'lnccabinetryvab@gmail.com'
    },
    'GHI': {
        'name': 'GHI',
        'address': '1807 48th Ave E Unit 110, Palmetto FL 34221',
        'contact': 'Kathryn Belfiore (941) 479-8070',
        'email': 'kbelfiore@ghicabinets.com'
    },
    'Linda': {
        'name': 'Linda / Dealer Cabinetry',
        'address': '202 West Georgia Ave, Bremen GA 30110',
        'contact': 'Linda Yang (678) 821-3505',
        'email': 'linda@dealercabinetry.com'
    }
}

# Warehouse ZIP codes for shipping quotes
WAREHOUSE_ZIPS = {
    'LI': '32148',
    'DL': '32256',
    'ROC': '30071',
    'Go Bravura': '77066',
    'Love-Milestone': '32824',
    'Cabinet & Stone': '77043',
    'DuraStone': '77037',
    'L&C Cabinetry': '23454',
    'GHI': '34221',
    'Linda': '30110'
}

# Keywords that indicate oversized shipment (need dimensions on RL quote)
OVERSIZED_KEYWORDS = ['OVEN', 'PANTRY', '96"', '96*', 'X96', '96X', '96H', '96 H']

app = FastAPI(title="CFC Order Workflow", version="5.8.4")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global for tracking last sync
last_auto_sync = None
auto_sync_running = False

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
    sku_prefix VARCHAR(100) PRIMARY KEY,
    warehouse_name VARCHAR(100) NOT NULL,
    warehouse_code VARCHAR(20),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Default warehouse mappings (from Supplier_Map)
INSERT INTO warehouse_mapping (sku_prefix, warehouse_name, warehouse_code) VALUES
-- LI
('GSP', 'LI', 'LI'),
('WSP', 'LI', 'LI'),
-- DL
('CS', 'DL', 'DL'),
('BNG', 'DL', 'DL'),
('UFS', 'DL', 'DL'),
('EBK', 'DL', 'DL'),
-- ROC
('EGD', 'ROC', 'ROC'),
('EMB', 'ROC', 'ROC'),
('BC', 'ROC', 'ROC'),
('DCH', 'ROC', 'ROC'),
('NJGR', 'ROC', 'ROC'),
('DCT', 'ROC', 'ROC'),
('DCW', 'ROC', 'ROC'),
('EJG', 'ROC', 'ROC'),
('SNW', 'ROC', 'ROC'),
-- Go Bravura
('HGW', 'Go Bravura', 'GB'),
('EMW', 'Go Bravura', 'GB'),
('EGG', 'Go Bravura', 'GB'),
('URC', 'Go Bravura', 'GB'),
('WWW', 'Go Bravura', 'GB'),
('NDG', 'Go Bravura', 'GB'),
('NCC', 'Go Bravura', 'GB'),
('NBW', 'Go Bravura', 'GB'),
('URW', 'Go Bravura', 'GB'),
('BX', 'Go Bravura', 'GB'),
-- Love-Milestone
('EDG', 'Love-Milestone', 'LOVE'),
('EWD', 'Love-Milestone', 'LOVE'),
('RND', 'Love-Milestone', 'LOVE'),
('RMW', 'Love-Milestone', 'LOVE'),
('NBLK', 'Love-Milestone', 'LOVE'),
('HSS', 'Love-Milestone', 'LOVE'),
('LGS', 'Love-Milestone', 'LOVE'),
('LGSS', 'Love-Milestone', 'LOVE'),
('SWO', 'Love-Milestone', 'LOVE'),
('EWT', 'Love-Milestone', 'LOVE'),
('DG', 'Love-Milestone', 'LOVE'),
('EWSCS', 'Love-Milestone', 'LOVE'),
('BGR', 'Love-Milestone', 'LOVE'),
('BESCS', 'Love-Milestone', 'LOVE'),
-- Cabinet & Stone
('CAWN', 'Cabinet & Stone', 'CS'),
('BSN', 'Cabinet & Stone', 'CS'),
('WOCS', 'Cabinet & Stone', 'CS'),
('ESCS', 'Cabinet & Stone', 'CS'),
('SIV', 'Cabinet & Stone', 'CS'),
('SAVNG', 'Cabinet & Stone', 'CS'),
('MSCS', 'Cabinet & Stone', 'CS'),
('SGCS', 'Cabinet & Stone', 'CS'),
-- DuraStone
('CMEN', 'DuraStone', 'DS'),
('NSLS', 'DuraStone', 'DS'),
('NBDS', 'DuraStone', 'DS'),
('NSN', 'DuraStone', 'DS'),
-- L&C Cabinetry
('EDD', 'L&C Cabinetry', 'LC'),
('RBLS', 'L&C Cabinetry', 'LC'),
('SWNG', 'L&C Cabinetry', 'LC'),
('MGLS', 'L&C Cabinetry', 'LC'),
('BG', 'L&C Cabinetry', 'LC'),
('SHLS', 'L&C Cabinetry', 'LC'),
-- GHI
('NOR', 'GHI', 'GHI'),
('SNS', 'GHI', 'GHI'),
('AKS', 'GHI', 'GHI'),
('APW', 'GHI', 'GHI'),
('GRSH', 'GHI', 'GHI')
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

CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    
    -- Customer info
    customer_name VARCHAR(255),
    company_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    
    -- Address
    street VARCHAR(255),
    street2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    
    -- Order details
    order_date TIMESTAMP WITH TIME ZONE,
    order_total DECIMAL(10,2),
    total_weight DECIMAL(10,2),
    comments TEXT,
    
    -- Warehouses (extracted from SKU prefixes, up to 4)
    warehouse_1 VARCHAR(100),
    warehouse_2 VARCHAR(100),
    warehouse_3 VARCHAR(100),
    warehouse_4 VARCHAR(100),
    
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
    ai_summary TEXT,
    ai_summary_updated_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Alerts/flags table (after orders so foreign key works)
CREATE TABLE order_alerts (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    alert_type VARCHAR(50) NOT NULL,
    alert_message TEXT,
    is_resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_alerts_order ON order_alerts(order_id);
CREATE INDEX idx_alerts_unresolved ON order_alerts(is_resolved) WHERE NOT is_resolved;

CREATE TABLE order_line_items (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    sku VARCHAR(100),
    sku_prefix VARCHAR(100),
    product_name TEXT,
    price DECIMAL(10,2),
    quantity INTEGER,
    line_total DECIMAL(10,2),
    warehouse VARCHAR(100)
);

CREATE TABLE order_events (
    event_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    source VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Email snippets for AI summary
CREATE TABLE order_email_snippets (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    email_from VARCHAR(255),
    email_to VARCHAR(255),
    email_subject VARCHAR(500),
    email_snippet TEXT,
    email_date TIMESTAMP WITH TIME ZONE,
    snippet_type VARCHAR(50),  -- 'customer', 'supplier', 'internal', 'payment'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Shipments table - each warehouse in an order is a separate shipment
CREATE TABLE order_shipments (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id) ON DELETE CASCADE,
    shipment_id VARCHAR(50) NOT NULL UNIQUE,  -- e.g., "5307-Li"
    warehouse VARCHAR(100) NOT NULL,
    status VARCHAR(50) DEFAULT 'needs_order',  -- needs_order, at_warehouse, needs_bol, ready_ship, shipped, delivered
    tracking VARCHAR(100),
    pro_number VARCHAR(50),
    bol_sent BOOLEAN DEFAULT FALSE,
    bol_sent_at TIMESTAMP WITH TIME ZONE,
    weight DECIMAL(10,2),
    ship_method VARCHAR(50),  -- LTL, Pirateship, Pickup, BoxTruck, LiDelivery
    sent_to_warehouse_at TIMESTAMP WITH TIME ZONE,
    warehouse_confirmed_at TIMESTAMP WITH TIME ZONE,
    shipped_at TIMESTAMP WITH TIME ZONE,
    delivered_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_orders_complete ON orders(is_complete);
CREATE INDEX idx_orders_date ON orders(order_date DESC);
CREATE INDEX idx_line_items_order ON order_line_items(order_id);
CREATE INDEX idx_events_order ON order_events(order_id);
CREATE INDEX idx_email_snippets_order ON order_email_snippets(order_id);
CREATE INDEX idx_shipments_order ON order_shipments(order_id);
CREATE INDEX idx_shipments_id ON order_shipments(shipment_id);

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
            upper_prefixes = [p.upper() for p in sku_prefixes]
            cur.execute(f"""
                SELECT DISTINCT warehouse_name
                FROM warehouse_mapping
                WHERE UPPER(sku_prefix) IN ({placeholders})
            """, upper_prefixes)
            warehouses = [row['warehouse_name'] for row in cur.fetchall()]
    
    return warehouses

# =============================================================================
# AI SUMMARY (ANTHROPIC CLAUDE API)
# =============================================================================

def call_anthropic_api(prompt: str, max_tokens: int = 1024) -> str:
    """Call Anthropic Claude API to generate summary"""
    if not ANTHROPIC_API_KEY:
        return "AI Summary not available - API key not configured"
    
    url = "https://api.anthropic.com/v1/messages"
    
    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "messages": [
            {"role": "user", "content": prompt}
        ]
    }
    
    data = json.dumps(payload).encode('utf-8')
    
    req = urllib.request.Request(url, data=data, method='POST')
    req.add_header("Content-Type", "application/json")
    req.add_header("x-api-key", ANTHROPIC_API_KEY)
    req.add_header("anthropic-version", "2023-06-01")
    
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            result = json.loads(response.read().decode())
            if result.get('content') and len(result['content']) > 0:
                return result['content'][0].get('text', '')
            return "No summary generated"
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else str(e)
        print(f"Anthropic API Error: {e.code} - {error_body}")
        return f"AI Summary error: {e.code}"
    except Exception as e:
        print(f"Anthropic API Exception: {e}")
        return f"AI Summary error: {str(e)}"

def generate_order_summary(order_id: str) -> str:
    """Generate AI summary for an order based on all available data"""
    
    # Gather all order data
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get order details
            cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
            order = cur.fetchone()
            
            if not order:
                return "Order not found"
            
            # Get email snippets
            cur.execute("""
                SELECT email_from, email_subject, email_snippet, email_date, snippet_type 
                FROM order_email_snippets 
                WHERE order_id = %s 
                ORDER BY email_date DESC
                LIMIT 20
            """, (order_id,))
            snippets = cur.fetchall()
            
            # Get events
            cur.execute("""
                SELECT event_type, event_data, created_at 
                FROM order_events 
                WHERE order_id = %s 
                ORDER BY created_at DESC
                LIMIT 10
            """, (order_id,))
            events = cur.fetchall()
    
    # Build context for AI
    context_parts = []
    
    # Order info
    context_parts.append(f"ORDER #{order_id}")
    context_parts.append(f"Customer: {order.get('company_name') or order.get('customer_name')}")
    context_parts.append(f"Order Total: ${order.get('order_total', 0)}")
    context_parts.append(f"Payment Received: {'Yes' if order.get('payment_received') else 'No'}")
    if order.get('tracking'):
        context_parts.append(f"Tracking: {order.get('tracking')}")
    if order.get('pro_number'):
        context_parts.append(f"PRO Number: {order.get('pro_number')}")
    if order.get('comments'):
        context_parts.append(f"Customer Comments: {order.get('comments')}")
    if order.get('notes'):
        context_parts.append(f"Internal Notes: {order.get('notes')}")
    
    # Warehouses
    warehouses = [order.get(f'warehouse_{i}') for i in range(1, 5) if order.get(f'warehouse_{i}')]
    if warehouses:
        context_parts.append(f"Warehouses: {', '.join(warehouses)}")
    
    # Email snippets
    if snippets:
        context_parts.append("\nEMAIL COMMUNICATIONS:")
        for s in snippets:
            date_str = s['email_date'].strftime('%m/%d') if s.get('email_date') else ''
            context_parts.append(f"- [{date_str}] From: {s.get('email_from', 'Unknown')}")
            context_parts.append(f"  Subject: {s.get('email_subject', '')}")
            if s.get('email_snippet'):
                context_parts.append(f"  {s['email_snippet'][:300]}")
    
    # Events (filter out sync noise)
    if events:
        important_events = [e for e in events if e.get('event_type') not in ('b2bwave_sync', 'auto_sync', 'status_check')]
        if important_events:
            context_parts.append("\nORDER EVENTS:")
            for e in important_events:
                date_str = e['created_at'].strftime('%m/%d %H:%M') if e.get('created_at') else ''
                context_parts.append(f"- [{date_str}] {e.get('event_type')}")
    
    context = "\n".join(context_parts)    

    # Create prompt
    prompt = f"""Write a brief order status summary.

Rules:
- Use simple bullet points (• symbol)
- NO headers, NO bold text, NO markdown formatting
- Only include notable information (special requests, issues, credits)
- Skip obvious info (order total, warehouse names) unless relevant to an issue
- 2-4 bullets maximum
- Plain conversational language
- Always end with "Next action:" if payment pending or action needed

Example good output:
- Customer will pay by check and pick up (no shipping needed)
- Next action: Wait for customer pickup with payment

Example bad output (too verbose):
- **Order Status:** Payment pending
- **Warehouse:** DL warehouse assigned
- **System Activity:** Multiple syncs detected

{context}"""    
    return call_anthropic_api(prompt)

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
    # Keep them separate for shipping integrations (RL Carriers, Pirateship)
    street = order.get('address', '')
    street2 = order.get('address2', '')  # Suite/Unit - kept separate
    city = order.get('city', '')
    state = order.get('province', '')  # B2BWave calls it 'province'
    zip_code = order.get('postal_code', '')
    
    # Comments
    comments = order.get('comments_customer', '')
    
    # Totals
    order_total = float(order.get('gross_total', 0) or 0)
    total_weight = float(order.get('total_weight', 0) or 0)
    
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
    
    # Get warehouses for SKU prefixes (up to 4)
    warehouses = get_warehouses_for_skus(sku_prefixes)
    warehouse_1 = warehouses[0] if len(warehouses) > 0 else None
    warehouse_2 = warehouses[1] if len(warehouses) > 1 else None
    warehouse_3 = warehouses[2] if len(warehouses) > 2 else None
    warehouse_4 = warehouses[3] if len(warehouses) > 3 else None
    
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
                    street, street2, city, state, zip_code, phone, email,
                    comments, order_total, total_weight, warehouse_1, warehouse_2, warehouse_3, warehouse_4,
                    is_trusted_customer
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    company_name = EXCLUDED.company_name,
                    street = EXCLUDED.street,
                    street2 = EXCLUDED.street2,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    phone = EXCLUDED.phone,
                    email = EXCLUDED.email,
                    comments = EXCLUDED.comments,
                    order_total = EXCLUDED.order_total,
                    total_weight = EXCLUDED.total_weight,
                    warehouse_1 = COALESCE(orders.warehouse_1, EXCLUDED.warehouse_1),
                    warehouse_2 = COALESCE(orders.warehouse_2, EXCLUDED.warehouse_2),
                    warehouse_3 = COALESCE(orders.warehouse_3, EXCLUDED.warehouse_3),
                    warehouse_4 = COALESCE(orders.warehouse_4, EXCLUDED.warehouse_4),
                    is_trusted_customer = EXCLUDED.is_trusted_customer,
                    updated_at = NOW()
                RETURNING order_id
            """, (
                order_id, order_date, customer_name, company_name,
                street, street2, city, state, zip_code, phone, email,
                comments, order_total, total_weight, warehouse_1, warehouse_2, warehouse_3, warehouse_4,
                is_trusted
            ))
            result = cur.fetchone()
            
            # Delete existing line items and re-insert
            cur.execute("DELETE FROM order_line_items WHERE order_id = %s", (order_id,))
            
            # Insert line items with warehouse info
            for item in line_items:
                sku = item.get('sku', '')
                prefix = sku.split('-')[0] if '-' in sku else ''
                # Look up warehouse for this item
                item_warehouse = None
                if prefix:
                    cur.execute("SELECT warehouse_name FROM warehouse_mapping WHERE UPPER(sku_prefix) = UPPER(%s)", (prefix,))
                    wh_row = cur.fetchone()
                    if wh_row:
                        item_warehouse = wh_row['warehouse_name']
                
                cur.execute("""
                    INSERT INTO order_line_items (order_id, sku, sku_prefix, product_name, quantity, price, warehouse)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (order_id, sku, prefix, item.get('product_name'), item.get('quantity'), item.get('price'), item_warehouse))
            
            # Log sync event
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, 'b2bwave_sync', %s, 'api')
            """, (order_id, json.dumps({'sku_prefixes': sku_prefixes})))
            
            # Auto-create shipments for each warehouse
            warehouses_list = [w for w in [warehouse_1, warehouse_2, warehouse_3, warehouse_4] if w]
            for wh in warehouses_list:
                # Create shipment_id like "5307-Li"
                # Clean warehouse name for ID (remove spaces, special chars)
                wh_short = wh.replace(' & ', '-').replace(' ', '-')
                shipment_id = f"{order_id}-{wh_short}"
                
                # Check if shipment already exists
                cur.execute("SELECT id FROM order_shipments WHERE shipment_id = %s", (shipment_id,))
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO order_shipments (order_id, shipment_id, warehouse, status)
                        VALUES (%s, %s, %s, 'needs_order')
                    """, (order_id, shipment_id, wh))
    
    return {
        'order_id': order_id,
        'customer_name': customer_name,
        'company_name': company_name,
        'city': city,
        'state': state,
        'zip_code': zip_code,
        'warehouse_1': warehouse_1,
        'warehouse_2': warehouse_2,
        'warehouse_3': warehouse_3,
        'warehouse_4': warehouse_4,
        'line_items_count': len(line_items)
    }

# =============================================================================
# AUTO-SYNC SCHEDULER
# =============================================================================

def run_auto_sync():
    """Background sync from B2BWave - runs every 15 minutes"""
    global last_auto_sync, auto_sync_running
    
    while True:
        time.sleep(AUTO_SYNC_INTERVAL_MINUTES * 60)  # Wait 15 min
        
        if not B2BWAVE_URL or not B2BWAVE_USERNAME or not B2BWAVE_API_KEY:
            print("[AUTO-SYNC] B2BWave not configured, skipping")
            continue
        
        try:
            auto_sync_running = True
            print(f"[AUTO-SYNC] Starting sync at {datetime.now()}")
            
            # Calculate date range
            since_date = (datetime.now(timezone.utc) - timedelta(days=AUTO_SYNC_DAYS_BACK)).strftime("%Y-%m-%d")
            
            # Fetch from B2BWave
            data = b2bwave_api_request("orders", {"submitted_at_gteq": since_date})
            orders_list = data if isinstance(data, list) else [data]
            
            synced = 0
            for order_data in orders_list:
                try:
                    sync_order_from_b2bwave(order_data)
                    synced += 1
                except Exception as e:
                    print(f"[AUTO-SYNC] Error syncing order: {e}")
            
            last_auto_sync = datetime.now(timezone.utc)
            print(f"[AUTO-SYNC] Completed: {synced} orders synced")
            
        except Exception as e:
            print(f"[AUTO-SYNC] Error: {e}")
        finally:
            auto_sync_running = False

@app.on_event("startup")
def start_auto_sync():
    """Start background sync thread on app startup"""
    if B2BWAVE_URL and B2BWAVE_USERNAME and B2BWAVE_API_KEY:
        thread = threading.Thread(target=run_auto_sync, daemon=True)
        thread.start()
        print(f"[AUTO-SYNC] Started - will sync every {AUTO_SYNC_INTERVAL_MINUTES} minutes")
    else:
        print("[AUTO-SYNC] B2BWave not configured, auto-sync disabled")

# =============================================================================
# ROUTES
# =============================================================================

@app.get("/")
def root():
    return {
        "status": "ok", 
        "service": "CFC Order Workflow", 
        "version": "5.8.4",
        "auto_sync": {
            "enabled": bool(B2BWAVE_URL and B2BWAVE_USERNAME and B2BWAVE_API_KEY),
            "interval_minutes": AUTO_SYNC_INTERVAL_MINUTES,
            "last_sync": last_auto_sync.isoformat() if last_auto_sync else None,
            "running": auto_sync_running
        }
    }

@app.get("/health")
def health():
    return {"status": "ok", "version": "5.8.4"}

@app.post("/check-payment-alerts")

def check_payment_alerts():

# =============================================================================
# ENTRYPOINT (for local dev / Render)
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    import os

    port = int(os.environ.get("PORT", 8000))  # Render provides PORT env var
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port
    )

    """
    Check for trusted customers who shipped but haven't paid after 1 business day.
    Should be called periodically (e.g., daily at 9 AM).
    """
    alerts_created = 0    
    with get_db() as conn:


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