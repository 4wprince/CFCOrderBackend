"""
CFC Order Workflow Backend - v5.7.1
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

app = FastAPI(title="CFC Order Workflow", version="5.7.1")

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
    sku_prefix VARCHAR(20) PRIMARY KEY,
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
    order_id VARCHAR(20) PRIMARY KEY,
    
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
    order_id VARCHAR(20) REFERENCES orders(order_id) ON DELETE CASCADE,
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

-- Email snippets for AI summary
CREATE TABLE order_email_snippets (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(20) REFERENCES orders(order_id) ON DELETE CASCADE,
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
    order_id VARCHAR(20) REFERENCES orders(order_id) ON DELETE CASCADE,
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
            cur.execute(f"""
                SELECT DISTINCT warehouse_name 
                FROM warehouse_mapping 
                WHERE sku_prefix IN ({placeholders})
            """, sku_prefixes)
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
    
    # Events
    if events:
        context_parts.append("\nORDER EVENTS:")
        for e in events:
            date_str = e['created_at'].strftime('%m/%d %H:%M') if e.get('created_at') else ''
            context_parts.append(f"- [{date_str}] {e.get('event_type')}")
    
    context = "\n".join(context_parts)
    
    # Create prompt
    prompt = f"""Summarize this order's status and any important information in bullet points. 
Focus on:
- Current status and what needs to happen next
- Any issues, questions, or special requests
- Payment status
- Shipping/tracking info if available
- Any notable communications

Keep it concise - max 5-7 bullet points.

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
                    comments, order_total, warehouse_1, warehouse_2, warehouse_3, warehouse_4,
                    is_trusted_customer
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                comments, order_total, warehouse_1, warehouse_2, warehouse_3, warehouse_4,
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
                    cur.execute("SELECT warehouse_name FROM warehouse_mapping WHERE sku_prefix = %s", (prefix,))
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
        "version": "5.7.1",
        "auto_sync": {
            "enabled": bool(B2BWAVE_URL and B2BWAVE_USERNAME and B2BWAVE_API_KEY),
            "interval_minutes": AUTO_SYNC_INTERVAL_MINUTES,
            "last_sync": last_auto_sync.isoformat() if last_auto_sync else None,
            "running": auto_sync_running
        }
    }

@app.get("/health")
def health():
    return {"status": "ok", "version": "5.7.1"}

@app.post("/create-shipments-table")
def create_shipments_table():
    """Create order_shipments table without resetting other tables"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_shipments (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(20) REFERENCES orders(order_id) ON DELETE CASCADE,
                    shipment_id VARCHAR(50) NOT NULL UNIQUE,
                    warehouse VARCHAR(100) NOT NULL,
                    status VARCHAR(50) DEFAULT 'needs_order',
                    tracking VARCHAR(100),
                    pro_number VARCHAR(50),
                    bol_sent BOOLEAN DEFAULT FALSE,
                    bol_sent_at TIMESTAMP WITH TIME ZONE,
                    weight DECIMAL(10,2),
                    ship_method VARCHAR(50),
                    sent_to_warehouse_at TIMESTAMP WITH TIME ZONE,
                    warehouse_confirmed_at TIMESTAMP WITH TIME ZONE,
                    shipped_at TIMESTAMP WITH TIME ZONE,
                    delivered_at TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_shipments_order ON order_shipments(order_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_shipments_id ON order_shipments(shipment_id)")
    return {"status": "ok", "message": "order_shipments table created"}

@app.post("/add-rl-fields")
def add_rl_shipping_fields():
    """Add RL Carriers shipping fields to order_shipments table"""
    with get_db() as conn:
        with conn.cursor() as cur:
            # Add RL quote fields
            fields_to_add = [
                ("origin_zip", "VARCHAR(10)"),
                ("rl_quote_number", "VARCHAR(50)"),
                ("rl_quote_price", "DECIMAL(10,2)"),
                ("rl_customer_price", "DECIMAL(10,2)"),
                ("rl_invoice_amount", "DECIMAL(10,2)"),
                ("has_oversized", "BOOLEAN DEFAULT FALSE")
            ]
            
            for field_name, field_type in fields_to_add:
                try:
                    cur.execute(f"ALTER TABLE order_shipments ADD COLUMN {field_name} {field_type}")
                except Exception as e:
                    # Column might already exist
                    conn.rollback()
                    pass
            
            conn.commit()
    return {"status": "ok", "message": "RL shipping fields added to order_shipments"}

@app.post("/init-db")
def init_db():
    """Initialize database schema (destructive!)"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    return {"status": "ok", "message": "Database schema initialized", "version": "5.6.1"}

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
    """List orders with optional filters, including shipments"""
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
            
            # Get shipments for all orders
            order_ids = [o['order_id'] for o in orders]
            shipments_by_order = {}
            if order_ids:
                cur.execute("""
                    SELECT * FROM order_shipments 
                    WHERE order_id = ANY(%s)
                    ORDER BY warehouse
                """, (order_ids,))
                for ship in cur.fetchall():
                    oid = ship['order_id']
                    if oid not in shipments_by_order:
                        shipments_by_order[oid] = []
                    # Convert decimals
                    if ship.get('weight'):
                        ship['weight'] = float(ship['weight'])
                    shipments_by_order[oid].append(dict(ship))
            
            # Convert decimals to floats for JSON and attach shipments
            for order in orders:
                for key in ['order_total', 'payment_amount', 'shipping_cost']:
                    if order.get(key):
                        order[key] = float(order[key])
                order['shipments'] = shipments_by_order.get(order['order_id'], [])
            
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

@app.post("/orders/{order_id}/generate-summary")
def generate_summary_endpoint(order_id: str, force: bool = False):
    """
    Generate AI summary for an order.
    If force=False and summary exists and is less than 1 hour old, returns cached.
    """
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check for existing recent summary
            cur.execute("""
                SELECT ai_summary, ai_summary_updated_at 
                FROM orders 
                WHERE order_id = %s
            """, (order_id,))
            order = cur.fetchone()
            
            if not order:
                raise HTTPException(status_code=404, detail="Order not found")
            
            # Return cached if recent and not forcing refresh
            if not force and order.get('ai_summary') and order.get('ai_summary_updated_at'):
                age = datetime.now(timezone.utc) - order['ai_summary_updated_at']
                if age < timedelta(hours=1):
                    return {
                        "status": "ok", 
                        "summary": order['ai_summary'],
                        "cached": True,
                        "updated_at": order['ai_summary_updated_at'].isoformat()
                    }
    
    # Generate new summary
    summary = generate_order_summary(order_id)
    
    # Save to database
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE orders 
                SET ai_summary = %s, ai_summary_updated_at = NOW(), updated_at = NOW()
                WHERE order_id = %s
            """, (summary, order_id))
    
    return {
        "status": "ok",
        "summary": summary,
        "cached": False,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

@app.post("/orders/{order_id}/add-email-snippet")
def add_email_snippet(
    order_id: str,
    email_from: str,
    email_subject: str,
    email_snippet: str,
    email_date: Optional[str] = None,
    snippet_type: str = "general"
):
    """Add an email snippet for an order (called by Google Script)"""
    with get_db() as conn:
        with conn.cursor() as cur:
            # Parse date
            parsed_date = None
            if email_date:
                try:
                    parsed_date = datetime.fromisoformat(email_date.replace('Z', '+00:00'))
                except:
                    parsed_date = datetime.now(timezone.utc)
            else:
                parsed_date = datetime.now(timezone.utc)
            
            cur.execute("""
                INSERT INTO order_email_snippets 
                (order_id, email_from, email_subject, email_snippet, email_date, snippet_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (order_id, email_from, email_subject, email_snippet[:1000], parsed_date, snippet_type))
    
    return {"status": "ok", "message": "Email snippet added"}

@app.get("/orders/{order_id}/supplier-sheet-data")
def get_supplier_sheet_data(order_id: str):
    """
    Get order data organized by warehouse for supplier sheet generation.
    Returns data formatted for creating Google Sheet with tabs per supplier.
    """
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get order details
            cur.execute("""
                SELECT * FROM orders WHERE order_id = %s
            """, (order_id,))
            order = cur.fetchone()
            
            if not order:
                raise HTTPException(status_code=404, detail="Order not found")
            
            # Get line items
            cur.execute("""
                SELECT * FROM order_line_items WHERE order_id = %s
            """, (order_id,))
            line_items = cur.fetchall()
    
    # Build customer info
    customer_name = order.get('customer_name') or ''
    company_name = order.get('company_name') or ''
    customer_display = company_name if company_name else customer_name
    if company_name and customer_name:
        customer_display = f"{company_name} ({customer_name})"
    
    street = order.get('street') or ''
    street2 = order.get('street2') or ''
    city = order.get('city') or ''
    state = order.get('state') or ''
    zip_code = order.get('zip_code') or ''
    phone = order.get('phone') or ''
    email = order.get('email') or ''
    
    address_parts = [street]
    if street2:
        address_parts.append(street2)
    address_parts.append(f"{city}, {state} {zip_code}")
    customer_address = ', '.join(filter(None, address_parts))
    
    comments = order.get('comments') or ''
    
    # Group items by warehouse
    warehouses = {}
    for item in line_items:
        wh = item.get('warehouse') or 'Unknown'
        if wh not in warehouses:
            # Get supplier info
            supplier_info = SUPPLIER_INFO.get(wh, {
                'name': wh,
                'address': '',
                'contact': '',
                'email': ''
            })
            warehouses[wh] = {
                'supplier_name': supplier_info['name'],
                'supplier_address': supplier_info['address'],
                'supplier_contact': supplier_info['contact'],
                'supplier_email': supplier_info['email'],
                'items': []
            }
        
        warehouses[wh]['items'].append({
            'quantity': item.get('quantity') or 1,
            'product_code': item.get('sku') or '',
            'product_name': item.get('product_name') or ''
        })
    
    return {
        "status": "ok",
        "order_id": order_id,
        "customer_name": customer_display,
        "customer_address": customer_address,
        "customer_phone": phone,
        "customer_email": email,
        "comments": comments,
        "warehouses": warehouses
    }

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

@app.patch("/orders/{order_id}/set-status")
def set_order_status(order_id: str, status: str, source: str = "web_ui"):
    """
    Set order to a specific status by resetting all checkpoints and setting appropriate ones.
    This allows moving orders backwards in the workflow.
    """
    # Map status to which checkpoints should be TRUE
    status_checkpoints = {
        'needs_payment_link': {},  # All false
        'awaiting_payment': {'payment_link_sent': True},
        'needs_warehouse_order': {'payment_link_sent': True, 'payment_received': True},
        'awaiting_warehouse': {'payment_link_sent': True, 'payment_received': True, 'sent_to_warehouse': True},
        'needs_bol': {'payment_link_sent': True, 'payment_received': True, 'sent_to_warehouse': True, 'warehouse_confirmed': True},
        'awaiting_shipment': {'payment_link_sent': True, 'payment_received': True, 'sent_to_warehouse': True, 'warehouse_confirmed': True, 'bol_sent': True},
        'complete': {'payment_link_sent': True, 'payment_received': True, 'sent_to_warehouse': True, 'warehouse_confirmed': True, 'bol_sent': True, 'is_complete': True}
    }
    
    if status not in status_checkpoints:
        raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
    
    checkpoints = status_checkpoints[status]
    
    with get_db() as conn:
        with conn.cursor() as cur:
            # Reset all checkpoints first, then set the ones we need
            cur.execute("""
                UPDATE orders SET
                    payment_link_sent = %s,
                    payment_received = %s,
                    sent_to_warehouse = %s,
                    warehouse_confirmed = %s,
                    bol_sent = %s,
                    is_complete = %s,
                    updated_at = NOW()
                WHERE order_id = %s
            """, (
                checkpoints.get('payment_link_sent', False),
                checkpoints.get('payment_received', False),
                checkpoints.get('sent_to_warehouse', False),
                checkpoints.get('warehouse_confirmed', False),
                checkpoints.get('bol_sent', False),
                checkpoints.get('is_complete', False),
                order_id
            ))
            
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Order not found")
            
            # Log event
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, 'status_change', %s, %s)
            """, (order_id, json.dumps({'new_status': status}), source))
            
            return {"status": "ok", "new_status": status}

# =============================================================================
# SHIPMENT MANAGEMENT
# =============================================================================

@app.get("/orders/{order_id}/shipments")
def get_order_shipments(order_id: str):
    """Get all shipments for an order"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM order_shipments 
                WHERE order_id = %s 
                ORDER BY warehouse
            """, (order_id,))
            shipments = cur.fetchall()
            return {"status": "ok", "shipments": shipments}

@app.get("/shipments")
def list_all_shipments(include_complete: bool = False):
    """List all shipments with order info"""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT s.*, o.customer_name, o.company_name, o.order_date,
                       o.street, o.street2, o.city, o.state, o.zip_code, o.phone,
                       o.payment_received, o.order_total
                FROM order_shipments s
                JOIN orders o ON s.order_id = o.order_id
                WHERE 1=1
            """
            if not include_complete:
                query += " AND s.status != 'delivered'"
            query += " ORDER BY o.order_date DESC, s.warehouse"
            
            cur.execute(query)
            shipments = cur.fetchall()
            
            # Convert decimals
            for s in shipments:
                if s.get('order_total'):
                    s['order_total'] = float(s['order_total'])
                if s.get('weight'):
                    s['weight'] = float(s['weight'])
            
            return {"status": "ok", "count": len(shipments), "shipments": shipments}

@app.patch("/shipments/{shipment_id}")
def update_shipment(shipment_id: str, 
                    status: Optional[str] = None,
                    tracking: Optional[str] = None,
                    pro_number: Optional[str] = None,
                    weight: Optional[float] = None,
                    ship_method: Optional[str] = None,
                    bol_sent: Optional[bool] = None,
                    origin_zip: Optional[str] = None,
                    rl_quote_number: Optional[str] = None,
                    rl_quote_price: Optional[float] = None,
                    rl_customer_price: Optional[float] = None,
                    rl_invoice_amount: Optional[float] = None,
                    has_oversized: Optional[bool] = None):
    """Update shipment fields"""
    
    valid_statuses = ['needs_order', 'at_warehouse', 'needs_bol', 'ready_ship', 'shipped', 'delivered']
    if status and status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    valid_methods = ['LTL', 'Pirateship', 'Pickup', 'BoxTruck', 'LiDelivery', None]
    if ship_method and ship_method not in valid_methods:
        raise HTTPException(status_code=400, detail=f"Invalid ship_method. Must be one of: {valid_methods}")
    
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Build dynamic update
            updates = []
            params = []
            
            if status is not None:
                updates.append("status = %s")
                params.append(status)
                # Set timestamp based on status
                if status == 'at_warehouse':
                    updates.append("sent_to_warehouse_at = NOW()")
                elif status == 'needs_bol':
                    updates.append("warehouse_confirmed_at = NOW()")
                elif status == 'shipped':
                    updates.append("shipped_at = NOW()")
                elif status == 'delivered':
                    updates.append("delivered_at = NOW()")
            
            if tracking is not None:
                updates.append("tracking = %s")
                params.append(tracking)
            
            if pro_number is not None:
                updates.append("pro_number = %s")
                params.append(pro_number)
            
            if weight is not None:
                updates.append("weight = %s")
                params.append(weight)
            
            if ship_method is not None:
                updates.append("ship_method = %s")
                params.append(ship_method)
            
            if bol_sent is not None:
                updates.append("bol_sent = %s")
                params.append(bol_sent)
                if bol_sent:
                    updates.append("bol_sent_at = NOW()")
            
            # RL Carriers fields
            if origin_zip is not None:
                updates.append("origin_zip = %s")
                params.append(origin_zip)
            
            if rl_quote_number is not None:
                updates.append("rl_quote_number = %s")
                params.append(rl_quote_number)
            
            if rl_quote_price is not None:
                updates.append("rl_quote_price = %s")
                params.append(rl_quote_price)
            
            if rl_customer_price is not None:
                updates.append("rl_customer_price = %s")
                params.append(rl_customer_price)
            
            if rl_invoice_amount is not None:
                updates.append("rl_invoice_amount = %s")
                params.append(rl_invoice_amount)
            
            if has_oversized is not None:
                updates.append("has_oversized = %s")
                params.append(has_oversized)
            
            if not updates:
                return {"status": "ok", "message": "No updates provided"}
            
            updates.append("updated_at = NOW()")
            params.append(shipment_id)
            
            query = f"UPDATE order_shipments SET {', '.join(updates)} WHERE shipment_id = %s RETURNING *"
            cur.execute(query, params)
            
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Shipment not found")
            
            # Check if all shipments for this order are delivered
            cur.execute("""
                SELECT COUNT(*) as total, 
                       COUNT(*) FILTER (WHERE status = 'delivered') as delivered
                FROM order_shipments 
                WHERE order_id = %s
            """, (result['order_id'],))
            counts = cur.fetchone()
            
            # If all delivered, mark order complete
            if counts['total'] > 0 and counts['total'] == counts['delivered']:
                cur.execute("""
                    UPDATE orders SET is_complete = TRUE, completed_at = NOW(), updated_at = NOW()
                    WHERE order_id = %s
                """, (result['order_id'],))
            
            return {"status": "ok", "shipment": dict(result)}

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

@app.get("/shipments/{shipment_id}/rl-quote-data")
def get_rl_quote_data(shipment_id: str):
    """Get pre-populated data for RL Carriers quote"""
    try:
        with get_db() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Get shipment and order info - use SELECT * to avoid column issues
                cur.execute("""
                    SELECT s.*, o.customer_name, o.company_name, o.street, o.city, o.state, o.zip_code,
                           o.phone, o.email, o.order_total
                    FROM order_shipments s
                    JOIN orders o ON s.order_id = o.order_id
                    WHERE s.shipment_id = %s
                """, (shipment_id,))
                
                shipment = cur.fetchone()
                if not shipment:
                    return {"status": "error", "message": f"Shipment {shipment_id} not found"}
                
                # Get warehouse zip
                warehouse = shipment['warehouse']
                origin_zip = WAREHOUSE_ZIPS.get(warehouse, '')
                
                # If warehouse not in our list, try fuzzy match
                if not origin_zip:
                    warehouse_lower = warehouse.lower().replace(' ', '').replace('&', '').replace('-', '')
                    for wh_name, wh_zip in WAREHOUSE_ZIPS.items():
                        wh_compare = wh_name.lower().replace(' ', '').replace('&', '').replace('-', '')
                        if wh_compare == warehouse_lower or warehouse_lower in wh_compare or wh_compare in warehouse_lower:
                            origin_zip = wh_zip
                            break
                
                # Get line items for this warehouse to check total weight and oversized
                cur.execute("""
                    SELECT sku, description, quantity, weight
                    FROM order_line_items
                    WHERE order_id = %s AND warehouse = %s
                """, (shipment['order_id'], warehouse))
                line_items = cur.fetchall()
                
                # Calculate weight for this shipment's items
                total_weight = 0
                has_oversized = False
                oversized_items = []
                
                for item in line_items:
                    if item.get('weight'):
                        try:
                            total_weight += float(item['weight']) * int(item.get('quantity', 1))
                        except:
                            pass
                    
                    # Check for oversized keywords in description
                    desc = (item.get('description') or '').upper()
                    for keyword in OVERSIZED_KEYWORDS:
                        if keyword in desc:
                            has_oversized = True
                            oversized_items.append(f"{item.get('sku')}: {item.get('description')}")
                            break
                
                # Check if single warehouse order
                cur.execute("""
                    SELECT COUNT(DISTINCT warehouse) as warehouse_count
                    FROM order_line_items
                    WHERE order_id = %s AND warehouse IS NOT NULL
                """, (shipment['order_id'],))
                wh_count = cur.fetchone()
                is_single_warehouse = wh_count and wh_count['warehouse_count'] <= 1
                
                return {
                    "status": "ok",
                    "shipment_id": shipment_id,
                    "order_id": shipment['order_id'],
                    "warehouse": warehouse,
                    "origin_zip": origin_zip,
                    "destination": {
                        "name": shipment.get('company_name') or shipment.get('customer_name') or '',
                        "street": shipment.get('street') or '',
                        "city": shipment.get('city') or '',
                        "state": shipment.get('state') or '',
                        "zip": shipment.get('zip_code') or ''
                    },
                    "weight": {
                        "calculated": round(total_weight, 1) if total_weight > 0 else None,
                        "shipment_weight": float(shipment['weight']) if shipment.get('weight') else None,
                        "is_single_warehouse": is_single_warehouse,
                        "needs_manual_entry": not is_single_warehouse and not shipment.get('weight')
                    },
                    "oversized": {
                        "detected": has_oversized,
                        "items": oversized_items
                    },
                    "existing_quote": {
                        "quote_number": shipment.get('rl_quote_number'),
                        "quote_price": float(shipment['rl_quote_price']) if shipment.get('rl_quote_price') else None,
                        "customer_price": float(shipment['rl_customer_price']) if shipment.get('rl_customer_price') else None
                    },
                    "rl_quote_url": "https://www.rlcarriers.com/freight/shipping/rate-quote"
                }
    except Exception as e:
        return {"status": "error", "message": str(e)}

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
