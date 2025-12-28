"""
checkout.py
B2BWave order checkout with R+L shipping quotes and Square payment
"""

import os
import json
import urllib.request
import urllib.error
import hmac
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any

# Config from environment
B2BWAVE_URL = os.environ.get("B2BWAVE_URL", "").strip().rstrip('/')
B2BWAVE_USERNAME = os.environ.get("B2BWAVE_USERNAME", "").strip()
B2BWAVE_API_KEY = os.environ.get("B2BWAVE_API_KEY", "").strip()

SQUARE_APP_ID = os.environ.get("SQUARE_APP_ID", "").strip()
SQUARE_ACCESS_TOKEN = os.environ.get("SQUARE_ACCESS_TOKEN", "").strip()
SQUARE_LOCATION_ID = os.environ.get("SQUARE_LOCATION_ID", "").strip()
SQUARE_ENVIRONMENT = os.environ.get("SQUARE_ENVIRONMENT", "sandbox").strip()  # sandbox or production

RL_QUOTE_API_URL = os.environ.get("RL_QUOTE_API_URL", "https://rl-quote-sandbox.onrender.com").strip()

CHECKOUT_BASE_URL = os.environ.get("CHECKOUT_BASE_URL", "").strip()  # Your checkout page URL

# Warehouse data
WAREHOUSES = {
    'LI': {'name': 'Liberty Industries', 'city': 'Interlachen', 'state': 'FL', 'zip': '32148'},
    'DL': {'name': 'DL Cabinetry', 'city': 'Jacksonville', 'state': 'FL', 'zip': '32256'},
    'ROC': {'name': 'ROC Cabinetry', 'city': 'Norcross', 'state': 'GA', 'zip': '30071'},
    'GHI': {'name': 'GHI Cabinets', 'city': 'Palmetto', 'state': 'FL', 'zip': '34221'},
    'Go Bravura': {'name': 'Go Bravura', 'city': 'Houston', 'state': 'TX', 'zip': '77066'},
    'ARTISAN': {'name': 'Artisan', 'city': 'Houston', 'state': 'TX', 'zip': '77066'},
    'Cabinet & Stone': {'name': 'Cabinet & Stone', 'city': 'Houston', 'state': 'TX', 'zip': '77043'},
    'Cabinet & Stone CA': {'name': 'Cabinet & Stone CA', 'city': 'Paramount', 'state': 'CA', 'zip': '90723'},
    'DuraStone': {'name': 'DuraStone', 'city': 'Houston', 'state': 'TX', 'zip': '77037'},
    'L&C': {'name': 'L&C Cabinetry', 'city': 'Virginia Beach', 'state': 'VA', 'zip': '23454'},
}

# SKU prefix to warehouse mapping
SKU_WAREHOUSE_MAP = {
    # LI
    'WSP': 'LI', 'GSP': 'LI', 'NBLK': 'LI',
    # DL
    'RW': 'DL', 'UFS': 'DL', 'CS': 'DL', 'EBK': 'DL',
    # ROC
    'EWD': 'ROC', 'EGD': 'ROC', 'EMB': 'ROC', 'BC': 'ROC', 
    'DCW': 'ROC', 'DCT': 'ROC', 'DCH': 'ROC', 'NJGR': 'ROC', 'EJG': 'ROC',
    # GHI
    'APW': 'GHI', 'AKS': 'GHI', 'GRSH': 'GHI', 'NOR': 'GHI', 'SNS': 'GHI', 'SNW': 'GHI',
    # Go Bravura
    'HGW': 'Go Bravura', 'EMW': 'Go Bravura', 'EGG': 'Go Bravura', 'URC': 'Go Bravura',
    'WWW': 'Go Bravura', 'NDG': 'Go Bravura', 'NCC': 'Go Bravura', 'NBW': 'Go Bravura',
    'BX': 'Go Bravura', 'URW': 'Go Bravura',
    # ARTISAN
    'HSS': 'ARTISAN', 'LGS': 'ARTISAN', 'LGSS': 'ARTISAN', 'DG': 'ARTISAN', 
    'EOK': 'ARTISAN', 'EWT': 'ARTISAN',
    # Cabinet & Stone (TX default, CA for MSCS)
    'BSN': 'Cabinet & Stone', 'SGCS': 'Cabinet & Stone', 'WOCS': 'Cabinet & Stone',
    'EWSCS': 'Cabinet & Stone', 'CAWN': 'Cabinet & Stone', 'ESCS': 'Cabinet & Stone', 'CS-': 'Cabinet & Stone',
    'MSCS': 'Cabinet & Stone CA',  # Ships from California
    # DuraStone
    'NSN': 'DuraStone', 'NBDS': 'DuraStone', 'CMEN': 'DuraStone', 'SIV': 'DuraStone',
    # L&C
    'SHLS': 'L&C', 'NS': 'L&C', 'RBLS': 'L&C', 'MGLS': 'L&C', 'BG': 'L&C', 'EDD': 'L&C', 'SWNG': 'L&C',
}

# Oversized detection keywords
OVERSIZED_KEYWORDS = ['PANTRY', 'OVEN', 'TALL', '96', 'BROOM', 'LINEN', 'UTILITY']


def get_warehouse_for_sku(sku: str) -> Optional[str]:
    """Get warehouse code from SKU prefix"""
    # Extract prefix (first part before dash or numbers)
    prefix = sku.split('-')[0] if '-' in sku else sku
    # Remove trailing numbers
    prefix = ''.join(c for c in prefix if not c.isdigit()).upper()
    
    return SKU_WAREHOUSE_MAP.get(prefix)


def is_oversized(product_name: str) -> bool:
    """Check if product is oversized based on name"""
    name_upper = product_name.upper()
    return any(keyword in name_upper for keyword in OVERSIZED_KEYWORDS)


def group_items_by_warehouse(line_items: list) -> Dict[str, list]:
    """Group order items by their source warehouse"""
    groups = {}
    
    for item in line_items:
        sku = item.get('sku', '') or item.get('product_sku', '')
        warehouse = get_warehouse_for_sku(sku)
        
        if not warehouse:
            warehouse = 'UNKNOWN'
        
        if warehouse not in groups:
            groups[warehouse] = []
        
        groups[warehouse].append(item)
    
    return groups


def calculate_shipment_weight(items: list) -> float:
    """Calculate total weight for items (placeholder - needs real weight data)"""
    # TODO: Look up actual weights from database
    # For now, estimate based on quantity
    total_weight = 0
    for item in items:
        qty = item.get('quantity', 1)
        # Rough estimate: 30 lbs per cabinet average
        total_weight += qty * 30
    
    return max(total_weight, 100)  # Minimum 100 lbs


def get_shipping_quote(origin_zip: str, dest_zip: str, weight: float, is_residential: bool, is_oversized: bool = False) -> Dict:
    """Get shipping quote from R+L Quote API"""
    try:
        url = f"{RL_QUOTE_API_URL}/quote/simple"
        params = {
            'origin_zip': origin_zip,
            'destination_zip': dest_zip,
            'weight_lbs': int(weight),
            'is_residential': 'true' if is_residential else 'false',
            'is_oversized': 'true' if is_oversized else 'false'
        }
        
        query_string = '&'.join(f"{k}={v}" for k, v in params.items())
        full_url = f"{url}?{query_string}"
        
        req = urllib.request.Request(full_url, method='POST')
        req.add_header('Content-Type', 'application/json')
        
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            return data
            
    except Exception as e:
        return {'success': False, 'error': str(e)}


def calculate_order_shipping(order_data: dict, dest_address: dict) -> Dict:
    """
    Calculate shipping for an entire order, grouped by warehouse
    
    Returns:
        {
            'shipments': [
                {'warehouse': 'LI', 'items': [...], 'quote': {...}},
                {'warehouse': 'ROC', 'items': [...], 'quote': {...}},
            ],
            'total_shipping': 250.00,
            'total_items': 1500.00,
            'grand_total': 1750.00
        }
    """
    line_items = order_data.get('line_items', []) or order_data.get('products', [])
    
    # Group by warehouse
    warehouse_groups = group_items_by_warehouse(line_items)
    
    # Determine if residential
    is_residential = True  # Default to residential, could be overridden by Smarty validation
    
    dest_zip = dest_address.get('zip', '') or dest_address.get('postal_code', '')
    
    shipments = []
    total_shipping = 0
    
    for warehouse_code, items in warehouse_groups.items():
        if warehouse_code == 'UNKNOWN':
            shipments.append({
                'warehouse': 'UNKNOWN',
                'warehouse_name': 'Unknown Warehouse',
                'items': items,
                'quote': {'success': False, 'error': 'Could not determine warehouse for items'},
                'shipping_cost': 0
            })
            continue
        
        warehouse = WAREHOUSES.get(warehouse_code)
        if not warehouse:
            continue
        
        # Calculate weight and check oversized
        weight = calculate_shipment_weight(items)
        oversized = any(is_oversized(item.get('name', '')) for item in items)
        
        # Get quote
        quote = get_shipping_quote(
            origin_zip=warehouse['zip'],
            dest_zip=dest_zip,
            weight=weight,
            is_residential=is_residential,
            is_oversized=oversized
        )
        
        shipping_cost = 0
        if quote.get('success') and quote.get('quote'):
            shipping_cost = quote['quote'].get('customer_price', 0)
        
        shipments.append({
            'warehouse': warehouse_code,
            'warehouse_name': warehouse['name'],
            'origin_zip': warehouse['zip'],
            'items': items,
            'weight': weight,
            'is_oversized': oversized,
            'quote': quote,
            'shipping_cost': shipping_cost
        })
        
        total_shipping += shipping_cost
    
    # Calculate item total
    total_items = 0
    for item in line_items:
        price = float(item.get('price', 0) or item.get('unit_price', 0) or 0)
        qty = int(item.get('quantity', 1) or 1)
        total_items += price * qty
    
    return {
        'shipments': shipments,
        'total_shipping': round(total_shipping, 2),
        'total_items': round(total_items, 2),
        'grand_total': round(total_items + total_shipping, 2),
        'destination': dest_address
    }


def fetch_b2bwave_order(order_id: str) -> Optional[Dict]:
    """Fetch order details from B2BWave API"""
    if not B2BWAVE_URL or not B2BWAVE_API_KEY:
        return None
    
    try:
        url = f"{B2BWAVE_URL}/api/orders/{order_id}.json"
        
        # Basic auth
        credentials = f"{B2BWAVE_USERNAME}:{B2BWAVE_API_KEY}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        req = urllib.request.Request(url)
        req.add_header('Authorization', f'Basic {encoded_credentials}')
        
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
            
    except Exception as e:
        print(f"[B2BWAVE] Error fetching order {order_id}: {e}")
        return None


def create_square_payment_link(amount_cents: int, order_id: str, customer_email: str) -> Optional[str]:
    """Create a Square payment link for the order"""
    if not SQUARE_ACCESS_TOKEN:
        return None
    
    try:
        # Square Checkout API
        base_url = "https://connect.squareupsandbox.com" if SQUARE_ENVIRONMENT == "sandbox" else "https://connect.squareup.com"
        url = f"{base_url}/v2/online-checkout/payment-links"
        
        payload = {
            "idempotency_key": f"order-{order_id}-{datetime.now().timestamp()}",
            "quick_pay": {
                "name": f"CFC Order #{order_id}",
                "price_money": {
                    "amount": amount_cents,
                    "currency": "USD"
                },
                "location_id": SQUARE_LOCATION_ID
            },
            "checkout_options": {
                "redirect_url": f"{CHECKOUT_BASE_URL}/payment-complete?order={order_id}",
                "ask_for_shipping_address": False
            },
            "pre_populated_data": {
                "buyer_email": customer_email
            }
        }
        
        data = json.dumps(payload).encode()
        
        req = urllib.request.Request(url, data=data, method='POST')
        req.add_header('Authorization', f'Bearer {SQUARE_ACCESS_TOKEN}')
        req.add_header('Content-Type', 'application/json')
        req.add_header('Square-Version', '2024-01-18')
        
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode())
            return result.get('payment_link', {}).get('url')
            
    except Exception as e:
        print(f"[SQUARE] Error creating payment link: {e}")
        return None


def generate_checkout_token(order_id: str) -> str:
    """Generate a secure token for checkout link"""
    secret = os.environ.get("CHECKOUT_SECRET", "default-secret-change-me")
    message = f"{order_id}-{datetime.now().strftime('%Y%m%d')}"
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()[:16]


def verify_checkout_token(order_id: str, token: str) -> bool:
    """Verify checkout token is valid"""
    expected = generate_checkout_token(order_id)
    return hmac.compare_digest(token, expected)
