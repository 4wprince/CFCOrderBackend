"""
gmail_sync.py
Gmail email scanning for CFC Order Workflow
Detects: Payment links sent, Payments received (Square), RL Quotes, Tracking numbers,
         Warehouse orders, BOL requests, Ready to ship, Delivery confirmations
"""

import os
import re
import json
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Tuple

# Gmail API Config - loaded from environment
GMAIL_CLIENT_ID = os.environ.get("GMAIL_CLIENT_ID", "").strip()
GMAIL_CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET", "").strip()
GMAIL_REFRESH_TOKEN = os.environ.get("GMAIL_REFRESH_TOKEN", "").strip()

# Alert email recipient
ALERT_EMAIL = "william@cabinetsforcontractors.com"

# Email sender patterns
SQUARE_PAYMENT_SENDER = "noreply@messaging.squareup.com"
RL_CARRIERS_SENDER = "rlloads@rlcarriers.com"
RL_DELIVERY_SENDER = "webnotificationsvc.prd@rlcarriers.com"

# Warehouse email addresses - used to detect "sent to warehouse"
WAREHOUSE_EMAILS = {
    # GHI / Love Touch
    "lovetoucheskitchen@gmail.com": "Love Touch",
    "lovetouchesservice@gmail.com": "Love Touch",
    "ghiorders@ghicabinets.com": "GHI",
    
    # DL Cabinetry - confirmation comes FROM this address
    "ecomm@dlcabinetry.com": "DL",
    "orders@dlcabinetry.com": "DL",
    
    # ROC - confirmation comes FROM weborders addresses
    "weborders@roccabinetry.com": "ROC-Atlanta",
    "weborders@roccabinetrytampa.com": "ROC-Tampa",
    "aaron@roccabinetry.com": "ROC",  # Could be Atlanta or Tampa - check invoice
    
    # Cabinet & Stone
    "orders@cabinetstonellc.com": "Cabinet & Stone",
    "info@cabinetstonellc.com": "Cabinet & Stone",
    
    # Go Bravura
    "orders@gobravura.com": "Go Bravura",
    "info@gobravura.com": "Go Bravura",
    
    # Durastone
    "info@durastoneusa.com": "Durastone",
    "orders@durastoneusa.com": "Durastone",
    
    # Dealer Cabinetry
    "orders@dealercabinetry.com": "Dealer Cabinetry",
    
    # LI (forwarder/delivery)
    "cabinetrydistribution@gmail.com": "LI",
    
    # LNC Cabinetry
    "lnccabinetryvab@gmail.com": "LNC",
}

# Domains to match warehouse emails (for incoming detection)
WAREHOUSE_DOMAINS = [
    "ghicabinets.com",
    "dlcabinetry.com",
    "roccabinetry.com",
    "roccabinetrytampa.com",
    "cabinetstonellc.com",
    "gobravura.com",
    "durastoneusa.com",
    "dealercabinetry.com",
]

# Cache access token
_access_token = None
_token_expires = None

def gmail_configured():
    """Check if Gmail credentials are configured"""
    return bool(GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET and GMAIL_REFRESH_TOKEN)

def get_gmail_access_token():
    """Get a fresh access token using the refresh token"""
    global _access_token, _token_expires
    
    # Return cached token if still valid
    if _access_token and _token_expires and datetime.now(timezone.utc) < _token_expires:
        return _access_token
    
    if not gmail_configured():
        print("[GMAIL] Not configured")
        return None
    
    try:
        token_data = urllib.parse.urlencode({
            'client_id': GMAIL_CLIENT_ID,
            'client_secret': GMAIL_CLIENT_SECRET,
            'refresh_token': GMAIL_REFRESH_TOKEN,
            'grant_type': 'refresh_token'
        }).encode()
        
        req = urllib.request.Request('https://oauth2.googleapis.com/token', data=token_data)
        
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            _access_token = data.get('access_token')
            # Token typically valid for 1 hour, we'll refresh at 50 min
            _token_expires = datetime.now(timezone.utc) + timedelta(minutes=50)
            return _access_token
            
    except Exception as e:
        print(f"[GMAIL] Token refresh error: {e}")
        return None

def gmail_api_request(endpoint, params=None, method="GET", body=None):
    """Make authenticated request to Gmail API"""
    token = get_gmail_access_token()
    if not token:
        return None
    
    url = f"https://gmail.googleapis.com/gmail/v1/users/me/{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    
    req = urllib.request.Request(url, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    
    if body:
        req.add_header("Content-Type", "application/json")
        req.data = json.dumps(body).encode()
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        print(f"[GMAIL] API error {e.code}: {e.read().decode()[:200]}")
        return None
    except Exception as e:
        print(f"[GMAIL] Request error: {e}")
        return None

def search_emails(query, max_results=50):
    """Search Gmail for messages matching query"""
    data = gmail_api_request("messages", {"q": query, "maxResults": max_results})
    if not data:
        return []
    return data.get("messages", [])

def get_email_content(message_id):
    """Get email details including subject, from, to, body, and attachment info"""
    data = gmail_api_request(f"messages/{message_id}", {"format": "full"})
    if not data:
        return None
    
    headers = {h['name'].lower(): h['value'] for h in data.get('payload', {}).get('headers', [])}
    
    # Extract body
    body = ""
    html_body = ""
    has_attachment = False
    payload = data.get('payload', {})
    
    def extract_body_from_parts(parts):
        nonlocal body, html_body, has_attachment
        for part in parts:
            mime_type = part.get('mimeType', '')
            
            # Check for attachments
            if part.get('filename') and part.get('body', {}).get('attachmentId'):
                has_attachment = True
            
            # Extract text body
            if mime_type == 'text/plain' and part.get('body', {}).get('data'):
                import base64
                body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
            
            # Extract HTML body (for parsing tables like RL Carriers)
            if mime_type == 'text/html' and part.get('body', {}).get('data'):
                import base64
                html_body = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8', errors='ignore')
            
            # Recurse into nested parts
            if 'parts' in part:
                extract_body_from_parts(part['parts'])
    
    # Handle simple body
    if 'body' in payload and payload['body'].get('data'):
        import base64
        body = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
    
    # Handle multipart
    if 'parts' in payload:
        extract_body_from_parts(payload['parts'])
    
    # Check for attachment in simple structure
    if payload.get('filename') and payload.get('body', {}).get('attachmentId'):
        has_attachment = True
    
    return {
        'id': message_id,
        'subject': headers.get('subject', ''),
        'from': headers.get('from', ''),
        'to': headers.get('to', ''),
        'date': headers.get('date', ''),
        'body': body,
        'html_body': html_body,
        'has_attachment': has_attachment
    }

# =============================================================================
# EXTRACTION HELPERS
# =============================================================================

def extract_order_ids(text: str) -> List[str]:
    """Extract all order IDs from text (4-5 digit numbers, typically starting with 5)"""
    if not text:
        return []
    
    order_ids = []
    
    # Pattern 1: "PO 5306" or "PO# 5306" or "Order 5306" or "#5306"
    matches = re.findall(r'(?:PO|order|#)\s*#?\s*(\d{4,5})\b', text, re.IGNORECASE)
    order_ids.extend(matches)
    
    # Pattern 2: "5299-Creative Spaces" or "5299-UFP-2" (order number with suffix)
    matches = re.findall(r'\b(\d{4,5})-[A-Za-z]', text)
    order_ids.extend(matches)
    
    # Pattern 3: JOB Name #5306 (DL Cabinetry format)
    matches = re.findall(r'JOB\s+Name\s*#?\s*(\d{4,5})', text, re.IGNORECASE)
    order_ids.extend(matches)
    
    # Pattern 4: Standalone 4-5 digit numbers starting with 5 (CFC order IDs)
    if not order_ids:
        matches = re.findall(r'\b(5\d{3,4})\b', text)
        order_ids.extend(matches)
    
    # Remove duplicates, preserve order
    seen = set()
    unique = []
    for oid in order_ids:
        if oid not in seen:
            seen.add(oid)
            unique.append(oid)
    
    return unique

def extract_order_id(text: str) -> Optional[str]:
    """Extract single order ID from text (backwards compatible)"""
    ids = extract_order_ids(text)
    return ids[0] if ids else None

def extract_payment_amount(text):
    """Extract dollar amount from text"""
    match = re.search(r'\$([\d,]+\.?\d*)', text)
    if match:
        return float(match.group(1).replace(',', ''))
    return None

def extract_customer_name(text):
    """Extract customer name from Square payment email"""
    match = re.search(r'payment received from\s+([^\n]+)', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None

def extract_mpo_from_rl_email(html_body: str, text_body: str) -> Optional[str]:
    """
    Extract MPO (our order number) from RL Carriers delivery notification.
    MPO is in a table row like: <td>MPO</td><td>5261</td>
    Or in text like: MPO    5261
    Handles formats: 5261, 5261-A, 5261-ROC
    """
    # Try HTML table first
    mpo_match = re.search(r'MPO\s*</td>\s*<td[^>]*>\s*(\d{4,5}(?:-[A-Za-z0-9]+)?)', html_body, re.IGNORECASE)
    if mpo_match:
        # Extract just the numeric part
        mpo = mpo_match.group(1)
        num_match = re.match(r'(\d{4,5})', mpo)
        return num_match.group(1) if num_match else None
    
    # Try text body
    mpo_match = re.search(r'MPO\s+(\d{4,5}(?:-[A-Za-z0-9]+)?)', text_body, re.IGNORECASE)
    if mpo_match:
        mpo = mpo_match.group(1)
        num_match = re.match(r'(\d{4,5})', mpo)
        return num_match.group(1) if num_match else None
    
    return None

def extract_warehouse_from_email(from_addr: str, body: str) -> Optional[str]:
    """Determine which warehouse an email is from/to"""
    from_lower = from_addr.lower()
    
    # Check exact email match
    for email, warehouse in WAREHOUSE_EMAILS.items():
        if email.lower() in from_lower:
            # Special case for ROC - check invoice number for Tampa vs Atlanta
            if warehouse == "ROC" and "aaron@roccabinetry.com" in from_lower:
                if re.search(r'Order\s*#\s*T\d+', body, re.IGNORECASE):
                    return "ROC-Tampa"
                else:
                    return "ROC-Atlanta"
            return warehouse
    
    # Check domain match
    for domain in WAREHOUSE_DOMAINS:
        if domain.lower() in from_lower:
            # Map domain to warehouse name
            if "ghicabinets" in domain:
                return "GHI"
            elif "dlcabinetry" in domain:
                return "DL"
            elif "roccabinetrytampa" in domain:
                return "ROC-Tampa"
            elif "roccabinetry" in domain:
                return "ROC-Atlanta"
            elif "cabinetstonellc" in domain:
                return "Cabinet & Stone"
            elif "gobravura" in domain:
                return "Go Bravura"
            elif "durastoneusa" in domain:
                return "Durastone"
            elif "dealercabinetry" in domain:
                return "Dealer Cabinetry"
    
    return None

def is_warehouse_email(email_addr: str) -> Tuple[bool, Optional[str]]:
    """Check if email address belongs to a warehouse. Returns (is_warehouse, warehouse_name)"""
    email_lower = email_addr.lower()
    
    for warehouse_email, warehouse_name in WAREHOUSE_EMAILS.items():
        if warehouse_email.lower() in email_lower:
            return True, warehouse_name
    
    for domain in WAREHOUSE_DOMAINS:
        if domain.lower() in email_lower:
            # Map to warehouse name
            warehouse = extract_warehouse_from_email(email_addr, "")
            return True, warehouse
    
    return False, None

def has_dimension_pattern(text: str) -> bool:
    """Check if text contains dimension patterns like 40x48x60 or 8x8x96"""
    return bool(re.search(r'\b\d+\s*x\s*\d+\s*x\s*\d+\b', text, re.IGNORECASE))

def has_bol_keywords(text: str) -> bool:
    """Check if text contains BOL/shipping keywords"""
    keywords = ['bol', 'r+l', 'r\\+l', 'rl carrier', 'ups', 'label', 'shipping label']
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)

def has_warehouse_ready_keywords(text: str) -> bool:
    """Check if text indicates warehouse has order ready (needs BOL)"""
    keywords = ['dimensions', 'pallet', 'ready to ship', 'ready for pickup', 'palletized']
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords) or has_dimension_pattern(text)

def has_order_keywords(text: str) -> bool:
    """Check if text indicates placing an order"""
    patterns = [
        r'i need\s+\d+\s+each',
        r'\d+\s+each',
        r'one order',
        r'an order',
        r'\d+\s+orders?',
        r'please order',
        r'order the following',
    ]
    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in patterns)

# =============================================================================
# ALERT EMAIL FUNCTION
# =============================================================================

def send_alert_email(subject: str, body: str) -> bool:
    """Send alert email to William for edge cases"""
    if not gmail_configured():
        print(f"[GMAIL] Cannot send alert - not configured: {subject}")
        return False
    
    try:
        import base64
        from email.mime.text import MIMEText
        
        message = MIMEText(body)
        message['to'] = ALERT_EMAIL
        message['subject'] = subject
        
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        
        result = gmail_api_request("messages/send", method="POST", body={"raw": raw})
        
        if result:
            print(f"[GMAIL] Alert sent: {subject}")
            return True
        else:
            print(f"[GMAIL] Failed to send alert: {subject}")
            return False
            
    except Exception as e:
        print(f"[GMAIL] Alert error: {e}")
        return False

# =============================================================================
# MAIN SYNC FUNCTION
# =============================================================================

def run_gmail_sync(db_conn, hours_back=2):
    """
    Main email sync function - scans Gmail and updates orders/shipments
    Returns dict with counts of what was processed
    """
    if not gmail_configured():
        print("[GMAIL] Not configured, skipping email sync")
        return {"status": "skipped", "reason": "not_configured"}
    
    print(f"[GMAIL] Starting email sync (last {hours_back} hours)")
    
    results = {
        "payment_links": 0,
        "payments_received": 0,
        "rl_quotes": 0,
        "tracking_numbers": 0,
        "at_warehouse": 0,
        "needs_bol": 0,
        "ready_ship": 0,
        "delivered": 0,
        "alerts_sent": 0,
        "errors": []
    }
    
    time_filter = f"newer_than:{hours_back}h"
    
    # =========================================================================
    # 1. Payment Links Sent (sent emails with square.link)
    # =========================================================================
    try:
        messages = search_emails(f"{time_filter} in:sent square.link")
        print(f"[GMAIL] Found {len(messages)} sent emails with square.link")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                # Only process if we sent it
                if 'cabinetsforcontractors' not in email['from'].lower() and 'william' not in email['from'].lower():
                    continue
                
                if 'square.link' not in email['body'].lower():
                    continue
                
                order_id = extract_order_id(email['subject'] + ' ' + email['body'])
                if order_id:
                    if update_order_payment_link_sent(db_conn, order_id, email):
                        results["payment_links"] += 1
                    
            except Exception as e:
                results["errors"].append(f"Payment link error: {e}")
                
    except Exception as e:
        results["errors"].append(f"Payment link search error: {e}")
    
    # =========================================================================
    # 2. Payments Received (Square notifications) - KEPT FOR BACKUP
    #    Primary payment detection is now via Square API
    # =========================================================================
    try:
        messages = search_emails(f'{time_filter} from:{SQUARE_PAYMENT_SENDER} subject:"payment received"')
        print(f"[GMAIL] Found {len(messages)} Square payment emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                amount = extract_payment_amount(email['subject'])
                customer_name = extract_customer_name(email['subject'])
                
                if amount and customer_name:
                    if match_payment_to_order(db_conn, amount, customer_name, email):
                        results["payments_received"] += 1
                        
            except Exception as e:
                results["errors"].append(f"Payment received error: {e}")
                
    except Exception as e:
        results["errors"].append(f"Payment search error: {e}")
    
    # =========================================================================
    # 3. AT WAREHOUSE - Outgoing emails TO warehouse addresses
    # =========================================================================
    try:
        messages = search_emails(f"{time_filter} in:sent")
        print(f"[GMAIL] Checking {len(messages)} sent emails for warehouse orders")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                # Check if sent to a warehouse
                is_warehouse, warehouse_name = is_warehouse_email(email['to'])
                if not is_warehouse:
                    continue
                
                # Extract order IDs
                text = email['subject'] + ' ' + email['body']
                order_ids = extract_order_ids(text)
                
                # Check for order-placing keywords if no clear order ID
                if not order_ids and not has_order_keywords(text):
                    continue
                
                for order_id in order_ids:
                    if update_shipment_status(db_conn, order_id, warehouse_name, 'at_warehouse', email):
                        results["at_warehouse"] += 1
                    
            except Exception as e:
                results["errors"].append(f"Warehouse order detection error: {e}")
                
    except Exception as e:
        results["errors"].append(f"Warehouse order search error: {e}")
    
    # =========================================================================
    # 4. AT WAREHOUSE - Incoming confirmations from ROC/DL
    # =========================================================================
    try:
        # ROC confirmations
        messages = search_emails(f'{time_filter} from:weborders@roccabinetry.com OR from:weborders@roccabinetrytampa.com')
        print(f"[GMAIL] Found {len(messages)} ROC confirmation emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                text = email['subject'] + ' ' + email['body']
                
                # Extract order ID from "PO Number# 5264-UFP-2"
                match = re.search(r'PO\s*Number\s*#?\s*(\d{4,5})', text, re.IGNORECASE)
                if match:
                    order_id = match.group(1)
                    
                    # Determine Tampa vs Atlanta
                    if 'roccabinetrytampa' in email['from'].lower():
                        warehouse = "ROC-Tampa"
                    elif re.search(r'Order\s*#\s*T\d+', text, re.IGNORECASE):
                        warehouse = "ROC-Tampa"
                    else:
                        warehouse = "ROC-Atlanta"
                    
                    if update_shipment_status(db_conn, order_id, warehouse, 'at_warehouse', email):
                        results["at_warehouse"] += 1
                    
            except Exception as e:
                results["errors"].append(f"ROC confirmation error: {e}")
        
        # DL confirmations
        messages = search_emails(f'{time_filter} from:ecomm@dlcabinetry.com subject:"order confirmation"')
        print(f"[GMAIL] Found {len(messages)} DL confirmation emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                # Extract from "JOB Name #5306"
                match = re.search(r'JOB\s+Name\s*#?\s*(\d{4,5})', email['subject'], re.IGNORECASE)
                if match:
                    order_id = match.group(1)
                    if update_shipment_status(db_conn, order_id, "DL", 'at_warehouse', email):
                        results["at_warehouse"] += 1
                    
            except Exception as e:
                results["errors"].append(f"DL confirmation error: {e}")
                
    except Exception as e:
        results["errors"].append(f"Warehouse confirmation search error: {e}")
    
    # =========================================================================
    # 5. NEEDS BOL - Warehouse says order is ready (has dimensions/pallet info)
    # =========================================================================
    try:
        # Search for dimension patterns and keywords from warehouse domains
        messages = search_emails(f'{time_filter} (dimensions OR pallet OR "ready to ship")')
        print(f"[GMAIL] Found {len(messages)} potential needs-BOL emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                # Must be from a warehouse
                warehouse = extract_warehouse_from_email(email['from'], email['body'])
                if not warehouse:
                    continue
                
                text = email['subject'] + ' ' + email['body']
                
                # Must have dimension pattern or ready keywords
                if not has_warehouse_ready_keywords(text):
                    continue
                
                order_ids = extract_order_ids(text)
                for order_id in order_ids:
                    if update_shipment_status(db_conn, order_id, warehouse, 'needs_bol', email):
                        results["needs_bol"] += 1
                    
            except Exception as e:
                results["errors"].append(f"Needs BOL detection error: {e}")
                
    except Exception as e:
        results["errors"].append(f"Needs BOL search error: {e}")
    
    # =========================================================================
    # 6. READY SHIP - Sent email with BOL/shipping keywords AND attachment
    # =========================================================================
    try:
        messages = search_emails(f'{time_filter} in:sent (BOL OR "R+L" OR RL OR UPS OR label) has:attachment')
        print(f"[GMAIL] Found {len(messages)} potential ready-ship emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                # Must have attachment
                if not email.get('has_attachment'):
                    continue
                
                text = email['subject'] + ' ' + email['body']
                
                # Must have BOL keywords
                if not has_bol_keywords(text):
                    continue
                
                order_ids = extract_order_ids(text)
                
                # Try to determine warehouse from recipient
                _, warehouse = is_warehouse_email(email['to'])
                
                for order_id in order_ids:
                    if update_shipment_status(db_conn, order_id, warehouse, 'ready_ship', email):
                        results["ready_ship"] += 1
                    
            except Exception as e:
                results["errors"].append(f"Ready ship detection error: {e}")
                
    except Exception as e:
        results["errors"].append(f"Ready ship search error: {e}")
    
    # =========================================================================
    # 7. DELIVERED - RL Carriers delivery notification with MPO
    # =========================================================================
    try:
        messages = search_emails(f'{time_filter} from:{RL_DELIVERY_SENDER}')
        print(f"[GMAIL] Found {len(messages)} RL delivery notification emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                # Check for "Delivered" status
                if 'delivered' not in email['subject'].lower() and 'delivered' not in email['body'].lower():
                    continue
                
                # Extract MPO (our order number)
                order_id = extract_mpo_from_rl_email(email.get('html_body', ''), email['body'])
                
                if order_id:
                    if update_order_complete(db_conn, order_id, email):
                        results["delivered"] += 1
                else:
                    # Edge case - MPO not found, send alert
                    send_alert_email(
                        f"Logic Issue with RL Delivery - MPO Missing",
                        f"RL Carriers delivery notification received but MPO field is empty or missing.\n\n"
                        f"Subject: {email['subject']}\n"
                        f"Date: {email['date']}\n\n"
                        f"Please check this delivery manually and update the order status."
                    )
                    results["alerts_sent"] += 1
                    
            except Exception as e:
                results["errors"].append(f"RL delivery detection error: {e}")
                
    except Exception as e:
        results["errors"].append(f"RL delivery search error: {e}")
    
    # =========================================================================
    # 8. RL Quote Numbers (existing)
    # =========================================================================
    try:
        messages = search_emails(f'{time_filter} ("RL Quote" OR "quote number" OR from:{RL_CARRIERS_SENDER})')
        print(f"[GMAIL] Found {len(messages)} potential RL quote emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                # Look for quote number pattern
                quote_match = re.search(r'(?:RL\s+)?Quote\s*(?:No|#)?[:\s]*(\d{6,10})', 
                                       email['body'], re.IGNORECASE)
                if quote_match:
                    quote_no = quote_match.group(1)
                    order_id = extract_order_id(email['subject'] + ' ' + email['body'])
                    if order_id:
                        update_order_rl_quote(db_conn, order_id, quote_no, email)
                        results["rl_quotes"] += 1
                        
            except Exception as e:
                results["errors"].append(f"RL quote error: {e}")
                
    except Exception as e:
        results["errors"].append(f"RL quote search error: {e}")
    
    # =========================================================================
    # 9. Tracking Numbers / PRO Numbers (existing)
    # =========================================================================
    try:
        messages = search_emails(f'{time_filter} (PRO OR tracking OR "has shipped")')
        print(f"[GMAIL] Found {len(messages)} potential tracking emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                text = email['subject'] + ' ' + email['body']
                
                # PRO number pattern
                pro_match = re.search(r'PRO\s*(?:#|Number)?[:\s]*([A-Z]{0,2}\d{8,10}(?:-\d)?)', 
                                     text, re.IGNORECASE)
                if pro_match:
                    pro_no = pro_match.group(1).upper()
                    order_id = extract_order_id(text)
                    if order_id:
                        update_order_tracking(db_conn, order_id, pro_no, 'PRO', email)
                        results["tracking_numbers"] += 1
                        continue
                
                # UPS tracking (1Z...)
                ups_match = re.search(r'\b(1Z[A-Z0-9]{16})\b', text)
                if ups_match:
                    order_id = extract_order_id(text)
                    if order_id:
                        update_order_tracking(db_conn, order_id, ups_match.group(1), 'UPS', email)
                        results["tracking_numbers"] += 1
                        
            except Exception as e:
                results["errors"].append(f"Tracking error: {e}")
                
    except Exception as e:
        results["errors"].append(f"Tracking search error: {e}")
    
    print(f"[GMAIL] Sync complete: {results}")
    return results

# =============================================================================
# DATABASE UPDATE FUNCTIONS
# =============================================================================

def update_order_payment_link_sent(conn, order_id, email):
    """Mark order as payment link sent"""
    with conn.cursor() as cur:
        # Check if already marked
        cur.execute("SELECT payment_link_sent FROM orders WHERE order_id = %s", (order_id,))
        row = cur.fetchone()
        if not row:
            print(f"[GMAIL] Order {order_id} not found")
            return False
        if row[0]:  # Already marked
            return False
        
        cur.execute("""
            UPDATE orders SET 
                payment_link_sent = TRUE,
                payment_link_sent_at = NOW(),
                updated_at = NOW()
            WHERE order_id = %s
        """, (order_id,))
        
        # Log event
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source)
            VALUES (%s, 'payment_link_sent', %s, 'gmail_sync')
        """, (order_id, json.dumps({'subject': email['subject'][:100]})))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: payment link sent")
        return True

def match_payment_to_order(conn, amount, customer_name, email):
    """Try to match a Square payment to an order (backup to Square API sync)"""
    from psycopg2.extras import RealDictCursor
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Skip if Square API already handled this
        cur.execute("""
            SELECT order_id FROM orders 
            WHERE payment_received = TRUE 
            AND payment_amount = %s
            AND payment_received_at > NOW() - INTERVAL '1 hour'
        """, (amount,))
        if cur.fetchone():
            return False  # Already processed by Square API
        
        # Try to find matching order by amount and customer name
        cur.execute("""
            SELECT order_id, customer_name, company_name, order_total, payment_received
            FROM orders 
            WHERE payment_received = FALSE
            AND (
                LOWER(customer_name) LIKE LOWER(%s)
                OR LOWER(company_name) LIKE LOWER(%s)
            )
            ORDER BY order_date DESC
            LIMIT 5
        """, (f'%{customer_name.split()[0]}%', f'%{customer_name.split()[0]}%'))
        
        candidates = cur.fetchall()
        
        for order in candidates:
            order_total = float(order['order_total'] or 0)
            # Payment might include shipping, so check if amount >= order total
            if amount >= order_total * 0.95:  # Allow 5% variance
                # Found match
                cur.execute("""
                    UPDATE orders SET 
                        payment_received = TRUE,
                        payment_received_at = NOW(),
                        payment_amount = %s,
                        updated_at = NOW()
                    WHERE order_id = %s
                """, (amount, order['order_id']))
                
                cur.execute("""
                    INSERT INTO order_events (order_id, event_type, event_data, source)
                    VALUES (%s, 'payment_received', %s, 'gmail_sync')
                """, (order['order_id'], json.dumps({
                    'amount': amount, 
                    'customer': customer_name,
                    'subject': email['subject'][:100]
                })))
                
                conn.commit()
                print(f"[GMAIL] Order {order['order_id']}: payment ${amount} received from {customer_name}")
                return True
        
        print(f"[GMAIL] No match for payment ${amount} from {customer_name}")
        return False

def update_shipment_status(conn, order_id: str, warehouse: Optional[str], new_status: str, email: dict) -> bool:
    """
    Update shipment status for a specific warehouse.
    If warehouse is None, updates the order-level status instead.
    
    Respects sticky manual overrides - only updates if:
    1. Current status is earlier in workflow, OR
    2. This is the first time we're setting this shipment's status
    """
    status_order = ['needs_order', 'at_warehouse', 'needs_bol', 'ready_ship', 'shipped', 'delivered']
    
    with conn.cursor() as cur:
        # Check if order exists
        cur.execute("SELECT order_id FROM orders WHERE order_id = %s", (order_id,))
        if not cur.fetchone():
            print(f"[GMAIL] Order {order_id} not found")
            return False
        
        if warehouse:
            # Update specific shipment
            shipment_id = f"{order_id}-{warehouse.replace(' ', '_')}"
            
            # Check current shipment status
            cur.execute("""
                SELECT status FROM order_shipments WHERE shipment_id = %s
            """, (shipment_id,))
            row = cur.fetchone()
            
            if row:
                current_status = row[0]
                current_idx = status_order.index(current_status) if current_status in status_order else -1
                new_idx = status_order.index(new_status) if new_status in status_order else -1
                
                # Only advance forward (email triggers can advance status)
                if new_idx <= current_idx:
                    print(f"[GMAIL] Shipment {shipment_id} already at {current_status}, not updating to {new_status}")
                    return False
            
            # Upsert shipment record
            cur.execute("""
                INSERT INTO order_shipments (order_id, shipment_id, warehouse, status, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (shipment_id) DO UPDATE SET
                    status = EXCLUDED.status,
                    updated_at = NOW()
            """, (order_id, shipment_id, warehouse, new_status))
            
            # Log event
            cur.execute("""
                INSERT INTO order_events (order_id, event_type, event_data, source)
                VALUES (%s, %s, %s, 'gmail_sync')
            """, (order_id, f'shipment_{new_status}', json.dumps({
                'warehouse': warehouse,
                'shipment_id': shipment_id,
                'subject': email['subject'][:100]
            })))
            
            conn.commit()
            print(f"[GMAIL] Shipment {shipment_id}: {new_status}")
            return True
        else:
            # No warehouse specified - update order level
            # This is a fallback, shouldn't happen often
            cur.execute("""
                UPDATE orders SET 
                    sent_to_warehouse = CASE WHEN %s = 'at_warehouse' THEN TRUE ELSE sent_to_warehouse END,
                    bol_sent = CASE WHEN %s = 'ready_ship' THEN TRUE ELSE bol_sent END,
                    updated_at = NOW()
                WHERE order_id = %s
            """, (new_status, new_status, order_id))
            
            conn.commit()
            print(f"[GMAIL] Order {order_id}: {new_status} (no warehouse specified)")
            return True

def update_order_complete(conn, order_id: str, email: dict) -> bool:
    """Mark order as complete (delivered)"""
    with conn.cursor() as cur:
        cur.execute("SELECT is_complete FROM orders WHERE order_id = %s", (order_id,))
        row = cur.fetchone()
        if not row:
            print(f"[GMAIL] Order {order_id} not found")
            return False
        if row[0]:  # Already complete
            return False
        
        cur.execute("""
            UPDATE orders SET 
                is_complete = TRUE,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE order_id = %s
        """, (order_id,))
        
        # Mark all shipments as delivered
        cur.execute("""
            UPDATE order_shipments SET 
                status = 'delivered',
                updated_at = NOW()
            WHERE order_id = %s
        """, (order_id,))
        
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source)
            VALUES (%s, 'delivered', %s, 'gmail_sync')
        """, (order_id, json.dumps({'subject': email['subject'][:100], 'carrier': 'RL Carriers'})))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: DELIVERED (RL Carriers)")
        return True

def update_order_rl_quote(conn, order_id, quote_no, email):
    """Update order with RL quote number"""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE orders SET 
                rl_quote_no = %s,
                updated_at = NOW()
            WHERE order_id = %s
        """, (quote_no, order_id))
        
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source)
            VALUES (%s, 'rl_quote_captured', %s, 'gmail_sync')
        """, (order_id, json.dumps({'quote_no': quote_no})))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: RL quote {quote_no}")
        return True

def update_order_tracking(conn, order_id, tracking_no, carrier, email):
    """Update order with tracking number"""
    with conn.cursor() as cur:
        tracking_text = f"{carrier} {tracking_no}" if carrier != 'PRO' else f"R+L PRO {tracking_no}"
        
        cur.execute("""
            UPDATE orders SET 
                tracking = %s,
                pro_number = CASE WHEN %s = 'PRO' THEN %s ELSE pro_number END,
                updated_at = NOW()
            WHERE order_id = %s
        """, (tracking_text, carrier, tracking_no, order_id))
        
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source)
            VALUES (%s, 'tracking_captured', %s, 'gmail_sync')
        """, (order_id, json.dumps({'tracking': tracking_no, 'carrier': carrier})))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: {carrier} tracking {tracking_no}")
        return True
