"""
gmail_sync.py
Gmail email scanning for CFC Order Workflow
Detects: Payment links sent, Payments received (Square), RL Quotes, Tracking numbers, LI Invoices
"""

import os
import re
import json
import base64
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

def parse_email_date(date_str):
    """Parse email date header into datetime. Returns None if parsing fails."""
    if not date_str:
        return None
    try:
        return parsedate_to_datetime(date_str)
    except Exception:
        # Try some common formats
        for fmt in ['%a, %d %b %Y %H:%M:%S %z', '%d %b %Y %H:%M:%S %z', '%Y-%m-%d %H:%M:%S']:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except:
                continue
        return None

def reactivate_if_archived(conn, order_id, email, reason):
    """
    Check if order is archived (is_complete=true) and reactivate it.
    This handles cases where customers email about completed orders.
    Returns True if order was reactivated, False otherwise.
    """
    with conn.cursor() as cur:
        # Check if order exists and is complete
        cur.execute("""
            SELECT is_complete, current_status FROM orders WHERE order_id = %s
        """, (order_id,))
        row = cur.fetchone()
        
        if not row:
            return False  # Order doesn't exist
        
        is_complete, current_status = row
        
        if not is_complete:
            return False  # Order is already active, nothing to do
        
        # Reactivate the order
        email_date = parse_email_date(email.get('date')) if email else None
        
        cur.execute("""
            UPDATE orders 
            SET is_complete = FALSE,
                completed_at = NULL,
                current_status = 'awaiting_shipment',
                updated_at = NOW()
            WHERE order_id = %s
        """, (order_id,))
        
        # Reset shipments to ready_ship status
        cur.execute("""
            UPDATE order_shipments 
            SET status = 'ready_ship',
                updated_at = NOW()
            WHERE order_id = %s
        """, (order_id,))
        
        # Log the reactivation
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source, created_at)
            VALUES (%s, 'order_reactivated', %s, 'gmail_sync', COALESCE(%s, NOW()))
        """, (order_id, json.dumps({
            'reason': reason,
            'email_subject': email.get('subject', '')[:100] if email else '',
            'previous_status': current_status
        }), email_date))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: REACTIVATED from archive - {reason}")
        return True

# Gmail API Config - loaded from environment
GMAIL_CLIENT_ID = os.environ.get("GMAIL_CLIENT_ID", "").strip()
GMAIL_CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET", "").strip()
GMAIL_REFRESH_TOKEN = os.environ.get("GMAIL_REFRESH_TOKEN", "").strip()

# LI Gmail (cfcinvoices42@gmail.com) - for reading LI invoices
GMAIL_LI_REFRESH_TOKEN = os.environ.get("GMAIL_LI_REFRESH_TOKEN", "").strip()

# Email sender patterns
SQUARE_PAYMENT_SENDER = "noreply@messaging.squareup.com"
RL_CARRIERS_SENDER = "rlloads@rlcarriers.com"

# Cache access tokens
_access_token = None
_token_expires = None
_li_access_token = None
_li_token_expires = None

def gmail_configured():
    """Check if Gmail credentials are configured"""
    return bool(GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET and GMAIL_REFRESH_TOKEN)

def gmail_li_configured():
    """Check if LI Gmail credentials are configured"""
    return bool(GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET and GMAIL_LI_REFRESH_TOKEN)

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

def get_li_gmail_access_token():
    """Get access token for LI Gmail (cfcinvoices42@gmail.com)"""
    global _li_access_token, _li_token_expires
    
    if _li_access_token and _li_token_expires and datetime.now(timezone.utc) < _li_token_expires:
        return _li_access_token
    
    if not gmail_li_configured():
        print("[GMAIL-LI] Not configured")
        return None
    
    try:
        token_data = urllib.parse.urlencode({
            'client_id': GMAIL_CLIENT_ID,
            'client_secret': GMAIL_CLIENT_SECRET,
            'refresh_token': GMAIL_LI_REFRESH_TOKEN,
            'grant_type': 'refresh_token'
        }).encode()
        
        req = urllib.request.Request('https://oauth2.googleapis.com/token', data=token_data)
        
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            _li_access_token = data.get('access_token')
            _li_token_expires = datetime.now(timezone.utc) + timedelta(minutes=50)
            return _li_access_token
            
    except Exception as e:
        print(f"[GMAIL-LI] Token refresh error: {e}")
        return None

def gmail_api_request(endpoint, params=None):
    """Make authenticated request to Gmail API"""
    token = get_gmail_access_token()
    if not token:
        return None
    
    url = f"https://gmail.googleapis.com/gmail/v1/users/me/{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        print(f"[GMAIL] API error {e.code}: {e.read().decode()[:200]}")
        return None
    except Exception as e:
        print(f"[GMAIL] Request error: {e}")
        return None

def li_gmail_api_request(endpoint, params=None):
    """Make authenticated request to LI Gmail API (cfcinvoices42)"""
    token = get_li_gmail_access_token()
    if not token:
        return None
    
    url = f"https://gmail.googleapis.com/gmail/v1/users/me/{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as e:
        print(f"[GMAIL-LI] API error {e.code}: {e.read().decode()[:200]}")
        return None
    except Exception as e:
        print(f"[GMAIL-LI] Request error: {e}")
        return None

def search_emails(query, max_results=50):
    """Search Gmail for messages matching query"""
    data = gmail_api_request("messages", {"q": query, "maxResults": max_results})
    if not data:
        return []
    return data.get("messages", [])

def search_li_emails(query, max_results=50):
    """Search LI Gmail (cfcinvoices42) for messages"""
    data = li_gmail_api_request("messages", {"q": query, "maxResults": max_results})
    if not data:
        return []
    return data.get("messages", [])

def get_li_email_content(message_id):
    """Get email content from LI Gmail"""
    data = li_gmail_api_request(f"messages/{message_id}", {"format": "full"})
    if not data:
        return None
    
    headers = {h['name'].lower(): h['value'] for h in data.get('payload', {}).get('headers', [])}
    
    body = ""
    html_body = ""
    payload = data.get('payload', {})
    
    if 'body' in payload and payload['body'].get('data'):
        body = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
    elif 'parts' in payload:
        for part in payload['parts']:
            mime_type = part.get('mimeType', '')
            part_data = part.get('body', {}).get('data')
            
            if mime_type == 'text/plain' and part_data:
                body = base64.urlsafe_b64decode(part_data).decode('utf-8', errors='ignore')
            elif mime_type == 'text/html' and part_data and not html_body:
                html_body = base64.urlsafe_b64decode(part_data).decode('utf-8', errors='ignore')
            
            if 'parts' in part:
                for subpart in part['parts']:
                    sub_mime = subpart.get('mimeType', '')
                    sub_data = subpart.get('body', {}).get('data')
                    if sub_mime == 'text/plain' and sub_data:
                        body = base64.urlsafe_b64decode(sub_data).decode('utf-8', errors='ignore')
                    elif sub_mime == 'text/html' and sub_data and not html_body:
                        html_body = base64.urlsafe_b64decode(sub_data).decode('utf-8', errors='ignore')
    
    if not body and html_body:
        body = re.sub(r'<[^>]+>', ' ', html_body)
        body = re.sub(r'\s+', ' ', body).strip()
    
    return {
        'id': message_id,
        'subject': headers.get('subject', ''),
        'from': headers.get('from', ''),
        'to': headers.get('to', ''),
        'date': headers.get('date', ''),
        'body': body
    }

def get_email_content(message_id):
    """Get email details including subject, from, body"""
    data = gmail_api_request(f"messages/{message_id}", {"format": "full"})
    if not data:
        return None
    
    headers = {h['name'].lower(): h['value'] for h in data.get('payload', {}).get('headers', [])}
    
    # Extract body - try text/plain first, then text/html
    body = ""
    html_body = ""
    payload = data.get('payload', {})
    
    if 'body' in payload and payload['body'].get('data'):
        body = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8', errors='ignore')
    elif 'parts' in payload:
        for part in payload['parts']:
            mime_type = part.get('mimeType', '')
            part_data = part.get('body', {}).get('data')
            
            if mime_type == 'text/plain' and part_data:
                body = base64.urlsafe_b64decode(part_data).decode('utf-8', errors='ignore')
            elif mime_type == 'text/html' and part_data and not html_body:
                html_body = base64.urlsafe_b64decode(part_data).decode('utf-8', errors='ignore')
            
            # Check nested parts (for multipart/alternative inside multipart/mixed)
            if 'parts' in part:
                for subpart in part['parts']:
                    sub_mime = subpart.get('mimeType', '')
                    sub_data = subpart.get('body', {}).get('data')
                    if sub_mime == 'text/plain' and sub_data:
                        body = base64.urlsafe_b64decode(sub_data).decode('utf-8', errors='ignore')
                    elif sub_mime == 'text/html' and sub_data and not html_body:
                        html_body = base64.urlsafe_b64decode(sub_data).decode('utf-8', errors='ignore')
    
    # If no plain text, use HTML (strip tags for basic text extraction)
    if not body and html_body:
        # Basic HTML tag stripping
        body = re.sub(r'<[^>]+>', ' ', html_body)
        body = re.sub(r'\s+', ' ', body).strip()
    
    return {
        'id': message_id,
        'subject': headers.get('subject', ''),
        'from': headers.get('from', ''),
        'to': headers.get('to', ''),
        'date': headers.get('date', ''),
        'body': body
    }

def extract_order_id(text):
    """Extract order ID from text (4-5 digit number)"""
    # Look for patterns like "order 5307" or "#5307" or "Order #5307"
    match = re.search(r'(?:order\s*#?\s*|#)(\d{4,5})\b', text, re.IGNORECASE)
    if match:
        return match.group(1)
    
    # Try standalone 4-5 digit numbers (less reliable)
    match = re.search(r'\b(\d{4,5})\b', text)
    if match:
        return match.group(1)
    
    return None

def extract_payment_amount(text):
    """Extract dollar amount from text"""
    match = re.search(r'\$([\d,]+\.?\d*)', text)
    if match:
        return float(match.group(1).replace(',', ''))
    return None

def extract_customer_name(text):
    """Extract customer name from Square payment email"""
    # Pattern: "$X payment received from Name"
    match = re.search(r'payment received from\s+([^\n]+)', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None

# Import needed for base64 in get_email_content
import urllib.parse

def run_gmail_sync(db_conn, hours_back=2):
    """
    Main email sync function - scans Gmail and updates orders
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
        "rl_delivered": 0,
        "saia_delivered": 0,
        "shipped_detected": 0,
        "canceled": 0,
        "reactivated": 0,
        "errors": []
    }
    
    time_filter = f"newer_than:{hours_back}h"
    
    # 1. Payment Links Sent (sent emails with square.link)
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
                    update_order_payment_link_sent(db_conn, order_id, email)
                    results["payment_links"] += 1
                    
            except Exception as e:
                results["errors"].append(f"Payment link error: {e}")
                try:
                    db_conn.rollback()
                except:
                    pass
                
    except Exception as e:
        results["errors"].append(f"Payment link search error: {e}")
    
    # Clean transaction state
    try:
        db_conn.rollback()
    except:
        pass
    
    # 2. Payments Received (Square notifications)
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
                    matched = match_payment_to_order(db_conn, amount, customer_name, email)
                    if matched:
                        results["payments_received"] += 1
                        
            except Exception as e:
                results["errors"].append(f"Payment received error: {e}")
                
    except Exception as e:
        results["errors"].append(f"Payment search error: {e}")
    
    # Clean transaction state
    try:
        db_conn.rollback()
    except:
        pass
    
    # 3. RL Quote Numbers
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
    
    # Clean transaction state before tracking
    try:
        db_conn.rollback()
    except:
        pass
    
    # 4. Tracking Numbers / PRO Numbers
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
                try:
                    db_conn.rollback()  # Recover from aborted transaction
                except:
                    pass
                
    except Exception as e:
        results["errors"].append(f"Tracking search error: {e}")
    
    # Ensure clean transaction state before LI invoices
    try:
        db_conn.rollback()
    except:
        pass
    
    # 5. LI Invoices (read directly from cfcinvoices42@gmail.com)
    results["li_invoices"] = 0
    
    if not gmail_li_configured():
        print("[GMAIL-LI] LI Gmail not configured, skipping LI invoice sync")
    else:
        try:
            # Search cfcinvoices42@gmail.com for LI invoices
            messages = search_li_emails(f'{time_filter} subject:"Cabinetry Distribution"')
            print(f"[GMAIL-LI] Found {len(messages)} LI invoice emails in cfcinvoices42")
            
            for msg in messages:
                try:
                    email = get_li_email_content(msg['id'])
                    if not email:
                        print(f"[GMAIL-LI] Could not get content for message {msg['id']}")
                        continue
                    
                    print(f"[GMAIL-LI] Checking: {email['subject'][:60]}")
                    
                    # Must be an invoice from Cabinetry Distribution
                    subject_and_body = (email['subject'] + " " + email['body']).lower()
                    if 'cabinetry distribution' not in subject_and_body:
                        continue
                    
                    # Robust PO extraction - handles: Po 5305, PO: 5305, PO#5305, P.O. 5305
                    po_match = re.search(r'\bP\.?O\.?\s*[:#]?\s*(\d{4,6})\b', subject_and_body, re.IGNORECASE)
                    if po_match:
                        order_id = po_match.group(1)
                        print(f"[GMAIL-LI] LI Invoice for order {order_id}")
                        update_li_shipment_delivered(db_conn, order_id, email)
                        results["li_invoices"] += 1
                    else:
                        print(f"[GMAIL-LI] No PO number found in email")
                        
                except Exception as e:
                    results["errors"].append(f"LI invoice error: {e}")
                    print(f"[GMAIL-LI] LI invoice error: {e}")
                    try:
                        db_conn.rollback()
                    except:
                        pass
                    
        except Exception as e:
            results["errors"].append(f"LI invoice search error: {e}")
            print(f"[GMAIL-LI] LI invoice search error: {e}")
    
    # Clean transaction state
    try:
        db_conn.rollback()
    except:
        pass
    
    # 6. R+L Carriers Delivered (marks shipment as delivered)
    results["rl_delivered"] = 0
    try:
        messages = search_emails(f'{time_filter} from:rlcarriers "has been Delivered"')
        print(f"[GMAIL] Found {len(messages)} R+L delivered emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                text = email['subject'] + ' ' + email['body']
                
                # Check for "has been Delivered" in subject
                if 'has been delivered' not in text.lower():
                    continue
                
                # Extract PRO number from subject (format: PRO I655817778)
                pro_match = re.search(r'PRO\s+([A-Z]?\d{9,10})', text, re.IGNORECASE)
                pro_number = pro_match.group(1) if pro_match else None
                
                # Extract MPO (order number) from body - "MPO 5247" or "MPO    5305"
                mpo_match = re.search(r'MPO\s+(\d{4,5})', text, re.IGNORECASE)
                if mpo_match:
                    order_id = mpo_match.group(1)
                    update_shipment_delivered(db_conn, order_id, None, pro_number, 'rl_delivered_email', email)
                    results["rl_delivered"] += 1
                    print(f"[GMAIL] R+L Delivered: Order {order_id}")
                else:
                    # Try extracting order from 4-digit number starting with 5 (likely order, not weight)
                    order_match = re.search(r'\b(5\d{3})\b', text)
                    if order_match:
                        order_id = order_match.group(1)
                        update_shipment_delivered(db_conn, order_id, None, pro_number, 'rl_delivered_email', email)
                        results["rl_delivered"] += 1
                        print(f"[GMAIL] R+L Delivered (inferred): Order {order_id}")
                        
            except Exception as e:
                results["errors"].append(f"R+L delivered error: {e}")
                try:
                    db_conn.rollback()
                except:
                    pass
                
    except Exception as e:
        results["errors"].append(f"R+L delivered search error: {e}")
    
    # Clean transaction state
    try:
        db_conn.rollback()
    except:
        pass
    
    # 7. SAIA Delivered
    results["saia_delivered"] = 0
    try:
        messages = search_emails(f'{time_filter} (SAIA tracking delivered)')
        print(f"[GMAIL] Found {len(messages)} SAIA emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                text = email['subject'] + ' ' + email['body']
                
                # SAIA tracking pattern (12 digits)
                saia_match = re.search(r'SAIA\s+(\d{12})', text, re.IGNORECASE)
                if saia_match:
                    saia_tracking = saia_match.group(1)
                    order_id = extract_order_id(text)
                    if order_id:
                        # Check if delivered
                        if 'delivered' in text.lower():
                            update_shipment_delivered(db_conn, order_id, None, f"SAIA {saia_tracking}", 'saia_delivered', email)
                            results["saia_delivered"] += 1
                        else:
                            update_shipment_shipped(db_conn, order_id, None, f"SAIA {saia_tracking}", 'saia_tracking', email)
                            
            except Exception as e:
                results["errors"].append(f"SAIA error: {e}")
                try:
                    db_conn.rollback()
                except:
                    pass
                
    except Exception as e:
        results["errors"].append(f"SAIA search error: {e}")
    
    # Clean transaction state
    try:
        db_conn.rollback()
    except:
        pass
    
    # 8. Shipped indicators (various patterns that indicate order shipped)
    results["shipped_detected"] = 0
    try:
        # Search for shipping confirmation patterns
        messages = search_emails(f'{time_filter} ("has tracking" OR "UPS tracking" OR "UPS label" OR "ready for pick" OR "pickup code" OR "Shipping & Handling")')
        print(f"[GMAIL] Found {len(messages)} potential shipped indicator emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                text = (email['subject'] + ' ' + email['body']).lower()
                order_id = extract_order_id(email['subject'] + ' ' + email['body'])
                
                if not order_id:
                    continue
                
                tracking_info = None
                source = None
                
                # Check various shipped patterns
                if 'has tracking' in text or 'tracking' in email['subject'].lower():
                    # Look for tracking number in body
                    tracking_match = re.search(r'(?:tracking|SAIA)\s+(\d{10,12})', email['body'], re.IGNORECASE)
                    if tracking_match:
                        tracking_info = tracking_match.group(1)
                    source = 'has_tracking_email'
                    
                elif 'ups tracking' in text or 'ups label' in text:
                    # UPS tracking
                    ups_match = re.search(r'(1Z[A-Z0-9]{16})', email['body'])
                    if ups_match:
                        tracking_info = ups_match.group(1)
                    source = 'ups_tracking_email'
                    
                elif 'ready for pick' in text or 'pickup code' in text:
                    source = 'ready_for_pickup'
                    
                elif 'shipping & handling' in text:
                    # DL Cabinetry pattern
                    ship_match = re.search(r'shipping\s*&\s*handling\s*\$?([\d.]+)', text, re.IGNORECASE)
                    if ship_match:
                        source = 'dl_shipping_confirmation'
                
                elif 'please see' in text and 'attach' in text:
                    source = 'attachment_shipped'
                
                if source:
                    update_shipment_shipped(db_conn, order_id, None, tracking_info, source, email)
                    results["shipped_detected"] += 1
                    
            except Exception as e:
                results["errors"].append(f"Shipped detection error: {e}")
                try:
                    db_conn.rollback()
                except:
                    pass
                
    except Exception as e:
        results["errors"].append(f"Shipped detection search error: {e}")
    
    # Clean transaction state
    try:
        db_conn.rollback()
    except:
        pass
    
    # 9. Cancel detection
    results["canceled"] = 0
    try:
        messages = search_emails(f'{time_filter} (cancel OR canceled OR cancelled) order')
        print(f"[GMAIL] Found {len(messages)} potential cancel emails")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                text = (email['subject'] + ' ' + email['body']).lower()
                
                # Must have cancel keyword
                if 'cancel' not in text:
                    continue
                
                order_id = extract_order_id(email['subject'] + ' ' + email['body'])
                if order_id:
                    mark_order_canceled(db_conn, order_id, 'Email mentioned cancel', email)
                    results["canceled"] += 1
                    
            except Exception as e:
                results["errors"].append(f"Cancel detection error: {e}")
                try:
                    db_conn.rollback()
                except:
                    pass
                
    except Exception as e:
        results["errors"].append(f"Cancel search error: {e}")
    
    # Clean transaction state
    try:
        db_conn.rollback()
    except:
        pass
    
    # 10. Reactivate archived orders with recent email activity
    # Only look at last 48 hours to avoid reactivating old orders
    results["reactivated"] = 0
    try:
        recent_filter = "newer_than:48h"
        messages = search_emails(f'{recent_filter} (order OR PO OR #)')
        print(f"[GMAIL] Checking {len(messages)} recent emails for archived order reactivation")
        
        for msg in messages:
            try:
                email = get_email_content(msg['id'])
                if not email:
                    continue
                
                order_id = extract_order_id(email['subject'] + ' ' + email['body'])
                if order_id:
                    if reactivate_if_archived(db_conn, order_id, email, 'New email received'):
                        results["reactivated"] += 1
                        
            except Exception as e:
                results["errors"].append(f"Reactivation error: {e}")
                try:
                    db_conn.rollback()
                except:
                    pass
                
    except Exception as e:
        results["errors"].append(f"Reactivation search error: {e}")
    
    print(f"[GMAIL] Sync complete: {results}")
    return results

# =============================================================================
# DATABASE UPDATE FUNCTIONS
# =============================================================================

def update_order_payment_link_sent(conn, order_id, email):
    """Mark order as payment link sent"""
    from psycopg2.extras import RealDictCursor
    
    with conn.cursor() as cur:
        # Check if already marked
        cur.execute("SELECT payment_link_sent FROM orders WHERE order_id = %s", (order_id,))
        row = cur.fetchone()
        if not row:
            print(f"[GMAIL] Order {order_id} not found")
            return False
        if row[0]:  # Already marked
            return False
        
        # Parse email date
        email_date = parse_email_date(email.get('date'))
        
        cur.execute("""
            UPDATE orders SET 
                payment_link_sent = TRUE,
                payment_link_sent_at = COALESCE(%s, NOW()),
                updated_at = NOW()
            WHERE order_id = %s
        """, (email_date, order_id))
        
        # Log event with actual email date
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source, created_at)
            VALUES (%s, 'payment_link_sent', %s, 'gmail_sync', COALESCE(%s, NOW()))
        """, (order_id, json.dumps({'subject': email['subject'][:100], 'email_date': email.get('date', '')}), email_date))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: payment link sent")
        return True

def match_payment_to_order(conn, amount, customer_name, email):
    """Try to match a Square payment to an order"""
    from psycopg2.extras import RealDictCursor
    
    email_date = parse_email_date(email.get('date'))
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
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
                        payment_received_at = COALESCE(%s, NOW()),
                        payment_amount = %s,
                        updated_at = NOW()
                    WHERE order_id = %s
                """, (email_date, amount, order['order_id']))
                
                cur.execute("""
                    INSERT INTO order_events (order_id, event_type, event_data, source, created_at)
                    VALUES (%s, 'payment_received', %s, 'gmail_sync', COALESCE(%s, NOW()))
                """, (order['order_id'], json.dumps({
                    'amount': amount, 
                    'customer': customer_name,
                    'subject': email['subject'][:100]
                }), email_date))
                
                conn.commit()
                print(f"[GMAIL] Order {order['order_id']}: payment ${amount} received from {customer_name}")
                return True
        
        print(f"[GMAIL] No match for payment ${amount} from {customer_name}")
        return False

def update_order_rl_quote(conn, order_id, quote_no, email):
    """Update order with RL quote number"""
    email_date = parse_email_date(email.get('date')) if email else None
    
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE orders SET 
                rl_quote_no = %s,
                updated_at = NOW()
            WHERE order_id = %s
        """, (quote_no, order_id))
        
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source, created_at)
            VALUES (%s, 'rl_quote_captured', %s, 'gmail_sync', COALESCE(%s, NOW()))
        """, (order_id, json.dumps({'quote_no': quote_no}), email_date))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: RL quote {quote_no}")
        return True

def update_order_tracking(conn, order_id, tracking_no, carrier, email):
    """Update order with tracking number"""
    email_date = parse_email_date(email.get('date')) if email else None
    
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
            INSERT INTO order_events (order_id, event_type, event_data, source, created_at)
            VALUES (%s, 'tracking_captured', %s, 'gmail_sync', COALESCE(%s, NOW()))
        """, (order_id, json.dumps({'tracking': tracking_no, 'carrier': carrier}), email_date))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: {carrier} tracking {tracking_no}")
        return True

def update_li_shipment_delivered(conn, order_id, email):
    """Mark LI shipment as delivered when invoice received"""
    email_date = parse_email_date(email.get('date')) if email else None
    
    with conn.cursor() as cur:
        # Check if shipment exists for this order with LI warehouse
        cur.execute("""
            SELECT id, status FROM order_shipments 
            WHERE order_id = %s AND warehouse = 'LI'
        """, (order_id,))
        
        row = cur.fetchone()
        if not row:
            print(f"[GMAIL] No LI shipment found for order {order_id}")
            return False
        
        shipment_id, current_status = row
        
        # Only update if not already delivered
        if current_status == 'delivered':
            print(f"[GMAIL] Order {order_id} LI shipment already delivered")
            return False
        
        # Update shipment status to delivered
        cur.execute("""
            UPDATE order_shipments 
            SET status = 'delivered',
                delivered_at = COALESCE(%s, NOW()),
                updated_at = NOW()
            WHERE order_id = %s AND warehouse = 'LI'
        """, (email_date, order_id))
        
        # Log event with actual email date
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source, created_at)
            VALUES (%s, 'li_invoice_received', %s, 'gmail_sync', COALESCE(%s, NOW()))
        """, (order_id, json.dumps({
            'email_subject': email.get('subject', ''),
            'warehouse': 'LI'
        }), email_date))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: LI shipment marked delivered (invoice received)")
        return True

def update_shipment_delivered(conn, order_id, warehouse, tracking_info, source_detail, email=None):
    """Mark any shipment as delivered"""
    email_date = parse_email_date(email.get('date')) if email else None
    
    with conn.cursor() as cur:
        # Check if shipment exists
        cur.execute("""
            SELECT id, status FROM order_shipments 
            WHERE order_id = %s AND warehouse = %s
        """, (order_id, warehouse))
        
        row = cur.fetchone()
        if not row:
            # Try without warehouse (mark first undelivered shipment)
            cur.execute("""
                SELECT id, status, warehouse FROM order_shipments 
                WHERE order_id = %s AND status != 'delivered'
                ORDER BY id LIMIT 1
            """, (order_id,))
            row = cur.fetchone()
            if row:
                warehouse = row[2]
            else:
                print(f"[GMAIL] No shipment found for order {order_id}")
                return False
        
        shipment_id, current_status = row[0], row[1]
        
        if current_status == 'delivered':
            return False
        
        cur.execute("""
            UPDATE order_shipments 
            SET status = 'delivered',
                delivered_at = COALESCE(%s, NOW()),
                updated_at = NOW(),
                tracking_number = COALESCE(tracking_number, %s)
            WHERE order_id = %s AND warehouse = %s
        """, (email_date, tracking_info, order_id, warehouse))
        
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source, created_at)
            VALUES (%s, 'shipment_delivered', %s, 'gmail_sync', COALESCE(%s, NOW()))
        """, (order_id, json.dumps({
            'warehouse': warehouse,
            'tracking': tracking_info,
            'source': source_detail
        }), email_date))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: {warehouse} shipment marked delivered")
        return True

def update_shipment_shipped(conn, order_id, warehouse, tracking_info, source_detail, email=None):
    """Mark shipment as shipped"""
    email_date = parse_email_date(email.get('date')) if email else None
    
    with conn.cursor() as cur:
        # If warehouse specified, use it
        if warehouse:
            cur.execute("""
                SELECT id, status FROM order_shipments 
                WHERE order_id = %s AND warehouse = %s
            """, (order_id, warehouse))
        else:
            # Find first unshipped shipment
            cur.execute("""
                SELECT id, status, warehouse FROM order_shipments 
                WHERE order_id = %s AND status NOT IN ('shipped', 'delivered')
                ORDER BY id LIMIT 1
            """, (order_id,))
        
        row = cur.fetchone()
        if not row:
            print(f"[GMAIL] No shipment found for order {order_id}")
            return False
        
        if not warehouse:
            warehouse = row[2] if len(row) > 2 else None
        
        current_status = row[1]
        if current_status in ('shipped', 'delivered'):
            return False
        
        cur.execute("""
            UPDATE order_shipments 
            SET status = 'shipped',
                shipped_at = COALESCE(%s, NOW()),
                updated_at = NOW(),
                tracking_number = COALESCE(tracking_number, %s)
            WHERE order_id = %s AND warehouse = %s
        """, (email_date, tracking_info, order_id, warehouse))
        
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source, created_at)
            VALUES (%s, 'shipment_shipped', %s, 'gmail_sync', COALESCE(%s, NOW()))
        """, (order_id, json.dumps({
            'warehouse': warehouse,
            'tracking': tracking_info,
            'source': source_detail
        }), email_date))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: {warehouse} shipment marked shipped")
        return True

def mark_order_canceled(conn, order_id, reason, email=None):
    """Mark order as canceled"""
    email_date = parse_email_date(email.get('date')) if email else None
    
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE orders 
            SET is_complete = TRUE,
                completed_at = COALESCE(%s, NOW()),
                notes = CONCAT(COALESCE(notes, ''), ' [CANCELED: ', %s, ']'),
                updated_at = NOW()
            WHERE order_id = %s
        """, (email_date, reason, order_id))
        
        cur.execute("""
            INSERT INTO order_events (order_id, event_type, event_data, source, created_at)
            VALUES (%s, 'order_canceled', %s, 'gmail_sync', COALESCE(%s, NOW()))
        """, (order_id, json.dumps({'reason': reason}), email_date))
        
        conn.commit()
        print(f"[GMAIL] Order {order_id}: marked canceled - {reason}")
        return True
