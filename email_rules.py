"""
email_rules.py
Email-based order status detection rules for CFC Orders
Processes emails to detect: delivered, shipped, canceled, ready for pickup
"""

import re
from datetime import datetime, timezone, timedelta

# =============================================================================
# ORDER NUMBER EXTRACTION
# =============================================================================

def extract_order_number(text):
    """
    Extract order number from email text with multiple patterns
    Priority order:
    1. MPO pattern (R+L emails): MPO 5247
    2. Explicit patterns: #5247, PO 5247, -(#5247), Order #5247
    3. 4-digit number starting with 5 (likely order, not weight)
    """
    text = text or ''
    
    # Pattern 1: MPO (R+L Carriers)
    mpo_match = re.search(r'MPO\s*[:#]?\s*(\d{4,5})', text, re.IGNORECASE)
    if mpo_match:
        return mpo_match.group(1)
    
    # Pattern 2: Explicit order patterns
    explicit_patterns = [
        r'#(\d{4,5})\b',                    # #5247
        r'PO\s*[:#]?\s*(\d{4,5})',           # PO 5247, PO: 5247
        r'-\(#(\d{4,5})\)',                  # -(#5247)
        r'Order\s*#?\s*(\d{4,5})',           # Order 5247, Order #5247
        r'JOB\s*Name\s*#?(\d{4,5})',         # JOB Name #5184
    ]
    
    for pattern in explicit_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    
    # Pattern 3: 4-digit starting with 5 (order numbers, not weights)
    # Orders starting with 5xxx are common, weights rarely start with 5
    five_match = re.search(r'\b(5\d{3})\b', text)
    if five_match:
        return five_match.group(1)
    
    # Pattern 4: Any 4-5 digit number as fallback
    any_match = re.search(r'\b(\d{4,5})\b', text)
    if any_match:
        return any_match.group(1)
    
    return None

# =============================================================================
# STATUS DETECTION RULES
# =============================================================================

def detect_delivered(subject, body):
    """
    Detect if email indicates order was delivered
    Returns: (is_delivered, order_id, carrier)
    """
    text = f"{subject} {body}".lower()
    original_text = f"{subject} {body}"
    
    # R+L Delivered: "R+L Carriers PRO I655817778 has been Delivered"
    if 'has been delivered' in text and ('r+l' in text or 'r&l' in text or 'rl carriers' in text):
        order_id = extract_order_number(original_text)
        # Extract PRO number
        pro_match = re.search(r'PRO\s*[:#]?\s*([A-Z]?\d{8,12})', original_text, re.IGNORECASE)
        pro_number = pro_match.group(1) if pro_match else None
        return True, order_id, 'R+L', pro_number
    
    # SAIA Delivered
    if 'delivered' in text and 'saia' in text:
        order_id = extract_order_number(original_text)
        tracking_match = re.search(r'SAIA\s*[:#]?\s*(\d{10,15})', original_text, re.IGNORECASE)
        tracking = tracking_match.group(1) if tracking_match else None
        return True, order_id, 'SAIA', tracking
    
    # Generic delivered
    if 'has been delivered' in text or 'was delivered' in text:
        order_id = extract_order_number(original_text)
        return True, order_id, 'Unknown', None
    
    return False, None, None, None


def detect_canceled(subject, body):
    """
    Detect if email indicates order was canceled
    Returns: (is_canceled, order_id)
    """
    text = f"{subject} {body}".lower()
    original_text = f"{subject} {body}"
    
    cancel_keywords = ['cancel', 'canceled', 'cancelled', 'cancellation']
    
    for keyword in cancel_keywords:
        if keyword in text:
            order_id = extract_order_number(original_text)
            return True, order_id
    
    return False, None


def detect_shipped(subject, body, has_attachment=False):
    """
    Detect if email indicates order was shipped
    Returns: (is_shipped, order_id, ship_method, tracking)
    """
    text = f"{subject} {body}".lower()
    original_text = f"{subject} {body}"
    
    ship_indicators = []
    tracking = None
    ship_method = None
    
    # Check for tracking numbers first
    # UPS: 1Z...
    ups_match = re.search(r'\b(1Z[A-Z0-9]{16})\b', original_text)
    if ups_match:
        tracking = ups_match.group(1)
        ship_method = 'UPS'
        ship_indicators.append('ups_tracking')
    
    # R+L PRO
    pro_match = re.search(r'PRO\s*[:#]?\s*([A-Z]?\d{8,12})', original_text, re.IGNORECASE)
    if pro_match:
        tracking = pro_match.group(1)
        ship_method = 'R+L'
        ship_indicators.append('pro_number')
    
    # SAIA
    saia_match = re.search(r'SAIA\s*[:#]?\s*(\d{10,15})', original_text, re.IGNORECASE)
    if saia_match:
        tracking = saia_match.group(1)
        ship_method = 'SAIA'
        ship_indicators.append('saia_tracking')
    
    # Generic tracking mention
    if 'tracking' in text and ('ups' in text or 'fedex' in text or 'usps' in text):
        ship_indicators.append('tracking_mentioned')
        if 'ups' in text:
            ship_method = 'UPS'
        elif 'fedex' in text:
            ship_method = 'FedEx'
    
    # "UPS tracking # is" pattern
    if 'ups tracking' in text:
        ship_indicators.append('ups_tracking_mentioned')
        ship_method = 'UPS'
    
    # Attachment-based indicators
    if has_attachment:
        if 'please see the attached' in text or 'see attached' in text:
            ship_indicators.append('attachment_shipped')
        if 'attaching the ups label' in text or 'attached ups label' in text:
            ship_indicators.append('ups_label_attached')
            ship_method = 'UPS'
        if 'attaching' in text and 'label' in text:
            ship_indicators.append('label_attached')
    
    # DL Cabinetry: "Shipping & Handling $XXX.XX"
    if re.search(r'shipping\s*[&]\s*handling\s*\$[\d,.]+', text):
        ship_indicators.append('shipping_handling_charge')
    
    # Ready for pickup
    pickup_patterns = [
        'ready for pick up',
        'ready for pickup', 
        'pickup code',
        'pick up code',
        'available for pickup',
        'ready to be picked up'
    ]
    for pattern in pickup_patterns:
        if pattern in text:
            ship_indicators.append('ready_pickup')
            ship_method = 'Customer Pickup'
            break
    
    # Determine if shipped
    is_shipped = len(ship_indicators) > 0
    order_id = extract_order_number(original_text) if is_shipped else None
    
    return is_shipped, order_id, ship_method, tracking, ship_indicators


def process_email(subject, body, email_date, has_attachment=False):
    """
    Process an email and determine what action to take
    Returns dict with action and details
    """
    result = {
        'action': None,
        'order_id': None,
        'details': {}
    }
    
    # Check for delivered (highest priority - immediate complete)
    is_delivered, order_id, carrier, tracking = detect_delivered(subject, body)
    if is_delivered and order_id:
        result['action'] = 'mark_delivered'
        result['order_id'] = order_id
        result['details'] = {
            'carrier': carrier,
            'tracking': tracking,
            'source': 'email_delivered'
        }
        return result
    
    # Check for canceled
    is_canceled, order_id = detect_canceled(subject, body)
    if is_canceled and order_id:
        result['action'] = 'mark_canceled'
        result['order_id'] = order_id
        result['details'] = {
            'source': 'email_canceled'
        }
        return result
    
    # Check for shipped (starts 5-day clock)
    is_shipped, order_id, ship_method, tracking, indicators = detect_shipped(subject, body, has_attachment)
    if is_shipped and order_id:
        result['action'] = 'mark_shipped'
        result['order_id'] = order_id
        result['details'] = {
            'ship_method': ship_method,
            'tracking': tracking,
            'indicators': indicators,
            'shipped_date': email_date,
            'source': 'email_shipped'
        }
        return result
    
    # No action detected, but might have order reference
    order_id = extract_order_number(f"{subject} {body}")
    if order_id:
        result['action'] = 'activity_detected'
        result['order_id'] = order_id
        result['details'] = {
            'email_date': email_date,
            'source': 'email_activity'
        }
    
    return result


# =============================================================================
# AUTO-COMPLETE LOGIC (5-day rule)
# =============================================================================

def check_auto_complete(order_id, last_shipped_date, last_email_date, current_time=None):
    """
    Check if order should be auto-completed based on 5-day rule:
    - If shipped AND no new emails for 5 days → complete
    - If new email arrives, reset 5-day clock
    
    Returns: (should_complete, days_since_last_activity)
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc)
    
    if not last_shipped_date:
        return False, None
    
    # Use the later of shipped date or last email date
    last_activity = last_email_date if last_email_date and last_email_date > last_shipped_date else last_shipped_date
    
    # Calculate days since last activity
    if isinstance(last_activity, str):
        last_activity = datetime.fromisoformat(last_activity.replace('Z', '+00:00'))
    
    days_since = (current_time - last_activity).days
    
    # Auto-complete if 5+ days with no activity
    should_complete = days_since >= 5
    
    return should_complete, days_since


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    # Test cases
    test_emails = [
        {
            'subject': 'R+L Carriers PRO I655817778 has been Delivered',
            'body': 'Your shipment MPO 5247 has been delivered.',
            'expected': 'mark_delivered'
        },
        {
            'subject': 'Re: Order Baker\'s Cabinetry-(#5156)',
            'body': 'The order is ready for pick up at the warehouse.',
            'expected': 'mark_shipped'
        },
        {
            'subject': 'Order Canceled',
            'body': 'Order #5264 has been canceled per customer request.',
            'expected': 'mark_canceled'
        },
        {
            'subject': 'Re: PO 5266',
            'body': 'please see the attached BOL',
            'expected': 'mark_shipped'
        },
        {
            'subject': 'DL Cabinetry order confirmation JOB Name #5184',
            'body': 'Shipping & Handling $271.76',
            'expected': 'mark_shipped'
        },
        {
            'subject': 'Re: Order Yoderbuilt-(#5280)',
            'body': 'UPS tracking # is 1Z999AA10123456784',
            'expected': 'mark_shipped'
        },
    ]
    
    print("Testing email rules...\n")
    for i, test in enumerate(test_emails):
        result = process_email(
            test['subject'], 
            test['body'], 
            datetime.now(timezone.utc),
            has_attachment='attached' in test['body'].lower()
        )
        status = "✓" if result['action'] == test['expected'] else "✗"
        print(f"{status} Test {i+1}: {test['subject'][:50]}...")
        print(f"   Expected: {test['expected']}, Got: {result['action']}")
        print(f"   Order ID: {result['order_id']}")
        print(f"   Details: {result['details']}\n")
