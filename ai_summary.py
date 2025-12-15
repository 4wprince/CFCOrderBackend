"""
ai_summary.py
AI Summary generation for CFC Order Workflow
Improved rules, critical comment detection, focused next actions
"""

import os
import re
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional, Dict, List, Tuple

# Anthropic API Config
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()

# =============================================================================
# CRITICAL COMMENT PATTERNS
# =============================================================================

# These patterns indicate comments that need RED highlighting in UI
CRITICAL_PATTERNS = [
    # Address changes
    (r'address.*(different|change|new|correct)', 'ADDRESS_CHANGE'),
    (r'ship\s*to\s*(different|another|new)', 'ADDRESS_CHANGE'),
    (r'(different|new|correct)\s*address', 'ADDRESS_CHANGE'),
    
    # Order modifications
    (r'add.*(to|this)\s*order', 'ORDER_MODIFICATION'),
    (r'(forgot|need)\s*to\s*(add|include)', 'ORDER_MODIFICATION'),
    (r'change\s*\w+\s*to\s*\w+', 'ORDER_MODIFICATION'),  # "change B12 to DB12"
    (r'swap|replace|substitute', 'ORDER_MODIFICATION'),
    
    # Combined orders
    (r'(goes|combine|ship)\s*with.*(order|earlier|previous)', 'COMBINED_ORDER'),
    (r'(earlier|previous|other)\s*order', 'COMBINED_ORDER'),
    (r'combine\s*(these|orders|shipment)', 'COMBINED_ORDER'),
    
    # Hold/Cancel
    (r"don'?t\s*ship", 'HOLD_ORDER'),
    (r'hold\s*(order|shipment|off)', 'HOLD_ORDER'),
    (r'cancel', 'CANCEL_ORDER'),
    (r'wait\s*(until|for|before)', 'HOLD_ORDER'),
    
    # Delivery instructions
    (r'liftgate', 'DELIVERY_INSTRUCTION'),
    (r'call\s*(before|when|prior)', 'DELIVERY_INSTRUCTION'),
    (r'appointment\s*(only|required|needed)', 'DELIVERY_INSTRUCTION'),
    (r'residential', 'DELIVERY_INSTRUCTION'),
    
    # Payment issues
    (r'(check|cash)\s*(on|at)\s*(delivery|pickup)', 'PAYMENT_INSTRUCTION'),
    (r'pay\s*(when|at|on)\s*(pickup|delivery)', 'PAYMENT_INSTRUCTION'),
]

# =============================================================================
# NOISE FILTERS
# =============================================================================

# Event types to exclude from summary context
NOISE_EVENT_TYPES = [
    'b2bwave_sync',
    'auto_sync', 
    'status_check',
    'gmail_sync',
    'system_sync',
]

# Phrases to filter out of summaries
NOISE_PHRASES = [
    'system sync',
    'sync event',
    'inventory update',
    'status update',
    'multiple syncs',
    'active processing',
]

# =============================================================================
# CORE FUNCTIONS
# =============================================================================

def detect_critical_comments(text: str) -> List[Dict]:
    """
    Scan text for critical patterns that need highlighting.
    Returns list of {pattern_type, matched_text, start, end}
    """
    if not text:
        return []
    
    critical_items = []
    text_lower = text.lower()
    
    for pattern, pattern_type in CRITICAL_PATTERNS:
        for match in re.finditer(pattern, text_lower, re.IGNORECASE):
            critical_items.append({
                'type': pattern_type,
                'matched_text': text[match.start():match.end()],
                'start': match.start(),
                'end': match.end(),
                'context': text[max(0, match.start()-20):min(len(text), match.end()+20)]
            })
    
    return critical_items

def build_order_context(order: Dict, snippets: List = None, events: List = None) -> str:
    """
    Build context string for AI from order data.
    Filters out noise and focuses on actionable info.
    """
    context_parts = []
    
    # Basic order info
    order_id = order.get('order_id', 'Unknown')
    customer = order.get('company_name') or order.get('customer_name') or 'Unknown'
    context_parts.append(f"ORDER #{order_id} - {customer}")
    
    # Payment status (important)
    order_total = order.get('order_total', 0)
    payment_received = order.get('payment_received', False)
    payment_amount = order.get('payment_amount')
    
    if payment_received:
        amt_str = f"${payment_amount:.2f}" if payment_amount else "confirmed"
        context_parts.append(f"Payment: RECEIVED ({amt_str})")
    else:
        context_parts.append(f"Payment: PENDING (Order total: ${order_total})")
    
    # Shipping info
    if order.get('tracking'):
        context_parts.append(f"Tracking: {order.get('tracking')}")
    if order.get('pro_number'):
        context_parts.append(f"PRO Number: {order.get('pro_number')}")
    
    # Warehouses
    warehouses = [order.get(f'warehouse_{i}') for i in range(1, 5) if order.get(f'warehouse_{i}')]
    if warehouses:
        context_parts.append(f"Warehouses: {', '.join(warehouses)}")
    
    # Customer comments - CRITICAL, check for flags
    comments = order.get('comments', '')
    if comments:
        critical = detect_critical_comments(comments)
        if critical:
            context_parts.append(f"⚠️ CUSTOMER COMMENTS (CRITICAL): {comments}")
        else:
            context_parts.append(f"Customer Comments: {comments}")
    
    # Internal notes
    notes = order.get('notes', '')
    if notes:
        context_parts.append(f"Internal Notes: {notes}")
    
    # Email snippets (filter and limit)
    if snippets:
        relevant_snippets = []
        for s in snippets[:10]:  # Limit to 10 most recent
            subject = s.get('email_subject', '')
            # Skip obvious system emails
            if any(noise in subject.lower() for noise in ['sync', 'automated', 'system']):
                continue
            relevant_snippets.append(s)
        
        if relevant_snippets:
            context_parts.append("\nRECENT EMAILS:")
            for s in relevant_snippets[:5]:  # Show top 5
                date_str = ''
                if s.get('email_date'):
                    try:
                        date_str = s['email_date'].strftime('%m/%d')
                    except:
                        pass
                from_addr = s.get('email_from', 'Unknown')
                # Shorten email addresses
                if '<' in from_addr:
                    from_addr = from_addr.split('<')[0].strip()
                context_parts.append(f"- [{date_str}] {from_addr}: {s.get('email_subject', '')}")
    
    # Events (filter out noise)
    if events:
        important_events = [
            e for e in events 
            if e.get('event_type') not in NOISE_EVENT_TYPES
        ]
        if important_events:
            context_parts.append("\nKEY EVENTS:")
            for e in important_events[:5]:
                date_str = ''
                if e.get('created_at'):
                    try:
                        date_str = e['created_at'].strftime('%m/%d')
                    except:
                        pass
                event_type = e.get('event_type', '').replace('_', ' ').title()
                context_parts.append(f"- [{date_str}] {event_type}")
    
    return "\n".join(context_parts)

def create_summary_prompt(context: str, critical_comments: List[Dict]) -> str:
    """
    Create the prompt for AI summary generation.
    """
    critical_section = ""
    if critical_comments:
        flags = list(set(c['type'] for c in critical_comments))
        critical_section = f"""
⚠️ CRITICAL FLAGS DETECTED: {', '.join(flags)}
These MUST be mentioned prominently in the summary.
"""
    
    prompt = f"""Write a brief, actionable order status summary.

RULES:
• 2-4 bullet points maximum
• Use • symbol for bullets
• NO headers, NO bold (**), NO markdown
• Plain conversational language
• Focus on what matters: payment status, shipping status, customer requests
• ALWAYS end with "Next action:" stating what needs to happen next
• If customer comments contain special requests, address changes, or order modifications - HIGHLIGHT THEM FIRST
{critical_section}
DO NOT INCLUDE:
• System sync activity
• Inventory updates  
• Generic "processing" status
• Redundant info already visible in UI

GOOD EXAMPLE:
• Customer wants to pick up Friday - needs confirmation
• Payment received ($2,305.67)
• Next action: Confirm Friday pickup availability with customer

BAD EXAMPLE:
• **Order Status:** In processing
• System Activity: Multiple sync events detected
• Warehouse: Assigned to LI

ORDER CONTEXT:
{context}

Write the summary now:"""
    
    return prompt

def call_anthropic_api(prompt: str, max_tokens: int = 500) -> str:
    """
    Call Anthropic API for summary generation.
    """
    if not ANTHROPIC_API_KEY:
        return "AI summary unavailable - API key not configured"
    
    try:
        request_body = json.dumps({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}]
        }).encode()
        
        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=request_body,
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01"
            }
        )
        
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
            if data.get("content") and len(data["content"]) > 0:
                return data["content"][0].get("text", "")
            return ""
            
    except urllib.error.HTTPError as e:
        print(f"[AI_SUMMARY] API error {e.code}: {e.read().decode()[:200]}")
        return f"AI summary error: {e.code}"
    except Exception as e:
        print(f"[AI_SUMMARY] Error: {e}")
        return f"AI summary error: {str(e)}"

def clean_summary(summary: str) -> str:
    """
    Clean up the AI-generated summary.
    Remove any markdown formatting that slipped through.
    """
    if not summary:
        return ""
    
    # Remove markdown bold
    summary = re.sub(r'\*\*([^*]+)\*\*', r'\1', summary)
    
    # Remove markdown headers
    summary = re.sub(r'^#+\s*', '', summary, flags=re.MULTILINE)
    
    # Normalize bullet points
    summary = re.sub(r'^[-*]\s*', '• ', summary, flags=re.MULTILINE)
    
    # Remove noise phrases that might have slipped in
    for phrase in NOISE_PHRASES:
        summary = re.sub(rf'\b{phrase}\b', '', summary, flags=re.IGNORECASE)
    
    # Clean up extra whitespace
    summary = re.sub(r'\n{3,}', '\n\n', summary)
    summary = re.sub(r' {2,}', ' ', summary)
    
    return summary.strip()

def generate_summary(order: Dict, snippets: List = None, events: List = None) -> Tuple[str, List[Dict]]:
    """
    Generate AI summary for an order.
    
    Returns:
        Tuple of (summary_text, critical_comments_list)
    """
    # Detect critical comments in customer comments and notes
    all_text = (order.get('comments', '') or '') + ' ' + (order.get('notes', '') or '')
    critical_comments = detect_critical_comments(all_text)
    
    # Build context
    context = build_order_context(order, snippets, events)
    
    # Create prompt
    prompt = create_summary_prompt(context, critical_comments)
    
    # Get AI response
    raw_summary = call_anthropic_api(prompt)
    
    # Clean up
    summary = clean_summary(raw_summary)
    
    return summary, critical_comments

def should_regenerate(order: Dict, event_type: str = None) -> bool:
    """
    Determine if an order's summary should be regenerated based on event.
    """
    # Always regenerate for these events
    significant_events = [
        'payment_received',
        'payment_link_sent',
        'sent_to_warehouse',
        'warehouse_confirmed',
        'tracking_captured',
        'pro_number_captured',
        'rl_quote_captured',
        'bol_sent',
        'status_change',
        'note_updated',
    ]
    
    if event_type and event_type in significant_events:
        return True
    
    # Don't regenerate for noise events
    if event_type and event_type in NOISE_EVENT_TYPES:
        return False
    
    # Default: don't regenerate
    return False
