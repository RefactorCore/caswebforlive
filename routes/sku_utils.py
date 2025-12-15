"""
Universal SKU Auto-Generation System
Works for ANY retail business type
"""
from models import db, Product
import re
from sqlalchemy.exc import IntegrityError
import logging


# ✅ UNIVERSAL CATEGORY PRESETS (Expandable)
INDUSTRY_CATEGORIES = {
    # Automotive
    'automotive': {
        'TIR': 'Tires',
        'FIL': 'Filters',
        'BRK': 'Brakes',
        'BAT': 'Battery',
        'OIL': 'Oil/Lubricants',
        'SPK': 'Spark Plugs',
        'WIP': 'Wipers',
        'MIR': 'Mirrors',
        'LGT': 'Lights',
        'CAB': 'Cables',
        'BLT': 'Belts',
    },
    
    # Construction/Hardware
    'construction': {
        'CEM': 'Cement',
        'SND': 'Sand',
        'GRV': 'Gravel',
        'PLY': 'Plywood',
        'PNT': 'Paint',
        'NAL': 'Nails/Screws',
        'WIR': 'Wire/Cable',
        'TUB': 'Pipes/Tubes',
        'TOL': 'Tools',
        'ELC': 'Electrical',
    },
    
    # Apparel/Boutique
    'apparel': {
        'DRS': 'Dresses',
        'TOP': 'Tops/Blouses',
        'PNT': 'Pants/Jeans',
        'SKT': 'Skirts',
        'SHO': 'Shoes',
        'BAG': 'Bags',
        'ACC': 'Accessories',
        'UND': 'Underwear',
        'SWT': 'Sweaters',
        'OUT': 'Outerwear',
    },
    
    # Beauty & Skincare
    'beauty': {
        'SKN': 'Skincare',
        'MKP': 'Makeup',
        'FRG': 'Fragrance',
        'HRC': 'Haircare',
        'BDY': 'Body Care',
        'TON': 'Toner',
        'SRM': 'Serum',
        'MST': 'Moisturizer',
        'CLN': 'Cleanser',
        'MSK': 'Mask',
    },
    
    # Food & Beverage
    'foodbev': {
        'MLK': 'Milk Tea',
        'COF': 'Coffee',
        'JCE': 'Juice',
        'SNK': 'Snacks',
        'SIN': 'Sinkers/Add-ons',
        'CUP': 'Cups',
        'SYR': 'Syrup',
        'PWD': 'Powder',
        'ICE': 'Ice/Frozen',
        'PCK': 'Packaging',
    },
    
    # General/Universal (Default)
    'general': {
        'PRD': 'Product',
        'ITM': 'Item',
        'GDS': 'Goods',
        'MRC': 'Merchandise',
        'SUP': 'Supplies',
    }
}


def generate_sku(product_name, category=None, custom_sku=None, industry=None):
    """
    Robust SKU generator with safer DB access and sensible fallbacks. 
    - Avoids transactional FOR UPDATE usage (not reliable outside explicit transactions). 
    - Minimizes chance of silently rolling back outer transactions.
    - Retries a few times if a race produces duplicates, then falls back to a timestamp+random suffix.
    """
    import re
    from datetime import datetime
    from random import randint

    if not product_name:
        product_name = 'PRODUCT'

    # 1) Custom SKU path (validate and return if unique)
    if custom_sku and str(custom_sku).strip():
        candidate = str(custom_sku).strip(). upper()

        # Validate allowed characters
        if not re.match(r'^[A-Z0-9-]+$', candidate):
            raise ValueError("SKU can only contain letters, numbers, and hyphens")

        # Auto-truncate long custom SKUs but keep a short hash suffix to reduce collisions
        MAX_LEN = 20  # ✅ CHANGED FROM 64 to 20
        if len(candidate) > MAX_LEN:
            import hashlib
            # Compute a short deterministic suffix from the original value
            suffix = hashlib.sha1(candidate.encode('utf-8')).hexdigest()[:4]. upper()
            # Reserve 1 char for the separator '-'
            truncate_len = MAX_LEN - (1 + len(suffix))
            candidate = (re.sub(r'[^A-Z0-9-]', '', candidate)[:truncate_len]). rstrip('-')
            candidate = f"{candidate}-{suffix}"

        if len(candidate) > MAX_LEN:
            # Defensive: ensure we never return > MAX_LEN
            candidate = candidate[:MAX_LEN]

        # Final format check + uniqueness
        if not re.match(r'^[A-Z0-9-]+$', candidate):
            raise ValueError("SKU can only contain letters, numbers, and hyphens after normalization")
        if Product.query.filter_by(sku=candidate).first():
            raise ValueError(f"SKU '{candidate}' already exists")
        # Return the normalized (and possibly truncated) SKU
        return candidate

    # 2) Determine prefix
    if category and str(category).strip():
        prefix = str(category).strip().upper()[:3]
    else:
        prefix = auto_detect_category(product_name, industry)

    # Normalize prefix to safe characters (A-Z0-9)
    prefix = re.sub(r'[^A-Z0-9]', 'X', prefix. upper())[:3] or 'PRD'

    # We'll attempt a few quick numeric increments based on existing SKUs
    MAX_RETRIES = 6
    pattern = re.compile(rf'^{re.escape(prefix)}-(\d+)$')

    for attempt in range(MAX_RETRIES):
        try:
            # Fetch only the SKU column (scalars) for this prefix
            rows = db.session.query(Product.sku).filter(Product.sku.like(f'{prefix}-%')).all()
            # rows is list of 1-tuples; flatten and extract numbers
            max_num = 0
            for row in rows:
                sku_val = row[0] if isinstance(row, (list, tuple)) else row
                if not isinstance(sku_val, str):
                    continue
                m = pattern.match(sku_val. strip(). upper())
                if m:
                    try:
                        n = int(m.group(1))
                        if n > max_num:
                            max_num = n
                    except Exception:
                        continue

            next_num = max_num + 1
            candidate = f"{prefix}-{next_num:05d}"

            # Double-check uniqueness before returning
            if not Product.query.filter_by(sku=candidate).first():
                return candidate

            # If collision (rare), try again in loop to recompute next_num
            continue

        except Exception:
            # Don't rollback the session here; just retry a few times. 
            # On database hiccup, sleep briefly (only if necessary) or continue.
            if attempt < MAX_RETRIES - 1:
                continue
            break

    # ✅ IMPROVED FALLBACK: Use shorter random suffix instead of timestamp
    for i in range(20):  # Try 20 times to find a unique short SKU
        random_suffix = randint(10000, 99999)  # 5-digit random number
        candidate = f"{prefix}-{random_suffix}"
        
        if not Product.query.filter_by(sku=candidate).first():
            if i > 0:
                logging.warning("generate_sku: used fallback (attempt %s) for prefix %s -> %s", i+1, prefix, candidate)
            return candidate

    # ✅ LAST RESORT: Use timestamp but keep it short
    base = datetime.utcnow().strftime('%y%m%d%H%M')  # YYMMDDHHMI -> 10 digits
    candidate = f"{prefix}-{base}"[:20]  # Ensure max 20 chars
    
    logging.warning("generate_sku: using timestamp fallback for prefix %s -> %s", prefix, candidate)
    return candidate

    # Last-resort deterministic compressed fallback
    logging.warning("generate_sku: exhausting fallback attempts for prefix %s, returning compressed fallback", prefix)
    return f"{prefix}-{base[:10]}{randint(10,99)}"

def auto_detect_category(product_name, industry=None):
    """
    Safe auto-detection of a 3-letter category prefix from product name.
    Returns a 3-letter uppercase code (defaults to 'PRD').
    """
    if not product_name:
        return 'PRD'
    name_lower = str(product_name).lower()

    # keyword map (reused from module-level mapping, but local copy is OK)
    keywords = {
        'TIR': ['tire', 'tires', 'gulong'],
        'FIL': ['filter', 'air filter', 'oil filter'],
        'BRK': ['brake', 'brakes', 'preno'],
        'OIL': ['oil', 'lubricant', 'langis'],
        'BAT': ['battery', 'baterya'],
        'SPK': ['spark plug', 'spark'],

        'CEM': ['cement', 'semento'],
        'SND': ['sand', 'buhangin'],
        'PLY': ['plywood', 'wood'],
        'PNT': ['paint', 'pintura'],

        'DRS': ['dress', 'damit'],
        'TOP': ['top', 'blouse', 'shirt'],
        'PNT': ['pants', 'jeans', 'slacks'],
        'SHO': ['shoes', 'sapatos'],
        'BAG': ['bag', 'purse'],

        'SKN': ['skin', 'skincare', 'face'],
        'MKP': ['makeup', 'lipstick', 'foundation'],
        'CLN': ['cleanser', 'wash'],
        'TON': ['toner'],
        'SRM': ['serum'],

        'MLK': ['milk tea', 'milktea'],
        'COF': ['coffee', 'kape'],
        'JCE': ['juice'],
        'SNK': ['snack'],
    }

    for prefix, kw_list in keywords.items():
        for kw in kw_list:
            if kw in name_lower:
                return prefix

    # If user provided industry hint, prefer a category from presets if available
    try:
        if industry and industry in INDUSTRY_CATEGORIES:
            # return first key for that industry as a reasonable default
            cats = list(INDUSTRY_CATEGORIES.get(industry, {}).keys())
            if cats:
                return cats[0][:3].upper()
    except Exception:
        pass

    return 'PRD'


def get_industry_categories(industry='general'):
    """
    Get category presets for a specific industry.
    
    Args:
        industry: Industry type ('automotive', 'construction', 'apparel', etc.)
    
    Returns:
        dict: Category code -> name mapping
    """
    return INDUSTRY_CATEGORIES.get(industry, INDUSTRY_CATEGORIES['general'])


def get_all_categories():
    """
    Get all available category presets across all industries.
    
    Returns:
        dict: Combined categories from all industries
    """
    combined = {}
    for industry_cats in INDUSTRY_CATEGORIES.values():
        combined.update(industry_cats)
    return combined


def get_category_suggestions():
    """
    Get suggested category prefixes for the bulk upload template.
    Returns a flattened list of all categories across industries.
    
    Returns:
        dict: Category prefix -> description mapping
    """
    suggestions = {}
    
    # Combine all industry categories into one dictionary
    for industry_name, categories in INDUSTRY_CATEGORIES.items():
        for prefix, description in categories.items():
            # Add industry hint to description
            if industry_name != 'general':
                suggestions[prefix] = f"{description} ({industry_name.title()})"
            else:
                suggestions[prefix] = description
    
    return dict(sorted(suggestions.items()))


def validate_sku(sku):
    """
    Validate SKU format and uniqueness. Returns (True, None) or (False, message).
    Uses a lightweight existence check (selects only the id) to reduce DB load.
    """
    import re
    if not sku or not str(sku).strip():
        return False, "SKU cannot be empty"

    candidate = str(sku).strip().upper()

    if len(candidate) > 64:
        return False, "SKU is too long (max 64 characters)"

    if not re.match(r'^[A-Z0-9-]+$', candidate):
        return False, "SKU can only contain letters, numbers, and hyphens"

    # lightweight existence check
    exists = db.session.query(Product.id).filter(Product.sku == candidate).first()
    if exists:
        return False, "SKU already exists"

    return True, None


def suggest_sku(product_name, industry=None):
    """
    Suggest multiple SKU options for a product.
    
    Args:
        product_name: Product name
        industry: Optional industry hint
    
    Returns:
        list: List of suggested SKUs
    """
    suggestions = []
    
    # Option 1: Auto-detected category
    auto_prefix = auto_detect_category(product_name, industry)
    suggestions.append({
        'sku': generate_sku(product_name, category=auto_prefix),
        'description': f'Auto-detected ({auto_prefix})'
    })
    
    # Option 2: Generic
    if auto_prefix != 'PRD':
        suggestions.append({
            'sku': generate_sku(product_name, category='PRD'),
            'description': 'Generic product code'
        })
    
    # Option 3: From product name initials
    words = re.sub(r'[^A-Za-z0-9\s]', '', product_name).split()
    if len(words) >= 2:
        initials = ''.join(word[0] for word in words[:3]).upper()
        suggestions.append({
            'sku': generate_sku(product_name, category=initials),
            'description': f'Name-based ({initials})'
        })
    
    return suggestions