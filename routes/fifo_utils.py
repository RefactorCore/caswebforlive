"""
FIFO Inventory Costing Utilities
"""
from models import db, InventoryLot, InventoryTransaction, Product
from sqlalchemy import func
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP, getcontext
from sqlalchemy import exc as sa_exc
import logging

getcontext().prec = 28


def to_decimal(value):
    """
    Coerce value (None, float, int, str, Decimal) -> Decimal quantized to 2dp.
    - Accepts strings with commas "1,234.56" and parentheses for negatives "(1,234.56)".
    - Returns Decimal('0.00') for invalid inputs (fail-safe).
    """
    if value is None or value == '':
        return Decimal('0.00')
    if isinstance(value, Decimal):
        return value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    if isinstance(value, int):
        return Decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    if isinstance(value, float):
        # Convert float via str to avoid binary float artifacts
        try:
            return Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        except Exception:
            return Decimal('0.00')
    # strings and other objects
    try:
        if isinstance(value, str):
            s = value.strip().replace(',', '')
            # support parentheses for negatives
            if s.startswith('(') and s.endswith(')'):
                s = '-' + s[1:-1]
            return Decimal(s).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        # fallback: try constructing from str()
        return Decimal(str(value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal('0.00')


def create_inventory_lot(product_id, quantity, unit_cost, purchase_id=None,
                         purchase_item_id=None, adjustment_id=None, movement_id=None, is_opening_balance=False):
    """
    Create a new inventory lot when receiving inventory.
    - Validates that product exists.
    - Ensures quantity is a positive integer (rejects fractional quantities).
    - Validates unit_cost is non-negative Decimal.
    Returns the unsaved InventoryLot instance (caller should commit/flush).
    """
    # Validate product exists early to fail-fast
    product = Product.query.get(product_id)
    if not product:
        raise ValueError(f"Product {product_id} not found")

    # Normalize and validate quantity
    try:
        # Accept numeric-like input but enforce integer units
        qty_int = int(quantity)
    except (TypeError, ValueError):
        raise ValueError("Quantity must be an integer")

    if qty_int <= 0:
        raise ValueError("Quantity must be positive")

    # Reject fractional values (e.g., '1.5') — int() would truncate silently
    # If caller passed a float that was fractional, explicitly reject.
    if isinstance(quantity, float) and not float(quantity).is_integer():
        raise ValueError("Quantity must be a whole integer value (no fractions allowed)")

    # Normalize and validate cost
    unit_cost = to_decimal(unit_cost)
    if unit_cost < Decimal('0.00'):
        raise ValueError("Unit cost cannot be negative")

    lot = InventoryLot(
        product_id=product_id,
        quantity_remaining=qty_int,
        unit_cost=unit_cost,
        purchase_id=purchase_id,
        purchase_item_id=purchase_item_id,
        adjustment_id=adjustment_id,
        movement_id=movement_id,
        is_opening_balance=bool(is_opening_balance),
        created_at=datetime.utcnow()
    )

    db.session.add(lot)
    return lot


def consume_inventory_fifo(product_id, quantity_needed, sale_id=None, sale_item_id=None,
                           ar_invoice_id=None, ar_invoice_item_id=None, adjustment_id=None, movement_id=None):
    """
    Consume inventory using FIFO method.
    Returns: (total_cogs: Decimal, transactions: [InventoryTransaction,...])
    Raises ValueError on insufficient stock or invalid inputs.

    NOTE: This function now accepts both adjustment_id (StockAdjustment) and movement_id (InventoryMovement).
    When creating InventoryTransaction records we will populate the appropriate FK column so we don't
    place movement ids into the adjustment_id column (which caused the FK error).
    """
    # Validate/normalize input
    try:
        qty_needed = int(quantity_needed)
    except (TypeError, ValueError):
        raise ValueError("Quantity must be an integer")

    if qty_needed <= 0:
        raise ValueError("Quantity must be positive")

    product = Product.query.get(product_id)
    if not product:
        raise ValueError(f"Product {product_id} not found")

    total_cogs = Decimal('0.00')
    remaining_to_consume = qty_needed
    transactions = []

    # Robust engine/dialect detection (handles various SQLAlchemy setups)
    try:
        bind = db.engine if hasattr(db, 'engine') else db.session.get_bind()
        engine_name = bind.dialect.name.lower()
    except Exception:
        engine_name = 'mysql'  # Default to MySQL/MariaDB
        
    if engine_name not in ('mysql', 'mariadb'):
        raise RuntimeError(f"Unsupported database:  {engine_name}. This application requires MariaDB/MySQL.")

    supports_skip_locked = True 

    # Helper to attempt applying the best available FOR UPDATE variant
    def _fetch_next_lot(query):
        # Prefer SKIP LOCKED when supported; gracefully fallback if the SQLAlchemy version/driver doesn't accept the flag
        try:
            if supports_skip_locked:
                try:
                    return query.with_for_update(skip_locked=True).first()
                except (TypeError, sa_exc.OperationalError, sa_exc.DatabaseError):
                    # try nowait variant next
                    try:
                        return query.with_for_update(nowait=True).first()
                    except (TypeError, sa_exc.OperationalError, sa_exc.DatabaseError):
                        # fall back to plain with_for_update()
                        try:
                            return query.with_for_update().first()
                        except Exception:
                            pass
        except Exception:
            # In case supports_skip_locked logic or the call itself raises, try safe fallbacks below
            pass

        # Final fallback: no locking (last resort — risky in concurrent scenarios)
        try:
            return query.first()
        except Exception:
            logging.exception("_fetch_next_lot: final fallback failed for product_id=%s", product_id)
            return None

    # Iteratively consume oldest lots
    safety_counter = 0
    MAX_LOTS = 10000
    while remaining_to_consume > 0:
        lot_query = InventoryLot.query.filter(
            InventoryLot.product_id == product_id,
            InventoryLot.quantity_remaining > 0
        ).order_by(InventoryLot.created_at.asc())

        lot = _fetch_next_lot(lot_query)

        if not lot:
            consumed_so_far = qty_needed - remaining_to_consume
            if consumed_so_far > 0:
                raise ValueError(
                    f"Inventory lots depleted mid-transaction for {product.name}. "
                    f"Consumed {consumed_so_far}/{qty_needed} units."
                )
            raise ValueError(f"No inventory lots available for product {product_id}")

        # Determine how much to take from this lot
        qty_from_lot = int(min(int(lot.quantity_remaining), remaining_to_consume))
        if qty_from_lot <= 0:
            raise ValueError(f"Lot {lot.id} has invalid quantity_remaining: {lot.quantity_remaining}")

        unit_cost = to_decimal(lot.unit_cost)
        cost_from_lot = (to_decimal(qty_from_lot) * unit_cost).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

        # Create InventoryTransaction: populate movement_id OR adjustment_id appropriately
        transaction = InventoryTransaction(
            lot_id=lot.id,
            quantity_used=qty_from_lot,
            unit_cost=unit_cost,
            total_cost=cost_from_lot,
            sale_id=sale_id,
            sale_item_id=sale_item_id,
            ar_invoice_id=ar_invoice_id,
            ar_invoice_item_id=ar_invoice_item_id,
            adjustment_id=adjustment_id if adjustment_id is not None else None,
            movement_id=movement_id if movement_id is not None else None,
            created_at=datetime.utcnow()
        )
        db.session.add(transaction)
        transactions.append(transaction)

        # Update lot and accumulate totals
        lot.quantity_remaining = int(lot.quantity_remaining) - qty_from_lot
        db.session.add(lot)  # ensure change tracked

        total_cogs += cost_from_lot
        remaining_to_consume -= qty_from_lot

        safety_counter += 1
        if safety_counter > MAX_LOTS:
            raise ValueError(
                f"Excessive lot fragmentation for product {product_id}. "
                f"Consumed {safety_counter} lots. Consider consolidating inventory lots."
            )

    # Single flush at end for performance; convert DB errors to descriptive messages
    try:
        db.session.flush()
    except Exception as e:
        raise ValueError(f"Database error during FIFO consumption flush: {str(e)}")

    if remaining_to_consume != 0:
        raise ValueError(f"FIFO consumption logic error: {remaining_to_consume} units still needed after processing")

    total_cogs = total_cogs.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    return total_cogs, transactions


def get_fifo_cost(product_id, quantity):
    """
    Calculate what the COGS would be for a given quantity without consuming.
    Useful for estimates and previews.

    Args:
        product_id: ID of the product
        quantity: Number of units

    Returns:
        Decimal: Estimated COGS
    """
    lots = InventoryLot.query.filter(
        InventoryLot.product_id == product_id,
        InventoryLot.quantity_remaining > 0
    ).order_by(InventoryLot.created_at.asc()).all()

    total_cost = Decimal('0.00')
    remaining = int(quantity)

    for lot in lots:
        if remaining <= 0:
            break
        qty_from_lot = int(min(lot.quantity_remaining, remaining))
        total_cost += (to_decimal(qty_from_lot) * to_decimal(lot.unit_cost))
        remaining -= qty_from_lot

    total_cost = total_cost.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    return total_cost

def safe_divide(numerator, denominator, default=Decimal('0.00')):
    """Safe decimal division with default value."""
    num = to_decimal(numerator)
    denom = to_decimal(denominator)
    
    if denom == Decimal('0.00'):
        return default
    
    try:
        return (num / denom).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    except Exception:
        return default

def get_weighted_average_cost(product_id):
    """
    Calculate the current weighted average cost for a product.
    This is useful for display purposes and reporting.

    Args:
        product_id: ID of the product

    Returns:
        Decimal: Weighted average cost per unit
    """
    result = db.session.query(
        func.coalesce(func.sum(InventoryLot.quantity_remaining * InventoryLot.unit_cost), 0),
        func.coalesce(func.sum(InventoryLot.quantity_remaining), 0)
    ).filter(
        InventoryLot.product_id == product_id,
        InventoryLot.quantity_remaining > 0
    ).first()

    total_value, total_qty = result

    total_value = to_decimal(total_value)
    total_qty = int(total_qty or 0)

    if total_qty == 0:
        return Decimal('0.00')

    avg = safe_divide(total_value, total_qty).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    return avg


def get_inventory_lots_summary(product_id):
    """
    Get a summary of all active inventory lots for a product.
    """
    lots = InventoryLot.query.filter(
        InventoryLot.product_id == product_id,
        InventoryLot.quantity_remaining > 0
    ).order_by(InventoryLot.created_at.asc()).all()

    summary = []
    for lot in lots:
        unit_cost = to_decimal(lot.unit_cost)
        total_value = (to_decimal(lot.quantity_remaining) * unit_cost).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        summary.append({
            'lot_id': lot.id,
            'quantity': int(lot.quantity_remaining),
            'unit_cost': unit_cost,
            'total_value': total_value,
            'created_at': lot.created_at,
            'age_days': (datetime.utcnow() - lot.created_at).days,
            'is_opening_balance': lot.is_opening_balance,
            'movement_id': getattr(lot, 'movement_id', None),
            'purchase_id': getattr(lot, 'purchase_id', None)
        })

    return summary


def reconcile_inventory_lots(product_id):
    """
    Reconcile inventory lots with the product quantity.
    Returns discrepancies if any.

    Args:
        product_id: ID of the product

    Returns:
        dict: Reconciliation results
    """
    product = Product.query.get(product_id)
    if not product:
        return {'error': 'Product not found'}

    lot_total = db.session.query(
        func.coalesce(func.sum(InventoryLot.quantity_remaining), 0)
    ).filter(
        InventoryLot.product_id == product_id
    ).scalar() or 0

    lot_total = int(lot_total)
    discrepancy = int(product.quantity) - lot_total

    return {
        'product_quantity': int(product.quantity),
        'lot_total': lot_total,
        'discrepancy': discrepancy,
        'is_balanced': discrepancy == 0
    }


def reverse_inventory_consumption(sale_id=None, ar_invoice_id=None, adjustment_id=None, movement_id=None):
    """
    Reverse FIFO inventory consumption for voided transactions.
    Restores inventory lots and deletes the consumption records.

    NOTE: queries will look at both adjustment_id and movement_id columns when appropriate.
    """
    # Build query for affected InventoryTransaction rows
    query = InventoryTransaction.query

    if sale_id:
        query = query.filter(InventoryTransaction.sale_id == sale_id)
    elif ar_invoice_id:
        query = query.filter(InventoryTransaction.ar_invoice_id == ar_invoice_id)
    elif adjustment_id:
        query = query.filter(InventoryTransaction.adjustment_id == adjustment_id)
    elif movement_id:
        query = query.filter(InventoryTransaction.movement_id == movement_id)
    else:
        raise ValueError("Must provide either sale_id, ar_invoice_id, adjustment_id, or movement_id")

    transactions = query.all()

    reversed_summary = {}

    for trans in transactions:
        # Restore the lot quantity
        lot = InventoryLot.query.get(trans.lot_id)
        if lot:
            lot.quantity_remaining = int(lot.quantity_remaining) + int(trans.quantity_used)

            # Track what we reversed
            product_id = lot.product_id
            if product_id not in reversed_summary:
                reversed_summary[product_id] = 0
            reversed_summary[product_id] += int(trans.quantity_used)

        # Delete the transaction record
        db.session.delete(trans)

    # After restoration, sync Product.quantity for affected products
    for pid in list(reversed_summary.keys()):
        total_remaining = db.session.query(func.coalesce(func.sum(InventoryLot.quantity_remaining), 0)).filter(InventoryLot.product_id == pid).scalar() or 0
        prod = Product.query.get(pid)
        if prod:
            prod.quantity = int(total_remaining)

    # Flush so callers can rely on in-session state prior to commit
    try:
        db.session.flush()
    except Exception:
        pass

    return reversed_summary