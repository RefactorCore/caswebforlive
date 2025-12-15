from flask import request, abort
from flask_login import current_user
from models import db, AuditLog, Account
from functools import lru_cache
from flask_caching import Cache
import logging
from sqlalchemy import func

cache = Cache()

def paginate_query(query, per_page=20):
    """Paginate SQLAlchemy query based on ?page= parameter.

    - Defensively parses page parameter.
    - Returns a Flask-SQLAlchemy Pagination object; raises a clear error if paginate() not available.
    """
    try:
        page_raw = request.args.get('page', 1)
        page = int(page_raw)
        if page < 1:
            page = 1
    except (ValueError, TypeError):
        page = 1

    # Ensure the query supports paginate (Flask-SQLAlchemy's BaseQuery has paginate)
    if not hasattr(query, 'paginate'):
        raise RuntimeError("paginate_query: provided query object does not support paginate().")
    try:
        return query.paginate(page=page, per_page=per_page, error_out=False)
    except Exception as e:
        # Avoid silent failure — log and re-raise for callers to handle.
        logging.exception("Error while paginating query: %s", e)
        raise


def log_action(action_description, user=None):
    """
    Create an AuditLog row for the action_description.

    - Does not commit (caller controls transaction).
    - Never raises on logging failures; logs internal exception instead to avoid breaking user flows.
    """
    try:
        # prefer explicit user object, otherwise the flask-login current_user if authenticated
        user_to_log = user
        if user_to_log is None:
            try:
                # current_user may not be available in some contexts; guard access
                from flask_login import current_user
                if getattr(current_user, 'is_authenticated', False):
                    user_to_log = current_user
            except Exception:
                user_to_log = None

        try:
            ip_addr = request.remote_addr
        except Exception:
            ip_addr = None

        log_entry = AuditLog(
            user_id=(user_to_log.id if user_to_log else None),
            action=(str(action_description) if action_description is not None else ''),
            ip_address=ip_addr
        )
        db.session.add(log_entry)
        # Do not commit here; caller's transaction should include this log when they commit.
        return log_entry
    except Exception:
        # Swallow exceptions to avoid breaking primary workflows, but record the failure to the app logger.
        logging.exception("Failed to create audit log for action: %s", action_description)
        try:
            db.session.rollback()
        except Exception:
            pass
        return None

def _prefer_memoize(timeout_seconds=3600):
    def _decorator(fn):
        try:
            # cache may be an uninitialized Cache() in import-time; guard usage
            if cache and hasattr(cache, 'memoize'):
                try:
                    return cache.memoize(timeout_seconds)(fn)
                except Exception:
                    # fallback to lru_cache if flask-caching can't decorate (e.g., not init'd)
                    return lru_cache(maxsize=128)(fn)
        except Exception:
            pass
        return lru_cache(maxsize=128)(fn)
    return _decorator
    

@_prefer_memoize(timeout_seconds=3600)
def get_system_account_code(name):
    """
    Retrieve account code by account name.

    - Case-insensitive lookup.
    - Uses flask-caching memoize when available (per-process lru_cache as fallback).
    - Raises LookupError if account not found (preserves existing behaviour).
    """
    if not name:
        raise LookupError("get_system_account_code: account name is required")

    try:
        name_norm = name.strip().lower()
    except Exception:
        name_norm = str(name).strip().lower()

    try:
        account = Account.query.filter(func.lower(Account.name) == name_norm).first()
    except Exception:
        logging.exception("get_system_account_code: DB query failed for account name %r", name)
        raise LookupError(f"Critical system account '{name}' not found (DB query error).")

    if not account:
        logging.error("get_system_account_code: Critical system account '%s' not found.", name)
        raise LookupError(f"Critical system account '{name}' not found.")
    return account.code


def clear_get_system_account_code_cache(name=None):
    """
    Invalidate cached entries for get_system_account_code.

    - Works with flask-caching's delete_memoized when available.
    - Also calls cache_clear on lru_cache-wrapped function if present.
    - If `name` provided, attempt to invalidate that specific key (flask cache supports args).
    """
    # 1) Try flask-caching invalidation
    try:
        if cache and hasattr(cache, 'delete_memoized'):
            # delete_memoized supports (func, *args) for specific keys
            if name is not None:
                try:
                    cache.delete_memoized(get_system_account_code, name)
                except TypeError:
                    # Some older flask-caching impls require just the function
                    cache.delete_memoized(get_system_account_code)
            else:
                cache.delete_memoized(get_system_account_code)
    except Exception:
        logging.exception("clear_get_system_account_code_cache: failed to clear flask cache for %r", name)

    # 2) Try lru_cache cache_clear if present on the callable
    try:
        clear_fn = getattr(get_system_account_code, 'cache_clear', None)
        if callable(clear_fn):
            clear_fn()
    except Exception:
        logging.exception("clear_get_system_account_code_cache: failed to clear lru_cache for %r", name)