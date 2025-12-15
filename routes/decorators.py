from functools import wraps
from flask_login import current_user
from flask import flash, redirect, url_for

def role_required(*roles):
    """
    Custom decorator to restrict access to users with specific roles.
    Example: @role_required('Admin', 'Accountant')
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                # This should be handled by @login_required, but act as safe fallback
                return redirect(url_for('core.login'))
            # Defensive role access check (handles missing attribute and case-insensitivity)
            user_role = getattr(current_user, 'role', None)
            if user_role is None:
                flash('You do not have permission to access this page.', 'danger')
                return redirect(url_for('core.index'))
            # normalize case for comparison
            allowed = {r.lower() for r in roles}
            if user_role.lower() not in allowed:
                flash('You do not have permission to access this page.', 'danger')
                return redirect(url_for('core.index'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator