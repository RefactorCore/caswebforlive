import os
import sys
import logging

from flask import Flask, redirect, url_for, request, flash
from flask_login import LoginManager, current_user
from flask_migrate import Migrate

from models import db, User, CompanyProfile, Account
from config import Config
from routes.accounts import accounts_bp
from routes.core import core_bp
from extensions import limiter
from routes.utils import cache
from routes.license_utils import verify_anti_tamper

logger = logging.getLogger(__name__)

def create_app():
    # Use RESOURCE_DIR for PyInstaller onefile data (sys._MEIPASS) or BASE_DIR otherwise
    resource_dir = getattr(sys, '_MEIPASS', str(Config.BASE_DIR))
    templates_path = os.path.join(resource_dir, 'templates')
    static_path = os.path.join(resource_dir, 'static')

    # Optional: keep working dir consistent when frozen
    if getattr(sys, 'frozen', False):
        try:
            os.chdir(str(Config.BASE_DIR))
        except Exception:
            logger.exception("Failed to chdir to BASE_DIR in frozen mode")

    app = Flask(
        __name__,
        instance_relative_config=True,
        template_folder=templates_path,
        static_folder=static_path,
        static_url_path='/static',
    )
    app.config.from_object(Config)

    # Anti-tamper check (run early)
    fail_fast = os.environ.get('ANTI_TAMPER_FAIL_FAST', '1') not in ('0', 'false', 'False')
    try:
        ok, mismatches = verify_anti_tamper(fail_fast=fail_fast)
    except Exception:
        logger.exception("Anti-tamper verification raised an exception; marking as tampered.")
        ok = False
        mismatches = ["anti-tamper check exception"]
    app.config['ANTI_TAMPER_OK'] = ok
    app.config['ANTI_TAMPER_MISMATCHES'] = mismatches

    # Cache and rate limiter
    app.config.setdefault('CACHE_TYPE', 'simple')
    cache.init_app(app)
    limiter.init_app(app)

    # Register blueprints (do this once)
    app.register_blueprint(accounts_bp)
    app.register_blueprint(core_bp)
    # Optional: register other blueprints (lazy imports to avoid circulars)
    try:
        from routes.ar_ap import ar_ap_bp
        app.register_blueprint(ar_ap_bp)
    except Exception:
        logger.exception("Failed to import/register ar_ap_bp")
    try:
        from routes.reports import reports_bp
        app.register_blueprint(reports_bp)
    except Exception:
        logger.exception("Failed to import/register reports_bp")
    try:
        from routes.users import user_bp
        app.register_blueprint(user_bp)
    except Exception:
        logger.exception("Failed to import/register user_bp")
    try:
        from routes.consignment import consignment_bp
        app.register_blueprint(consignment_bp)
    except Exception:
        logger.exception("Failed to import/register consignment_bp")
    try:
        from routes.void_transactions import void_bp
        app.register_blueprint(void_bp)
    except Exception:
        logger.exception("Failed to import/register void_bp")

    # DB and migrations
    db.init_app(app)
    Migrate(app, db)

    @app.before_request
    def check_anti_tamper():
        try:
            if not app.config.get('ANTI_TAMPER_OK', True):
                whitelist = ('core.login', 'core.logout', 'core.license_expired', 'static')
                if not (request.endpoint and any(request.endpoint.startswith(ep) for ep in whitelist)):
                    flash('Security violation detected. Service is locked down. Contact administrator.', 'danger')
                    return redirect(url_for('core.login'))
        except Exception:
            logger.exception("Error during anti-tamper before_request check")

    # --- Login Manager ---
    login_manager = LoginManager()
    login_manager.login_view = 'core.login'
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            return None
        try:
            return db.session.get(User, uid)
        except Exception:
            try:
                return User.query.get(uid)
            except Exception:
                logger.exception("Failed to load user id=%r", user_id)
                return None

    # Jinja filters
    def money_filter(value):
        """Format a Decimal/number to a 2-decimal string without currency symbol."""
        try:
            from routes.ar_ap import to_decimal as _to_decimal
            return format(_to_decimal(value), '0.2f')
        except Exception:
            logger.exception("money_filter failed for value=%r", value)
            return "0.00"

    def num_filter(value):
        """Return a native float safe for use with tojson / JS numeric usage."""
        try:
            from routes.ar_ap import to_decimal as _to_decimal
            return float(_to_decimal(value))
        except Exception:
            logger.exception("num_filter failed for value=%r", value)
            return 0.0

    app.jinja_env.filters['money'] = money_filter
    app.jinja_env.filters['num'] = num_filter

    @app.before_request
    def check_setup():
        # Allow these endpoints even when expired
        allowed_when_expired = (
            'core.setup_license', 'core.setup_company', 'core.setup_admin',
            'core.login', 'core.logout', 'core.settings', 'static'
        )
        if request.endpoint and any(request.endpoint.startswith(ep) for ep in allowed_when_expired):
            return

        # License expiration check
        company = CompanyProfile.query.first()
        if company and company.license_data_json and current_user.is_authenticated:
            try:
                from routes.license_utils import get_days_until_expiration
                import json
                license_data = json.loads(company.license_data_json)
                days_left = get_days_until_expiration(license_data)
                if days_left is not None and days_left < 0:
                    if request.endpoint not in ('core.license_expired', 'core.settings'):
                        flash('Your license has expired. Please contact support to renew.', 'danger')
                        return redirect(url_for('core.license_expired'))
            except Exception:
                logger.exception("License check error")

        # Initial setup checks
        if not current_user.is_authenticated and request.endpoint != 'core.login':
            if not CompanyProfile.query.first():
                return redirect(url_for('core.setup_license'))
            elif not User.query.filter_by(role='Admin').first():
                return redirect(url_for('core.setup_admin'))

    # Context processor
    @app.context_processor
    def inject_company_profile():
        company = CompanyProfile.query.first()
        return dict(company=company)

    return app


def seed_essential_data(app):
    """Seeds essential data (Admin user and COA) if the database is empty."""
    accounts_to_seed = [
        ('101', 'Cash', 'Asset'),
        ('102', 'Petty Cash', 'Asset'),
        ('110', 'Accounts Receivable', 'Asset'),
        ('120', 'Inventory', 'Asset'),
        ('121', 'Creditable Withholding Tax', 'Asset'),
        ('132', 'Consignment Goods on Hand', 'Asset'),
        ('201', 'Accounts Payable', 'Liability'),
        ('220', 'Consignment Payable', 'Liability'),
        ('301', 'Capital', 'Equity'),
        ('302', 'Opening Balance Equity', 'Equity'),
        ('401', 'Sales Revenue', 'Revenue'),
        ('402', 'Other Revenue', 'Revenue'),
        ('405', 'Sales Returns', 'Revenue'),
        ('407', 'Discounts Allowed', 'Expense'),
        ('408', 'Consignment Commission Revenue', 'Revenue'),
        ('501', 'COGS', 'Expense'),
        ('601', 'VAT Payable', 'Liability'),
        ('602', 'VAT Input', 'Asset'),
        ('505', 'Inventory Loss', 'Expense'),
        ('406', 'Inventory Gain', 'Revenue'),
        ('510', 'Rent Expense', 'Expense'),
        ('511', 'Utilities Expense', 'Expense'),
        ('512', 'Communication Expense', 'Expense'),
        ('520', 'Salaries and Wages', 'Expense'),
        ('521', 'Employee Benefits', 'Expense'),
        ('530', 'Repairs and Maintenance', 'Expense'),
    ]

    with app.app_context():
        if Account.query.count() == 0:
            print("Seeding Chart of Accounts...")
            try:
                for code, name, typ in accounts_to_seed:
                    db.session.add(Account(code=code, name=name, type=typ))
                db.session.commit()
                print("Chart of Accounts seeded.")
            except Exception as e:
                db.session.rollback()
                print(f"Error seeding COA: {e}")