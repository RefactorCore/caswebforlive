
from flask import Flask, redirect, url_for, request, flash, session
from flask_login import LoginManager, current_user
from routes.accounts import accounts_bp
from flask_migrate import Migrate

from models import db, User, CompanyProfile, Account
from config import Config
from datetime import datetime
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from extensions import limiter
from passlib.hash import pbkdf2_sha256
from routes.utils import cache
import json
import os
import logging
from routes.license_utils import verify_anti_tamper

logger = logging.getLogger(__name__)




def create_app():
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(Config)

    # Anti-tamper check (run early)
    fail_fast = os.environ.get('ANTI_TAMPER_FAIL_FAST', '1') not in ('0', 'false', 'False')
    try:
        ok, mismatches = verify_anti_tamper(fail_fast=fail_fast)
    except Exception:
        # If verification fails unexpectedly, treat as tampered (and log)
        logger.exception("Anti-tamper verification raised an exception; marking as tampered.")
        ok = False
        mismatches = ["anti-tamper check exception"]
    # If not failing fast, you can mark the app as tampered and block requests later:
    app.config['ANTI_TAMPER_OK'] = ok
    app.config['ANTI_TAMPER_MISMATCHES'] = mismatches

    # Configure cache in app.config then init
    app.config.setdefault('CACHE_TYPE', 'simple')
    cache.init_app(app)

    # Initialize rate limiter (from extensions)
    limiter.init_app(app)

    # Register blueprints and initialize DB/migrations
    app.register_blueprint(accounts_bp)
    db.init_app(app)
    migrate = Migrate(app, db)


    @app.before_request
    def check_anti_tamper():
        # Block all non-whitelisted endpoints if anti-tamper failed (soft mode)
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
    # Ensure this points to your actual login endpoint (blueprint_name.view_func_name)
    login_manager.login_view = 'core.login'
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        # Be defensive: user_id can be None or non-numeric
        try:
            uid = int(user_id)
        except (TypeError, ValueError):
            return None
        # Use db.session.get for SQLAlchemy >=1.4; adapt if you use an older API:
        try:
            return db.session.get(User, uid)
        except Exception:
            # Fallback for older SQLAlchemy versions or unexpected errors:
            try:
                return User.query.get(uid)
            except Exception:
                logger.exception("Failed to load user id=%r", user_id)
                return None

    # Register template filters inside create_app to avoid import-time issues
    def money_filter(value):
        """
        Format a Decimal/number to a 2-decimal string WITHOUT currency symbol.
        Templates should add the currency symbol where needed (e.g., ₱{{ value | money }}).
        """
        try:
            # lazy import to avoid circular imports at module import time
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

    # Additional blueprint registrations (if you have others)
    try:
        # lazy import to avoid import-time failures / circular imports
        from routes.void_transactions import void_bp as _void_bp
        app.register_blueprint(_void_bp)
    except Exception:
        # import or registration failed — log full exception but continue startup
        logger.exception("Failed to import or register void_bp blueprint")

    # Return the configured Flask app
    return app

    app.jinja_env.filters['money'] = money_filter
    app.jinja_env.filters['num'] = num_filter

    @app.before_request
    def check_setup():
        from routes.license_utils import get_days_until_expiration
        import json
        
        # ✅ Allow these endpoints even when expired
        allowed_when_expired = ('core.setup_license', 'core.setup_company', 'core.setup_admin', 
                                'core.login', 'core.logout', 'core.settings', 'static')
        
        if request.endpoint and any(request.endpoint.startswith(ep) for ep in allowed_when_expired):
            return

        # Check license expiration (block access if expired)
        company = CompanyProfile.query.first()
        if company and company.license_data_json and current_user.is_authenticated:
            try: 
                license_data = json.loads(company.license_data_json)
                days_left = get_days_until_expiration(license_data)
                
                if days_left is not None and days_left < 0:
                    # ✅ License expired - redirect to renewal page
                    if request.endpoint not in ('core.license_expired', 'core.settings'):
                        flash('⚠️ Your license has expired.  Please contact support to renew.', 'danger')
                        return redirect(url_for('core.license_expired'))  # Create this route
            except Exception as e:
                # ✅ Log the error instead of silently ignoring
                print(f"❌ License check error: {e}")
                import traceback
                traceback.print_exc()

        # Existing setup checks...  
        if not current_user.is_authenticated and request.endpoint != 'core.login':
            if not CompanyProfile.query.first():
                return redirect(url_for('core.setup_license'))
            elif not User.query.filter_by(role='Admin').first():
                return redirect(url_for('core.setup_admin'))



    # --- Context Processor ---
    @app.context_processor
    def inject_company_profile():
        """Injects company profile data into all templates."""
        company = CompanyProfile.query.first()
        return dict(company=company)

    # --- Blueprints ---
    from routes.core import core_bp
    from routes.ar_ap import ar_ap_bp
    from routes.reports import reports_bp
    from routes.users import user_bp
    from routes.consignment import consignment_bp
    from routes.void_transactions import void_bp


    app.register_blueprint(core_bp)
    app.register_blueprint(ar_ap_bp)
    app.register_blueprint(reports_bp)
    app.register_blueprint(user_bp)
    app.register_blueprint(consignment_bp)
    app.register_blueprint(void_bp)

    return app

def seed_essential_data(app):
    """Seeds essential data (Admin user and COA) if the database is empty."""
    
    # Define the Chart of Accounts list here
    accounts_to_seed = [
        ('101','Cash','Asset'),
        ('102','Petty Cash','Asset'),
        ('110', 'Accounts Receivable', 'Asset'),
        ('120','Inventory','Asset'),
        ('121', 'Creditable Withholding Tax', 'Asset'),
        ('132', 'Consignment Goods on Hand', 'Asset'),
        ('201','Accounts Payable','Liability'),
        ('220', 'Consignment Payable', 'Liability'), 
        ('301','Capital','Equity'),
        ('302', 'Opening Balance Equity', 'Equity'),
        ('401','Sales Revenue','Revenue'),
        ('402','Other Revenue','Revenue'),
        ('405', 'Sales Returns', 'Revenue'),
        ('407', 'Discounts Allowed', 'Expense'),
        ('408', 'Consignment Commission Revenue', 'Revenue'),
        ('501','COGS','Expense'),
        ('601','VAT Payable','Liability'),
        ('602','VAT Input','Asset'),
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
        # Check 1: Check for existing accounts
        if Account.query.count() == 0:
            print("🌱 Seeding Chart of Accounts...")
            try:
                for code, name, typ in accounts_to_seed:
                    a = Account(code=code, name=name, type=typ)
                    db.session.add(a)
                db.session.commit()
                print("✅ Chart of Accounts seeded.")
            except Exception as e:
                db.session.rollback()
                print(f"❌ Error seeding COA: {e}")




if __name__ == '__main__': 
    app = create_app()
    with app.app_context():
        # Verify MariaDB connection
        try: 
            engine_name = db.engine.dialect.name
            if engine_name not in ('mysql', 'mariadb'):
                raise RuntimeError(f"❌ Unsupported database:  {engine_name}. MariaDB/MySQL required.")
            print(f"✅ Connected to {engine_name.upper()} database")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            exit(1)
        
        # 1. Create all tables
        db.create_all()
        
        # 2. Seed the essential data
        seed_essential_data(app)
        
    # Listen on all interfaces for LAN access (offline web app)
    print(f"🌐 Starting offline web app on http://0.0.0.0:5000")
    app.run(
    host='0.0.0.0', 
    port=5000, 
    debug=False,  # ✅ Disable debug mode
    threaded=True  # Enable multi-threading for better performance
)



