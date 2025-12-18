import os
import sys
import time
import socket
import threading
import logging
import webbrowser
from pathlib import Path

from config import Config
from app import create_app, seed_essential_data
from models import db

def get_lan_ip() -> str:
    """Return the host's LAN IP (best-effort), fallback to 127.0.0.1."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Doesn't need to be reachable; used to pick the right interface
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        try:
            s.close()
        except Exception:
            pass
    return ip

def open_browser_later(url: str, delay: float = 1.5):
    """Open default browser to URL after a short delay (so server is ready)."""
    def _open():
        try:
            webbrowser.open(url, new=2)  # new=2 -> new tab, if possible
        except Exception:
            pass
    threading.Timer(delay, _open).start()

def get_log_directory():
    """
    Get a suitable log directory with write permissions.
    
    Priority: 
    1. LOG_DIR environment variable
    2. User's AppData/Roaming (Windows) or ~/.local/share (Linux)
    3. Fallback to BASE_DIR/logs if writable
    """
    # Check environment variable first
    env_log_dir = os.environ.get('CORETALLY_LOG_DIR')
    if env_log_dir:
        log_dir = Path(env_log_dir)
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            # Test write permission
            test_file = log_dir / '.write_test'
            test_file.touch()
            test_file.unlink()
            return log_dir
        except Exception:
            logging.warning(f"Cannot write to LOG_DIR: {log_dir}")
    
    # Platform-specific user data directory
    try:
        if os.name == 'nt':  # Windows
            base = Path(os.environ.get('APPDATA', Path.home() / 'AppData' / 'Roaming'))
            log_dir = base / 'CoreTally' / 'logs'
        else:  # Linux/Mac
            log_dir = Path.home() / '.local' / 'share' / 'coretally' / 'logs'
        
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir
    except Exception:
        pass
    
    # Fallback:  try BASE_DIR/logs
    try:
        log_dir = Config.BASE_DIR / 'logs'
        log_dir.mkdir(exist_ok=True)
        return log_dir
    except Exception: 
        # Last resort: temp directory
        import tempfile
        return Path(tempfile.gettempdir()) / 'coretally_logs'

def initialize_database(app):
    """Initialize database tables and seed essential data if needed."""
    with app.app_context():
        try:
            # Check if database needs initialization
            from models import Account, User, CompanyProfile
            
            # Try to query - if tables don't exist, this will fail
            try:
                Account.query.first()
                logging.info("Database tables already exist")
            except Exception:  
                logging.info("Creating database tables...")
                db.create_all()
                logging.info("Database tables created successfully")
            
            # Seed essential data if Chart of Accounts is empty
            if Account.query.count() == 0:
                logging.info("Seeding essential data...")
                seed_essential_data(app)
                logging.info("Essential data seeded successfully")
            else:
                logging.info("Database already contains data")
                
        except Exception as e:  
            logging.exception(f"Error initializing database: {e}")
            raise

if __name__ == '__main__':  
    # ✅ Get log directory with fallback options
    log_dir = get_log_directory()
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"WARNING: Could not create log directory {log_dir}:  {e}")
        import tempfile
        log_dir = Path(tempfile.gettempdir())
    
    logfile = log_dir / 'coretally.log'
    
    # ✅ Add log rotation to prevent huge log files
    from logging.handlers import RotatingFileHandler
    
    # Create handlers
    file_handler = RotatingFileHandler(
        logfile,
        maxBytes=10*1024*1024,  # 10 MB
        backupCount=5,  # Keep 5 backup files
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Configure logging
    logging.basicConfig(
        level=os.environ.get('LOGLEVEL', 'INFO'),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        handlers=[file_handler, console_handler]
    )
    
    logging.info(f"📁 Logging to:  {logfile}")
    logging.info(f"🖥️  Running from: {Config.BASE_DIR}")

    app = create_app()
    
    # Initialize database on startup
    logging.info("Checking database initialization...")
    try:
        initialize_database(app)
    except Exception as e:  
        logging.error(f"Failed to initialize database:  {e}")
        logging.error("Please check your database configuration in db_config.ini")
        sys.exit(1)

    # Bind to all interfaces so other devices on LAN can connect
    host_bind = os.environ.get('FLASK_HOST', '0.0.0.0')
    port = int(os.environ.get('FLASK_PORT', '5000'))

    # Build a user-friendly URL using the host's LAN IP
    lan_ip = get_lan_ip()
    url = f'http://{lan_ip}:{port}/'

    # Open default browser shortly after starting
    open_browser_later(url, delay=1.5)

    # Prefer Waitress for production serving
    use_waitress = os.environ.get('USE_WAITRESS', '1') not in ('0', 'false', 'False')
    if use_waitress:
        try:
            from waitress import serve
            threads = int(os.environ.get('WAITRESS_THREADS', '8'))
            logging.info(f"🚀 Starting Coretally at {url} (Waitress, threads={threads})")
            serve(app, host=host_bind, port=port, threads=threads)
        except Exception:  
            logging.exception("Waitress failed; falling back to Flask dev server")
            debug = getattr(Config, 'DEBUG', False)
            app.run(host=host_bind, port=port, debug=debug, use_reloader=False)
    else:
        # Dev-only fallback
        debug = getattr(Config, 'DEBUG', False)
        logging.info(f"🚀 Starting Coretally at {url} (Flask dev server)")
        app.run(host=host_bind, port=port, debug=debug, use_reloader=False)