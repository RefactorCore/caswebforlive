import os
import configparser
from pathlib import Path
import sys

class Config:
    # Determine if running as .exe or script
    if getattr(sys, 'frozen', False):
        # Running as . exe
        BASE_DIR = Path(sys.executable).parent
    else:
        # Running as script
        BASE_DIR = Path(__file__).parent
    
    CONFIG_FILE = BASE_DIR / 'db_config.ini'
    SECRET_FILE = BASE_DIR / '.secret_key'
    
    # Load external config if exists
    if CONFIG_FILE.exists():
        config_parser = configparser.ConfigParser()
        config_parser.read(CONFIG_FILE)
        
        # Build database URL from config
        db_host = config_parser.get('database', 'host', fallback='localhost')
        db_port = config_parser.get('database', 'port', fallback='3306')
        db_user = config_parser.get('database', 'username', fallback='coretally_app')
        db_pass = config_parser.get('database', 'password', fallback='')
        db_name = config_parser.get('database', 'database', fallback='app')
        
        SQLALCHEMY_DATABASE_URI = f'mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?charset=utf8mb4'
        
        # Get other settings
        VAT_RATE = float(config_parser.get('app', 'vat_rate', fallback='0.12'))
        DEBUG = config_parser.getboolean('app', 'debug', fallback=False)
    else:
        # Fallback to environment variables
        SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', '')
        VAT_RATE = float(os.environ.get('VAT_RATE', 0.12))
        DEBUG = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    # Handle SECRET_KEY
    SECRET_KEY = None
    if CONFIG_FILE.exists():
        config_parser = configparser.ConfigParser()
        config_parser.read(CONFIG_FILE)
        SECRET_KEY = config_parser.get('app', 'secret_key', fallback=None)
        if SECRET_KEY == 'AUTO_GENERATED':
            SECRET_KEY = None
    
    if not SECRET_KEY:
        if SECRET_FILE.exists():
            with open(SECRET_FILE, 'r') as f:
                SECRET_KEY = f.read().strip()
        else:
            SECRET_KEY = os.urandom(32).hex()
            with open(SECRET_FILE, 'w') as f:
                f.write(SECRET_KEY)
            try:
                os.chmod(SECRET_FILE, 0o600)
            except Exception:
                pass  # Windows compatibility
    
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping':  True,
        'pool_recycle': 280,
        'pool_size': 10,
        'max_overflow': 20,
        'connect_args':  {
            'charset': 'utf8mb4',
            'connect_timeout': 10,
        }
    }
    
    SESSION_COOKIE_SECURE = False
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = 3600