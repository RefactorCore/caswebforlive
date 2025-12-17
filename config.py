import os
import configparser
from pathlib import Path
import sys

class Config:
    if getattr(sys, 'frozen', False):
        BASE_DIR = Path(sys.executable).parent
    else:
        BASE_DIR = Path(__file__).parent

    RESOURCE_DIR = Path(getattr(sys, '_MEIPASS', BASE_DIR))

    CONFIG_FILE_RUNTIME = BASE_DIR / 'db_config.ini'
    CONFIG_FILE_BUNDLED = RESOURCE_DIR / 'db_config.ini'

    # Prefer storing the secret near the EXE, but fall back to a user-writable location
    def _user_secret_path():
        try:
            if os.name == 'nt':
                base = Path(os.environ.get('APPDATA', str(Path.home() / 'AppData' / 'Roaming')))
                return base / 'caswebforlive' / '.secret_key'
            else:
                return Path.home() / '.caswebforlive' / '.secret_key'
        except Exception:
            return Path.home() / '.secret_key'

    SECRET_FILE = BASE_DIR / '.secret_key'
    USER_SECRET_FILE = _user_secret_path()

    config_parser = configparser.ConfigParser()
    if CONFIG_FILE_RUNTIME.exists():
        config_parser.read(CONFIG_FILE_RUNTIME)
    elif CONFIG_FILE_BUNDLED.exists():
        config_parser.read(CONFIG_FILE_BUNDLED)

    if config_parser.sections():
        db_host = config_parser.get('database', 'host', fallback='localhost')
        db_port = config_parser.get('database', 'port', fallback='3306')
        db_user = config_parser.get('database', 'username', fallback='coretally_app')
        db_pass = config_parser.get('database', 'password', fallback='')
        db_name = config_parser.get('database', 'database', fallback='app')
        SQLALCHEMY_DATABASE_URI = f'mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?charset=utf8mb4'
        VAT_RATE = float(config_parser.get('app', 'vat_rate', fallback='0.12'))
        DEBUG = config_parser.getboolean('app', 'debug', fallback=False)
    else:
        SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL', '')
        VAT_RATE = float(os.environ.get('VAT_RATE', 0.12))
        DEBUG = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

    if not SQLALCHEMY_DATABASE_URI:
        raise RuntimeError("DATABASE_URL not configured and no db_config.ini present.")

    SECRET_KEY = None
    if config_parser.sections():
        SECRET_KEY = config_parser.get('app', 'secret_key', fallback=None)
        if SECRET_KEY == 'AUTO_GENERATED':
            SECRET_KEY = None

    if not SECRET_KEY:
        try:
            # Try runtime-local secret
            if Config.SECRET_FILE.exists():
                SECRET_KEY = Config.SECRET_FILE.read_text().strip()
            else:
                # Try user-writable secret
                if not Config.USER_SECRET_FILE.parent.exists():
                    Config.USER_SECRET_FILE.parent.mkdir(parents=True, exist_ok=True)
                if Config.USER_SECRET_FILE.exists():
                    SECRET_KEY = Config.USER_SECRET_FILE.read_text().strip()
                else:
                    SECRET_KEY = os.urandom(32).hex()
                    # Prefer writing to user-writable dir
                    Config.USER_SECRET_FILE.write_text(SECRET_KEY)
                    try:
                        os.chmod(Config.USER_SECRET_FILE, 0o600)
                    except Exception:
                        pass
        except Exception:
            SECRET_KEY = os.urandom(32).hex()

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping': True,
        'pool_recycle': 280,
        'pool_size': 10,
        'max_overflow': 20,
        'connect_args': {
            'charset': 'utf8mb4',
            'connect_timeout': 10,
        }
    }

    SESSION_COOKIE_SECURE = False
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = 3600