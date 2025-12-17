import os
import sys
import time
import socket
import threading
import logging
import webbrowser

from config import Config
from app import create_app

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

if __name__ == '__main__':
    # Log to file when console is hidden; keep it simple and robust
    log_dir = Config.BASE_DIR / 'logs'
    try:
        log_dir.mkdir(exist_ok=True)
    except Exception:
        pass
    logfile = log_dir / 'coretally.log'

    logging.basicConfig(
        level=os.environ.get('LOGLEVEL', 'INFO'),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        handlers=[
            logging.FileHandler(logfile, encoding='utf-8'),
            # You can also add a StreamHandler for dev runs with console
        ],
    )

    app = create_app()

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
            logging.info(f"Starting Coretally at {url} (Waitress, threads={threads})")
            serve(app, host=host_bind, port=port, threads=threads)
        except Exception:
            logging.exception("Waitress failed; falling back to Flask dev server")
            debug = getattr(Config, 'DEBUG', False)
            app.run(host=host_bind, port=port, debug=debug, use_reloader=False)
    else:
        # Dev-only fallback
        debug = getattr(Config, 'DEBUG', False)
        logging.info(f"Starting Coretally at {url} (Flask dev server)")
        app.run(host=host_bind, port=port, debug=debug, use_reloader=False)