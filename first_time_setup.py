import configparser
import sys
from pathlib import Path

def get_base_dir():
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent
    return Path(__file__).parent

def run_setup():
    print("=" * 60)
    print("CAS Web Accounting - First Time Setup")
    print("=" * 60)

    config = configparser.ConfigParser()

    # Database settings
    print("\n[DATABASE CONFIGURATION]")
    db_host = input("Database Host [localhost]: ").strip() or 'localhost'
    db_port = input("Database Port [3306]: ").strip() or '3306'
    db_user = input("Database Username:  ").strip()
    db_pass = input("Database Password: ").strip()
    db_name = input("Database Name: ").strip()

    config['database'] = {
        'host': db_host,
        'port': db_port,
        'username': db_user,
        'password': db_pass,
        'database': db_name
    }

    # App settings
    print("\n[APPLICATION SETTINGS]")
    vat_rate = input("VAT Rate [0.12]: ").strip() or '0.12'

    config['app'] = {
        'secret_key': 'AUTO_GENERATED',
        'vat_rate': vat_rate,
        'debug': 'False'
    }

    # License settings
    config['license'] = {
        'public_key_path': 'vendor_public_key.pem'  # ensure this file is bundled or path is correct
    }

    # Save config next to the executable/script
    base_dir = get_base_dir()
    config_file = base_dir / 'db_config.ini'
    with open(config_file, 'w') as f:
        config.write(f)

    print(f"\n✅ Configuration saved to {config_file}")
    print("\nYou can now run CASWebAccounting.exe")
    input("\nPress Enter to continue...")

if __name__ == '__main__':
    run_setup()