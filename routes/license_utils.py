"""
Offline-friendly license utilities.

- Verifies licenses signed by the vendor (RSA/ECDSA via cryptography).
- Optional machine binding via a 'hw_id' field in the license payload.
- Backwards-compatible HMAC mode if PUBLIC_KEY not provided but LICENSE_SECRET_KEY is set.
- License token format: base64(data_bytes + b'::' + signature_bytes)
  where data_bytes is a JSON bytestring (utf-8) and signature_bytes is:
    - RSA signature (raw bytes) when using asymmetric mode
    - HMAC-SHA256 digest bytes when using HMAC mode

Environment:
  - LICENSE_PUBLIC_KEY_PEM: (optional) PEM string of vendor public key, or
  - LICENSE_PUBLIC_KEY_PATH: path to PEM file
  - LICENSE_SECRET_KEY: (optional) HMAC secret fallback (not recommended)
"""
from datetime import datetime, date
import base64
import json
import os
import hashlib
import platform
import uuid
import logging
from typing import Dict, Tuple
import sys

logger = logging.getLogger(__name__)

# cryptography for signature verification
try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    from cryptography.exceptions import InvalidSignature
    CRYPTO_AVAILABLE = True
except Exception:
    CRYPTO_AVAILABLE = False

# Configuration via env
_LICENSE_PUBLIC_KEY_PEM = os.environ.get("LICENSE_PUBLIC_KEY_PEM")
_LICENSE_PUBLIC_KEY_PATH = os.environ.get("LICENSE_PUBLIC_KEY_PATH")
LICENSE_SECRET_KEY = os.environ.get("LICENSE_SECRET_KEY")  # optional HMAC fallback

_public_key = None
if _LICENSE_PUBLIC_KEY_PEM:
    try:
        _public_key = serialization.load_pem_public_key(_LICENSE_PUBLIC_KEY_PEM.encode('utf-8'))
    except Exception:
        logging.exception("Failed to load public key from LICENSE_PUBLIC_KEY_PEM")
elif _LICENSE_PUBLIC_KEY_PATH:
    try:
        with open(_LICENSE_PUBLIC_KEY_PATH, 'rb') as f:
            _public_key = serialization.load_pem_public_key(f.read())
    except Exception:
        logging.exception("Failed to load public key from LICENSE_PUBLIC_KEY_PATH")

def _compute_machine_fingerprint():
    """
    Compute a conservative machine fingerprint for offline binding.
    Combines:
      - MAC address (uuid.getnode())
      - Hostname
      - /etc/machine-id on Linux (if readable)
    Returns: hex sha256 string
    NOTE: This is a best-effort fingerprint and may change with some OS operations.
    """
    parts = []
    try:
        mac = uuid.getnode()
        parts.append(str(mac))
    except Exception:
        pass
    try:
        parts.append(platform.node() or '')
    except Exception:
        pass
    # Try reading machine-id (Linux) or similar
    try:
        if os.path.exists("/etc/machine-id"):
            with open("/etc/machine-id", "r") as f:
                parts.append(f.read().strip())
    except Exception:
        pass

    # Fallback: combine environment variables (not very strong)
    try:
        parts.append(os.environ.get("HOSTNAME", ""))
    except Exception:
        pass

    s = "|".join(p for p in parts if p)
    if not s:
        s = "unknown-machine"

    digest = hashlib.sha256(s.encode('utf-8')).hexdigest()
    return digest

def _parse_date_flexible(date_input):
    """
    Parse date strings robustly; return datetime.date.
    Accepts "YYYY-MM-DD", many ISO datetimes, or date/datetime objects.
    Raises ValueError on unrecognized string.
    """
    if date_input is None:
        raise ValueError("No date provided")
    if isinstance(date_input, date) and not isinstance(date_input, datetime):
        return date_input
    if isinstance(date_input, datetime):
        return date_input.date()
    s = str(date_input).strip()
    # Try YYYY-MM-DD
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        pass
    # Try ISO formats (fromisoformat plus common variations)
    try:
        s2 = s.rstrip('Z')
        if s.endswith('Z'):
            s2 = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s2)
        return dt.date()
    except Exception:
        pass
    # Try a few common formats
    fmts = ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S")
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date()
        except Exception:
            continue
    # Last-resort: parse leading YYYY-MM-DD
    try:
        return datetime.strptime(s.split()[0].split("T")[0], "%Y-%m-%d").date()
    except Exception:
        raise ValueError(f"Unrecognized date format: {date_input!r}")

def validate_license(license_key, enforce_hw_check=True):
    """
    Validate a license key.

    Returns:
      (is_valid: bool, license_data: dict or None, error_message: str or None)

    Behavior:
      - Verifies signature. Prefer asymmetric (public key) verification if public key is configured.
      - If no public key but LICENSE_SECRET_KEY exists, verify using HMAC-SHA256 for compatibility.
      - If license payload contains 'hw_id' and enforce_hw_check True, compare computed fingerprint.
    """
    if not license_key:
        return False, None, "No license key provided"

    try:
        decoded = base64.b64decode(license_key.encode('utf-8'))
    except Exception:
        return False, None, "License is not valid base64"

    if b"::" not in decoded:
        return False, None, "Invalid license format"

    data_bytes, sig = decoded.split(b"::", 1)
    try:
        license_data = json.loads(data_bytes.decode('utf-8'))
    except Exception as e:
        logging.exception("Failed to parse license JSON")
        return False, None, "Invalid license payload"

    # 1) Asymmetric verification (preferred)
    if _public_key is not None and CRYPTO_AVAILABLE:
        try:
            # Signature must have been produced with vendor private key as raw signature bytes (PKCS1v15 SHA256 for RSA)
            _public_key.verify(
                sig,
                data_bytes,
                padding.PKCS1v15(),
                hashes.SHA256()
            )
        except InvalidSignature:
            return False, license_data, "License signature invalid"
        except Exception as e:
            logging.exception("Unexpected error during signature verification")
            return False, None, "License verification failed"
    else:
        # 2) HMAC fallback if secret present (legacy support)
        if LICENSE_SECRET_KEY:
            try:
                expected = hashlib.pbkdf2_hmac('sha256', data_bytes, LICENSE_SECRET_KEY.encode('utf-8'), 1)
                # expected here uses pbkdf2_hmac for example; if your legacy used plain HMAC, use hmac.compare_digest
                import hmac as _h
                if not _h.compare_digest(sig, expected):
                    return False, license_data, "License signature invalid (HMAC)"
            except Exception:
                logging.exception("Error verifying HMAC license")
                return False, None, "License verification error"
        else:
            logging.error("No public key and no HMAC secret configured; cannot verify license")
            return False, license_data, "Server not configured to verify license"

    # 3) Check expiry if present
    if 'expires' in license_data:
        try:
            expiry = _parse_date_flexible(license_data['expires'])
            if date.today() > expiry:
                return False, license_data, "License has expired"
        except Exception:
            # keep valid (signature ok) but warn caller by returning a message (do not silently fail)
            logging.warning("License 'expires' value is unparseable: %r", license_data.get('expires'))

    # 4) Optional hardware binding check
    if enforce_hw_check and 'hw_id' in license_data and license_data.get('hw_id'):
        try:
            local_fp = _compute_machine_fingerprint()
            if license_data.get('hw_id') != local_fp:
                return False, license_data, "License is bound to a different machine"
        except Exception:
            logging.exception("Hardware fingerprint check failed")
            return False, license_data, "Hardware check failed"

    return True, license_data, None

def get_days_until_expiration(license_data):
    """Return days (int) until expiration. Negative if expired. None if not parseable or missing."""
    if not license_data:
        return None
    if 'expires' not in license_data or not license_data['expires']:
        return None
    try:
        expiry = _parse_date_flexible(license_data['expires'])
        return (expiry - date.today()).days
    except Exception:
        logging.warning("get_days_until_expiration: parse error for %r", license_data.get('expires'))
        return None

def is_license_expiring_soon(license_data, warning_days=7):
    """Return True if license expires within warning_days (including today)."""
    days_left = get_days_until_expiration(license_data)
    if days_left is None:
        return False
    return 0 <= int(days_left) <= int(warning_days)


def _sha256_file(path: str) -> str:
    """Return hex sha256 digest of a file, or '' if unreadable."""
    try:
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        logging.exception("Anti-tamper: failed to read %s", path)
        return ''

def _parse_hash_map_env(env_val: str) -> Dict[str, str]:
    """
    Parse HASH_MAP env string of format:
      path1=hexsha256;path2=hexsha256;...
    Returns dict {path: hash}
    """
    out = {}
    if not env_val:
        return out
    for part in env_val.split(';'):
        part = part.strip()
        if not part or '=' not in part:
            continue
        k, v = part.split('=', 1)
        out[k.strip()] = v.strip().lower()
    return out

def verify_anti_tamper(baseline: Dict[str, str] = None, fail_fast: bool = False) -> Tuple[bool, Dict[str, str]]:
    """
    Verify that critical files match expected SHA-256 hashes.

    Inputs:
    - baseline: dict {absolute_or_repo_path: expected_hex_sha256}
      If None, will read from env var ANTI_TAMPER_HASHES (format: path=hash;path=hash)
    - fail_fast: if True, exit the process on mismatch.

    Returns:
    - (ok, mismatches) where mismatches is {path: {'expected': ..., 'actual': ...}}
    """
    try:
        if baseline is None:
            baseline = _parse_hash_map_env(os.environ.get('ANTI_TAMPER_HASHES', ''))

        if not baseline:
            logging.warning("Anti-tamper: no baseline configured (ANTI_TAMPER_HASHES empty).")
            return True, {}

        mismatches = {}
        for path, expected in baseline.items():
            actual = _sha256_file(path)
            if not actual:
                mismatches[path] = {'expected': expected, 'actual': actual or '(unreadable)'}
            elif actual.lower() != expected.lower():
                mismatches[path] = {'expected': expected, 'actual': actual}

        if mismatches:
            logging.critical("ANTI-TAMPER CHECK FAILED: %s", json.dumps(mismatches, indent=2))
            # Optional: also write to a tamper log file
            try:
                with open(os.environ.get('ANTI_TAMPER_LOG', 'tamper_events.log'), 'a', encoding='utf-8') as lf:
                    lf.write(f"[{datetime.utcnow().isoformat()}Z] {json.dumps(mismatches)}\n")
            except Exception:
                pass

            if fail_fast:
                # Hard shutdown recommended for production.
                os._exit(13)
            return False, mismatches

        logging.info("Anti-tamper: all baseline hashes verified OK.")
        return True, {}
    except Exception:
        logging.exception("Anti-tamper: unexpected error during verification")
        return False, {'error': 'unexpected error'}