"""
Custom functions for pydicom/deid recipes (FORMAT dicom).

Uses MD5-based UID hashing exclusively (per request).

Batch-level variables are expected to be injected into each file's identifier dict
by the runner:
  - uid_root (str)
  - uid_trunc (int)
  - uid_override (bool)
  - date_shift_days (int)

Patient mapping is loaded from CSV once per run, keyed by from_patient_id:
  from_patient_id,to_patient_id,to_patient_name,collection_name,site_name
"""
from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from deid.utils import parse_keyvalue_pairs

PATIENT_MAP: Dict[str, Dict[str, str]] = {}

def load_patient_map_csv(path: str) -> None:
    """Load mapping CSV keyed by from_patient_id."""
    global PATIENT_MAP
    p = Path(path)
    with p.open("r", newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        required = {"from_patient_id","to_patient_id","to_patient_name","collection_name","site_name"}
        missing = required - set((r.fieldnames or []))
        if missing:
            raise ValueError(f"patient_map CSV missing headers: {sorted(missing)}")
        out: Dict[str, Dict[str, str]] = {}
        for row in r:
            k = (row.get("from_patient_id") or "").strip()
            if not k:
                continue
            out[k] = {kk: (row.get(kk) or "").strip() for kk in required}
        PATIENT_MAP = out

def _get(item: Dict[str, Any], key: str, default: Any) -> Any:
    return item.get(key, default)

def hash_uid(item: Dict[str, Any], value: Any, field: Any, dicom: Any, **kwargs) -> Optional[str]:
    """Deterministic MD5-based UID hashing.

    Special behavior:
      - If uid_root begins with '!' then strip the '!' and force override=True.
        (This allows recipe authors to request re-hashing even for UIDs that
         already start with the configured root.)
    """
    uid = getattr(dicom, field.name, None) if hasattr(field, "name") else None
    if uid in (None, ""):
        return None
    uid = str(uid)

    uid_root = _get(item, "uid_root", "1.3.6.1.4.1.14519.5.2.1")
    trunc = int(_get(item, "uid_trunc", 64))
    override = bool(_get(item, "uid_override", False))

    # Support "!" prefix to mean "force override"
    if isinstance(uid_root, str) and uid_root.startswith("!"):
        uid_root = uid_root[1:]
        override = True

    if uid.startswith(uid_root) and not override:
        return uid

    md5 = hashlib.md5(uid.encode("utf-8"))
    return f"{uid_root}.{int(md5.hexdigest(), 16)}"[:trunc]

def shift_date_or_datetime(item: Dict[str, Any], value: Any, field: Any, dicom: Any, **kwargs) -> Optional[str]:
    """Shift DA (YYYYMMDD) or DT (YYYYMMDDHHMMSS[.ffffff]) by date_shift_days."""
    days = int(_get(item, "date_shift_days", 0))
    v = getattr(dicom, field.name, None) if hasattr(field, "name") else None
    if v in (None, ""):
        return None
    s = str(v)

    # DA
    if len(s) == 8 and s.isdigit():
        try:
            dt = datetime.strptime(s, "%Y%m%d") + timedelta(days=days)
            return dt.strftime("%Y%m%d")
        except Exception:
            return None

    # DT with optional fractional seconds
    frac = ""
    if "." in s:
        base, frac_tail = s.split(".", 1)
        frac = "." + frac_tail
    else:
        base = s

    fmts = ["%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y%m%d%H", "%Y%m%d"]
    for fmt in fmts:
        try:
            base_len = len(datetime.now().strftime(fmt))
            dt = datetime.strptime(base[:base_len], fmt) + timedelta(days=days)
            return dt.strftime(fmt) + frac
        except Exception:
            continue
    return None

def _orig_patient_id(item: Dict[str, Any], dicom: Any) -> Optional[str]:
    pid = item.get("PatientID")
    if pid in (None, ""):
        pid = getattr(dicom, "PatientID", None)
    return str(pid) if pid not in (None, "") else None

def map_patient_id(item: Dict[str, Any], value: Any, field: Any, dicom: Any, **kwargs) -> Optional[str]:
    pid = _orig_patient_id(item, dicom)
    if not pid:
        return None
    row = PATIENT_MAP.get(pid)
    return (row or {}).get("to_patient_id") or None

def map_patient_name(item: Dict[str, Any], value: Any, field: Any, dicom: Any, **kwargs) -> Optional[str]:
    pid = _orig_patient_id(item, dicom)
    if not pid:
        return None
    row = PATIENT_MAP.get(pid)
    return (row or {}).get("to_patient_name") or None

def hashname(item: Dict[str, Any], value: Any, field: Any, dicom: Any, **kwargs) -> Optional[str]:
    """CTP-like @hashname: prefix + first N hex of SHA256(value)."""
    opts = parse_keyvalue_pairs(kwargs.get("extras"))
    prefix = opts.get("prefix") or ""
    length = int(opts.get("length") or 4)

    v = getattr(dicom, field.name, None) if hasattr(field, "name") else None
    if v in (None, ""):
        return None
    s = str(v).strip().encode("utf-8", errors="ignore")
    h = hashlib.sha256(s).hexdigest().upper()
    return f"{prefix}{h[:length]}"
