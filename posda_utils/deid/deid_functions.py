"""
Custom functions for pydicom/deid recipes (FORMAT dicom).

Uses MD5-based UID hashing exclusively.

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
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from pydicom.dataset import Dataset
from pydicom.sequence import Sequence
#from deid.utils import parse_keyvalue_pairs

PATIENT_MAP: Dict[str, Dict[str, str]] = {}


CID7050 = {
    "113100": "Basic Application Confidentiality Profile",
    "113101": "Clean Pixel Data Option",
    "113102": "Clean Recognizable Visual Features Option",
    "113103": "Clean Graphics Option",
    "113104": "Clean Structured Content Option",
    "113105": "Clean Descriptors Option",
    "113106": "Retain Longitudinal With Full Dates Option",
    "113107": "Retain Longitudinal Temporal Information Modified Dates Option",
    "113108": "Retain Patient Characteristics Option",
    "113109": "Retain Device Identity Option",
    "113110": "Retain UIDs",
    "113111": "Retain Safe Private Option",
}


def load_patient_map_csv(path: str) -> None:
    """Load mapping CSV keyed by from_patient_id."""
    global PATIENT_MAP
    p = Path(path)
    with p.open("r", newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        required = {"from_patient_id","to_patient_id","to_patient_name","collection_name","site_code","site_name","batch_number","date_shift","diagnosis_date","baseline_date","uid_root"}
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


def parse_keyvalue_pairs(pairs: str | None) -> dict[str, Any]:
    """
    Parse key-value pairs from a string, handling quoted values with spaces.
    Example: txt="Per DICOM PS 3.15 AnnexE. Details in 0012,0064" foo=bar
    """
    import re
    # print("parse_keyvalue_pairs input:", pairs)
    values = {}
    if not pairs:
        # print("parse_keyvalue_pairs output:", values)
        return values
    # Split respecting quotes
    # This regex finds key=value pairs, where value may be quoted (single/double) or unquoted
    token_pattern = re.compile(r'(\w+)=((?:"[^"]*"|\'[^"]*\'|[^\s]+))')
    pos = 0
    while pos < len(pairs):
        m = token_pattern.match(pairs, pos)
        if m:
            key, value = m.groups()
            # Remove surrounding quotes after capturing the full value
            if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            # Convert booleans and nulls
            if value == "true":
                value = True
            elif value == "false":
                value = False
            elif value.lower() in ["none", "null"]:
                value = None
            values[key.strip()] = value
            pos = m.end()
        else:
            # Skip whitespace or any non-matching character
            pos += 1
    # print("parse_keyvalue_pairs output:", values)
    return values


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

    uid_root = item.get("uid_root", "1.3.6.1.4.1.14519.5.2.1")
    trunc = int(item.get("uid_trunc", 64))
    override = bool(item.get("uid_override", False))

    # Support "!" prefix to mean "force override"
    if isinstance(uid_root, str) and uid_root.startswith("!"):
        uid_root = uid_root[1:]
        override = True

    if uid.startswith(uid_root) and not override:
        return uid

    md5 = hashlib.md5(uid.encode("utf-8"))
    return f"{uid_root}.{int(md5.hexdigest(), 16)}"[:trunc]


def increment_date(item: Dict[str, Any], value: Any, field: Any, dicom: Any, **kwargs) -> Optional[str]:
    """Shift DA (YYYYMMDD) or DT (YYYYMMDDHHMMSS[.ffffff]) by date_shift_days."""
    days = int(item.get("date_shift_days", 0))
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


def set_deid_method(item: Dict[str, Any], value: Any, field: Any, dicom: Any, **kwargs) -> Optional[list[str]]:
    """
    Set DeidentificationMethod (0012,0063).
    If the tag exists with a value, add txt as a new value to the list.
    If it doesn't exist, create the list with txt as the only value.
    DeidentificationMethod is a multi-valued LO (list of strings).
    """
    opts = parse_keyvalue_pairs(kwargs.get("extras"))
    txt_str = opts.get("txt")  
    if not txt_str:
        return None

    # If tag doesn't exist or is None, create new list
    if not hasattr(dicom, field) or getattr(dicom, field) is None:
        return [txt_str]

    # If tag exists, append txt if not already present
    existing = getattr(dicom, field) or []
    # Ensure existing is a list of strings
    if isinstance(existing, str):
        existing = [existing]
    elif not isinstance(existing, list):
        existing = list(existing)

    if txt_str not in existing:
        existing.append(txt_str)

    return existing


def set_deid_method_codes(item: Dict[str, Any], value: Any, field: Any, dicom: Any, **kwargs) -> Optional[Sequence]:
    """
    Set DeidentificationMethodCodeSequence (0012,0064) from CID 7050 codes.

    Supports parameters via deid_func:
      - codes="113100/113101/..."  (slash-delimited)
    """
    opts = parse_keyvalue_pairs(kwargs.get("extras"))

    # allow override from recipe parameters, else fall back to injected item defaults
    codes_str = (opts.get("codes") or item.get("deid_method_codes") or "").strip()
    codes = [c.strip() for c in codes_str.split("/") if c.strip()]
    if not codes:
        return None

    new_items = []
    for code_value in codes:
        item_ds = Dataset()
        item_ds.CodeValue = code_value
        item_ds.CodingSchemeDesignator = "DCM"
        item_ds.CodeMeaning = CID7050.get(code_value, "De-identification Method")
        new_items.append(item_ds)

    if not hasattr(dicom, field) or getattr(dicom, field) is None:
        return Sequence(new_items)

    # append mode
    existing = getattr(dicom, field) or Sequence([])

    existing_codes = set()
    for it in existing:
        cv = getattr(it, "CodeValue", None)
        csd = getattr(it, "CodingSchemeDesignator", None)
        if cv and csd:
            existing_codes.add((str(csd), str(cv)))

    for it in new_items:
        key = (it.CodingSchemeDesignator, it.CodeValue)
        if key not in existing_codes:
            existing.append(it)

    return Sequence(existing)


def hash_name(item: Dict[str, Any], value: Any, field: Any, dicom: Any, **kwargs) -> Optional[str]:
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


