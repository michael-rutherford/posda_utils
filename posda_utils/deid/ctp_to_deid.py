#!/usr/bin/env python3
"""
Convert a CTP (Clinical Trials Processor) anonymizer .script into a pydicom/deid DICOM recipe.

Key points for private tags:
- CTP uses t="GGGG[PRIVATE CREATOR]OO" (OO is the element offset, 2 hex digits).
- deid supports private creator syntax:  GGGG,"PRIVATE CREATOR",OO  (optionally with parentheses)
  See deid docs "Private creator Syntax for Private Tags".

This converter emits that private creator syntax so rules target the correct private creator,
without assuming any fixed element high-byte like 0x10.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ELEM_RE = re.compile(r"<e\s+([^>]+)>(.*?)</e>", re.DOTALL)
ATTR_RE = re.compile(r'(\w+)="([^"]*)"')
# CTP private tag format: GGGG[PRIVATE CREATOR]OO  (OO is 2 hex digits offset)
CTP_PRIVATE_TAG_RE = re.compile(r"^(?P<group>[0-9A-Fa-f]{4})\[(?P<creator>.*?)\](?P<offset>[0-9A-Fa-f]{2})$")

def sanitize_keyword(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", name or "")

def ctp_private_to_deid_private_syntax(tag_t: str) -> Optional[str]:
    """
    Convert CTP private tag spec to deid private creator syntax:
        GGGG,"PRIVATE CREATOR",OO
    """
    m = CTP_PRIVATE_TAG_RE.match(tag_t or "")
    if not m:
        return None
    group = m.group("group").upper()
    creator = m.group("creator")
    off = m.group("offset").upper()
    # deid requires creator in double quotes
    creator_escaped = creator.replace('"', '\"')
    return f'{group},"{creator_escaped}",{off}'

@dataclass(frozen=True)
class CtpElement:
    t: str
    n: str
    body: str

    @property
    def field(self) -> str:
        # Prefer keyword if present; otherwise use t, but if t is private creator format,
        # use deid's private creator syntax.
        k = sanitize_keyword(self.n)
        if k:
            return k
        priv = ctp_private_to_deid_private_syntax(self.t)
        return priv if priv else self.t

def parse_elements(text: str) -> List[CtpElement]:
    out: List[CtpElement] = []
    for m in ELEM_RE.finditer(text):
        attrs = {k: v for (k, v) in ATTR_RE.findall(m.group(1))}
        out.append(CtpElement(
            t=(attrs.get("t","") or "").strip(),
            n=(attrs.get("n","") or "").strip(),
            body=((m.group(2) or "")).strip(),
        ))
    return out

def translate(e: CtpElement, literals: Dict[str,str]) -> str:
    body = (e.body or "").strip()
    if body == "removed":
        body = "@remove()"

    field = e.field

    if body.startswith("@remove"):
        return f"REMOVE {field}"
    if body.startswith("@keep"):
        return f"KEEP {field}"
    if body.startswith("@empty"):
        return f"BLANK {field}"
    if body.startswith("@process"):
        return f"KEEP {field}  # CTP @process"
    if body.startswith("@hashuid"):
        return f"REPLACE {field} func:hash_uid"
    if body.startswith("@incrementdate"):
        return f"REPLACE {field} func:shift_date_or_datetime"
    if body.startswith("@lookup"):
        if field == "PatientID":
            return "REPLACE PatientID func:map_patient_id"
        if field == "PatientName":
            return "REPLACE PatientName func:map_patient_name"
        return f"# NOTE: lookup not auto-mapped for {field}: {body}"
    if "@hashname" in body:
        prefix = body.split("@hashname", 1)[0].strip().replace('"', "")
        mlen = re.search(r"@hashname\([^,]+,\s*([0-9]+)\s*\)", body)
        length = int(mlen.group(1)) if mlen else 4
        return f"REPLACE {field} func:hashname prefix={json.dumps(prefix)} length={length}"
    if body.startswith("@always()"):
        rest = body.replace("@always()", "", 1).strip()
        if rest.startswith("@append"):
            m = re.search(r"\{(.*)\}", rest, re.DOTALL)
            lit = (m.group(1) if m else "").strip()
        else:
            lit = rest.strip()
        varname = f"lit_{field}"
        literals[varname] = lit
        return f"REPLACE {field} var:{varname}"

    return f"# TODO: Unhandled: t={e.t} n={e.n} body={body}"

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("ctp_script")
    ap.add_argument("--out-recipe", default="converted.deid.dicom")
    ap.add_argument("--out-vars-json", default=None)
    args = ap.parse_args()

    src = Path(args.ctp_script)
    text = src.read_text(errors="replace")
    elems = parse_elements(text)

    literals: Dict[str,str] = {}
    lines = [
        "FORMAT dicom",
        "",
        "%header",
        f"# Generated from CTP script: {src.name}",
        "",
    ]
    for e in elems:
        lines.append(translate(e, literals))

    Path(args.out_recipe).write_text("\n".join(lines) + "\n", encoding="utf-8")

    if args.out_vars_json:
        Path(args.out_vars_json).write_text(json.dumps(literals, indent=2), encoding="utf-8")

    print(f"Wrote recipe: {args.out_recipe}")
    if args.out_vars_json:
        print(f"Wrote vars json: {args.out_vars_json}")

if __name__ == "__main__":
    main()
