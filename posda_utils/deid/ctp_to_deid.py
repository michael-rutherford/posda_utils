#!/usr/bin/env python3
"""
Convert a CTP (Clinical Trials Processor) anonymizer .script into a pydicom/deid DICOM recipe.

Output conventions:
- Use Keywords for all standard elements. However "(GGGG,EEEE)" or "GGGGEEEE" can be used.
- Add the keyword (from CTP "n") as a trailing comment:  # Keyword
- For CTP private tags t="GGGG[PRIVATE CREATOR]OO", emit deid private-creator syntax WITH PARENS:
    (GGGG,"PRIVATE CREATOR",OO)

Special CTP conveniences handled:
- DeidentificationMethodCodeSequence (0012,0064): CTP allows value
    113100/113101/...
  to mean "populate code sequence items from CID 7050 (scheme DCM)".
  We emit:
    REPLACE (0012,0064) deid_func:set_deid_method_codes codes="113100/..." mode="replace"

@always() handling:
- For simple scalar literals like @always()YES or @always()MODIFIED, emit a direct literal:
    REPLACE (0012,0062) "YES"
- For complex @always() @append{...} blocks, emit var: and record in vars JSON.
- For @always()@lookup(..., <key>) emit a direct func call:
    REPLACE (....) deid_func:map_patient which="pid|pname|cname|sname"
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from typing import Iterator, Tuple, Literal, Union


COMMENT_RE = re.compile(r"<!--(.*?)-->", re.DOTALL)
ELEM_RE = re.compile(r"<e\s+([^>]+)>(.*?)</e>", re.DOTALL)
PARAM_RE = re.compile(r"<p\s+[^>]*t=\"(?P<key>[^\"]+)\"[^>]*>(?P<val>.*?)</p>", re.DOTALL)
REM_RE = re.compile(r"<r\s+([^>]+)>(.*?)</r>", re.DOTALL)
ATTR_RE = re.compile(r'(\w+)="([^"]*)"')

CTP_PRIVATE_TAG_RE = re.compile(r"^(?P<group>[0-9A-Fa-f]{4})\[(?P<creator>.*?)\](?P<offset>[0-9A-Fa-f]{2})$")

# Unified token regex: comment, <e>, <p>
TOKEN_RE = re.compile(
    r"(?P<comment><!--.*?-->)"
    r"|(?P<elem><e\s+[^>]+>.*?</e>)"
    r"|(?P<param><p\s+[^>]+>.*?</p>)",
    re.DOTALL
)


def sanitize_keyword(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", name or "")


def tag8_to_paren(tag8: str) -> Optional[str]:
    if not tag8:
        return None
    s = tag8.strip()
    if re.fullmatch(r"[0-9A-Fa-f]{8}", s):
        return f"({s[:4].upper()},{s[4:].upper()})"
    return None


def ctp_private_to_deid_private_syntax(tag_t: str) -> Optional[str]:
    """
    Convert CTP private tag spec to deid private creator syntax:
        (GGGG,"PRIVATE CREATOR",OO)
    """
    m = CTP_PRIVATE_TAG_RE.match(tag_t or "")
    if not m:
        return None
    group = m.group("group").upper()
    creator = m.group("creator")
    off = m.group("offset").upper()
    creator_escaped = creator.replace('"', '\"')
    return f'({group},"{creator_escaped}",{off})'


def _is_simple_literal(rest: str) -> bool:
    """
    Decide if content can be emitted as a direct quoted literal.

    We treat as "simple" if:
    - it's non-empty after stripping
    - it does NOT start with '@' (e.g., not '@append...')
    - it does NOT contain braces (used by @append blocks)
    """
    s = (rest or "").strip()
    if not s:
        return False
    if s.startswith("@"):
        return False
    if "{" in s or "}" in s:
        return False
    return True


@dataclass(frozen=True)
class CtpElement:
    t: str
    n: str
    body: str
    en: str

    @property
    def keyword(self) -> Optional[str]:
        return sanitize_keyword(self.n) or None

    @property
    def field(self) -> str:
        # Use keyword for standard tags, tag for private
        priv = ctp_private_to_deid_private_syntax(self.t)
        if priv:
            return self.t  # Keep tag for private
        par = tag8_to_paren(self.t)
        if par:
            return self.keyword or par
        return self.keyword or self.t

    @property
    def comment(self) -> str:
        return self.keyword or (self.n.strip() if self.n else "")

Token = Tuple[Literal["comment"], str] | Tuple[Literal["elem"], CtpElement]


def parse_elements(text: str) -> List[CtpElement]:
    out: List[CtpElement] = []
    for m in ELEM_RE.finditer(text):
        attrs = {k: v for (k, v) in ATTR_RE.findall(m.group(1))}
        out.append(
            CtpElement(
                t=(attrs.get("t", "") or "").strip(),
                n=(attrs.get("n", "") or "").strip(),
                en=(attrs.get("en", "") or "").strip(),
                body=((m.group(2) or "")).strip(),
            )
        )
    return out


def parse_params(text: str) -> Dict[str, str]:
    """Parse <p t=\"KEY\">VALUE</p> labels into a dict of defaults."""
    out: Dict[str, str] = {}
    for m in PARAM_RE.finditer(text or ""):
        k = (m.group("key") or "").strip()
        v = (m.group("val") or "").strip()
        if k:
            out[k] = v
    return out


def iter_tokens(text: str):
    """Yield ('comment', str), ('elem', CtpElement), ('param', (key, val)), and ('blank', None) tokens in document order, preserving blank lines."""
    pos = 0
    text = text or ""
    for m in TOKEN_RE.finditer(text):
        start, end = m.start(), m.end()
        # Emit blank lines between tokens
        between = text[pos:start]
        for line in between.splitlines(keepends=True):
            if line.strip() == "":
                yield ("blank", None)
        if m.group("comment"):
            raw = m.group("comment")
            cm = COMMENT_RE.match(raw)
            body = (cm.group(1) if cm else raw)
            yield ("comment", body.strip())
        elif m.group("elem"):
            chunk = m.group("elem")
            em = ELEM_RE.match(chunk or "")
            if not em:
                continue
            attrs = {k: v for (k, v) in ATTR_RE.findall(em.group(1))}
            yield ("elem", CtpElement(
                t=(attrs.get("t", "") or "").strip(),
                n=(attrs.get("n", "") or "").strip(),
                en=(attrs.get("en", "") or "").strip(),
                body=((em.group(2) or "")).strip(),
            ))
        elif m.group("param"):
            chunk = m.group("param")
            pm = PARAM_RE.match(chunk or "")
            if not pm:
                continue
            k = (pm.group("key") or "").strip()
            v = (pm.group("val") or "").strip()
            yield ("param", (k, v))
        pos = end
    # Handle trailing blank lines after last token
    trailing = text[pos:]
    for line in trailing.splitlines(keepends=True):
        if line.strip() == "":
            yield ("blank", None)


def translate(e: CtpElement, literals: Dict[str, str]) -> dict:
    body = (e.body or "").strip()
    if body == "removed":
        body = "@remove()"

    # Detect and handle @always
    always_mode = False
    if "@always" in body:
        always_mode = True
        body = re.sub(r"@always\(\)", "", body).strip()

    op = "ADD" if always_mode else "REPLACE"

    # Determine tag format for private tags
    priv_tag = ctp_private_to_deid_private_syntax(e.t)
    if priv_tag:
        a='a'

    group = e.t[:4] if len(e.t) >= 8 else None
    is_private_odd = False
    if group and group[-1] in '13579':
        is_private_odd = True
    # For ADD actions on private tags with odd group, use tag format 00130010
    # For other actions, use special format (GGGG,"PRIVATE CREATOR",OO)
    if op == "ADD" and (priv_tag or is_private_odd):
        field = e.t
    elif priv_tag:
        field = priv_tag
    else:
        field = e.field
    cmt_part = ""

    labels = []
    headers = []

    # DeidentificationMethod
    if e.t.lower() == "00120063" or sanitize_keyword(e.n) == "DeidentificationMethod":
        if body.startswith("@append"):
            m = re.search(r"\{(.*)\}", body, re.DOTALL)
            txt = (m.group(1) if m else "").strip()
            headers.append(f'ADD {field} deid_func:set_deid_method txt="{txt}"')
            return {"labels": labels, "headers": headers}      

    # DeidentificationMethodCodeSequence
    if e.t.lower() == "00120064" or sanitize_keyword(e.n) == "DeidentificationMethodCodeSequence":
        if re.fullmatch(r"[0-9]{6}(?:/[0-9]{6})*", body):
            headers.append(f'ADD {field} deid_func:set_deid_method_codes codes="{body}"')
            return {"labels": labels, "headers": headers}

    if body.startswith("@process"):
        headers.append(f"#### CTP @process: {field}{cmt_part}")
    elif body.startswith("@remove"):
        headers.append(f"REMOVE {field}{cmt_part}")
    elif body.startswith("@keep"):
        headers.append(f"KEEP {field}{cmt_part}")
    elif body.startswith("@empty"):
        headers.append(f"BLANK {field}{cmt_part}")
    elif body.startswith("@hashuid"):
        headers.append(f"REPLACE {field} deid_func:hash_uid{cmt_part}")
    elif body.startswith("@incrementdate"):
        headers.append(f"REPLACE {field} deid_func:increment_date{cmt_part}")
    
    # Simple direct substitutions for known patterns
    elif "@lookup(PatientID,ptid)" in body or "@lookup(this,ptid)" in body:
        headers.append(f"{op} {field} var:pat_id")
    elif "@lookup(PatientID,cname)" in body:
        headers.append(f"{op} {field} var:collection_name")
    elif "@lookup(PatientID,sname)" in body:
        headers.append(f"{op} {field} var:site_name")
    elif "@param(@SITEID)" in body:
        headers.append(f"{op} {field} var:site_id")
    elif "@modifydate(StudyDate,*,1,1)" in body:
        headers.append(f"{op} {field} var:study_year")
    elif "@hashname" in body:
        prefix = body.split("@hashname", 1)[0].strip().replace('"', "")
        mlen = re.search(r"@hashname\([^,]+,\s*([0-9]+)\s*\)", body)
        length = int(mlen.group(1)) if mlen else 4
        headers.append(f"{op} {field} deid_func:hash_name prefix={json.dumps(prefix)} length={length}{cmt_part}")
    
    if not labels and not headers:
        rest = body
        if _is_simple_literal(rest):
            val = rest.strip()
            # Only quote if used as a parameter in a function
            if 'deid_func:' in val or 'var:' in val:
                headers.append(f"{op} {field} {json.dumps(val)}{cmt_part}")
            else:
                headers.append(f"{op} {field} {val}{cmt_part}")
        else:
            rest = rest.strip()
            if rest.startswith("@append"):
                m = re.search(r"\{(.*)\}", rest, re.DOTALL)
                lit = (m.group(1) if m else "").strip()
            else:
                lit = rest
            varname = f"lit_{e.t.lower()}"
            literals[varname] = lit
            headers.append(f"{op} {field} var:{varname}{cmt_part}")

    if labels or headers:
        return {"labels": labels, "headers": headers}
    
    return {"labels": [], "headers": [f"#### TODO: Unhandled: t={e.t} n={e.n} body={body}"]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("ctp_script")
    ap.add_argument("--out-recipe", default="converted.deid.dicom")
    ap.add_argument("--out-vars-json", default=None)
    args = ap.parse_args()

    src = Path(args.ctp_script)
    # Remove <script> and </script> lines from input
    text = src.read_text(errors="replace")
    text = "\n".join(line for line in text.splitlines() if "<script>" not in line and "</script>" not in line)

    # Convert XML-style comments (<!-- ... -->) to Python-style (# ...) comments, handling multi-line comments
    def convert_xml_comments(lines):
        out_lines = []
        in_comment = False
        comment_buffer = []
        for line in lines:
            stripped = line.lstrip()
            if not in_comment and stripped.startswith('<!--'):
                in_comment = True
                # Remove opening <!--
                comment_line = stripped[4:]
                # Check if --> is on the same line
                if '-->' in comment_line:
                    idx = comment_line.find('-->')
                    comment_text = comment_line[:idx]
                    out_lines.append('# ' + comment_text.strip())
                    in_comment = False
                else:
                    comment_buffer.append(comment_line.strip())
                continue
            if in_comment:
                # Check if --> is in this line
                if '-->' in stripped:
                    idx = stripped.find('-->')
                    comment_buffer.append(stripped[:idx].strip())
                    # Output all buffered comment lines
                    for cmt in comment_buffer:
                        if cmt.strip():
                            out_lines.append('# ' + cmt.strip())
                    in_comment = False
                    comment_buffer = []
                else:
                    comment_buffer.append(stripped.strip())
                continue
            out_lines.append(line)
        # If file ends while still in comment, flush buffer
        if comment_buffer:
            for cmt in comment_buffer:
                if cmt.strip():
                    out_lines.append('# ' + cmt.strip())
        return out_lines

    lines = convert_xml_comments(text.splitlines())
    literals: Dict[str, str] = {}
    output_lines = [f"# Generated from CTP script: {src.name}", "FORMAT dicom", "", "%labels"]

    param_rename = {
        "DATEINC": "date_shift_days",
        "SITEID": "site_code",
        "SITENAME": "site_name",
        "UIDROOT": "uid_root",
    }

    # First pass: collect all line types and extra labels
    line_types = []  # (type, content, idx)
    extra_labels = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "":
            line_types.append(("blank", "", i))
        elif stripped.startswith("#"):
            line_types.append(("comment", line, i))
        elif PARAM_RE.fullmatch(stripped):
            m = PARAM_RE.fullmatch(stripped)
            k = (m.group("key") or "").strip()
            v = (m.group("val") or "").strip()
            k = param_rename.get(k, k)
            line_types.append(("param", f"ADD {k} {v}", i))
        elif REM_RE.fullmatch(stripped):
            m = REM_RE.fullmatch(stripped)
            attrs = {k: v for (k, v) in ATTR_RE.findall(m.group(1))}
            t = (attrs.get("t", "") or "").strip().lower()
            en = (attrs.get("en", "") or "").strip().upper()

            # CTP en="T" means enabled
            enabled = "true" if en == "T" else "false"

            r_map = {
                "curves": "remove_curves",
                "overlays": "remove_overlays",
                "privategroups": "remove_private_groups",
                "unspecifiedelements": "remove_unspecified",
            }

            key = r_map.get(t)
            if key:
                extra_labels.append(f"ADD {key} {enabled}")
            else:
                line_types.append(("comment", f"# Unhandled CTP <r> directive t={t} en={en}", i))            
        elif ELEM_RE.fullmatch(stripped):
            m = ELEM_RE.fullmatch(stripped)
            attrs = {k: v for (k, v) in ATTR_RE.findall(m.group(1))}
            elem = CtpElement(
                t=(attrs.get("t", "") or "").strip(),
                n=(attrs.get("n", "") or "").strip(),
                en=(attrs.get("en", "") or "").strip(),
                body=((m.group(2) or "")).strip(),
            )
            result = translate(elem, literals)
            line_types.append(("element", result, i))
            for label in result.get("labels", []):
                extra_labels.append(label)
        else:
            line_types.append(("comment", f"# {line.strip()}", i))

    # Find the index to insert extra labels: after last param, before first element
    last_param_idx = -1
    first_element_idx = None
    for idx, (typ, _, _) in enumerate(line_types):
        if typ == "param":
            last_param_idx = idx
        if typ == "element" and first_element_idx is None:
            first_element_idx = idx
    insert_idx = first_element_idx if last_param_idx == -1 else last_param_idx + 1

    # Second pass: build output lines, inserting extra labels at the right spot
    output_lines = [f"# Generated from CTP script: {src.name}", "FORMAT dicom", "", "%labels"]
    extra_labels_printed = False
    for idx, (typ, content, _) in enumerate(line_types):
        if typ == "param":
            if content:
                output_lines.append(content)
            # If this is the last param, add a blank line after
            if idx == last_param_idx:
                output_lines.append("")
                if extra_labels: 
                    for label in extra_labels:
                        output_lines.append(label)
                    output_lines.append("")
                    extra_labels_printed = True                       
        elif typ == "comment" or typ == "blank":
            if content:
                output_lines.append(content)
        elif typ == "element":
            # If there were no params, print extra labels once before the first element
            if (not extra_labels_printed) and extra_labels:
                output_lines.append("")
                for label in extra_labels:
                    output_lines.append(label)
                output_lines.append("")
                extra_labels_printed = True

            # Insert header section before first element
            if idx == first_element_idx:
                output_lines.append("")
                output_lines.append("%header")

            for header in content.get("headers", []):
                output_lines.append(header)
    output_lines.append("")
    Path(args.out_recipe).write_text("\n".join(output_lines) + "\n", encoding="utf-8")

    if args.out_vars_json:
        Path(args.out_vars_json).write_text(json.dumps(literals, indent=2), encoding="utf-8")

    print(f"Wrote recipe: {args.out_recipe}")
    if args.out_vars_json:
        print(f"Wrote vars json: {args.out_vars_json}")


if __name__ == "__main__":
    main()
