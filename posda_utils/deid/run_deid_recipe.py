#!/usr/bin/env python3
"""
Run a pydicom/deid recipe over a directory of DICOM files.

Uses the official deid pipeline (get_identifiers -> replace_identifiers):
  https://pydicom.github.io/deid/examples/func-replace/  (concept)
  https://pydicom.github.io/deid/examples/deid-dataset/  (dataset example)

Custom functions are provided via a module (deid_functions.py).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pydicom

from deid.config import DeidRecipe
from deid.dicom import get_files, get_identifiers, replace_identifiers

from typing import Callable, Dict

from . import deid_functions

import logging
logging.basicConfig(level=logging.ERROR)

def _get_labels(recipe: DeidRecipe) -> dict:
    """Return dict of ADD labels from %labels (application defaults)."""
    out = {}
    for lab in (getattr(recipe, "deid", {}) or {}).get("labels") or []:
        if (lab.get("action") or "").upper() != "ADD":
            continue
        k = (lab.get("field") or "").strip()
        v = (lab.get("value") or "").strip()
        if k:
            out[k] = v
    return out


def _register_deid_funcs(funcs: Dict[str, Callable]) -> None:
    """
    Register custom functions so they can be called via deid_func:<name> with parameters.
    """

    try:
        import deid.dicom.actions as actions_mod
    except Exception:
        try:
            import deid.dicom.action as actions_mod  # some variants
        except Exception as e:
            raise RuntimeError(f"Could not import deid actions module: {e}")

    registry = None
    if hasattr(actions_mod, "deid_funcs") and isinstance(getattr(actions_mod, "deid_funcs"), dict):
        registry = getattr(actions_mod, "deid_funcs")

    if registry:
        registry.update(funcs)

    return None


def _parse_bool(v, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _remove_curve_overlay_groups(ds, remove_curves: bool, remove_overlays: bool) -> bool:
    """
    Remove Curve groups (50xx) and Overlay groups (60xx) from the dataset.
    Returns True if anything was removed.
    """
    if not (remove_curves or remove_overlays):
        return False

    to_delete = []
    for elem in ds.iterall():  # includes nested sequences
        g = elem.tag.group
        if remove_curves and (0x5000 <= g <= 0x50FF):
            to_delete.append(elem.tag)
        elif remove_overlays and (0x6000 <= g <= 0x60FF):
            to_delete.append(elem.tag)

    changed = False
    for tag in set(to_delete):
        if tag in ds:
            del ds[tag]
            changed = True
    return changed


def main() -> None:

    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True, help="Input directory containing DICOM files")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--recipe", required=True, help="Path to deid recipe")
    ap.add_argument("--patient-map", required=True, help="CSV mapping file")

    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    recipe = DeidRecipe(args.recipe)
    recipe_labels = _get_labels(recipe)

    remove_curves = _parse_bool(recipe_labels.get("remove_curves"), default=False)
    remove_overlays = _parse_bool(recipe_labels.get("remove_overlays"), default=False)
    remove_private_groups = _parse_bool(recipe_labels.get("remove_private_groups"), default=False)
    remove_unspecified = _parse_bool(recipe_labels.get("remove_unspecified"), default=False)

    if remove_unspecified:
        print("WARNING: remove_unspecified=true requested, but allowlist mode is not implemented. Ignoring.")

    deid_functions.load_patient_map_csv(args.patient_map)

    # Make custom functions available via deid_func:<name> (supports parameters)
    _register_deid_funcs({
        "hash_uid": deid_functions.hash_uid,
        "hash_name": deid_functions.hash_name,
        "increment_date": deid_functions.increment_date,
        "set_deid_method": deid_functions.set_deid_method,
        "set_deid_method_codes": deid_functions.set_deid_method_codes,
    })

    dicom_files = list(get_files(str(in_dir)))

    items = get_identifiers(
        dicom_files=dicom_files,
        force=True,
        # config=?,
        strip_sequences=False,
        remove_private=remove_private_groups,
        # disable_skip=True        
        expand_sequences=True,
    )

    # Inject batch-level variables so your func:hash_uid can read them from `item`
    for item in items.values():
        old_pat_id = item.get("(0010,0020)").element.value

        # labels: recipe %labels < per-patient CSV
        uid_root = recipe_labels.get("uid_root", "")
        date_shift_days = recipe_labels.get("date_shift_days", 0)
        site_code = recipe_labels.get("site_code", "")
        site_name = recipe_labels.get("site_name", "")
        deid_method_codes = recipe_labels.get("deid_method_codes", "")

        pat_id = None
        pat_name = None
        collection_name = None
        study_year = None

        # override from CSV per patient if present
        if old_pat_id and old_pat_id in deid_functions.PATIENT_MAP:
            row = deid_functions.PATIENT_MAP[old_pat_id]

            if row.get("to_patient_id"):
                pat_id = row["to_patient_id"]              
            if row.get("to_patient_name"):
                pat_name = row["to_patient_name"]            
            if row.get("uid_root"):                
                uid_root = row["uid_root"]
            if row.get("date_shift"):
                date_shift_days = row["date_shift"]
            if row.get("collection_name"):
                collection_name = row["collection_name"]
            if row.get("site_code"):
                site_code = row["site_code"]
            if row.get("site_name"):
                site_name = row["site_name"]

        try:
            date_shift_days = int(date_shift_days)
        except Exception:
            date_shift_days = 0

        # Extract study date, modify to Jan 1 with same year, and set to item["study_year"]
        study_date_elem = item.get("(0008,0020)")  # Study Date DICOM tag
        if study_date_elem and hasattr(study_date_elem, "element"):
            study_date_val = study_date_elem.element.value
            if study_date_val:
                # Expecting YYYYMMDD format
                try:
                    year = str(study_date_val)[:4]
                    study_year = f"{year}0101"
                except Exception:
                    study_year = None
            else:
                study_year = None
        else:
            study_year = None

        item["pat_id"] = str(pat_id)
        item["pat_name"] = str(pat_name)
        item["collection_name"] = str(collection_name)

        item["uid_root"] = str(uid_root)
        item["date_shift_days"] = int(date_shift_days)
        
        item["site_code"] = str(site_code)
        item["site_name"] = str(site_name)
        item["deid_method_codes"] = str(deid_method_codes)

        item["study_year"] = str(study_year)

        # # function registry for func:<name>
        # item["hash_uid"] = deid_functions.hash_uid
        # item["hash_name"] = deid_functions.hash_name
        # item["increment_date"] = deid_functions.increment_date
        # item["set_deid_method_codes"] = deid_functions.set_deid_method_codes         

    replace_identifiers(
        dicom_files=dicom_files,
        ids=items,
        deid=recipe,
        save=True,
        overwrite=True,
        output_folder=str(out_dir),
        force=True,
        # config=?,
        strip_sequences=False,
        remove_private=remove_private_groups,
        # disable_skip=True
    )

    if remove_curves or remove_overlays:
        for p in out_dir.rglob("*"):
            if not p.is_file():
                continue
            try:
                ds = pydicom.dcmread(str(p), force=True)
            except Exception:
                continue

            changed = _remove_curve_overlay_groups(
                ds,
                remove_curves=remove_curves,
                remove_overlays=remove_overlays,
            )
            if changed:
                ds.save_as(str(p))


    # for src, ds in cleaned.items():
    #     src_p = Path(src)
    #     rel = src_p.relative_to(in_dir)
    #     out_path = out_dir / rel
    #     out_path.parent.mkdir(parents=True, exist_ok=True)
    #     ds.save_as(str(out_path))

    # print(f"Files processed: {len(cleaned)}")
    # print(f"Date shift days used: {date_shift_days}")


if __name__ == "__main__":
    main()
