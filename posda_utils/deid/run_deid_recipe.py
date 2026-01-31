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

from deid.config import DeidRecipe
from deid.dicom import get_files, get_identifiers, replace_identifiers

from . import deid_functions

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-dir", required=True, help="Input directory containing DICOM files")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--recipe", required=True, help="Path to deid recipe")
    ap.add_argument("--patient-map", required=True, help="CSV mapping file")

    ap.add_argument("--uid-root", default="1.3.6.1.4.1.14519.5.2.1")
    ap.add_argument("--uid-trunc", type=int, default=64)
    ap.add_argument("--uid-override", action="store_true")
    ap.add_argument("--date-shift-days", type=int, default=0, help="Fixed date shift in days")

    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    recipe = DeidRecipe(args.recipe)
    deid_functions.load_patient_map_csv(args.patient_map)

    # TODO: Load files from Posda File Ids.

    dicom_files = list(get_files(str(in_dir)))

    items = get_identifiers(
        dicom_files=dicom_files,
        force=True,
        # config=?,
        strip_sequences=False,
        remove_private=False,
        # disable_skip=True        
        expand_sequences=True,
    )

    # Inject batch-level variables so your func:hash_uid can read them from `item`
    for item in items.values():
        item["uid_root"] = args.uid_root
        item["uid_trunc"] = int(args.uid_trunc)
        item["uid_override"] = bool(args.uid_override)
        item["date_shift_days"] = int(args.date_shift_days)

        # function registry for func:<name>
        item["hash_uid"] = deid_functions.hash_uid
        item["shift_date_or_datetime"] = deid_functions.shift_date_or_datetime
        item["map_patient_id"] = deid_functions.map_patient_id        
        item["map_patient_name"] = deid_functions.map_patient_name
        item["hashname"] = deid_functions.hashname        

    cleaned_items = replace_identifiers(
        dicom_files=dicom_files,
        ids=items,
        deid=recipe,
        save=True,
        overwrite=True,
        output_folder=str(out_dir),
        force=True,
        # config=?,
        strip_sequences=False,
        remove_private=False,
        # disable_skip=True

    )

    a='a'

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
