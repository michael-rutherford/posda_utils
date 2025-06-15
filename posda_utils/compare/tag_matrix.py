# posda_utils/compare/tag_matrix.py

import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import logging
from sqlalchemy import Table, Column, MetaData, String, Text, text

from posda_utils.io.reader import DicomFile
from posda_utils.db.database import DBManager

logger = logging.getLogger(__name__)

def _process_uid_batch_exec(ref_uids, shared_data):
    dicom_data = shared_data["dicom_data"]
    uid_maps = shared_data["uid_maps"]
    ref_label = shared_data["ref_label"]

    results = []
    for ref_uid in ref_uids:
        tag_union = set()
        tag_data = {}

        for label, dcm_dict in dicom_data.items():
            if label == ref_label:
                uid = ref_uid
            else:
                uid = uid_maps.get(label, {}).get(ref_uid, ref_uid)

            dcm = dcm_dict.get(uid)
            if dcm and dcm.exists:
                full_dict = dcm.meta_dict | dcm.header_dict
                tag_data[label] = full_dict
                tag_union.update(full_dict.keys())

        for tag in sorted(tag_union):
            row = {
                "sop_uid": ref_uid,
                "tag_path": tag,
                "tag": None,
                "tag_name": None,
                "tag_vm": None,
                "tag_vr": None
            }

            for label, tag_dict in tag_data.items():
                value = tag_dict.get(tag, {}).get("value")
                row[f"{label}_value"] = value

                if row["tag"] is None:
                    row["tag"] = tag_dict.get(tag, {}).get("label")
                    element = tag_dict.get(tag, {}).get("element")
                    row["tag_name"] = getattr(element, "name", None)
                    row["tag_vm"] = str(getattr(element, "VM", None))
                    row["tag_vr"] = getattr(element, "VR", None)

            results.append(row)

    return results

def _build_dicomfile_from_row(row):
    dcm = DicomFile()
    pixel_data = row.get("pixel_data")
    dcm.from_json(row["meta_data"], row["header_data"], pixel_data, row)
    return row["sop_instance_uid"], dcm

class TagMatrixBuilder:
    def __init__(self, db_manager, groups, uid_maps=None):
        self.db = db_manager
        self.groups = groups
        self.uid_maps = uid_maps or {}
        self.ref_label = groups[0]
        self.label_to_df = {}
        self.dicom_data = {}
        self._tag_table = None

    def build_matrix(self, cpus=None, batch_size=100, table_name="tag_matrix", overwrite=True):
        self._load_index_from_db()
        self._load_dicom_files(cpus)
        all_ref_uids = sorted(self.dicom_data[self.ref_label].keys())
        cpus = cpus or multiprocessing.cpu_count()

        if overwrite:
            try:
                self.db.session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                self.db.session.commit()
                logger.info(f"Dropped existing table '{table_name}'.")
            except Exception as e:
                self.db.session.rollback()
                logger.error(f"Failed to drop table '{table_name}': {e}")

        self._prepare_tag_table(table_name)

        batches = self._batch_uids(all_ref_uids, batch_size)
        shared_data = {
            "dicom_data": self.dicom_data,
            "uid_maps": self.uid_maps,
            "ref_label": self.ref_label
        }

        with ProcessPoolExecutor(max_workers=cpus) as executor:
            futures = [
                executor.submit(_process_uid_batch_exec, batch, shared_data)
                for batch in batches
            ]

            for future in tqdm(as_completed(futures), total=len(futures), desc="Building Tag Matrix"):
                rows = future.result()
                self._write_batch_to_db(rows)

    def _prepare_tag_table(self, table_name):
        metadata = MetaData()
        sample_cols = ["sop_uid", "tag_path", "tag", "tag_name", "tag_vm", "tag_vr"] + [f"{g}_value" for g in self.groups]
        columns = [Column(c, Text) for c in sample_cols]
        self._tag_table = Table(table_name, metadata, *columns)
        self._tag_table.create(self.db.engine, checkfirst=True)
        logger.info(f"Created table '{table_name}'.")

    def _write_batch_to_db(self, rows):
        try:
            with self.db.engine.begin() as conn:
                conn.execute(self._tag_table.insert(), rows)
            logger.info(f"Inserted {len(rows)} rows.")
        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")

    def _batch_uids(self, uid_list, batch_size):
        return [uid_list[i:i + batch_size] for i in range(0, len(uid_list), batch_size)]

    def _load_index_from_db(self):
        for group in self.groups:
            query = "SELECT * FROM dicom_index WHERE group_name = :group"
            df = self.db.run_query(query, df=True, params={"group": group})
            self.label_to_df[group] = df

    def _load_dicom_files(self, cpus=None):
        cpus = cpus or multiprocessing.cpu_count()
        self.dicom_data = {}

        for label, df in self.label_to_df.items():
            self.dicom_data[label] = {}
            rows = df.to_dict(orient="records")

            with ProcessPoolExecutor(max_workers=cpus) as executor:
                futures = [executor.submit(_build_dicomfile_from_row, row) for row in rows]            

                for future in tqdm(as_completed(futures), total=len(futures), desc=f"Loading {label}"):
                    uid, dcm = future.result()
                    self.dicom_data[label][uid] = dcm

    def _get_dicom_for_uid(self, ref_uid):
        label_to_dicom = {}
        for label in self.label_to_df:
            if label == self.ref_label:
                uid = ref_uid
            else:
                uid = self.uid_maps.get(label, {}).get(ref_uid, ref_uid)
            dicom = self.dicom_data.get(label, {}).get(uid)
            label_to_dicom[label] = dicom if dicom and dicom.exists else None
        return label_to_dicom

    def _gather_tag_rows(self, sop_uid, label_to_dicom):
        tag_union = set()
        tag_data = {}

        for label, dcm in label_to_dicom.items():
            if dcm:
                combined = dcm.meta_dict | dcm.header_dict
                tag_data[label] = combined
                tag_union.update(combined.keys())

        rows = []
        for tag in sorted(tag_union):
            row = {
                "sop_uid": sop_uid,
                "tag_path": tag,
                "tag": None,
                "tag_name": None,
                "tag_vm": None,
                "tag_vr": None
            }

            for label, tag_dict in tag_data.items():
                value = tag_dict.get(tag, {}).get("value")
                row[f"{label}_value"] = value

                if row["tag"] is None:
                    row["tag"] = tag_dict.get(tag, {}).get("label")
                    element = tag_dict.get(tag, {}).get("element")
                    row["tag_name"] = getattr(element, "name", None)
                    row["tag_vm"] = getattr(element, "VM", None)
                    row["tag_vr"] = getattr(element, "VR", None)

            rows.append(row)

        return rows
