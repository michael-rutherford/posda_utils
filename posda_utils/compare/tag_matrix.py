import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import multiprocessing
import logging
from sqlalchemy import Table, Column, MetaData, Text, text, bindparam
import pyarrow as pa

from posda_utils.io.reader import DicomFile

logger = logging.getLogger(__name__)

def build_dicomfile(row):
    dcm = DicomFile()
    pixel_data = row.get("pixel_data")
    dcm.from_json(row["meta_data"], row["header_data"], pixel_data, row)
    dcm._combined_dict = dcm.meta_dict | dcm.header_dict
    return row["sop_instance_uid"], dcm

def process_batch(ref_uids, label_to_rows):
    group_data_batches = {}
    for label, rows in label_to_rows.items():
        with ThreadPoolExecutor() as tpool:
            futures = [tpool.submit(build_dicomfile, row) for row in rows]
            dcm_dict = {uid: dcm for uid, dcm in (f.result() for f in as_completed(futures))}
            group_data_batches[label] = dcm_dict

    results = []
    for ref_uid in ref_uids:
        tag_union = set()
        tag_data = {}

        for label, dcm_dict in group_data_batches.items():
            dcm = dcm_dict.get(ref_uid)
            if dcm and dcm.exists:
                full_dict = getattr(dcm, '_combined_dict', dcm.meta_dict | dcm.header_dict)
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

            for label in group_data_batches:
                row[f"{label}_value"] = None

            for label, tag_dict in tag_data.items():
                value = tag_dict.get(tag, {}).get("value")
                row[f"{label}_value"] = value

                if row["tag"] is None:
                    row["tag"] = tag_dict.get(tag, {}).get("label")
                    element = tag_dict.get(tag, {}).get("element")
                    row["tag_name"] = getattr(element, "name", None)
                    row["tag_vm"] = getattr(element, "VM", None)
                    row["tag_vr"] = getattr(element, "VR", None)

            results.append(row)

    return results

class TagMatrixBuilder:
    def __init__(self, db_manager, groups, uid_maps=None):
        self.db = db_manager
        self.groups = groups
        self.uid_maps = uid_maps or {}
        self.ref_label = groups[0]
        self.label_to_uids = {}
        self._tag_table = None

    def build_matrix(self, 
                     cpus=None, 
                     batch_size=100, 
                     table_name="tag_matrix", 
                     overwrite=True,
                     multiproc=True,
                     batch_of_batches=None):
        self._load_uids_from_db()
        cpus = cpus or multiprocessing.cpu_count()
        batch_of_batches = batch_of_batches or cpus

        all_uids = set()
        for group in self.groups:
            all_uids.update(self.label_to_uids[group])
        all_ref_uids = sorted(all_uids)

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

        def fetch_label_rows(batch):
            label_to_rows = {}
            for label in self.groups:
                batch_uids = set(batch)
                query = text("""
                    SELECT sop_instance_uid, header_data, meta_data, pixel_data FROM dicom_index 
                    WHERE group_name = :group AND sop_instance_uid IN :uids
                """).bindparams(bindparam('uids', expanding=True))
                params = {"group": label, "uids": list(batch_uids)}
                df = self.db.run_query(query, df=True, params=params)
                #label_to_rows[label] = df.to_dict(orient="records") if df is not None else []
                label_to_rows[label] = pa.Table.from_pandas(df).to_pylist() if df is not None else []
            return label_to_rows

        if multiproc:
            for i in range(0, len(batches), batch_of_batches):
                chunk = batches[i:i + batch_of_batches]
                with ProcessPoolExecutor(max_workers=cpus) as executor:
                    future_to_batch = {
                        executor.submit(process_batch, batch, fetch_label_rows(batch)): batch
                        for batch in chunk
                    }

                    for future in tqdm(as_completed(future_to_batch), total=len(future_to_batch), desc=f"Processing batches {i}-{i+len(chunk)} of {len(batches)}"):
                        try:
                            rows = future.result()
                            self._write_batch_to_db(rows)
                        except Exception as e:
                            logger.error(f"Failed to process batch in parallel: {e}")
        else:
            for batch in tqdm(batches, desc="Building Tag Matrix"):
                try:
                    label_to_rows = fetch_label_rows(batch)
                    rows = process_batch(batch, label_to_rows)
                    self._write_batch_to_db(rows)
                except Exception as e:
                    logger.error(f"Failed to process batch: {e}")

    def _prepare_tag_table(self, table_name):
        metadata = MetaData()
        sample_cols = ["sop_uid", "tag_path", "tag", "tag_name", "tag_vm", "tag_vr"] + [f"{g}_value" for g in self.groups]
        columns = [Column(c, Text, nullable=True) for c in sample_cols]
        self._tag_table = Table(table_name, metadata, *columns)
        self._tag_table.create(self.db.engine, checkfirst=True)
        logger.info(f"Created table '{table_name}'.")

    def _write_batch_to_db(self, rows):
        try:
            with self.db.engine.begin() as conn:
                conn.execute(self._tag_table.insert(), rows)
        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")

    def _batch_uids(self, uid_list, batch_size):
        return [uid_list[i:i + batch_size] for i in range(0, len(uid_list), batch_size)]

    def _load_uids_from_db(self):
        logger.info("Load the UIDs for each group")
        self.label_to_uids = {}
        for group in self.groups:
            query = "SELECT sop_instance_uid FROM dicom_index WHERE group_name = :group"
            df = self.db.run_query(query, df=True, params={"group": group})
            self.label_to_uids[group] = set(df["sop_instance_uid"])
