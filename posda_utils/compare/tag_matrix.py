# posda_utils/compare/tag_matrix.py

import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import multiprocessing

from posda_utils.io.reader import DicomFile

def _process_uid_task_exec(ref_uid, shared_data):
    dicom_data = shared_data["dicom_data"]
    uid_maps = shared_data["uid_maps"]
    ref_label = shared_data["ref_label"]

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

    rows = []
    for tag in sorted(tag_union):
        row = {
            "sop_uid": ref_uid,
            "tag_path": tag,
            "tag": None,
            "tag_name": None,
        }

        for label, tag_dict in tag_data.items():
            value = tag_dict.get(tag, {}).get("value")
            row[f"{label}_value"] = value

            if row["tag"] is None:
                row["tag"] = tag_dict.get(tag, {}).get("label")
                element = tag_dict.get(tag, {}).get("element")
                row["tag_name"] = getattr(element, "name", None)

        rows.append(row)

    return rows

def _build_dicomfile_from_json(uid, meta_json, header_json, info):
    dcm = DicomFile()
    dcm.from_json(meta_json, header_json, info)
    return dcm

class TagMatrixBuilder:
    def __init__(self, label_to_df, uid_maps=None):
        """
        label_to_df: dict of label -> DataFrame containing DICOM index with MetaData and HeaderData
        uid_maps: optional dict of label -> {ref_uid -> target_uid}
        """
        self.label_to_df = label_to_df
        self.uid_maps = uid_maps or {}
        self.ref_label = next(iter(label_to_df))
        self.dicom_data = {}

    def build_matrix(self, cpus=None):
        self._load_dicom_files(cpus)
        all_ref_uids = sorted(self.dicom_data[self.ref_label].keys())
        cpus = cpus or multiprocessing.cpu_count()

        shared_data = {
            "dicom_data": self.dicom_data,
            "uid_maps": self.uid_maps,
            "ref_label": self.ref_label
        }

        with ProcessPoolExecutor(max_workers=cpus) as executor:
            futures = [
                executor.submit(_process_uid_task_exec, uid, shared_data)
                for uid in all_ref_uids
            ]

            results = []
            for future in tqdm(as_completed(futures), total=len(futures), desc="Building Tag Matrix"):
                result = future.result()
                results.extend(result)

        return pd.DataFrame(results)

    def _load_dicom_files(self, cpus=None):
        cpus = cpus or multiprocessing.cpu_count()

        self.dicom_data = {}
        for label, df in self.label_to_df.items():
            self.dicom_data[label] = {}

            tasks = [
                (row["SOPInstanceUID"], row.MetaData, row.HeaderData, row.to_dict())
                for _, row in df.iterrows()
            ]

            results = {}
            with ProcessPoolExecutor(max_workers=cpus) as executor:
                future_map = {
                    executor.submit(_build_dicomfile_from_json, uid, meta, header, info): uid
                    for uid, meta, header, info in tasks
                }

                for future in tqdm(as_completed(future_map), total=len(future_map), desc=f"Loading {label}"):
                    uid = future_map[future]
                    dcm = future.result()
                    results[uid] = dcm

            self.dicom_data[label] = results

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
            }

            for label, tag_dict in tag_data.items():
                value = tag_dict.get(tag, {}).get("value")
                row[f"{label}_value"] = value

                if row["tag"] is None:
                    row["tag"] = tag_dict.get(tag, {}).get("label")
                    element = tag_dict.get(tag, {}).get("element")
                    row["tag_name"] = getattr(element, "name", None)

            rows.append(row)

        return rows
