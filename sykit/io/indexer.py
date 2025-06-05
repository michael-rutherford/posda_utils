# sykit/io/indexer.py

import os
import math
import logging
import sqlite3 as sql
from glob import glob
from tqdm import tqdm
import pandas as pd
import concurrent.futures as futures
from pydicom.errors import InvalidDicomError

from sykit.io.reader import DicomFile
from sykit.io.hasher import hash_data


class DirectoryIndexer:
    def __init__(self, retain_pixel_data=False):
        self.retain_pixel_data = retain_pixel_data

    # Index all DICOM files in a directory; return DataFrame or write to SQLite
    def index_directory(self, directory_path, multiproc=True, cpus=4, output=None, table_name="dicom_index"):
        files = self._get_all_files(directory_path)
        batches = self._batch(files, cpus)
        all_data = []

        if multiproc:
            with futures.ProcessPoolExecutor(max_workers=cpus) as executor:
                futures_list = [executor.submit(self._index_batch, batch, self.retain_pixel_data) for batch in batches]
                for future in tqdm(futures.as_completed(futures_list), total=len(futures_list), desc="Indexing DICOM files"):
                    all_data.extend(future.result())
        else:
            for batch in tqdm(batches, desc="Indexing DICOM files"):
                all_data.extend(self._index_batch(batch, self.retain_pixel_data))

        df = pd.DataFrame(all_data)

        if output:
            self._write_to_sqlite(df, output, table_name)
            return None
        return df

    # Index a batch of files; extract fields and hash metadata
    def _index_batch(self, file_paths, retain_pixel=False):
        results = []

        for path in file_paths:
            try:
                dcm_file = DicomFile()
                dcm_file.from_dicom_path(path, retain_pixel_data=retain_pixel)

                if not dcm_file.exists:
                    continue

                pixel_digest = None
                pixel_size = None

                if retain_pixel and dcm_file.pixel_data:
                    pixel_size, pixel_digest = hash_data(dcm_file.pixel_data)

                result = {
                    "FilePath": path,
                    "SOPClassUID": getattr(dcm_file.header, "SOPClassUID", None),
                    "Modality": getattr(dcm_file.header, "Modality", None),
                    "PatientID": getattr(dcm_file.header, "PatientID", None),
                    "StudyInstanceUID": getattr(dcm_file.header, "StudyInstanceUID", None),
                    "SeriesInstanceUID": getattr(dcm_file.header, "SeriesInstanceUID", None),
                    "SOPInstanceUID": getattr(dcm_file.header, "SOPInstanceUID", None),
                    "InstanceNumber": getattr(dcm_file.header, "InstanceNumber", None),
                    "BodyPartExamined": getattr(dcm_file.header, "BodyPartExamined", None),
                    "Manufacturer": getattr(dcm_file.header, "Manufacturer", None),
                    "ManufacturerModelName": getattr(dcm_file.header, "ManufacturerModelName", None),
                    "HeaderData": dcm_file.header.to_json(),
                    "MetaData": dcm_file.meta.to_json(),
                    "PixelDigest": pixel_digest,
                    "PixelSize": pixel_size
                }

                header_size, header_digest = hash_data(result["HeaderData"])
                meta_size, meta_digest = hash_data(result["MetaData"])

                result.update({
                    "HeaderDigest": header_digest,
                    "HeaderSize": header_size,
                    "MetaDigest": meta_digest,
                    "MetaSize": meta_size
                })

                results.append(result)

            except InvalidDicomError:
                continue
            except Exception as e:
                logging.error(f"Error reading {path}: {e}")

        return results

    # Return all file paths under a directory (recursive)
    def _get_all_files(self, directory_path):
        return [f for f in glob(os.path.join(directory_path, "**", "*"), recursive=True) if os.path.isfile(f)]

    # Split a list of files into batches based on CPU count
    def _batch(self, files, cpus):
        batch_size = max(1, min(100, math.ceil(len(files) / cpus)))
        return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

    # Write a DataFrame to a SQLite table
    def _write_to_sqlite(self, df, db_path, table_name):
        conn = sql.connect(db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
