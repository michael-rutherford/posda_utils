# posda_utils/io/indexer.py

import os
import math
import logging
from glob import glob
from tqdm import tqdm
import concurrent.futures as futures
import pandas as pd
from pydicom.errors import InvalidDicomError

from posda_utils.io.reader import DicomFile
from posda_utils.db.models import DicomIndex

logger = logging.getLogger(__name__)

class DicomIndexer:
    def index_directory(self,
                        directory_path,
                        multiproc=True,
                        cpus=4,
                        group_name=None,
                        retain_pixel_data=False,
                        db_manager=None):
        files = self._get_all_files(directory_path)
        batches = self._batch(files, cpus)
        all_records = []

        if multiproc:
            with futures.ProcessPoolExecutor(max_workers=cpus) as executor:
                futures_list = [
                    executor.submit(self._index_batch, batch, retain_pixel_data, group_name)
                    for batch in batches
                ]
                for future in tqdm(futures.as_completed(futures_list), total=len(futures_list), desc="Indexing DICOM file batches"):
                    all_records.extend(future.result())
        else:
            for batch in tqdm(batches, desc="Indexing DICOM file batches"):
                all_records.extend(self._index_batch(batch, retain_pixel_data, group_name))

        df = pd.DataFrame(all_records)

        if db_manager:
            db_manager.create_table_from_model(DicomIndex)
            self._write_to_db(df, DicomIndex, db_manager, group_name)

        return df

    def _index_batch(self, file_paths, retain_pixels, group_name):
        results = []
        for path in file_paths:
            try:
                dcm_file = DicomFile()
                dcm_file.from_dicom_path(path, retain_pixel_data=retain_pixels)
                if dcm_file.exists:
                    results.append(dcm_file.to_index_row(group_name=group_name))
            except InvalidDicomError:
                continue
            except Exception as e:
                logger.error(f"Error reading {path}: {e}")
        return results

    def _write_to_db(self, df, orm_model, db_manager, group_name=None):
        records = df.to_dict(orient="records")
        try:
            deleted = 0
            if group_name:
                deleted = db_manager.session.query(orm_model)\
                    .filter(orm_model.group_name == group_name)\
                    .delete(synchronize_session=False)
                db_manager.session.commit()
                logger.info(f"Deleted {deleted} existing records.")

            db_manager.session.bulk_insert_mappings(orm_model, records)
            db_manager.session.commit()
            logger.info(f"Wrote {len(df)} records to '{orm_model.__tablename__}'.")
        except Exception as e:
            db_manager.session.rollback()
            logger.error(f"Failed to write to ORM table: {e}")
            raise

    def _get_all_files(self, directory_path):
        return [f for f in glob(os.path.join(directory_path, "**", "*"), recursive=True) if os.path.isfile(f)]

    def _batch(self, files, cpus):
        batch_size = max(1, min(100, math.ceil(len(files) / cpus)))
        return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]
