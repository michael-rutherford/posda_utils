# posda_utils/compare/directory_compare.py

import pandas as pd
import logging
import concurrent.futures as futures

from posda_utils.compare.file_compare import DicomFileComparer
from posda_utils.io.reader import DicomFile


class DicomDirectoryComparer:
    def __init__(self, multiproc=False, cpus=1, batch_size=1):
        self.multiproc = multiproc
        self.cpus = cpus
        self.batch_size = batch_size
        self.file_comparer = DicomFileComparer()

    def _build_base_record(self, d1_row, d2_row, d1_label, d2_label):
        return {
            f'{d1_label}_class': d1_row.get('SOPClassUID'),
            f'{d1_label}_modality': d1_row.get('Modality'),
            f'{d1_label}_patient': d1_row.get('PatientID'),
            f'{d1_label}_study': d1_row.get('StudyInstanceUID'),
            f'{d1_label}_series': d1_row.get('SeriesInstanceUID'),
            f'{d1_label}_instance': d1_row.get('SOPInstanceUID'),
            f'{d1_label}_path': d1_row.get('FilePath'),

            f'{d2_label}_class': d2_row.get('SOPClassUID') if d2_row is not None else None,
            f'{d2_label}_modality': d2_row.get('Modality') if d2_row is not None else None,
            f'{d2_label}_patient': d2_row.get('PatientID') if d2_row is not None else None,
            f'{d2_label}_study': d2_row.get('StudyInstanceUID') if d2_row is not None else None,
            f'{d2_label}_series': d2_row.get('SeriesInstanceUID') if d2_row is not None else None,
            f'{d2_label}_instance': d2_row.get('SOPInstanceUID') if d2_row is not None else None,
            f'{d2_label}_path': d2_row.get('FilePath') if d2_row is not None else None,
        }

    def compare_dicom_batch(self, dir_01_batch, dir_02_df, uid_map, d1_label, d2_label):
        result_list = []

        for idx, d1_row in dir_01_batch.iterrows():
            d1_file = DicomFile()
            d1_file.from_json(d1_row.MetaData, d1_row.HeaderData, d1_row)

            try:
                d2_row = dir_02_df.loc[uid_map[idx]]
                d2_file = DicomFile()
                d2_file.from_json(d2_row.MetaData, d2_row.HeaderData, d2_row)
            except KeyError:
                d2_row = None
                d2_file = DicomFile()  # empty comparison object

            base_record = self._build_base_record(d1_row, d2_row, d1_label, d2_label)
            result_list.extend(
                self.file_comparer.compare(base_record, d1_file, d1_label, d2_file, d2_label)
            )

        return result_list

    def compare_directories(self, dir_01_df, d1_label, dir_02_df, d2_label, uid_map, data_writer, table_name):
        logging.info("Comparing DICOM directories")
        data_writer.empty_table('analysis', table_name)

        all_results = []

        if self.multiproc:
            batches = [dir_01_df.iloc[i:i + self.batch_size] for i in range(0, len(dir_01_df), self.batch_size)]
            with futures.ProcessPoolExecutor(max_workers=self.cpus) as executor:
                futures_list = [
                    executor.submit(
                        self.compare_dicom_batch,
                        batch,
                        dir_02_df,
                        uid_map,
                        d1_label,
                        d2_label
                    ) for batch in batches
                ]
                for future in futures.as_completed(futures_list):
                    all_results.extend(future.result())
        else:
            all_results.extend(self.compare_dicom_batch(dir_01_df, dir_02_df, uid_map, d1_label, d2_label))

        if all_results:
            result_df = pd.DataFrame(all_results)
            data_writer.write_to_table('analysis', result_df, table_name, mode='replace')
