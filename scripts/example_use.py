import os
import json
import sqlite3 as sql
import logging
from datetime import datetime

import warnings
import pydicom
warnings.filterwarnings("ignore", module="pydicom")
# from pydicom.errors import InvalidDicomWarning
# warnings.filterwarnings("ignore", category=InvalidDicomWarning)

from posda_utils.io.reader import DicomFile
from posda_utils.io.hasher import hash_file, hash_uid, hash_uid_list
from posda_utils.io.indexer import DicomIndexer

from posda_utils.compare.file_compare import DicomFileComparer
from posda_utils.compare.tag_matrix import TagMatrixBuilder

from posda_utils.posda.api import PosdaAPI
from posda_utils.posda.db import PosdaDB

from posda_utils.db.database import DBManager
from posda_utils.db.models import Base, DicomIndex, DicomCompare
from sqlalchemy.orm import Session


config_file = r'D:\Cloud\University of Arkansas for Medical Sciences\Work - General\PW\posda_pw.json'
with open(config_file) as f:
    config_data = json.load(f)

dcm_path = r'D:\data\dicom\Healthy-Total-Body-CTs\manifest-1690389403229\Healthy-Total-Body-CTs\Healthy-Total-Body-CTs-001\03-06-2001-NA-CT SOFT512x512 90min-90701\203.000000-CT SOFT512x512 90min-22772'
dcm_file_path_1 = rf"{dcm_path}\1-122.dcm"
dcm_file_path_2 = rf"{dcm_path}\1-123.dcm"

activity = 1271
timepoint = 7923
study_uid = '2.25.186760260011452398476108131496539831798'
series_uid = '2.25.45367868844278747809947145409050295798'

dcm_file_id = 85550324
nifti_file_id = 155149761

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)

def write_to_sqlite(df, db_path, table_name):
    conn = sql.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()

def example_read_dicom():
    dcm = DicomFile()
    dcm.from_dicom_path(dcm_file_path_1)

    logger.info("Meta tags:", list(dcm.meta_dict.keys())[:5])
    logger.info("Header tags:", list(dcm.header_dict.keys())[:5])

def example_hashing():
    logger.info("File hash:", hash_file(dcm_file_path_1))

    logger.info("Single UID hash:", hash_uid("1.2.3.4.5"))

    uids = ["1.2.3.4", "1.2.840.113619"]
    logger.info("Batch UID hash:", hash_uid_list(uids))

def example_index_directory():
    
    db_path=r"C:\data\test\dcm_index_test.db"
    conn_string = f"sqlite:///{db_path}"
    
    directories = ['dcm_index_test_01',
                   'dcm_index_test_02']

    with DBManager(conn_string, echo=False) as db:
        
        indexer = DicomIndexer()
        
        for directory in directories:    
            df = indexer.index_directory(
                directory_path=dcm_path,
                multiproc=True,
                cpus=8,
                group_name=directory,
                retain_pixel_data=False,
                db_manager=db)

def example_file_compare():
    d1 = DicomFile()
    d2 = DicomFile()
    d1.from_dicom_path(dcm_file_path_1)
    d2.from_dicom_path(dcm_file_path_2)

    d1_label = "d1"
    d2_label = "d2"

    # Build the base record using file-level identifiers from both files
    base_record = {
        f"{d1_label}_path": dcm_file_path_1,
        f"{d2_label}_path": dcm_file_path_2,
        f"{d1_label}_class": getattr(d1.header, "SOPClassUID", None),
        f"{d2_label}_class": getattr(d2.header, "SOPClassUID", None),
        f"{d1_label}_modality": getattr(d1.header, "Modality", None),
        f"{d2_label}_modality": getattr(d2.header, "Modality", None),
        f"{d1_label}_patient": getattr(d1.header, "PatientID", None),
        f"{d2_label}_patient": getattr(d2.header, "PatientID", None),
        f"{d1_label}_study": getattr(d1.header, "StudyInstanceUID", None),
        f"{d2_label}_study": getattr(d2.header, "StudyInstanceUID", None),
        f"{d1_label}_series": getattr(d1.header, "SeriesInstanceUID", None),
        f"{d2_label}_series": getattr(d2.header, "SeriesInstanceUID", None),
        f"{d1_label}_instance": getattr(d1.header, "SOPInstanceUID", None),
        f"{d2_label}_instance": getattr(d2.header, "SOPInstanceUID", None),
    }

    comparer = DicomFileComparer()
    results = comparer.compare(base_record, d1, d1_label, d2, d2_label)

    # Print sample differences
    for row in results:
        if row["different"]:
            logger.info(f"{row['tag']}: {row[f'{d1_label}_value']} != {row[f'{d2_label}_value']}")

def example_posda_api():
    api_host = config_data['tcia']['api_host']
    api_auth = config_data['tcia']['api_auth']

    with PosdaAPI(api_host, api_auth) as api:

        # study = api.get_study(study_uid)
        # #study_series = api.get_study_series(study_uid) - Doesn't work
        # series = api.get_series(series_uid)
        # series_files = api.get_series_files(f'{series_uid}:{timepoint}') # add timepoint to restrict
    
        output_path = r'C:\data\test\download'

        # dicom
        # dicom_info = api.get_file_info(dcm_file_id)
        # dicom_pixels = api.get_file_pixels(dcm_file_id)
        # dicom_data = api.get_file_data(dcm_file_id)
        # dicom_path = api.get_file_path(dcm_file_id)
        # dicom_details = api.get_file_details(dcm_file_id)
        # dicom_dump = api.get_dicom_dump(dcm_file_id)   
    
        api.download_series(series_uid, timepoint, output_path)

        api.download_file(dcm_file_id, output_path)
        api.download_file(dcm_file_id, output_path, 'test_dicom.dcm')
    
        # nifti
        # nifti_info = api.get_file_info(nifti_file_id)
        # nifti_pixels = api.get_file_pixels(nifti_file_id)
        # nifti_data = api.get_file_data(nifti_file_id)
        # nifti_path = api.get_file_path(nifti_file_id)
        # nifti_details = api.get_file_details(nifti_file_id) # doesn't work for nifti
        # nifti_dump = api.get_dicom_dump(nifti_file_id)
    
        api.download_file(nifti_file_id, output_path)
        api.download_file(nifti_file_id, output_path, 'test_nifti.nii.gz')
    
        # # multiple files
        # file_ids = api.get_series_files(series_uid, timepoint)
        # api.download_files(file_ids, output_path, structured_path=True, max_workers=5, overwrite=False)

def example_posda_db():
    conn_data = {
        "un": config_data['tcia']['un'],
        "pw": config_data['tcia']['pw'],
        "host": config_data['tcia']['host'],
        "port": config_data['tcia']['port']
    }

    with PosdaDB(conn_data, db_name="posda_files") as db:

        # # Insert
        # insert_sql = """
        #     INSERT INTO public.michael_test (a, b, c)
        #     VALUES (:a, :b, :c)
        # """
        # db.run_write(insert_sql,
        #     [{"a": "hello", "b": 1, "c": True},
        #      {"a": "there", "b": 2, "c": False},
        #      {"a": "friend", "b": 3, "c": True}]),

        # data_dict = db.run_query("SELECT * FROM public.michael_test", df=False)
        # print(data_dict)

        # # Update
        # update_sql = """
        #     UPDATE public.michael_test 
        #     SET a = :a, b = :b, c = :c 
        #     WHERE id = :id
        # """
        # db.run_write(update_sql, 
        #     [{"id": 1, "a": "see", "b": 10, "c": False},
        #      {"id": 2, "a": "you", "b": 9, "c": True},
        #      {"id": 3, "a": "later", "b": 8, "c": False}])

        # # Query
        # data_dict = db.run_query("SELECT * FROM public.michael_test", df=False)
        # print(data_dict)
    
        # Function
        timepoint_list = db.get_timepoint_files(3792)
        a='a'

def example_tag_matrix():
    
    db_path=r"C:\data\test\dcm_index_test.db"
    conn_string = f"sqlite:///{db_path}"    
    groups = ['dcm_index_test_01','dcm_index_test_02']

    with DBManager(conn_string, echo=False) as db:
        builder = TagMatrixBuilder(db, groups)
        df = builder.build_matrix(multiproc=True, cpus=36, batch_size=100)
        
def example_compare_posda_files():    
    # file_id_1 = 4352452
    # file_id_2 = 6548052

    file_id_1 = 13061912
    file_id_2 = 38249376
    
    api_host = config_data['tcia']['api_host']
    api_auth = config_data['tcia']['api_auth']

    conn_data = {
        "un": config_data['tcia']['un'],
        "pw": config_data['tcia']['pw'],
        "host": config_data['tcia']['host'],
        "port": config_data['tcia']['port']
    }

    with PosdaAPI(api_host, api_auth) as api, PosdaDB(conn_data, db_name="temp") as db:
        file_data_1 = api.get_file_data(file_id_1)
        file_data_2 = api.get_file_data(file_id_2)

        if not file_data_1 or not file_data_2:
            logger.error("Failed to retrieve one or both DICOM files.")
            return

        d1 = DicomFile()
        d2 = DicomFile()
        d1.from_dicom_bytes(file_data_1)
        d2.from_dicom_bytes(file_data_2)

        base_record = {
            "origin_path": f"posda:{file_id_1}",
            "terminal_path": f"posda:{file_id_2}",
            "origin_instance": getattr(d1.header_data, "SOPInstanceUID", None),
            "terminal_instance": getattr(d2.header_data, "SOPInstanceUID", None),
        }

        comparer = DicomFileComparer()
        results = comparer.compare(base_record, d1, "origin", d2, "terminal")

        db.create_table_from_model(DicomCompare)
        db.truncate_table(DicomCompare.__tablename__)
        db.insert_dicom_comparison(file_id_1, file_id_2, results, "origin", "terminal")




if __name__ == "__main__":

    start_time = datetime.now()
    logger.info(f"Process started at {start_time}")

    #example_read_dicom()
    #example_hashing()
    #example_index_directory()
    #example_file_compare()
    #example_posda_api()
    #example_posda_db()
    #example_tag_matrix()
    example_compare_posda_files()

    end_time = datetime.now()
    elapsed_time = end_time - start_time
    seconds_in_day = 24 * 60 * 60
    duration = divmod(elapsed_time.days * seconds_in_day + elapsed_time.seconds, 60)

    logger.info(f'Complete - Duration: {duration}')




