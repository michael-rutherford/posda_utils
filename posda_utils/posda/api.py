# posda_utils/posda/api.py

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


class PosdaAPI:
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def __init__(self, api_url, auth_token):
        self.api_url = api_url.rstrip('/')
        self.headers = { 'Authorization': f'Bearer {auth_token}' }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)

        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=Retry(total=3, backoff_factor=0.3))
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def query_posda_api(self, endpoint):
        url = f"{self.api_url}{endpoint}"
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                return resp, url, True
            print(f'Bad response: {resp.status_code} - {resp.text}')
        except Exception as e:
            print(f'Error processing request: {e}')
        return None, url, False

    def get_study(self, study_instance_uid):
        resp, _, success = self.query_posda_api(f'/studies/{study_instance_uid}')
        return resp.json() if success else None

    # # investigate - Doesn't work
    # def get_study_series(self, study_instance_uid):
    #     resp, _, success = self.query_posda_api(f'/studies/{study_instance_uid}/series')
    #     return resp.json() if success else None

    def get_series(self, series_instance_uid):
        resp, _, success = self.query_posda_api(f'/series/{series_instance_uid}')
        return resp.json() if success else None

    def get_series_files(self, series_instance_uid, timepoint=None):
        sid = f"{series_instance_uid}:{timepoint}" if timepoint else series_instance_uid
        resp, _, success = self.query_posda_api(f'/series/{sid}/files')
        return resp.json().get('file_ids', []) if success else []

    def get_file_info(self, file_id):
        resp, _, success = self.query_posda_api(f'/files/{file_id}')
        return resp.json() if success else None

    def get_file_pixels(self, file_id):
        resp, _, success = self.query_posda_api(f'/files/{file_id}/pixels')
        return resp.content if success else None

    def get_file_data(self, file_id):
        resp, _, success = self.query_posda_api(f'/files/{file_id}/data')
        return resp.content if success else None

    def get_file_path(self, file_id):
        resp, _, success = self.query_posda_api(f'/files/{file_id}/path')
        return resp.json() if success else None

    def get_file_details(self, file_id):
        resp, _, success = self.query_posda_api(f'/files/{file_id}/details')
        return resp.json() if success else None

    def get_dicom_dump(self, file_id):
        resp, _, success = self.query_posda_api(f'/dump/{file_id}')
        return resp.text if success else None

    def download_series(self, series_instance_uid, timepoint=None, output_dir=None):
        sid = f"{series_instance_uid}:{timepoint}" if timepoint else series_instance_uid
        file_ids = self.get_series_files(sid)

        series_dir = os.path.join(output_dir, series_instance_uid)

        self.download_files(file_ids, series_dir, structured_path=False, max_workers=4, overwrite=False)

    def download_file(self, file_id, file_dir, file_name=None, overwrite=False):

        if not file_name:
            file_name = f"{file_id}"
            file_info = self.get_file_info(file_id)
            ext = None
            if file_info:
                match file_info['file_type']:
                    case type if type.startswith('parsed dicom file'):
                        ext = '.dcm'
                    case type if type.startswith('Nifti Image (gzipped)'):
                        ext = '.nii.gz'
                    case type if type.startswith('Nifti Image'):
                        ext = 'nii'                    
                    case type if type.startswith('TIFF image data'):
                        ext = '.tif'
                    case type if type.contains('ASCII'):
                        ext = '.txt'
                    case type if type.startswith('PDF document'):
                        ext = '.pdf'
                    case type if type.startswith('text/csv'):
                        ext = '.csv'
                    case type if type.startswith('HTML document'):
                        ext = '.html'
                    case type if type.startswith('XML  document'):
                        ext = '.xml'
                    case type if type.startswith('JSON data'):
                        ext = '.json'
                    case type if type.startswith('GIF image data'):
                        ext = '.gif'
                    case type if type.startswith('JPEG image data'):
                        ext = '.jpg'
                    case type if type.startswith('PNG image data'):
                        ext = '.png'                    
                    case type if type.startswith('gzip compressed data'):
                        ext = '.gz'
                    case type if type.startswith('Zip archive data'):
                        ext = '.zip'                    
                    case type if type.startswith('Perl script'):
                        ext = '.pl'
                    case type if type.startswith('Python script'):
                        ext = '.py'
                    case type if type.startswith('SQLite'):
                        ext = '.db'
                    case _:
                        ext = None

                file_name = f"{file_id}{ext if ext else ''}"

        download_path = os.path.join(file_dir, file_name)

        if not overwrite and os.path.exists(download_path):
            return download_path

        file_content = self.get_file_data(file_id)
        if file_content:
            os.makedirs(file_dir, exist_ok=True)
            with open(download_path, 'wb') as f:
                f.write(file_content)
            return download_path
        return None
    
    def _download_file_thread(self, file_id, output_dir, structured_path, overwrite):
        try:
            if structured_path:
                file_details = self.get_file_details(file_id)
                if not file_details:
                    return f"{file_id}: no file details"
            
                patient_id = file_details.get('patient_id', 'unknown')
                study_uid = file_details.get('study_instance_uid', 'unknown')
                series_uid = file_details.get('series_instance_uid', 'unknown')
                sop_uid = file_details.get('sop_instance_uid', 'unknown')

                file_dir = os.path.join(output_dir, patient_id, study_uid, series_uid)
                file_name = f"{sop_uid}.dcm"
            else:
                file_dir = output_dir
                file_name = f"{file_id}.dcm"

            path = self.download_file(file_id, file_dir, file_name, overwrite)
            if not path:
                return f"{file_id}: failed to download"
            return None

        except Exception as e:
            return f"{file_id}: error - {e}"

    def download_files(self, file_ids, output_dir, structured_path=True, max_workers=1, overwrite=False):
        print(f"Downloading {len(file_ids)} files using {'structured' if structured_path else 'flat'} path layout...")

        errors = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._download_file_thread, fid, output_dir, structured_path, overwrite)
                for fid in file_ids
            ]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading Files"):
                result = future.result()
                if result is not None:
                    errors.append(result)

        if errors:
            print("\nErrors encountered:")
            for err in errors:
                print(f" - {err}")
