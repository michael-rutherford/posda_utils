# posda_utils/io/reader.py

import pydicom as dcm
import chardet
import logging
import base64
from io import BytesIO
from pydicom.errors import InvalidDicomError

from posda_utils.io.hasher import hash_data


class DicomFile:
    def __init__(self):
        self.exists = False
        self.info = None
        
        self.pixel_data = None
        self.pixel_digest = None
        self.pixel_size = 0

        self.meta_json = None
        self.meta_data = None
        self.meta_digest = None
        self.meta_size = 0
        self.meta_dict = {}
        
        self.header_json = None
        self.header_data = None
        self.header_digest = None
        self.header_size = 0
        self.header_dict = {}

    def from_json(self, meta_json, header_json, pixel_data, info=None):
        """Load DICOM file from JSON representations of meta and header."""
        self.exists = True
        self.info = info

        self.meta_data = dcm.Dataset.from_json(meta_json)
        self.meta_json = meta_json
        self.meta_size, self.meta_digest = hash_data(meta_json)
        self.meta_dict = self._index_elements(self.meta_data)

        self.header_data = dcm.Dataset.from_json(header_json)
        self.header_json = header_json
        self.header_size, self.header_digest = hash_data(header_json)
        self.header_dict = self._index_elements(self.header_data)

        if pixel_data:
            self.pixel_data = pixel_data
            try:
                pixel_bytes = base64.b64decode(pixel_data)
                self.pixel_size, self.pixel_digest = hash_data(pixel_bytes)
            except Exception as e:
                logging.warning(f"Could not decode pixel data from JSON: {e}")
                self.pixel_size, self.pixel_digest = None, None

    def from_dicom_path(self, dicom_path, retain_pixel_data=False):
        """Load and parse a DICOM file from disk."""
        try:
            dataset = dcm.dcmread(dicom_path, force=False)

            self.exists = True
            self.info = {"FilePath": dicom_path}

            if "PixelData" in dataset:
                pixel_data = dataset.PixelData
                if isinstance(pixel_data, (bytes, bytearray, memoryview)):
                    self.pixel_size, self.pixel_digest = hash_data(pixel_data)
                    if retain_pixel_data:
                        self.pixel_data = base64.b64encode(pixel_data).decode("utf-8")
                else:
                    logging.warning(f"Unsupported PixelData type in {dicom_path}: {type(pixel_data)}")
                    self.pixel_size = self.pixel_digest = self.pixel_data = None
                
            self.meta_data = dataset.file_meta
            self.meta_json = self.meta_data.to_json()
            self.meta_size, self.meta_digest = hash_data(self.meta_json)
            self.meta_dict = self._index_elements(self.meta_data)
            
            self.header_data = dataset.copy()
            if "PixelData" in self.header_data:
                del self.header_data.PixelData
            if hasattr(self.header_data, "file_meta"):
                del self.header_data.file_meta
            self.header_json = self.header_data.to_json()
            self.header_size, self.header_digest = hash_data(self.header_json)
            self.header_dict = self._index_elements(self.header_data)

        except InvalidDicomError:
            logging.warning(f"Invalid DICOM file: {dicom_path}")
        except Exception as e:
            logging.error(f"Failed to read DICOM file {dicom_path}: {e}")

    def from_dicom_bytes(self, byte_data, retain_pixel_data=False):
        """Load and parse a DICOM file from raw bytes."""
        try:
            dataset = dcm.dcmread(BytesIO(byte_data), force=False)

            self.exists = True
            self.info = {"Source": "memory"}

            if "PixelData" in dataset:
                pixel_data = dataset.PixelData
                if isinstance(pixel_data, (bytes, bytearray, memoryview)):
                    self.pixel_size, self.pixel_digest = hash_data(pixel_data)
                    if retain_pixel_data:
                        self.pixel_data = base64.b64encode(pixel_data).decode("utf-8")
                else:
                    logging.warning("Unsupported PixelData type in memory")
                    self.pixel_size = self.pixel_digest = self.pixel_data = None

            self.meta_data = dataset.file_meta
            self.meta_json = self.meta_data.to_json()
            self.meta_size, self.meta_digest = hash_data(self.meta_json)
            self.meta_dict = self._index_elements(self.meta_data)

            self.header_data = dataset.copy()
            if "PixelData" in self.header_data:
                del self.header_data.PixelData
            if hasattr(self.header_data, "file_meta"):
                del self.header_data.file_meta
            self.header_json = self.header_data.to_json()
            self.header_size, self.header_digest = hash_data(self.header_json)
            self.header_dict = self._index_elements(self.header_data)

        except InvalidDicomError:
            logging.warning("Invalid DICOM byte stream")
        except Exception as e:
            logging.error(f"Failed to read DICOM bytes: {e}")

    def to_index_row(self, group_name=None):
        return {
            "group_name": group_name,

            "file_path": self.info.get("FilePath") if self.info else None,
        
            "sop_class_uid": getattr(self.header_data, "SOPClassUID", None),
            "modality": getattr(self.header_data, "Modality", None),
            "patient_id": getattr(self.header_data, "PatientID", None),
            "study_instance_uid": getattr(self.header_data, "StudyInstanceUID", None),
            "series_instance_uid": getattr(self.header_data, "SeriesInstanceUID", None),
            "sop_instance_uid": getattr(self.header_data, "SOPInstanceUID", None),

            "header_data": self.header_json,
            "header_digest": self.header_digest,
            "header_size": self.header_size,

            "meta_data": self.meta_json,
            "meta_digest": self.meta_digest,
            "meta_size": self.meta_size,

            "pixel_data": self.pixel_data if isinstance(self.pixel_data, str) else None,
            "pixel_digest": self.pixel_digest,
            "pixel_size": self.pixel_size,
        }

    def _index_elements(self, dataset, elements=None, depth=0, count=0, label=None):
        if elements is None:
            elements = {}

        ignore_values = {'Pixel Data', 'Overlay Data', 'File Meta Information Version'}

        for element in dataset:
            tag_str = str(element.tag).replace(', ', ',')

            if element.is_private and element.private_creator:
                part_01 = tag_str[1:5]
                part_02 = tag_str[6:8]
                part_03 = tag_str[8:10]
                tag_str = f'({part_01},"{element.private_creator}",{part_03})'

            append = f'[<{str(count).zfill(4)}>]' if count else '[<0000>]'
            
            tag_path = f'<{tag_str}>' if depth == 0 else f'{label}{append}<{tag_str}>'

            element_info = {
                'label': f'<{tag_str}>',
                'vr': element.VR,
                'vm': element.VM,
                'value': self._safe_value(element.name, element.value, ignore_values),
                'element': element
            }

            elements[tag_path] = element_info

            if element.VR == 'SQ':
                for i, item in enumerate(element.value or []):
                    self._index_elements(item, elements, depth + 1, i, tag_path)

        return elements

    def _safe_value(self, name, value, ignore_values):
        if name in ignore_values:
            return '<REMOVED>' if value else '<>'
        return f'<{str(self._convert_bytes(value)).strip()}>' if value is not None else '<>'

    def _convert_bytes(self, value):
        if not isinstance(value, bytes):
            return value
        try:
            encoding = chardet.detect(value)['encoding'] or 'utf-8'
            return value.decode(encoding)
        except Exception:
            return value
