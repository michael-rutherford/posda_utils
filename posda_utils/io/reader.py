# posda_utils/io/reader.py

import pydicom as dcm
import chardet
import logging
from pydicom.errors import InvalidDicomError


class DicomFile:
    def __init__(self):
        self.exists = False
        self.info = None
        self.meta = None
        self.header = None
        self.pixel_data = None
        self.meta_dict = {}
        self.header_dict = {}

    def from_json(self, meta_json, header_json, info=None):
        """Load DICOM file from JSON representations of meta and header."""
        self.exists = True
        self.info = info
        self.meta = dcm.Dataset.from_json(meta_json)
        self.header = dcm.Dataset.from_json(header_json)
        self.pixel_data = None
        self.meta_dict = self._index_elements(self.meta)
        self.header_dict = self._index_elements(self.header)

    def from_dicom_path(self, dicom_path, retain_pixel_data=False):
        """Load and parse a DICOM file from disk."""
        try:
            dataset = dcm.dcmread(dicom_path, force=False)

            self.exists = True
            self.info = {"FilePath": dicom_path}
            self.meta = dataset.file_meta
            self.header = dataset

            if retain_pixel_data and "PixelData" in dataset:
                self.pixel_data = dataset.PixelData
            else:
                self.pixel_data = None
                if "PixelData" in self.header:
                    del self.header.PixelData

            self.meta_dict = self._index_elements(self.meta)
            self.header_dict = self._index_elements(self.header)

        except InvalidDicomError:
            logging.warning(f"Invalid DICOM file: {dicom_path}")
        except Exception as e:
            logging.error(f"Failed to read DICOM file {dicom_path}: {e}")

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
