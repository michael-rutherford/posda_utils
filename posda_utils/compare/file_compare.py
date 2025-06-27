# posda_utils/compare/file_compare.py

class DicomFileComparer:
    def __init__(self):
        pass

    # Compare two DicomFile objects and return tag-by-tag differences
    def compare(self, base_record, dicom_01, dicom_01_label, dicom_02, dicom_02_label):
        
        comparison = []

        d1_dict = dicom_01.meta_dict | dicom_01.header_dict if dicom_01.exists else {}
        d2_dict = dicom_02.meta_dict | dicom_02.header_dict if dicom_02.exists else {}

        tag_keys = sorted(set(d1_dict.keys()) | set(d2_dict.keys()))

        for tag_key in tag_keys:
            row = base_record.copy()

            tag_01 = d1_dict.get(tag_key, {})
            tag_02 = d2_dict.get(tag_key, {})

            row["tag"] = tag_01.get("label") or tag_02.get("label")
            row["tag_path"] = tag_key

            try:
                element = tag_01["element"]
                row["tag_group"] = element.tag.group
                row["tag_element"] = element.tag.element
                row["tag_name"] = element.name
                row["tag_keyword"] = element.keyword
                row["tag_vr"] = element.VR
                row["tag_vm"] = element.VM
                row["is_private"] = element.is_private
                row["private_creator"] = element.private_creator
            except KeyError:
                try:
                    element = tag_02["element"]
                    row["tag_group"] = element.tag.group
                    row["tag_element"] = element.tag.element
                    row["tag_name"] = element.name
                    row["tag_keyword"] = element.keyword
                    row["tag_vr"] = element.VR
                    row["tag_vm"] = element.VM
                    row["is_private"] = element.is_private
                    row["private_creator"] = element.private_creator
                except KeyError:
                    row.update({
                        "tag_group": None,
                        "tag_element": None,
                        "tag_name": None,
                        "tag_keyword": None,
                        "tag_vr": None,
                        "tag_vm": None,
                        "is_private": None,
                        "private_creator": None
                    })

            row[f"{dicom_01_label}_value"] = tag_01.get("value") if not row["tag_vr"] == "SQ" else "<REMOVED>"
            row[f"{dicom_02_label}_value"] = tag_02.get("value") if not row["tag_vr"] == "SQ" else "<REMOVED>"
            row["different"] = tag_01.get("value") != tag_02.get("value")

            comparison.append(row)

        return comparison
