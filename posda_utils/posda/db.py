# posda_utils/posda/db.py

from posda_utils.db.database import DBManager
from posda_utils.db.models import DicomCompare

class PosdaDB(DBManager):
    def __init__(self, conn_data, db_name, echo=False):
        conn_string = f"postgresql+psycopg2://{conn_data['un']}:{conn_data['pw']}@{conn_data['host']}:{conn_data['port']}/{db_name}"
        super().__init__(conn_string, echo=echo)

    def test_connection(self):
        try:
            with self.engine.connect() as conn:
                print("Connection successful")
        except Exception as e:
            print(f"Connection error: {e}")

    def get_recent_timepoint(self, file_id):
        query = f"""
            SELECT MAX(atf.activity_timepoint_id) 
            FROM activity_timepoint_file atf 
            WHERE atf.file_id = {file_id}
        """
        result = self.run_query(query)
        return result[0][0] if result else None

    def get_timepoint_files(self, tp):
        query = f"""
            SELECT atf.file_id 
            FROM activity_timepoint_file atf 
            WHERE atf.activity_timepoint_id = {tp}
        """
        result = self.run_query(query)
        return [row[0] for row in result] if result else []

    def insert_dicom_comparison(self, file_id_1, file_id_2, results, label_1="origin", label_2="terminal", only_diff=False):

        # Helper function to sanitize strings
        def sanitize_string(value):
            if isinstance(value, str):
                return value.replace("\x00", "")  # Remove NUL characters
            return value

        compare_rows = []
        for row in results:
            if only_diff and not row.get("different"):
                continue
            compare_rows.append(DicomCompare(
                origin_file_id=file_id_1,
                terminal_file_id=file_id_2,
                tag=sanitize_string(row.get("tag")),
                tag_path=sanitize_string(row.get("tag_path")),
                tag_group=sanitize_string(row.get("tag_group")),
                tag_element=sanitize_string(row.get("tag_element")),
                tag_name=sanitize_string(row.get("tag_name")),
                tag_keyword=sanitize_string(row.get("tag_keyword")),
                tag_vr=sanitize_string(row.get("tag_vr")),
                tag_vm=sanitize_string(row.get("tag_vm")),
                is_private=row.get("is_private"),
                private_creator=sanitize_string(row.get("private_creator")),
                origin_value=sanitize_string(row.get(f"{label_1}_value")),
                terminal_value=sanitize_string(row.get(f"{label_2}_value")),
                is_different=row.get("different"),
            ))

        if compare_rows:
            with self._get_session() as session:
                session.add_all(compare_rows)
                session.commit()