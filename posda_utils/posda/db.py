# posda_utils/posda/db.py

from posda_utils.db.database import DBManager

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
