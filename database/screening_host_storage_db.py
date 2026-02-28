from database.db_manager import DatabaseManager
class ScreeningHostStorageManager:
    def __init__(self, db_connection, connection_type="mysql"):
        self.db_connection = db_connection
        self.connection_type = connection_type
    def _get_connection(self):
        return DatabaseManager.get_connection(self.db_connection, self.connection_type)
    def search_records(self, table_name, search_column, search_value):
        conn = self._get_connection()
        cursor = conn.cursor()
        ph = "?" if self.connection_type == "sqlite" else "%s"
        cursor.execute(f"SELECT * FROM {table_name} WHERE {search_column} LIKE {ph}", (f"%{search_value}%",))
        if self.connection_type == "sqlite":
            columns = [description[0] for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        return cursor.fetchall()