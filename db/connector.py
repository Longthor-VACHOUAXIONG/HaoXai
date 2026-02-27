def execute_query(self, query):
    try:
        cursor = self.connection.cursor()
        cursor.execute(query)
        # Ensure consistent return type for all queries
        results = cursor.fetchall() if cursor.description else []
        self.connection.commit()
        return results or []  # Force empty list instead of None
    except Exception as e:
        self.connection.rollback()
        raise e