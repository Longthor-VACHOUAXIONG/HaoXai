import sqlite3
import re
import os

class Simulator:
    def __init__(self, db_path):
        self.db_path = db_path
        self.db_type = 'sqlite'

    def analyze_question(self, question):
        q = question.lower()
        analysis = {'intent': 'search', 'keywords': [], 'limit': 10, 'filters': {}}
        
        # 1. Extract specifically formatted filters like "Field: Value" or "Field=Value"
        filters = {}
        # This matches "Key: Value" or "Key=Value" where Value can be alphanumeric
        filter_matches = re.findall(r'(\w+)\s*[:=]\s*(\w+)', q)
        filter_keys = set()
        for field, value in filter_matches:
            if len(field) >= 3 and len(value) >= 2:
                filters[field] = value.upper()
                filter_keys.add(field)
        
        analysis['filters'] = filters
        
        # 2. Extract remaining words as keywords
        words = re.findall(r'\b[a-z0-9_]{3,}\b', q)
        
        # Add all words as keywords (uppercase for matching)
        analysis['keywords'] = [w.upper() for w in words if w not in filter_keys and w not in ['the', 'and', 'for', 'with', 'from', 'where', 'what', 'find', 'search', 'show', 'list', 'how', 'many', 'this', 'that', 'are', 'was', 'were', 'been', 'have', 'has', 'had', 'did', 'does', 'doing', 'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'need', 'needs', 'dare', 'dares', 'ought', 'used', 'use', 'using', 'get', 'got', 'gets', ' gotten', 'become', 'became', 'becomes', 'seem', 'seems', 'seemed', 'look', 'looks', 'looked', 'appear', 'appears', 'appeared', 'happen', 'happens', 'happened', 'try', 'tries', 'tried', 'trying', 'want', 'wants', 'wanted', 'wanting', 'like', 'likes', 'liked', 'liking', 'love', 'loves', 'loved', 'loving', 'hate', 'hates', 'hated', 'hating', 'prefer', 'prefers', 'preferred', 'preferring', 'hope', 'hopes', 'hoped', 'hoping', 'wish', 'wishes', 'wished', 'wishing', 'believe', 'believes', 'believed', 'believing', 'think', 'thinks', 'thought', 'thinking', 'suppose', 'supposes', 'supposed', 'supposing', 'expect', 'expects', 'expected', 'expecting', 'imagine', 'imagines', 'imagined', 'imagining', 'feel', 'feels', 'felt', 'feeling', 'see', 'sees', 'saw', 'seen', 'seeing', 'watch', 'watches', 'watched', 'watching', 'hear', 'hears', 'heard', 'hearing', 'notice', 'notices', 'noticed', 'noticing', 'let', 'lets', 'letting', 'make', 'makes', 'made', 'making', 'help', 'helps', 'helped', 'helping', 'force', 'forces', 'forced', 'forcing', 'drive', 'drives', 'drove', 'driven', 'driving']]
        
        quoted = re.findall(r'["\']([^"\']+)["\']', question)
        analysis['keywords'].extend([q.strip().upper() for q in quoted])
        analysis['keywords'] = list(dict.fromkeys(analysis['keywords']))
        return analysis

    def run(self, question):
        analysis = self.analyze_question(question)
        print(f"Keywords: {analysis['keywords']}")
        print(f"Filters: {analysis['filters']}")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bathost'")
        if not cursor.fetchone():
            print("bathost table not found.")
            return

        cursor.execute("PRAGMA table_info(bathost)")
        columns = [c[1] for c in cursor.fetchall()]
        col_names = columns
        
        # 1. Filters
        filter_conditions = []
        filter_params = []
        for field, value in analysis['filters'].items():
            matched_col = None
            for col in col_names:
                if col.lower() == field.lower():
                    matched_col = col
                    break
            if not matched_col:
                for col in col_names:
                    if field.lower() in col.lower() or col.lower() in field.lower():
                        matched_col = col
                        break
            if matched_col:
                filter_conditions.append(f'LOWER("{matched_col}") LIKE LOWER(?)')
                filter_params.append(f'%{value}%')

        # 2. Keywords
        keyword_conditions = []
        keyword_params = []
        for keyword in analysis['keywords']:
            keyword_str = str(keyword).strip()
            for col in col_names:
                keyword_conditions.append(f'LOWER("{col}") LIKE LOWER(?)')
                keyword_params.append(f'%{keyword_str}%')
        
        params = []
        where_clause = ""
        if filter_conditions:
            where_clause = " WHERE (" + " AND ".join(filter_conditions) + ")"
            params.extend(filter_params)
            if keyword_conditions:
                where_clause += " AND (" + " OR ".join(keyword_conditions) + ")"
                params.extend(keyword_params)
        elif keyword_conditions:
            where_clause = " WHERE (" + " OR ".join(keyword_conditions) + ")"
            params.extend(keyword_params)

        if not where_clause:
            print("No conditions generated.")
            return

        sql = f'SELECT * FROM bathost {where_clause} LIMIT 10'
        print(f"Executing SQL: {sql}")
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        
        if not rows:
            print("No rows matched.")
        else:
            print(f"Found {len(rows)} matching rows.")
            for i, row in enumerate(rows):
                row_dict = dict(zip(columns, row))
                print(f"\nRow {i+1}:")
                for k, v in row_dict.items():
                    # Check which field matched which keyword
                    for kw in analysis['keywords']:
                        if v and str(kw).lower() in str(v).lower():
                            print(f"  [MATCH] {k}={v} matched '{kw}'")
                        elif not v and str(kw).lower() in "":
                            pass
                    if k in ['Id', 'Bagid', 'Sourceid', 'Bag_ID']:
                        print(f"  • {k}: {v}")

        conn.close()

if __name__ == "__main__":
    sim = Simulator('CAN2Database.db')
    sim.run("Bagid: A109")
