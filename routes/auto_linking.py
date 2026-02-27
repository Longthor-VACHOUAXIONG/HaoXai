"""
Auto-Linking Routes
Web interface for automatic bulk linking
"""
from flask import Blueprint, request, jsonify, render_template, session, current_app
import sqlite3
import mysql.connector
from database.db_manager_flask import DatabaseManagerFlask

auto_linking_bp = Blueprint('auto_linking', __name__, url_prefix='/auto-link')

def get_db_connection():
    """Get database connection from session"""
    db_type = session.get('db_type', 'sqlite')
    print(f"[DEBUG] auto_linking.get_db_connection: db_type={db_type}")
    
    if db_type == 'sqlite':
        db_conn = session.get('db_path')
    else:
        db_conn = session.get('db_params')
        print(f"[DEBUG] getting db_params: {db_conn}")
        
    if not db_conn:
        print("[ERROR] No db_conn found in session")
        print(f"[DEBUG] Session keys: {list(session.keys())}")
        return None, None
    
    return DatabaseManagerFlask.get_connection(db_conn, db_type), db_type


@auto_linking_bp.route('/')
def auto_link_page():
    """Auto-linking UI page"""
    return render_template('auto_linking.html')


@auto_linking_bp.route('/get-tables', methods=['GET'])
def get_tables():
    """Get list of available tables in the database"""
    conn, conn_type = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'No database connected'}), 400
    
    try:
        cursor = conn.cursor()
        
        if conn_type == 'sqlite':
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        else:
            cursor.execute("SHOW TABLES")
        
        # Ensure all table names are strings
        tables = [str(row[0]) for row in cursor.fetchall() if row and row[0] is not None]
        print(f"[DEBUG] Found tables: {tables}")
        
        # Categorize tables
        # Improved logic: treat as link table if it ends with '_link' OR contains '_link_' OR starts with 'link_'
        # This handles cases where user names table "source_target_link" or "link_source_target"
        data_tables = []
        link_tables = []
        
        for t in tables:
            # Standardized link detection
            is_link = DatabaseManagerFlask.is_link_table(t)
            
            # Special exclusions
            if t in ['RecycleBin', 'projects', 'blast_results', 'blast_hits', 'sqlite_sequence']:
                continue
                
            if is_link:
                link_tables.append(t)
            else:
                data_tables.append(t)
        
        return jsonify({
            'success': True,
            'data_tables': data_tables,
            'link_tables': link_tables,
            'all_tables': tables
        })
    
    except Exception as e:
        print(f"[ERROR] get_tables failed: {e}")
        return jsonify({'error': str(e)}), 500


@auto_linking_bp.route('/create-link-table', methods=['POST'])
def create_link_table():
    """Create a new link table for two data tables"""
    data = request.json
    source_table = data.get('source_table')
    target_table = data.get('target_table')
    link_table_name = data.get('link_table_name')
    
    if not all([source_table, target_table, link_table_name]):
        return jsonify({'error': 'source_table, target_table, and link_table_name are required'}), 400
    
    conn, conn_type = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'No database connected'}), 400
    
    try:
        cursor = conn.cursor()
        
        # Check if table already exists
        if conn_type == 'sqlite':
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{link_table_name}'")
        else:
            cursor.execute(f"SHOW TABLES LIKE '{link_table_name}'")
        
        if cursor.fetchone():
            return jsonify({'error': f'Table {link_table_name} already exists'}), 400
        
        # Helper to detect primary key (moved from preview_matches for reuse)
        def get_primary_key_info(table_name):
            if conn_type == 'sqlite':
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                for col in columns:
                    if col[5] == 1: return col[1], col[2]
                col_names = [col[1] for col in columns]
                for id_name in ['Id', 'id', 'ID']:
                    if id_name in col_names:
                        # Find the row for this id to get its type
                        for col in columns:
                            if col[1] == id_name: return id_name, col[2]
                return 'rowid', 'INTEGER'
            else:
                cursor.execute(f"SHOW KEYS FROM `{table_name}` WHERE Key_name = 'PRIMARY'")
                res = cursor.fetchone()
                pk_name = res[4] if res else 'id'
                # Get type
                cursor.execute(f"DESCRIBE `{table_name}`")
                for row in cursor.fetchall():
                    if row[0].lower() == pk_name.lower(): return row[0], row[1]
                return pk_name, 'INT'

        # Generate column names
        clean_source = source_table.replace('sample_', '').replace('_data', '').lower()
        clean_target = target_table.replace('sample_', '').replace('_data', '').lower()
        source_fk = f"{clean_source}_data_id"
        target_fk = f"{clean_target}_data_id"
        
        # Detect types
        source_id_col, source_type = get_primary_key_info(source_table)
        target_id_col, target_type = get_primary_key_info(target_table)
        
        def map_type(t):
            t_upper = str(t or '').upper()
            if "TEXT" in t_upper or "CHAR" in t_upper or "VARCHAR" in t_upper:
                return "VARCHAR(255)" if conn_type == 'mysql' else "TEXT"
            if "INT" in t_upper: return t
            return "VARCHAR(255)" if conn_type == 'mysql' else "TEXT"

        source_lk_type = map_type(source_type)
        target_lk_type = map_type(target_type)

        print(f"[DEBUG] Creating link table: {link_table_name} ({source_fk}: {source_lk_type}, {target_fk}: {target_lk_type})")
        
        if conn_type == 'sqlite':
            create_sql = f'''CREATE TABLE "{link_table_name}" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "{source_fk}" {source_lk_type} NOT NULL,
    "{target_fk}" {target_lk_type} NOT NULL,
    "link_date" DATETIME DEFAULT CURRENT_TIMESTAMP,
    "notes" TEXT
)'''
        else:
            create_sql = f'''CREATE TABLE `{link_table_name}` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `{source_fk}` {source_lk_type} NOT NULL,
    `{target_fk}` {target_lk_type} NOT NULL,
    `link_date` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `notes` TEXT,
    INDEX (`{source_fk}`),
    INDEX (`{target_fk}`)
)'''
        
        cursor.execute(create_sql)
        conn.commit()
        
        return jsonify({
            'success': True,
            'message': f'Link table {link_table_name} created successfully',
            'table_name': link_table_name,
            'columns': {
                'source_fk': source_fk,
                'target_fk': target_fk
            }
        })
    
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500


@auto_linking_bp.route('/get-columns', methods=['POST'])
def get_columns():
    """Get columns for a specific table"""
    data = request.json
    table_name = data.get('table_name')
    
    if not table_name:
        return jsonify({'error': 'Table name required'}), 400
    
    conn, conn_type = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'No database connected'}), 400
    
    try:
        cursor = conn.cursor()
        
        if conn_type == 'sqlite':
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
        else:
            cursor.execute(f"DESCRIBE {table_name}")
            columns = [row[0] for row in cursor.fetchall()]
        
        return jsonify({
            'success': True,
            'table': table_name,
            'columns': columns
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@auto_linking_bp.route('/preview-matches', methods=['POST'])
def preview_matches():
    """Preview matching records before creating links"""
    data = request.json
    source_table = data.get('source_table')
    target_table = data.get('target_table')
    match_columns = data.get('match_columns', [])  # [{'source': 'col1', 'target': 'col2'}]
    
    if not all([source_table, target_table, match_columns]):
        return jsonify({'error': 'Missing required parameters'}), 400
    
    conn, conn_type = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'No database connected'}), 400
    
    try:
        cursor = conn.cursor()
        
        # Auto-detect primary key columns
        def get_primary_key(table_name):
            if conn_type == 'sqlite':
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                # Look for INTEGER PRIMARY KEY or pk=1
                for col in columns:
                    if col[5] == 1:  # pk flag
                        return col[1]  # column name
                # Check for common ID column names
                col_names = [col[1] for col in columns]
                for id_name in ['Id', 'id', 'ID']:
                    if id_name in col_names:
                        return id_name
                return 'rowid'  # Fallback to rowid
            else:
                cursor.execute(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'")
                result = cursor.fetchone()
                return result[4] if result else 'id'
        
        source_id_column = get_primary_key(source_table)
        target_id_column = get_primary_key(target_table)
        
        print(f"[DEBUG] Preview: {source_table}.{source_id_column} → {target_table}.{target_id_column}")
        
        # Helper function to quote column names
        def quote_col(col_name):
            if conn_type == 'sqlite':
                return f'"{col_name}"'
            else:
                return f'`{col_name}`'
        
        # Build JOIN condition
        join_conditions = []
        for match in match_columns:
            join_conditions.append(f"s.{quote_col(match['source'])} = t.{quote_col(match['target'])}")
        
        join_clause = " AND ".join(join_conditions)
        
        # Preview query
        query = f"""
            SELECT 
                s.{quote_col(source_id_column)} as source_id,
                t.{quote_col(target_id_column)} as target_id,
                {', '.join([f"s.{quote_col(m['source'])}" for m in match_columns])}
            FROM `{source_table}` s
            JOIN `{target_table}` t ON {join_clause}
            LIMIT 100
        """
        
        print(f"[DEBUG] Preview query:\n{query}")
        
        try:
            cursor.execute(query)
        except Exception as e:
            error_msg = f"Query execution error: {str(e)}\nQuery: {query}"
            print(f"[ERROR] {error_msg}")
            return jsonify({'error': str(e)}), 500
        matches = cursor.fetchall()
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Format results
        results = []
        for row in matches:
            results.append(dict(zip(columns, row)))
        
        return jsonify({
            'success': True,
            'match_count': len(matches),
            'matches': results,
            'has_more': len(matches) == 100
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@auto_linking_bp.route('/execute-linking', methods=['POST'])
def execute_linking():
    """Execute automatic bulk linking"""
    data = request.json
    source_table = data.get('source_table')
    target_table = data.get('target_table')
    link_table = data.get('link_table')
    match_columns = data.get('match_columns', [])
    source_id_column = data.get('source_id_column')
    target_id_column = data.get('target_id_column')
    notes = data.get('notes', 'Auto-linked via UI')
    
    # Trigger options
    create_trigger = data.get('create_trigger', False)
    bidirectional_trigger = data.get('bidirectional_trigger', True)
    trigger_notes = data.get('trigger_notes', 'Auto-linked by trigger')
    
    if not all([source_table, target_table, link_table, match_columns]):
        return jsonify({'error': 'Missing required parameters'}), 400
    
    conn, conn_type = get_db_connection()
    
    if not conn:
        return jsonify({'error': 'No database connected'}), 400
    
    try:
        cursor = conn.cursor()
        
        # Auto-detect primary key columns if not provided
        def get_primary_key(table_name):
            if conn_type == 'sqlite':
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                # Look for INTEGER PRIMARY KEY or pk=1
                for col in columns:
                    if col[5] == 1:  # pk flag
                        return col[1]  # column name
                # Check for common ID column names
                col_names = [col[1] for col in columns]
                for id_name in ['Id', 'id', 'ID']:
                    if id_name in col_names:
                        return id_name
                return 'rowid'  # Fallback to rowid
            else:
                cursor.execute(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'")
                result = cursor.fetchone()
                return result[4] if result else 'id'
        
        if not source_id_column:
            source_id_column = get_primary_key(source_table)
        if not target_id_column:
            target_id_column = get_primary_key(target_table)
        
        print(f"[DEBUG] Auto-linking: {source_table}.{source_id_column} → {target_table}.{target_id_column}")
        
        # Helper function to quote column names
        def quote_col(col_name):
            if conn_type == 'sqlite':
                return f'"{col_name}"'
            else:
                return f'`{col_name}`'
        
        # Build JOIN condition
        join_conditions = []
        for match in match_columns:
            join_conditions.append(f"s.{quote_col(match['source'])} = t.{quote_col(match['target'])}")
        
        join_clause = " AND ".join(join_conditions)
        
        # Get link table structure
        if conn_type == 'sqlite':
            cursor.execute(f"PRAGMA table_info('{link_table}')")
            link_columns = [row[1] for row in cursor.fetchall()]
        else:
            cursor.execute(f"DESCRIBE `{link_table}`")
            link_columns = [row[0] for row in cursor.fetchall()]
        
        # Determine column names in link table
        source_fk = None
        target_fk = None
        
        # Smart detection: find columns that contain source/target table names
        for col in link_columns:
            col_lower = col.lower()
            
            # More precise matching - check for exact table name matches first
            source_exact = f"{source_table.lower()}_data_id" in col_lower
            target_exact = f"{target_table.lower()}_data_id" in col_lower
            
            # Check if this column references the source table
            if source_exact and '_id' in col_lower:
                if not source_fk:
                    source_fk = col
            # Check if this column references the target table  
            elif target_exact and '_id' in col_lower:
                if not target_fk:
                    target_fk = col
        
        # If exact matches didn't work, try partial matches (but be more careful)
        if not source_fk or not target_fk:
            for col in link_columns:
                col_lower = col.lower()
                # Check if this column references the source table
                if source_table.lower() in col_lower and '_id' in col_lower:
                    # Avoid false positives by checking if the column name starts with table name
                    if col_lower.startswith(source_table.lower()) or f"_{source_table.lower()}_" in col_lower:
                        if not source_fk:
                            source_fk = col
                # Check if this column references the target table
                elif target_table.lower() in col_lower and '_id' in col_lower:
                    # Avoid false positives by checking if the column name starts with table name
                    if col_lower.startswith(target_table.lower()) or f"_{target_table.lower()}_" in col_lower:
                        if not target_fk:
                            target_fk = col
        
        # Special handling for sequences_consensus_sequences_link table
        if link_table == 'sequences_consensus_sequences_link':
            if source_table == 'sequences':
                source_fk = 'sequences_data_id'
            elif source_table == 'consensus_sequences':
                source_fk = 'consensus_sequences_data_id'
            
            if target_table == 'sequences':
                target_fk = 'sequences_data_id'
            elif target_table == 'consensus_sequences':
                target_fk = 'consensus_sequences_data_id'
        
        # Fallback: look for common patterns
        if not source_fk or not target_fk:
            for col in link_columns:
                if 'bat_data_id' in col or 'market_data_id' in col or 'rodenthost_data_id' in col or 'freezer_storage_id' in col:
                    if not source_fk:
                        source_fk = col
                elif 'screening_data_id' in col or 'sequence_id' in col or any(x in col for x in ['bat_data_id', 'swab_data_id', 'tissue_data_id']):
                    if not target_fk and col != source_fk:
                        target_fk = col
        
        print(f"[DEBUG] Detected FK columns: {source_fk} (source), {target_fk} (target)")
        
        if not source_fk or not target_fk:
            error_msg = f"Could not detect foreign key columns in {link_table}. Available columns: {link_columns}"
            print(f"[ERROR] {error_msg}")
            return jsonify({'error': error_msg}), 400
        
        # Find matches and create links
        select_query = f"""
            SELECT 
                s.{quote_col(source_id_column)} as source_id,
                t.{quote_col(target_id_column)} as target_id,
                {', '.join([f"s.{quote_col(m['source'])}" for m in match_columns])}
            FROM `{source_table}` s
            JOIN `{target_table}` t ON {join_clause}
            WHERE NOT EXISTS (
                SELECT 1 FROM `{link_table}` 
                WHERE `{source_fk}` = s.{quote_col(source_id_column)} 
                AND `{target_fk}` = t.{quote_col(target_id_column)}
            )
        """
        
        print(f"[DEBUG] Query:\n{select_query}")
        
        try:
            cursor.execute(select_query)
        except Exception as e:
            error_msg = f"Query execution error: {str(e)}\nQuery: {select_query}"
            print(f"[ERROR] {error_msg}")
            return jsonify({'error': str(e)}), 500
        matches = cursor.fetchall()
        
        links_created = 0
        errors = []
        
        for match in matches:
            source_id = match[0]
            target_id = match[1]
            match_value = match[2] if len(match) > 2 else ''
            
            try:
                # Build insert based on available columns
                insert_cols = [source_fk, target_fk]
                insert_vals = [source_id, target_id]
                
                # Add matching column values
                if match_columns and len(match) > 2:
                    match_value = match[2]  # The first match column value
                    
                    # Check which columns the link table needs and add them
                    if 'source_id' in link_columns:
                        insert_cols.append('source_id')
                        insert_vals.append(match_value)
                    if 'sample_id' in link_columns and 'sample_id' not in insert_cols:
                        insert_cols.append('sample_id')
                        insert_vals.append(match_value)
                    if 'field_id' in link_columns and 'field_id' not in insert_cols:
                        insert_cols.append('field_id')
                        insert_vals.append(match_value)
                
                if 'notes' in link_columns:
                    insert_cols.append('notes')
                    insert_vals.append(notes)
                
                placeholders = ', '.join(['?' if conn_type == 'sqlite' else '%s'] * len(insert_vals))
                
                insert_query = f"""
                    INSERT INTO `{link_table}` ({', '.join([quote_col(c) for c in insert_cols])})
                    VALUES ({placeholders})
                """
                
                cursor.execute(insert_query, insert_vals)
                links_created += 1
                
            except Exception as e:
                errors.append(f"Row {source_id}→{target_id}: {str(e)}")
        
        conn.commit()
        
        # Create triggers if requested
        trigger_created = False
        trigger_error = None
        if create_trigger:
            try:
                trigger_created = create_auto_link_triggers(
                    cursor, conn_type, source_table, target_table, link_table,
                    match_columns, source_id_column, target_id_column,
                    bidirectional_trigger, trigger_notes, source_fk, target_fk
                )
                print(f"[DEBUG] Triggers created: {trigger_created}")
            except Exception as e:
                # Handle privilege error gracefully
                err_str = str(e)
                if "SUPER privilege" in err_str or "1227" in err_str:
                    trigger_error = "Triggers could not be created because the database user lacks 'SUPER' privileges (common on remote servers). Bulk linking completed, but future additions won't be automatic."
                    print(f"[WARNING] {trigger_error}")
                else:
                    print(f"[ERROR] Failed to create triggers: {e}")
                    errors.append(f"Trigger creation failed: {err_str}")
        
        conn.commit()
        
        # Send real-time notifications
        try:
            from utils.realtime import notify_links_created
            socketio = current_app.socketio
            if socketio:
                # 1. Specialized notification for the auto-link page
                notify_links_created(socketio, link_table, links_created, trigger_created)
                
                # 2. Standardized notification for the main dashboard (which listens for data_inserted)
                if links_created > 0:
                    DatabaseManagerFlask.emit_realtime_event('inserted', link_table, {
                        'count': links_created,
                        'total_matches': len(matches)
                    })
                
                print(f"[REALTIME] Sent link creation notifications for {link_table}")
        except Exception as e:
            print(f"[WARNING] Real-time notification failed: {e}")
        
        return jsonify({
            'success': True,
            'links_created': links_created,
            'total_matches': len(matches),
            'trigger_created': trigger_created,
            'trigger_error': trigger_error,
            'errors': errors if errors else None
        })
    
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500


@auto_linking_bp.route('/get-link-stats', methods=['GET'])
def get_link_stats():
    """Get statistics for all link tables"""
    try:
        conn, conn_type = get_db_connection()
        
        if not conn:
            return jsonify({'error': 'No database connected'}), 400
        
        cursor = conn.cursor()
        
        # Get all link tables using standardized discovery
        tables = DatabaseManagerFlask.get_tables(conn, conn_type)
        link_tables = [t for t in tables if DatabaseManagerFlask.is_link_table(t)]
        
        # Exclusions
        exclude = set(['RecycleBin', 'projects', 'blast_results', 'blast_hits', 'sqlite_sequence'])
        link_tables = [t for t in link_tables if t not in exclude]
        
        stats = []
        for table in link_tables:
            try:
                # Fix quoting for table names to prevent SQL errors
                if conn_type == 'sqlite':
                    query = f'SELECT COUNT(*) FROM "{table}"'
                else:
                    query = f"SELECT COUNT(*) FROM `{table}`"
                    
                # Use a new cursor for the count query to be safe
                count_cursor = conn.cursor()
                count_cursor.execute(query)
                result = count_cursor.fetchone()
                count_cursor.close()
                
                count = result[0] if result else 0
                stats.append({
                    'table': table,
                    'count': count
                })
            except Exception as e:
                print(f"[ERROR] Failed to get stats for table {table}: {e}")
                # Continue with other tables
                stats.append({
                    'table': table,
                    'count': 0,
                    'error': str(e)
                })
        
        return jsonify({
            'success': True,
            'stats': stats
        })
    
    except Exception as e:
        print(f"[ERROR] get_link_stats failed: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def create_auto_link_triggers(cursor, conn_type, source_table, target_table, link_table,
                            match_columns, source_id_column, target_id_column,
                            bidirectional_trigger, trigger_notes, source_fk, target_fk):
    """Create automatic linking triggers for the specified tables"""
    
    def quote_col(col_name):
        if conn_type == 'sqlite':
            return f'"{col_name}"'
        else:
            return f'`{col_name}`'
    
    triggers_created = 0
    
    # Build match condition for trigger
    match_conditions = []
    for match in match_columns:
        match_conditions.append(f"NEW.{quote_col(match['source'])} = t.{quote_col(match['target'])}")
    match_clause = " AND ".join(match_conditions)
    
    # Create trigger for source table insertions
    trigger_name_source = f"auto_link_{source_table}_{target_table}"
    
    if conn_type == 'sqlite':
        trigger_sql_source = f'''
            CREATE TRIGGER IF NOT EXISTS {trigger_name_source}
            AFTER INSERT ON {source_table}
            BEGIN
                INSERT OR IGNORE INTO {link_table}
                ({source_fk}, {target_fk}, link_date, notes)
                SELECT 
                    NEW.{quote_col(source_id_column)},
                    t.{quote_col(target_id_column)},
                    datetime('now'),
                    '{trigger_notes}'
                FROM {target_table} t
                WHERE {match_clause}
                AND NOT EXISTS (
                    SELECT 1 FROM {link_table} l
                    WHERE l.{source_fk} = NEW.{quote_col(source_id_column)}
                    AND l.{target_fk} = t.{quote_col(target_id_column)}
                );
            END
        '''
    else:
        trigger_sql_source = f'''
            CREATE TRIGGER IF NOT EXISTS `{trigger_name_source}`
            AFTER INSERT ON `{source_table}`
            FOR EACH ROW
            BEGIN
                INSERT IGNORE INTO `{link_table}`
                ({quote_col(source_fk)}, {quote_col(target_fk)}, link_date, notes)
                SELECT 
                    NEW.{quote_col(source_id_column)},
                    t.{quote_col(target_id_column)},
                    NOW(),
                    '{trigger_notes}'
                FROM `{target_table}` t
                WHERE {match_clause}
                AND NOT EXISTS (
                    SELECT 1 FROM `{link_table}` l
                    WHERE l.{quote_col(source_fk)} = NEW.{quote_col(source_id_column)}
                    AND l.{quote_col(target_fk)} = t.{quote_col(target_id_column)}
                );
            END
        '''
    
    try:
        cursor.execute(trigger_sql_source)
        triggers_created += 1
        print(f"[DEBUG] Created trigger: {trigger_name_source}")
    except Exception as e:
        print(f"[ERROR] Failed to create source trigger: {e}")
        raise
    
    # Create reverse trigger if bidirectional
    if bidirectional_trigger:
        trigger_name_target = f"auto_link_{target_table}_{source_table}"
        
        # Build reverse match condition
        reverse_match_conditions = []
        for match in match_columns:
            reverse_match_conditions.append(f"NEW.{quote_col(match['target'])} = t.{quote_col(match['source'])}")
        reverse_match_clause = " AND ".join(reverse_match_conditions)
        
        if conn_type == 'sqlite':
            trigger_sql_target = f'''
                CREATE TRIGGER IF NOT EXISTS {trigger_name_target}
                AFTER INSERT ON {target_table}
                BEGIN
                    INSERT OR IGNORE INTO {link_table}
                    ({source_fk}, {target_fk}, link_date, notes)
                    SELECT 
                        s.{quote_col(source_id_column)},
                        NEW.{quote_col(target_id_column)},
                        datetime('now'),
                        '{trigger_notes}'
                    FROM {source_table} s
                    WHERE {reverse_match_clause}
                    AND NOT EXISTS (
                        SELECT 1 FROM {link_table} l
                        WHERE l.{source_fk} = s.{quote_col(source_id_column)}
                        AND l.{target_fk} = NEW.{quote_col(target_id_column)}
                    );
                END
            '''
        else:
            trigger_sql_target = f'''
                CREATE TRIGGER IF NOT EXISTS `{trigger_name_target}`
                AFTER INSERT ON `{target_table}`
                FOR EACH ROW
                BEGIN
                    INSERT IGNORE INTO `{link_table}`
                    ({quote_col(source_fk)}, {quote_col(target_fk)}, link_date, notes)
                    SELECT 
                        s.{quote_col(source_id_column)},
                        NEW.{quote_col(target_id_column)},
                        NOW(),
                        '{trigger_notes}'
                    FROM `{source_table}` s
                    WHERE {reverse_match_clause}
                    AND NOT EXISTS (
                        SELECT 1 FROM `{link_table}` l
                        WHERE l.{quote_col(source_fk)} = s.{quote_col(source_id_column)}
                        AND l.{quote_col(target_fk)} = NEW.{quote_col(target_id_column)}
                    );
                END
            '''
        
        try:
            cursor.execute(trigger_sql_target)
            triggers_created += 1
            print(f"[DEBUG] Created reverse trigger: {trigger_name_target}")
        except Exception as e:
            print(f"[ERROR] Failed to create reverse trigger: {e}")
            raise
    
    return triggers_created > 0
