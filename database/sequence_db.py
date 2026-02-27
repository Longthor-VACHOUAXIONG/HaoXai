"""
Sequence Database Manager
Handles storage and retrieval of sequences, consensus sequences, and BLAST results
"""
import json
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional
from database.db_manager import DatabaseManager
from database.sample_manager import SampleManager


class SequenceDBManager:
    """Manager for sequence-related database operations"""
    
    def __init__(self, db_connection, connection_type='mysql'):
        """
        Initialize the sequence database manager
        
        Args:
            db_connection: Database connection info (path for SQLite, dict for MySQL)
            connection_type: 'sqlite' or 'mysql'
        """
        self.connection_type = connection_type
        self.db_connection = db_connection  # Store connection info, not the connection itself
        self._initialize_tables()
        
        # Initialize sample manager for handling sample-virus relationships
        self.sample_manager = SampleManager(db_connection, connection_type)
    
    def _get_connection(self):
        """Get a thread-safe database connection"""
        return DatabaseManager.get_connection(self.db_connection, self.connection_type)
    
    def _initialize_tables(self):
        """Create tables if they don't exist"""
        # Always auto-validate and fix schema before proceeding
        try:
            from auto_validate_schema import auto_validate_database_schema
            print("[DEBUG] Auto-validating database schema before initialization...")
            if not auto_validate_database_schema(self.db_connection):
                print("[ERROR] Failed to auto-validate database schema")
                return
        except ImportError:
            print("[WARNING] Auto schema validator not available, proceeding with normal initialization...")
        
        schema_file = 'database/schema_sequences_sqlite.sql' if self.connection_type == 'sqlite' else 'database/schema_sequences.sql'
        
        try:
            with open(schema_file, 'r') as f:
                schema = f.read()
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if self.connection_type == 'sqlite':
                # For SQLite, use executescript which handles multiple statements
                cursor.executescript(schema)
            else:
                # For MySQL, split by semicolon and execute each statement
                # The previous logic filtered out blocks starting with -- which caused tables to be skipped
                raw_statements = schema.split(';')
                for raw_statement in raw_statements:
                    statement = raw_statement.strip()
                    if statement:
                        try:
                            # MySQL handles -- comments fine, no need to strip them manually
                            # if they are part of the statement
                            cursor.execute(statement)
                        except Exception as stmt_err:
                            # Ignore empty query errors or harmless warnings
                            print(f"[DEBUG] SQL Execution info: {stmt_err}")
                            print(f"[DEBUG] Statement start: {statement[:50]}...")
            
            conn.commit()
            print(f"[INFO] Sequence database tables initialized from {schema_file}")
            
        except FileNotFoundError:
            print(f"[WARNING] Schema file {schema_file} not found. Creating tables manually...")
            self._create_tables_manually()
        except Exception as e:
            print(f"[ERROR] Failed to initialize tables from schema file: {e}")
            import traceback
            traceback.print_exc()
            print(f"[INFO] Falling back to manual table creation...")
            self._create_tables_manually()
    
    def _create_tables_manually(self):
        """Fallback method to create tables if schema file is missing"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if self.connection_type == 'sqlite':
            # SQLite table creation
            cursor.executescript('''
                CREATE TABLE IF NOT EXISTS sequences (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename TEXT NOT NULL,
                    upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    file_hash TEXT UNIQUE,
                    sequence TEXT NOT NULL,
                    sequence_length INTEGER NOT NULL,
                    group_name TEXT,
                    detected_direction TEXT CHECK(detected_direction IN ('Forward', 'Reverse', 'Unknown')) DEFAULT 'Unknown',
                    quality_score REAL,
                    avg_quality REAL,
                    min_quality REAL,
                    max_quality REAL,
                    overall_grade TEXT CHECK(overall_grade IN ('Excellent', 'Good', 'Acceptable', 'Poor', 'Needs Work', 'Unknown')) DEFAULT 'Unknown',
                    grade_score INTEGER,
                    issues TEXT,  -- JSON string
                    likely_swapped INTEGER DEFAULT 0,
                    direction_mismatch INTEGER DEFAULT 0,
                    complementarity_score REAL,
                    ambiguity_count INTEGER,
                    ambiguity_percent REAL,
                    virus_type TEXT,
                    reference_used INTEGER DEFAULT 0,
                    processing_method TEXT,
                    sample_id TEXT,
                    target_sequence TEXT,
                    uploaded_by TEXT,
                    project_name TEXT,
                    notes TEXT
                );
                
                CREATE TABLE IF NOT EXISTS consensus_sequences (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    consensus_name TEXT NOT NULL,
                    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    consensus_sequence TEXT NOT NULL,
                    original_length INTEGER NOT NULL,
                    trimmed_length INTEGER NOT NULL,
                    group_name TEXT,
                    file_count INTEGER DEFAULT 1,
                    source_file_ids TEXT,  -- JSON array
                    sample_id TEXT,
                    target_sequence TEXT,
                    virus_type TEXT,
                    trim_method TEXT,
                    quality_threshold REAL,
                    uploaded_by TEXT,
                    project_name TEXT,
                    notes TEXT
                );
                
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    project_name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    created_by TEXT,
                    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS blast_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    consensus_id INTEGER NOT NULL,
                    blast_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    blast_mode TEXT CHECK(blast_mode IN ('viruses', 'all')) DEFAULT 'viruses',
                    database_used TEXT DEFAULT 'nt',
                    program TEXT DEFAULT 'blastn',
                    query_name TEXT,
                    query_length INTEGER,
                    total_hits INTEGER DEFAULT 0,
                    execution_time REAL,
                    status TEXT CHECK(status IN ('success', 'failed', 'no_hits')) DEFAULT 'success',
                    error_message TEXT,
                    FOREIGN KEY (consensus_id) REFERENCES consensus_sequences(id) ON DELETE CASCADE
                );
                
                CREATE TABLE IF NOT EXISTS blast_hits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    blast_result_id INTEGER NOT NULL,
                    hit_rank INTEGER NOT NULL,
                    accession TEXT,
                    title TEXT,
                    organism TEXT,
                    query_coverage REAL,
                    identity_percent REAL,
                    evalue REAL,
                    bit_score REAL,
                    align_length INTEGER,
                    query_from INTEGER,
                    query_to INTEGER,
                    hit_from INTEGER,
                    hit_to INTEGER,
                    gaps INTEGER,
                    FOREIGN KEY (blast_result_id) REFERENCES blast_results(id) ON DELETE CASCADE
                );
            ''')
        else:
            # MySQL table creation
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sequences (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    filename VARCHAR(255) NOT NULL,
                    upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    file_hash VARCHAR(64) UNIQUE,
                    sequence TEXT NOT NULL,
                    sequence_length INT NOT NULL,
                    group_name VARCHAR(255),
                    detected_direction ENUM('Forward', 'Reverse', 'Unknown') DEFAULT 'Unknown',
                    quality_score FLOAT,
                    avg_quality FLOAT,
                    min_quality FLOAT,
                    max_quality FLOAT,
                    overall_grade ENUM('Excellent', 'Good', 'Acceptable', 'Poor', 'Needs Work') DEFAULT 'Unknown',
                    grade_score INTEGER,
                    issues JSON,
                    likely_swapped INTEGER DEFAULT 0,
                    direction_mismatch INTEGER DEFAULT 0,
                    complementarity_score FLOAT,
                    ambiguity_count INTEGER,
                    ambiguity_percent FLOAT,
                    virus_type VARCHAR(100),
                    reference_used INTEGER DEFAULT 0,
                    processing_method VARCHAR(255),
                    sample_id VARCHAR(255),
                    target_sequence VARCHAR(255),
                    uploaded_by VARCHAR(100),
                    project_name VARCHAR(255),
                    notes TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS consensus_sequences (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    consensus_name VARCHAR(255) NOT NULL,
                    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    consensus_sequence TEXT NOT NULL,
                    original_length INT NOT NULL,
                    trimmed_length INT NOT NULL,
                    group_name VARCHAR(255),
                    file_count INT DEFAULT 1,
                    source_file_ids JSON,
                    sample_id VARCHAR(255),
                    target_sequence VARCHAR(255),
                    virus_type VARCHAR(100),
                    trim_method VARCHAR(255),
                    quality_threshold FLOAT,
                    uploaded_by VARCHAR(100),
                    project_name VARCHAR(255),
                    notes TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS projects (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    project_name VARCHAR(255) NOT NULL UNIQUE,
                    description TEXT,
                    created_by VARCHAR(100),
                    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blast_results (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    consensus_id INT NOT NULL,
                    blast_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    blast_mode ENUM('viruses', 'all') DEFAULT 'viruses',
                    database_used VARCHAR(100) DEFAULT 'nt',
                    program VARCHAR(50) DEFAULT 'blastn',
                    query_name VARCHAR(255),
                    query_length INT,
                    total_hits INT DEFAULT 0,
                    execution_time FLOAT,
                    status ENUM('success', 'failed', 'no_hits') DEFAULT 'success',
                    error_message TEXT,
                    FOREIGN KEY (consensus_id) REFERENCES consensus_sequences(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS blast_hits (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    blast_result_id INT NOT NULL,
                    hit_rank INT NOT NULL,
                    accession VARCHAR(100),
                    title TEXT,
                    organism VARCHAR(255),
                    query_coverage FLOAT,
                    identity_percent FLOAT,
                    evalue DOUBLE,
                    bit_score FLOAT,
                    align_length INT,
                    query_from INT,
                    query_to INT,
                    hit_from INT,
                    hit_to INT,
                    gaps INT,
                    FOREIGN KEY (blast_result_id) REFERENCES blast_results(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            ''')
        
        conn.commit()
        print("✅ Manual table creation completed successfully!")
        
        try:
            pass
        except Exception as e:
            print(f"❌ Error during manual table creation: {e}")
            return False
        finally:
            if conn:
                conn.close()
        
        return True
    
    def _calculate_hash(self, sequence: str) -> str:
        """Calculate MD5 hash of sequence"""
        return hashlib.md5(sequence.encode()).hexdigest()
    
    def save_sequence(self, seq_data: Dict[str, Any]) -> int:
        """
        Save a sequence to database
        
        Args:
            seq_data: Dictionary with sequence information
            
        Returns:
            int: Inserted sequence ID
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Calculate hash to prevent duplicates
        file_hash = self._calculate_hash(seq_data.get('sequence', ''))
        
        # Check for duplicate sequence
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        check_query = f"SELECT id FROM sequences WHERE file_hash = {placeholder}"
        cursor.execute(check_query, (file_hash,))
        existing = cursor.fetchone()
        
        if existing:
            print(f"[DUPLICATE] Sequence already exists with ID: {existing[0]}")
            return None  # Return None to indicate duplicate was skipped
        
        # Prepare issues as JSON
        issues = seq_data.get('issues', [])
        if self.connection_type == 'sqlite':
            issues_json = json.dumps(issues)
        else:
            # For MySQL, pass list directly and it will handle JSON
            issues_json = json.dumps(issues)
            
        # Lookup internal sample ID if sample_id is provided
        db_sample_id = None
        user_sample_id = seq_data.get('sample_id')
        if user_sample_id:
            db_sample = self.sample_manager.find_sample_by_any_id(user_sample_id)
            if db_sample:
                db_sample_id = db_sample.get('sample_id') or db_sample.get('id')
                print(f"[LINK] Linked sequence to internal sample ID: {db_sample_id}")
        
        query = f'''
            INSERT INTO sequences (
                filename, file_hash, sequence, sequence_length,
                group_name, detected_direction, quality_score,
                avg_quality, min_quality, max_quality,
                overall_grade, grade_score, issues,
                likely_swapped, direction_mismatch, complementarity_score,
                ambiguity_count, ambiguity_percent, virus_type,
                reference_used, processing_method, sample_id, db_sample_id,
                target_sequence, uploaded_by, project_name, notes
            ) VALUES (
                {placeholder}, {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder}, {placeholder}
            )
        '''
        
        values = (
            seq_data.get('filename'),
            file_hash,
            seq_data.get('sequence'),
            seq_data.get('sequence_length'),
            seq_data.get('group'),
            seq_data.get('detected_direction', 'Unknown'),
            seq_data.get('quality_score'),
            seq_data.get('avg_quality'),
            seq_data.get('min_quality'),
            seq_data.get('max_quality'),
            seq_data.get('overall_grade', 'Unknown'),
            seq_data.get('grade_score'),
            issues_json,
            seq_data.get('likely_swapped', False),
            seq_data.get('direction_mismatch', False),
            seq_data.get('complementarity_score'),
            seq_data.get('ambiguity_count'),
            seq_data.get('ambiguity_percent'),
            seq_data.get('virus_type'),
            seq_data.get('reference_used', False),
            seq_data.get('processing_method'),
            user_sample_id,
            db_sample_id,
            seq_data.get('target_sequence'),
            seq_data.get('uploaded_by'),
            seq_data.get('project_name'),
            seq_data.get('notes')
        )
        
        try:
            cursor.execute(query, values)
            conn.commit()
            sequence_id = cursor.lastrowid
            
            # Register sample-virus relationship if sample_id and virus_type are present
            sample_id = seq_data.get('sample_id')
            virus_type = seq_data.get('virus_type')
            if sample_id and virus_type:
                try:
                    self.sample_manager.register_sample_virus(sample_id, virus_type)
                    print(f"[SAMPLE] Registered sample-virus relationship: {sample_id} -> {virus_type}")
                except Exception as e:
                    print(f"[WARNING] Failed to register sample-virus relationship: {e}")
            
            return sequence_id
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Failed to save sequence: {e}")
            raise
    
    def save_consensus(self, consensus_data: Dict[str, Any], source_seq_ids: List[int] = None) -> int:
        """
        Save a consensus sequence to database
        
        Args:
            consensus_data: Dictionary with consensus information
            source_seq_ids: List of sequence IDs used to create this consensus
            
        Returns:
            int: Inserted consensus ID
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        source_ids = source_seq_ids or []
        if self.connection_type == 'sqlite':
            source_ids_json = json.dumps(source_ids)
        else:
            # For MySQL, pass list as JSON string
            source_ids_json = json.dumps(source_ids)
        
        # Use correct placeholder for database type
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        
        # Check for duplicate consensus sequence
        consensus_name = consensus_data.get('name') or consensus_data.get('group')
        check_query = f"SELECT id FROM consensus_sequences WHERE consensus_name = {placeholder}"
        cursor.execute(check_query, (consensus_name,))
        existing = cursor.fetchone()
        
        if existing:
            print(f"[DUPLICATE] Consensus already exists with ID: {existing[0]}")
            return existing[0]  # Return existing ID so we can attach BLAST results
        
        # Lookup internal sample ID if sample_id is provided
        db_sample_id = None
        user_sample_id = consensus_data.get('sample_id')
        if user_sample_id:
            db_sample = self.sample_manager.find_sample_by_any_id(user_sample_id)
            if db_sample:
                db_sample_id = db_sample.get('sample_id') or db_sample.get('id')
                print(f"[LINK] Linked consensus to internal sample ID: {db_sample_id}")
        
        query = f'''
            INSERT INTO consensus_sequences (
                consensus_name, consensus_sequence, original_length,
                trimmed_length, group_name, file_count,
                source_file_ids, sample_id, db_sample_id, target_sequence,
                virus_type, trim_method, quality_threshold,
                uploaded_by, project_name, notes
            ) VALUES (
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder}
            )
        '''
        
        values = (
            consensus_data.get('name') or consensus_data.get('group'),
            consensus_data.get('consensus'),
            consensus_data.get('original_length'),
            consensus_data.get('trimmed_length'),
            consensus_data.get('group'),
            consensus_data.get('file_count', 1),
            source_ids_json,
            user_sample_id,
            db_sample_id,
            consensus_data.get('target_sequence'),
            consensus_data.get('virus_type'),
            consensus_data.get('trim_method'),
            consensus_data.get('quality_threshold'),
            consensus_data.get('uploaded_by'),
            consensus_data.get('project_name'),
            consensus_data.get('notes')
        )
        
        try:
            cursor.execute(query, values)
            conn.commit()
            consensus_id = cursor.lastrowid
            
            # Register sample-virus relationship if sample_id and virus_type are present
            sample_id = consensus_data.get('sample_id')
            virus_type = consensus_data.get('virus_type')
            if sample_id and virus_type:
                try:
                    self.sample_manager.register_sample_virus(sample_id, virus_type)
                    print(f"[SAMPLE] Registered sample-virus relationship for consensus: {sample_id} -> {virus_type}")
                except Exception as e:
                    print(f"[WARNING] Failed to register sample-virus relationship for consensus: {e}")
            
            return consensus_id
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Failed to save consensus: {e}")
            raise
    
    def save_blast_results(self, consensus_id: int, blast_data: Dict[str, Any]) -> int:
        """
        Save BLAST results for a consensus sequence.
        Deletes existing BLAST results for this consensus_id before inserting new ones.
        
        Args:
            consensus_id: ID of consensus sequence that was BLASTed
            blast_data: Dictionary with BLAST result information
            
        Returns:
            int: Inserted blast_result ID
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Delete existing BLAST results for this consensus to prevent duplicates
        try:
            if self.connection_type == 'sqlite':
                # Delete hits first (foreign key constraint)
                cursor.execute("""
                    DELETE FROM blast_hits 
                    WHERE blast_result_id IN (
                        SELECT id FROM blast_results WHERE consensus_id = ?
                    )
                """, (consensus_id,))
                # Delete blast results
                cursor.execute("DELETE FROM blast_results WHERE consensus_id = ?", (consensus_id,))
            else:
                # For MySQL, use CASCADE delete
                cursor.execute("DELETE FROM blast_results WHERE consensus_id = %s", (consensus_id,))
        except Exception as e:
            print(f"[ERROR] Failed to delete existing BLAST results: {e}")
        
        # Insert new BLAST results
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        
        query = f'''
            INSERT INTO blast_results (
                consensus_id, blast_date, blast_mode,
                database_used, program, query_name,
                query_length, total_hits, execution_time,
                status, error_message
            ) VALUES (
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}, {placeholder},
                {placeholder}, {placeholder}
            )
        '''
        
        values = (
            consensus_id,
            datetime.now(),
            blast_data.get('blast_mode', 'viruses'),
            blast_data.get('database_used', 'nt'),
            blast_data.get('program', 'blastn'),
            blast_data.get('query_name'),
            blast_data.get('query_length'),
            blast_data.get('total_hits', 0),
            blast_data.get('execution_time'),
            blast_data.get('status', 'success'),
            blast_data.get('error_message')
        )
        
        try:
            cursor.execute(query, values)
            conn.commit()
            blast_result_id = cursor.lastrowid
            
            # Insert BLAST hits
            hits = blast_data.get('hits', [])
            for hit in hits:
                hit_query = f'''
                    INSERT INTO blast_hits (
                        blast_result_id, hit_rank, accession,
                        title, organism, query_coverage,
                        identity_percent, evalue, bit_score,
                        align_length, query_from, query_to,
                        hit_from, hit_to, gaps
                    ) VALUES (
                        {placeholder}, {placeholder}, {placeholder},
                        {placeholder}, {placeholder}, {placeholder},
                        {placeholder}, {placeholder}, {placeholder},
                        {placeholder}, {placeholder}, {placeholder},
                        {placeholder}, {placeholder}, {placeholder}
                    )
                '''
                
                hit_values = (
                    blast_result_id,
                    hit.get('hit_rank'),
                    hit.get('accession'),
                    hit.get('title'),
                    hit.get('organism'),
                    hit.get('query_coverage'),
                    hit.get('identity_percent'),
                    hit.get('evalue'),
                    hit.get('bit_score'),
                    hit.get('align_length'),
                    hit.get('query_from'),
                    hit.get('query_to'),
                    hit.get('hit_from'),
                    hit.get('hit_to'),
                    hit.get('gaps')
                )
                
                cursor.execute(hit_query, hit_values)
            
            conn.commit()
            print(f"[BLAST] Saved {len(hits)} hits for consensus ID {consensus_id}")
            
            return blast_result_id
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Failed to save BLAST results: {e}")
            raise
    
    def get_sequences_by_group(self, group_name: str) -> List[Dict[str, Any]]:
        """Get all sequences for a specific group"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        query = f"SELECT * FROM sequences WHERE group_name = {placeholder} ORDER BY filename"
        cursor.execute(query, (group_name,))
        
        sequences = []
        for row in cursor.fetchall():
            sequences.append({
                'id': row[0],
                'filename': row[1],
                'sequence': row[2],
                'sequence_length': row[3],
                'detected_direction': row[4],
                'quality_score': row[5],
                'overall_grade': row[6],
                'virus_type': row[7],
                'upload_date': row[8]
            })
        
        conn.close()
        return sequences
    
    def get_consensus_by_group(self, group_name: str) -> Dict[str, Any]:
        """Get consensus sequence for a specific group"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        query = f"SELECT * FROM consensus_sequences WHERE group_name = {placeholder}"
        cursor.execute(query, (group_name,))
        
        result = cursor.fetchone()
        if result:
            consensus = {
                'id': result[0],
                'consensus_name': result[1],
                'consensus_sequence': result[2],
                'original_length': result[3],
                'trimmed_length': result[4],
                'group_name': result[5],
                'virus_type': result[6],
                'created_date': result[7]
            }
        else:
            consensus = None
        
        conn.close()
        return consensus
    
    def get_blast_results(self, consensus_id: int) -> List[Dict[str, Any]]:
        """Get BLAST results for a consensus sequence"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        query = f"""
            SELECT br.*, bh.*
            FROM blast_results br
            LEFT JOIN blast_hits bh ON br.id = bh.blast_result_id
            WHERE br.consensus_id = {placeholder}
            ORDER BY bh.hit_rank
        """
        cursor.execute(query, (consensus_id,))
        
        results = []
        current_result = None
        
        for row in cursor.fetchall():
            if current_result is None or row[0] != current_result['id']:
                current_result = {
                    'id': row[0],
                    'blast_date': row[1],
                    'blast_mode': row[2],
                    'database_used': row[3],
                    'program': row[4],
                    'query_name': row[5],
                    'query_length': row[6],
                    'total_hits': row[7],
                    'execution_time': row[8],
                    'status': row[9],
                    'error_message': row[10],
                    'hits': []
                }
                results.append(current_result)
            
            # Add hit to current result
            current_result['hits'].append({
                'hit_rank': row[11],
                'accession': row[12],
                'title': row[13],
                'organism': row[14],
                'query_coverage': row[15],
                'identity_percent': row[16],
                'evalue': row[17],
                'bit_score': row[18],
                'align_length': row[19],
                'query_from': row[20],
                'query_to': row[21],
                'hit_from': row[22],
                'hit_to': row[23],
                'gaps': row[24]
            })
        
        conn.close()
        return results
    
    def delete_sequence(self, sequence_id: int) -> bool:
        """Delete a sequence from database"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Use unified delete_records to ensure Recycle Bin integration
            DatabaseManager.delete_records(conn, 'sequences', f"id = {sequence_id}")
            print(f"[INFO] Deleted sequence ID {sequence_id} and moved to RecycleBin")
            return True
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Failed to delete sequence: {e}")
            return False
        finally:
            conn.close()
    
    def delete_consensus(self, consensus_id: int) -> bool:
        """Delete a consensus sequence from database"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Use unified delete_records to ensure Recycle Bin integration
            DatabaseManager.delete_records(conn, 'consensus_sequences', f"id = {consensus_id}")
            print(f"[INFO] Deleted consensus ID {consensus_id} and moved to RecycleBin")
            return True
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Failed to delete consensus: {e}")
            return False
        finally:
            conn.close()