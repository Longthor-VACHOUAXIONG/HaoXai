"""
Sample Manager Module
Handles sample-virus relationships and sample management operations
"""
from typing import Dict, List, Any, Optional, Tuple
from database.db_manager import DatabaseManager
import json
from datetime import datetime


class SampleManager:
    """Manager for sample-virus relationships and sample operations"""
    
    def __init__(self, db_connection, connection_type='mysql'):
        """
        Initialize the sample manager
        
        Args:
            db_connection: Database connection info
            connection_type: 'sqlite' or 'mysql'
        """
        self.connection_type = connection_type
        self.db_connection = db_connection
        self._initialize_sample_tables()
    
    def _get_connection(self):
        """Get a thread-safe database connection"""
        return DatabaseManager.get_connection(self.db_connection, self.connection_type)
    
    def _initialize_sample_tables(self):
        """Initialize sample management tables if they don't exist"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Create sample_viruses table to track sample-virus relationships
        if self.connection_type == 'sqlite':
            cursor.executescript('''
                CREATE TABLE IF NOT EXISTS sample_viruses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sample_id TEXT NOT NULL,
                    virus_type TEXT NOT NULL,
                    first_detected DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    sequence_count INTEGER DEFAULT 0,
                    consensus_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'Active',
                    notes TEXT,
                    
                    UNIQUE(sample_id, virus_type)
                );
                
                CREATE TABLE IF NOT EXISTS sample_summary (
                    sample_id TEXT PRIMARY KEY,
                    total_sequences INTEGER DEFAULT 0,
                    total_consensus INTEGER DEFAULT 0,
                    virus_types TEXT,  -- JSON string for SQLite
                    first_detected DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'Active',
                    notes TEXT
                );
                
                CREATE INDEX IF NOT EXISTS idx_sample_viruses_sample_id ON sample_viruses(sample_id);
                CREATE INDEX IF NOT EXISTS idx_sample_viruses_virus_type ON sample_viruses(virus_type);
                CREATE INDEX IF NOT EXISTS idx_sample_viruses_status ON sample_viruses(status);
                CREATE INDEX IF NOT EXISTS idx_sample_summary_status ON sample_summary(status);
                CREATE INDEX IF NOT EXISTS idx_sample_summary_last_updated ON sample_summary(last_updated);
            ''')
        else:
            # MySQL version
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sample_viruses (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    sample_id VARCHAR(200) NOT NULL,
                    virus_type VARCHAR(100) NOT NULL,
                    first_detected DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    sequence_count INT DEFAULT 0,
                    consensus_count INT DEFAULT 0,
                    status VARCHAR(50) DEFAULT 'Active',
                    notes TEXT,
                    
                    UNIQUE KEY unique_sample_virus (sample_id, virus_type),
                    INDEX idx_sample_id (sample_id),
                    INDEX idx_virus_type (virus_type),
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sample_summary (
                    sample_id VARCHAR(200) PRIMARY KEY,
                    total_sequences INT DEFAULT 0,
                    total_consensus INT DEFAULT 0,
                    virus_types JSON,
                    first_detected DATETIME DEFAULT CURRENT_TIMESTAMP,
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    status VARCHAR(50) DEFAULT 'Active',
                    notes TEXT,
                    
                    INDEX idx_status (status),
                    INDEX idx_last_updated (last_updated)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            ''')
        
        conn.commit()
        print("[INFO] Sample management tables initialized")
    
    def register_sample_virus(self, sample_id: str, virus_type: str, notes: str = None) -> int:
        """
        Register a virus type for a sample
        
        Args:
            sample_id: Sample identifier
            virus_type: Virus type detected
            notes: Optional notes
            
        Returns:
            int: ID of the sample_virus record
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        
        # Try to insert new record
        query = f'''
            INSERT INTO sample_viruses (sample_id, virus_type, notes)
            VALUES ({placeholder}, {placeholder}, {placeholder})
        '''
        
        try:
            cursor.execute(query, (sample_id, virus_type, notes))
            conn.commit()
            record_id = cursor.lastrowid
            print(f"[SAMPLE] Registered new sample-virus: {sample_id} -> {virus_type}")
            
            # Update sample summary
            self._update_sample_summary(sample_id)
            
            return record_id
            
        except Exception as e:
            if 'UNIQUE' in str(e) or 'duplicate' in str(e).lower():
                # Record already exists, update it
                update_query = f'''
                    UPDATE sample_viruses 
                    SET last_updated = CURRENT_TIMESTAMP, 
                        sequence_count = (
                            SELECT COUNT(*) FROM sequences 
                            WHERE sample_id = {placeholder} AND virus_type = {placeholder}
                        ),
                        notes = COALESCE({placeholder}, notes)
                    WHERE sample_id = {placeholder} AND virus_type = {placeholder}
                '''
                cursor.execute(update_query, (sample_id, virus_type, notes, sample_id, virus_type))
                conn.commit()
                
                # Get the existing record ID
                select_query = f"SELECT id FROM sample_viruses WHERE sample_id = {placeholder} AND virus_type = {placeholder}"
                cursor.execute(select_query, (sample_id, virus_type))
                result = cursor.fetchone()
                record_id = result[0] if result else None
                
                print(f"[SAMPLE] Updated existing sample-virus: {sample_id} -> {virus_type}")
                
                # Update sample summary
                self._update_sample_summary(sample_id)
                
                return record_id
            else:
                raise e
    
    def _update_sample_summary(self, sample_id: str):
        """Update the sample summary table"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        
        # Get virus types and counts for this sample
        virus_query = f'''
            SELECT virus_type, 
                   COUNT(*) as sequence_count,
                   COUNT(DISTINCT id) as unique_sequences
            FROM sequences 
            WHERE sample_id = {placeholder}
            GROUP BY virus_type
        '''
        cursor.execute(virus_query, (sample_id,))
        virus_data = cursor.fetchall()
        
        # Get consensus counts
        consensus_query = f'''
            SELECT virus_type, COUNT(*) as consensus_count
            FROM consensus_sequences 
            WHERE sample_id = {placeholder}
            GROUP BY virus_type
        '''
        cursor.execute(consensus_query, (sample_id,))
        consensus_data = cursor.fetchall()
        
        # Calculate totals
        total_sequences = sum(row[1] for row in virus_data)
        total_consensus = sum(row[1] for row in consensus_data)
        virus_types = list(set(row[0] for row in virus_data + consensus_data))
        
        # Prepare virus types JSON
        if self.connection_type == 'sqlite':
            virus_types_json = json.dumps(virus_types)
        else:
            virus_types_json = json.dumps(virus_types)
        
        # Update or insert sample summary
        upsert_query = f'''
            INSERT INTO sample_summary (sample_id, total_sequences, total_consensus, virus_types)
            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder})
            ON CONFLICT(sample_id) DO UPDATE SET
                total_sequences = excluded.total_sequences,
                total_consensus = excluded.total_consensus,
                virus_types = excluded.virus_types,
                last_updated = CURRENT_TIMESTAMP
        ''' if self.connection_type == 'sqlite' else f'''
            INSERT INTO sample_summary (sample_id, total_sequences, total_consensus, virus_types)
            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder})
            ON DUPLICATE KEY UPDATE
                total_sequences = VALUES(total_sequences),
                total_consensus = VALUES(total_consensus),
                virus_types = VALUES(virus_types),
                last_updated = CURRENT_TIMESTAMP
        '''
        
        cursor.execute(upsert_query, (sample_id, total_sequences, total_consensus, virus_types_json))
        conn.commit()
    
    def get_sample_viruses(self, sample_id: str) -> List[Dict[str, Any]]:
        """
        Get all virus types detected for a sample
        
        Args:
            sample_id: Sample identifier
            
        Returns:
            List of virus information for the sample
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        query = f'''
            SELECT sv.*, 
                   COUNT(s.id) as actual_sequence_count,
                   COUNT(cs.id) as actual_consensus_count
            FROM sample_viruses sv
            LEFT JOIN sequences s ON sv.sample_id = s.sample_id AND sv.virus_type = s.virus_type
            LEFT JOIN consensus_sequences cs ON sv.sample_id = cs.sample_id AND sv.virus_type = cs.virus_type
            WHERE sv.sample_id = {placeholder}
            GROUP BY sv.id
            ORDER BY sv.first_detected
        '''
        
        cursor.execute(query, (sample_id,))
        results = cursor.fetchall()
        
        # Convert to list of dictionaries
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in results]
    
    def get_samples_with_multiple_viruses(self) -> List[Dict[str, Any]]:
        """
        Get samples that have multiple virus types detected
        
        Returns:
            List of samples with multiple viruses
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        query = '''
            SELECT sample_id, 
                   COUNT(DISTINCT virus_type) as virus_count,
                   GROUP_CONCAT(DISTINCT virus_type) as virus_types,
                   SUM(sequence_count) as total_sequences,
                   SUM(consensus_count) as total_consensus
            FROM sample_viruses
            GROUP BY sample_id
            HAVING virus_count > 1
            ORDER BY virus_count DESC, sample_id
        ''' if self.connection_type == 'sqlite' else '''
            SELECT sample_id, 
                   COUNT(DISTINCT virus_type) as virus_count,
                   GROUP_CONCAT(DISTINCT virus_type SEPARATOR ', ') as virus_types,
                   SUM(sequence_count) as total_sequences,
                   SUM(consensus_count) as total_consensus
            FROM sample_viruses
            GROUP BY sample_id
            HAVING virus_count > 1
            ORDER BY virus_count DESC, sample_id
        '''
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in results]
    
    def get_sample_summary(self, sample_id: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive summary for a sample
        
        Args:
            sample_id: Sample identifier
            
        Returns:
            Sample summary or None if not found
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        query = f'SELECT * FROM sample_summary WHERE sample_id = {placeholder}'
        
        cursor.execute(query, (sample_id,))
        result = cursor.fetchone()
        
        if result:
            columns = [desc[0] for desc in cursor.description]
            summary = dict(zip(columns, result))
            
            # Parse virus types JSON
            if summary.get('virus_types'):
                if isinstance(summary['virus_types'], str):
                    summary['virus_types'] = json.loads(summary['virus_types'])
            
            # Get detailed virus information
            summary['viruses'] = self.get_sample_viruses(sample_id)
            
            return summary
        
        return None
    
    def search_samples_by_virus(self, virus_type: str) -> List[str]:
        """
        Get all samples that have a specific virus type
        
        Args:
            virus_type: Virus type to search for
            
        Returns:
            List of sample IDs
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        query = f'SELECT DISTINCT sample_id FROM sample_viruses WHERE virus_type = {placeholder} ORDER BY sample_id'
        
        cursor.execute(query, (virus_type,))
        results = cursor.fetchall()
        
        return [row[0] for row in results]
    
    def update_sequence_counts(self, sample_id: str, virus_type: str = None):
        """
        Update sequence counts for a sample (call after sequence operations)
        
        Args:
            sample_id: Sample identifier
            virus_type: Specific virus type to update (optional)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        
        if virus_type:
            # Update specific virus type
            query = f'''
                UPDATE sample_viruses 
                SET sequence_count = (
                    SELECT COUNT(*) FROM sequences 
                    WHERE sample_id = {placeholder} AND virus_type = {placeholder}
                ),
                consensus_count = (
                    SELECT COUNT(*) FROM consensus_sequences 
                    WHERE sample_id = {placeholder} AND virus_type = {placeholder}
                ),
                last_updated = CURRENT_TIMESTAMP
                WHERE sample_id = {placeholder} AND virus_type = {placeholder}
            '''
            cursor.execute(query, (sample_id, virus_type, sample_id, virus_type, sample_id, virus_type))
        else:
            # Update all virus types for this sample
            query = f'''
                UPDATE sample_viruses sv
                SET sequence_count = (
                    SELECT COUNT(*) FROM sequences s 
                    WHERE s.sample_id = sv.sample_id AND s.virus_type = sv.virus_type
                ),
                consensus_count = (
                    SELECT COUNT(*) FROM consensus_sequences cs 
                    WHERE cs.sample_id = sv.sample_id AND cs.virus_type = sv.virus_type
                ),
                last_updated = CURRENT_TIMESTAMP
                WHERE sv.sample_id = {placeholder}
            '''
            cursor.execute(query, (sample_id,))
        
        conn.commit()
        
        # Update sample summary
        self._update_sample_summary(sample_id)

    def find_sample_by_any_id(self, id_string: str) -> Optional[Dict[str, Any]]:
        """
        Find a sample record in the CAN2.samples table by searching through 
        multiple identifier columns (source_id, saliva_id, etc.)
        """
        if not id_string:
            return None
            
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # We need to search in the samples table which is in the CAN2 database
        # For MySQL/MariaDB this is fine if we are connected to CAN2
        # For SQLite this might be a separate database file if not unified
        
        id_cols = [
            'source_id', 'saliva_id', 'anal_id', 'urine_id', 'ecto_id',
            'blood_id', 'tissue_id', 'intestine_id', 'plasma_id', 'adipose_id'
        ]
        
        placeholder = '?' if self.connection_type == 'sqlite' else '%s'
        
        # Build a big OR query
        where_parts = [f"{col} = {placeholder}" for col in id_cols]
        query = f"SELECT * FROM samples WHERE {' OR '.join(where_parts)} LIMIT 1"
        
        # Provide the same ID string for every placeholder
        params = tuple([id_string] * len(id_cols))
        
        try:
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if result:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, result))
        except Exception as e:
            print(f"[DEBUG] Search for sample '{id_string}' failed (maybe table doesn't exist yet): {e}")
            
        return None