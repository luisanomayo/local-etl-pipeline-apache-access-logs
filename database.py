
#--import libraries--
import sqlite3
from pathlib import Path
from typing import Iterable, Mapping, Any, Tuple


#A. CREATE TABLE STATEMENT
CREATE_LOGS_TABLE = """
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ip TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    protocol TEXT NOT NULL,
    status INTEGER NOT NULL CHECK (status BETWEEN 100 AND 599),
    bytes INTEGER NOT NULL,
    referrer TEXT NOT NULL,
    user_agent TEXT NOT NULL,
    signature_hash TEXT NOT NULL UNIQUE
);
"""

CREATE_ERRORS_TABLE = """
CREATE TABLE IF NOT EXISTS errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_line TEXT NOT NULL UNIQUE,
    error_reason TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

# CREATE OR INITIATE DATABASE
def init_db(db_path: Path):
    """Initialize the SQLite database and ensure tables exist."""
    
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(CREATE_LOGS_TABLE)
        cursor.execute(CREATE_ERRORS_TABLE)
        conn.commit()
        
def insert_logs(db_path: Path, logs: Iterable[Mapping[str, Any]]):
    """
    Inserts parsed log entries into the logs table, skipping duplicates,
    and logs errors into the errors table.
    
    Args:
    - db_path: Path to the SQLite database file.
    - logs: Iterable of processed log dictionaries to insert.
    
    Returns:
    number of rows inserted into logs table.
    """
    log_tuples = [
        (
            log['ip'],
            log['timestamp'],
            log['method'],
            log['path'],
            log['protocol'],
            log['status'],
            log['bytes'],
            log['referrer'],
            log['user_agent'],
            log['signature_hash']
        )
        for log in logs if 'error' not in log
    ]
    
    if not log_tuples:
        return 0  # No valid logs to insert
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        #count before insert
        cursor.execute("SELECT COUNT(*) FROM logs;")
        before_count = cursor.fetchone()[0]
        
        #insert logs, ignoring duplicates based on signature_hash
        cursor.executemany(
            """
            INSERT OR IGNORE INTO logs (
                ip, timestamp, method, path, protocol,
                status, bytes, referrer, user_agent, signature_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            log_tuples
        )
        conn.commit()
        
        #count after insert
        cursor.execute("SELECT COUNT(*) FROM logs;")
        after_count = cursor.fetchone()[0]
        
        return after_count - before_count  # number of new rows inserted

#insert errors
def insert_errors(db_path: Path, errors: Iterable[Mapping[str, str]]):
    """
    Insert error rows into the errors table
    Args:
    - db_path: Path to the SQLite database file.
    - errors: Iterable of error dictionaries to insert.
    
    Returns:
    number of rows inserted into errors table.
    """
    
    error_tuples = [
        (err['raw_line'], err['error_reason'])
        for err in errors
    ]
    
    if not error_tuples:
        return 0  # No errors to insert
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT INTO errors(
                raw_line,
                error_reason
                ) VALUES (?, ?);
                """, error_tuples
        )
        conn.commit()
        return cursor.rowcount
    
    
    #run samples
def get_run_counts(db_path: Path):
    """
    Health check to inspect DB contents.
        
    Args:
    - db_path: Path to the SQLite database file.
        
    Returns:
    (logs_count, distinct_signature_hash_count, errors_count)
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
            
        #insert count
        cursor.execute("SELECT COUNT(*) FROM logs;")
        logs_count = cursor.fetchone()[0]
            
            
        #distinct signature hash count
        cursor.execute("SELECT COUNT(DISTINCT signature_hash) FROM logs;")
        deduped_count = cursor.fetchone()[0]
            
        #errors count
        cursor.execute("SELECT COUNT(*) FROM errors;")
        errors_count = cursor.fetchone()[0]
            
    return logs_count, deduped_count, errors_count