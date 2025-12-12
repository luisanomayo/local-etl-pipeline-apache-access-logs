import sqlite3
from pathlib import Path
from typing import Dict, List, Any

def top_endpoints(db_path: Path, limit: int = 10):
    """
    Retrieve the top requested endpoints from the logs database.
    
    Args:
    - db_path: Path to the SQLite database file.
    - limit: Number of top endpoints to retrieve.
    
    Returns:
    List of tuples containing (path, request_count).
    """
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT path, COUNT(*) as request_count, SUM (bytes) as total_bytes
            FROM logs
            GROUP BY path
            ORDER BY request_count DESC
            LIMIT ?;
            """, (limit,)
        )
        rows = cursor.fetchall()
        
    return [
        {
            "path": row[0],
            "hits":int(row[1]),
            "total_bytes":int(row[2]) if row[2] is not None else 0,
        }
        for row in rows
    ]
    
def status_distribution(db_path: Path):
    """Return count of each HTTP status code in the logs."""
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM logs
            GROUP BY status
            ORDER BY status;"""
        )
        rows = cursor.fetchall()
        
    return[{"status": int(row[0]),
            "count": int(row[1])} for row in rows]
    
    
    #top ips
def top_ips(db_path: Path, limit:int = 10):
    """Return top N client IPs by hit count + bytes."""
        
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT ip, COUNT(*) AS hit_count, SUM(bytes) AS total_bytes
            FROM logs
            GROUP BY ip
            ORDER BY hit_count DESC
            LIMIT ?;
            """, (limit,)
        )
        rows = cursor.fetchall()
        
        return [
            {
                "ip": row[0],
                "hits": int(row[1]),
                "total_bytes": int(row[2]) if row[2] is not None else 0,
            }
            for row in rows
        ]
        
def build_summary(db_path: Path):
    """Return a JSON-ready summary of all aggregates."""
    return {
        "top_endpoints": top_endpoints(db_path),
        "status_distribution": status_distribution(db_path),
        "top_ips": top_ips(db_path),
    }