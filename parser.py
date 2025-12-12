
#--import libraries --

import re
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional


#A.regex pattern for apache combined log format
LOG_PATTERN = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ '                          # IP and the two "-" fields
    r'\[(?P<timestamp>[^\]]+)\] '                     # [17/May/2015:10:05:03 +0000]
    r'"(?P<method>\S+) (?P<path>\S+) (?P<protocol>\S+)" '  # "GET /path HTTP/1.1"
    r'(?P<status>\d{3}) '                             # 200
    r'(?P<bytes>\S+) '                                # 203023 or '-'
    r'"(?P<referrer>[^"]*)" '                         # "http://... or "-" 
    r'"(?P<user_agent>[^"]*)"'                        # "Mozilla/5.0 ..."
)
#B. function to parse a single log line
def parse_log_line(raw_line:str):
    """ parse a single apache combined log line 
    into a dictionary.
    Returns:
    parsed_dict: Dict with parsed fields or None if invalid line.
    """
    #1. handle whitespace only line
    if not raw_line.strip():
        return None, "Empty or whitespace line"
    
    #2. remove trailing newline
    raw_line = raw_line.rstrip('\n')
    
    #3. apply regex pattern
    match = LOG_PATTERN.match(raw_line)
    if not match:
        return None, "Line does not match Apache combined log format"
    
    data = match.groupdict()
    
    #4. validate and transform fields
        #status
    try:
        status = int(data['status'])    
    except ValueError:
        return None, f"Invalid status code: {data['status']}"
    if not (100 <= status <= 599):
        return None, f"Status code out of range: {status}"
    data['status'] = status
    
        #bytes
    raw_bytes = data['bytes']
    if raw_bytes == '-':
        data['bytes'] = 0
    else:
        try:
            byte_count = int(raw_bytes)
        except ValueError:
            return None, f"Invalid byte value: {raw_bytes}"
            
        #timestamp
    raw_timestamp = data['timestamp']
    try:
        dt = datetime.strptime(raw_timestamp, '%d/%b/%Y:%H:%M:%S %z')
        data['timestamp'] = dt.isoformat()
    except ValueError:
        return None, f"Invalid timestamp format: {raw_timestamp}"
    
        #create signature hash for deduplication
    hash_input = f"{data['ip']}|{data['timestamp']}|{data['path']}"
    data['signature_hash'] = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()
        
    return data, None

#C. apply parse function to multiple lines
def parse_log_file(log_path: Path):
    """
    parse all the lines from a log file.
    
    Args:
    log_path: Path to the log file.
    
    Returns:
    parsed_logs: List of parsed log dictionaries, to be inserted into DB.
    errors: List of error messages for invalid logs.
    """
    
    parsed_logs = []
    errors = []
    
    with log_path.open('r', encoding='utf-8') as log_file:
        for line in log_file:
            parsed,error = parse_log_line(line)
            if error:
                errors.append(
                    {
                        "raw_line": line.rstrip('\n'),
                        "error_reason": error
                    }
                )
            else:
                parsed_logs.append(parsed)
                
    return parsed_logs, errors
