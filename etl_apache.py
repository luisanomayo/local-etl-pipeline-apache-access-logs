import json
import argparse
from pathlib import Path
from typing import Any, Dict, List


from parser import parse_log_file
from database import init_db, insert_logs, insert_errors, get_run_counts
from summarizer import build_summary


#execute etl and show sample for visual inspection
def run_etl(log_file: Path, db_path: Path, sample_size: int = 3,
            print_samples: bool = True, print_json_summary: bool = True
):
    """
    Run the ETL pipeline: parse log file, insert into DB, and print samples + summary.
    
    Args:
    - log_file: Path to the Apache log file.
    - db_path: Path to the SQLite database file.
    - sample_size: Number of sample log entries to print.
    - print_samples: Whether to print sample log entries.
    - print_json_summary: Whether to print summary in JSON format.
    
    Returns:
    summary: Dictionary containing ETL summary statistics.
    """
    
    print(f"Input Apache log file: {log_file}")
    
    #1. initialize database
    print("\n Initializing database:")
    init_db(db_path)
    
    
    #2. parse log file
    print("\n Parsing Apache log file:")
    parsed_logs, errors = parse_log_file(log_file)
    print(f"Parsed {len(parsed_logs)} valid log entries.")
    print(f"Found {len(errors)} error entries.")
    
    if print_samples:
        if parsed_logs:
            print(f"\n Sample parsed logs:")
            for row in parsed_logs[:sample_size]:
                print(row)
        if errors:
            print(f"\n Sample error logs:")
            for row in errors[:sample_size]:
                print(f"{row['error_reason']} --> {row['raw_line'][:50]}")
    
    #3. insert into database
    print("\n Inserting parsed logs into database:")
    inserted_logs = insert_logs(db_path, parsed_logs)
    
    print(f"\n Inserting error logs into database:")
    inserted_errors = insert_errors(db_path, errors)
    
    #3 health check counts
    logs_count, distinct_hash_count, errors_count = get_run_counts(db_path)
    print(f"\n Database health check:")
    print(f"New inserts count: {logs_count}")
    print(f"Distinct signature hashes in DB: {distinct_hash_count}")
    print(f"Total error entries in DB: {errors_count}")
    
    #4. print summary
    summary = build_summary(db_path)
    if print_json_summary:
        print("\nETL Summary (JSON):")
        print(json.dumps(summary, indent=4))
    
    
    return summary


#CLI tooling entrypoint
def main():
    parser = argparse.ArgumentParser(
        description="Apache log ETL: parse, load into SQLite, and summarise."
    )

    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to the Apache log file (e.g. data/logs/apache_logs.txt)",
    )

    parser.add_argument(
        "--db",
        "-d",
        default="db/logs.db",
        help="Path to the SQLite database file (default: db/logs.db)",
    )

    parser.add_argument(
        "--no-samples",
        action="store_true",
        help="Disable printing sample parsed/error rows.",
    )

    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Disable printing the JSON summary (still runs ETL).",
    )

    args = parser.parse_args()

    log_file = Path(args.input)
    db_path = Path(args.db)

    if not log_file.exists():
        raise FileNotFoundError(f"Log file does not exist: {log_file}")

    run_etl(
        log_file=log_file,
        db_path=db_path,
        sample_size=3,
        print_samples=not args.no_samples,
        print_json_summary=not args.no_summary,
    )


if __name__ == "__main__":
    main()