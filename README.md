# Apache Log ETL – Phase 1

This project implements a simple ETL pipeline for Apache access logs:

1. **Extract & Transform**: Parse raw Apache combined logs with a regex-based parser.
2. **Load**: Store valid entries in a SQLite database (`logs` table) and malformed lines in an `errors` table.
3. **Summarise**: Generate a JSON summary of:
   - Top 10 endpoints by hit count (+ total bytes)
   - Status code distribution
   - Top client IPs by hit count (+ total bytes)

Everything runs in a single CLI command.

---

## Project Structure

```text
.
├── etl_apache.py      # CLI entrypoint – runs full ETL + summary
├── parser.py          # Apache combined log parser + validation + signature hash
├── database.py        # SQLite init + inserts + basic health checks
├── summarizer.py      # Aggregate queries for top endpoints, status codes, and IPs
├── data/
│   └── logs/
│       └── apache_logs.txt   # Sample Apache log file (input)
└── db/
    └── logs.db        # SQLite DB (created at runtime, ignored in git)

## Required libraries:
# This project uses only the Python standard library.
# No external dependencies required.
# Python 3.10+ recommended.
