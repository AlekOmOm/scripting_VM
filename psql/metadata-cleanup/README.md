# postgres metadata cleanup & backup pipeline

automated archival system maintaining ≤7d data in main database with 7d backup buffer.

## overview

```
temporal windows:
  recent   (0-7d)    → main db only
  backup   (7-14d)   → archive db only  
  obsolete (>14d)    → purged

flow:
  main db (postgres.backtest.*) → archive db (postgres_archive.backtest.*) → purge
```

## architecture

**components**
- main db: `postgres` (port 5432) - primary operational data
- archive db: `postgres_archive` - historical backup storage
- pipeline: `/usr/local/bin/main.sh` - orchestrator script

**schema structure**
```sql
backtest.metadata (hash PK, created, ...)
backtest.signals  (FK hash ON DELETE CASCADE)
backtest.fills    (FK hash ON DELETE CASCADE)
```

## functional requirements

- **FR-1** verify archive database connectivity before execution
- **FR-2** detect duplicate archival to ensure idempotency  
- **FR-3** copy backup window data (metadata + dependent tables)
- **FR-4** validate row counts before main database cleanup
- **FR-5** delete validated rows using FK cascade cleanup
- **FR-6** purge obsolete rows (>14d) from archive
- **FR-7** structured logging + prometheus metrics
- **FR-8** rollback on failures, non-zero exit codes

## installation

```bash
# as root
sudo ./install.sh

# creates:
#   /usr/local/bin/main.sh (executable)
#   postgres_archive database + schema
#   cron job: monday 02:30 Europe/Stockholm
#   /var/log/postgres-cleanup.log
```

## usage

**manual execution**
```bash
# test run (as postgres user)
sudo -u postgres /usr/local/bin/main.sh

# check logs
tail -f /var/log/postgres-cleanup.log
```

**cron schedule**
- runs weekly: monday 02:30 Europe/Stockholm
- timezone-aware (handles DST transitions)
- output redirected to syslog + log file

## testing

```bash
# run comprehensive test suite
./test.sh

# creates isolated test databases
# validates all pipeline operations
# verifies data integrity + FK cascades
```

## monitoring

**prometheus metrics** (if node_exporter available)
```
postgres_cleanup_status{} 0|1           # success/failure
postgres_cleanup_runtime_seconds{}       # execution time
postgres_cleanup_last_run_timestamp{}    # unix timestamp
```

**log analysis**
```bash
# recent executions
grep "pipeline completed" /var/log/postgres-cleanup.log | tail -5

# failure analysis  
grep "FATAL\|ERROR" /var/log/postgres-cleanup.log
```

## constraints

- **runtime**: <5 minutes @ 100M rows
- **memory**: ≤1GB RAM limit (ulimit enforced)
- **idempotent**: safe to re-run same time window
- **credentials**: interactive prompt (avoid .pgpass)

## failure handling

| failure point | effect | recovery |
|---------------|---------|----------|
| archive db unreachable | abort before changes | manual intervention |
| row count mismatch | abort, preserve main data | investigate + retry |
| delete failure | partial state possible | manual cleanup required |
| purge failure | archive growth | continues, retry next run |

## directory structure

```
psql/metadata-cleanup/
├── main.sh      # primary orchestrator script
├── install.sh   # deployment automation  
├── test.sh      # validation suite
└── README.md    # this file
```

## implementation notes

**copy mechanism**: `pg_dump --data-only | psql` pipeline
- avoids intermediate files
- preserves data types + constraints
- efficient for large datasets

**validation strategy**: `COUNT(*)` verification
- pre-archive: source row counts
- post-archive: target row counts  
- pre-delete: main database state
- post-delete: cleanup verification

**dependency handling**: FK cascade deletion
- delete only metadata rows
- signals + fills removed automatically
- maintains referential integrity

## troubleshooting

**common issues**

1. **archive database missing**
   ```bash
   # recreate via install script
   sudo ./install.sh
   ```

2. **permission errors**
   ```bash
   # check postgres user permissions
   sudo -u postgres psql -c "SELECT current_user;"
   ```

3. **disk space constraints**
   ```bash
   # monitor database sizes
   psql -c "SELECT pg_database_size('postgres_archive');"
   ```

4. **timezone configuration**
   ```bash
   # verify system timezone
   timedatectl status
   ```

**debugging workflow**
1. check prerequisites: `psql`, `pg_dump`, database connectivity
2. validate archive database schema matches main
3. examine recent log entries for specific error context
4. test with smaller time windows if needed
5. verify cron job configuration + permissions

## version history

- **0.4** (2025-07-22): initial implementation
  - temporal window archival
  - comprehensive validation + error handling
  - prometheus metrics integration
  - automated installation + testing