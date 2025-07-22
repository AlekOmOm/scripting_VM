#!/bin/bash
#
# postgres metadata cleanup & backup pipeline
# version: 0.4 – 2025-07-22
#
# purpose: maintain ≤7d data in main db with 7d backup buffer
# schedule: monday 02:30 Europe/Stockholm (cron)
#

set -euo pipefail

# === CONFIGURATION ===
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly LOG_PREFIX="psql-cleanup"
readonly MAIN_DB_HOST="${PGHOST:-localhost}"
readonly MAIN_DB_PORT="${PGPORT:-5432}"
readonly MAIN_DB_NAME="postgres"
readonly ARCHIVE_DB_NAME="postgres_archive"
readonly SCHEMA="backtest"

# temporal windows (days)
readonly RECENT_DAYS=7
readonly BACKUP_DAYS=14
readonly PURGE_DAYS=14

# runtime constraints
readonly MAX_RUNTIME_MINUTES=5
readonly MAX_MEMORY_GB=1

# === LOGGING ===
log() {
    local level="$1"; shift
    echo "$(date -Iseconds) [$level] $LOG_PREFIX: $*" | tee -a /var/log/postgres-cleanup.log
    logger -t "$LOG_PREFIX" "$level: $*"
}

log_info() { log "INFO" "$@"; }
log_warn() { log "WARN" "$@"; }
log_error() { log "ERROR" "$@"; }
log_fatal() { log "FATAL" "$@"; exit 1; }

# === UTILITY FUNCTIONS ===
timeout_cmd() {
    timeout "${MAX_RUNTIME_MINUTES}m" "$@"
}

psql_main() {
    timeout_cmd psql -h "$MAIN_DB_HOST" -p "$MAIN_DB_PORT" -d "$MAIN_DB_NAME" -t -A "$@"
}

psql_archive() {
    timeout_cmd psql -h "$MAIN_DB_HOST" -p "$MAIN_DB_PORT" -d "$ARCHIVE_DB_NAME" -t -A "$@"
}

pg_dump_main() {
    timeout_cmd pg_dump -h "$MAIN_DB_HOST" -p "$MAIN_DB_PORT" -d "$MAIN_DB_NAME" "$@"
}

# get count of rows matching condition
get_row_count() {
    local db_func="$1"
    local table="$2" 
    local where_clause="$3"
    
    $db_func -c "SELECT COUNT(*) FROM ${SCHEMA}.${table} WHERE ${where_clause};" 2>/dev/null || echo "0"
}

# === VALIDATION FUNCTIONS ===
verify_backup_db() {
    log_info "verifying backup database connectivity"
    
    if ! psql_archive -c "SELECT 1;" >/dev/null 2>&1; then
        log_fatal "backup database $ARCHIVE_DB_NAME unreachable"
    fi
    
    # verify schema exists
    local schema_exists
    schema_exists=$(psql_archive -c "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '$SCHEMA';")
    if [[ "$schema_exists" != "1" ]]; then
        log_fatal "schema $SCHEMA does not exist in backup database"
    fi
    
    log_info "backup database verified"
}

check_slice_archived() {
    local backup_window_start="NOW() - INTERVAL '$BACKUP_DAYS days'"
    local backup_window_end="NOW() - INTERVAL '$RECENT_DAYS days'"
    
    log_info "checking if backup window slice already archived"
    
    local archived_count
    archived_count=$(get_row_count "psql_archive" "metadata" "created > $backup_window_start AND created <= $backup_window_end")
    
    if [[ "$archived_count" -gt 0 ]]; then
        log_info "slice already archived ($archived_count rows), skipping"
        return 0
    fi
    
    log_info "slice not archived, proceeding"
    return 1
}

# === CORE OPERATIONS ===
select_backup_hashes() {
    local backup_window_start="NOW() - INTERVAL '$BACKUP_DAYS days'"
    local backup_window_end="NOW() - INTERVAL '$RECENT_DAYS days'"
    
    log_info "selecting hashes in backup window"
    
    psql_main -c "
        SELECT hash 
        FROM ${SCHEMA}.metadata 
        WHERE created > $backup_window_start 
          AND created <= $backup_window_end
        ORDER BY hash;" > /tmp/backup_hashes.txt
    
    local hash_count
    hash_count=$(wc -l < /tmp/backup_hashes.txt)
    log_info "selected $hash_count hashes for backup"
    
    echo "$hash_count"
}

archive_slice() {
    local hash_count="$1"
    
    if [[ "$hash_count" -eq 0 ]]; then
        log_info "no rows to archive"
        return 0
    fi
    
    log_info "archiving $hash_count metadata rows and dependencies"
    
    # create hash list for WHERE IN clause
    local hash_list
    hash_list=$(tr '\n' ',' < /tmp/backup_hashes.txt | sed 's/,$//')
    
    if [[ -z "$hash_list" ]]; then
        log_error "empty hash list generated"
        return 1
    fi
    
    # archive each table in dependency order
    for table in metadata signals fills; do
        log_info "archiving table: $table"
        
        # dump data with hash filter
        if ! pg_dump_main \
            --data-only \
            --table="${SCHEMA}.${table}" \
            --where="hash IN ($hash_list)" | \
            psql_archive; then
            log_error "failed to archive table: $table"
            return 1
        fi
        
        log_info "archived table: $table"
    done
    
    log_info "archive operation completed"
    return 0
}

validate_archive() {
    local backup_window_start="NOW() - INTERVAL '$BACKUP_DAYS days'"
    local backup_window_end="NOW() - INTERVAL '$RECENT_DAYS days'"
    
    log_info "validating archive integrity"
    
    # validate each table
    for table in metadata signals fills; do
        local main_count archive_count
        
        # count in main db (backup window)
        main_count=$(get_row_count "psql_main" "$table" "created > $backup_window_start AND created <= $backup_window_end")
        
        # count in archive db (same window)  
        archive_count=$(get_row_count "psql_archive" "$table" "created > $backup_window_start AND created <= $backup_window_end")
        
        log_info "validation $table: main=$main_count archive=$archive_count"
        
        if [[ "$main_count" != "$archive_count" ]]; then
            log_error "row count mismatch for $table: main=$main_count archive=$archive_count"
            return 1
        fi
    done
    
    log_info "archive validation passed"
    return 0
}

cleanup_main_db() {
    local backup_window_start="NOW() - INTERVAL '$BACKUP_DAYS days'"
    local backup_window_end="NOW() - INTERVAL '$RECENT_DAYS days'"
    
    log_info "cleaning up main database (backup window)"
    
    # count rows before deletion
    local pre_delete_count
    pre_delete_count=$(get_row_count "psql_main" "metadata" "created > $backup_window_start AND created <= $backup_window_end")
    
    if [[ "$pre_delete_count" -eq 0 ]]; then
        log_info "no rows to delete from main database"
        return 0
    fi
    
    # delete metadata rows (FK cascade handles signals/fills)
    local deleted_count
    deleted_count=$(psql_main -c "
        DELETE FROM ${SCHEMA}.metadata 
        WHERE created > $backup_window_start 
          AND created <= $backup_window_end;
        SELECT ROW_COUNT();" | tail -1)
    
    log_info "deleted $deleted_count metadata rows from main database"
    
    # validate deletion count
    if [[ "$deleted_count" != "$pre_delete_count" ]]; then
        log_error "deletion count mismatch: expected=$pre_delete_count actual=$deleted_count"
        return 1
    fi
    
    log_info "main database cleanup completed"
    return 0
}

purge_obsolete() {
    local purge_cutoff="NOW() - INTERVAL '$PURGE_DAYS days'"
    
    log_info "purging obsolete rows (>$PURGE_DAYS days) from backup database"
    
    local total_purged=0
    
    # purge each table in reverse dependency order
    for table in fills signals metadata; do
        local purged_count
        purged_count=$(psql_archive -c "
            DELETE FROM ${SCHEMA}.${table} 
            WHERE created <= $purge_cutoff;
            SELECT ROW_COUNT();" | tail -1)
        
        log_info "purged $purged_count obsolete rows from $table"
        total_purged=$((total_purged + purged_count))
    done
    
    log_info "total obsolete rows purged: $total_purged"
    return 0
}

# === METRICS & MONITORING ===
emit_metrics() {
    local status="$1"
    local runtime_seconds="$2"
    
    # prometheus metrics (if node_exporter textfile collector available)
    local metrics_file="/var/lib/node_exporter/textfile_collector/postgres_cleanup.prom"
    
    if [[ -d "$(dirname "$metrics_file")" ]]; then
        cat > "$metrics_file" << EOF
# HELP postgres_cleanup_status Last cleanup run status (0=success, 1=failure)
# TYPE postgres_cleanup_status gauge
postgres_cleanup_status $status

# HELP postgres_cleanup_runtime_seconds Runtime of last cleanup operation
# TYPE postgres_cleanup_runtime_seconds gauge  
postgres_cleanup_runtime_seconds $runtime_seconds

# HELP postgres_cleanup_last_run_timestamp Unix timestamp of last cleanup run
# TYPE postgres_cleanup_last_run_timestamp gauge
postgres_cleanup_last_run_timestamp $(date +%s)
EOF
        log_info "metrics emitted to $metrics_file"
    fi
}

# === MAIN EXECUTION ===
main() {
    local start_time
    start_time=$(date +%s)
    
    log_info "postgres metadata cleanup & backup pipeline starting"
    log_info "config: recent=${RECENT_DAYS}d backup=${BACKUP_DAYS}d purge=${PURGE_DAYS}d"
    
    # ensure temp cleanup on exit
    trap 'rm -f /tmp/backup_hashes.txt' EXIT
    
    # FR-1: verify backup db reachable
    verify_backup_db
    
    # FR-2: detect if slice already archived
    if check_slice_archived; then
        local runtime=$(($(date +%s) - start_time))
        emit_metrics 0 "$runtime"
        log_info "pipeline completed (already archived) in ${runtime}s"
        return 0
    fi
    
    # FR-3: copy backup window rows to archive
    local hash_count
    hash_count=$(select_backup_hashes)
    
    if ! archive_slice "$hash_count"; then
        log_fatal "archive operation failed"
    fi
    
    # FR-4: validate counts before deletion
    if ! validate_archive; then
        log_fatal "archive validation failed, aborting cleanup"
    fi
    
    # FR-5: delete validated rows from main (FK cascade)
    if ! cleanup_main_db; then
        log_fatal "main database cleanup failed"
    fi
    
    # FR-6: purge obsolete rows from backup
    if ! purge_obsolete; then
        log_warn "obsolete row purging failed, continuing"
    fi
    
    local runtime=$(($(date +%s) - start_time))
    emit_metrics 0 "$runtime"
    
    log_info "pipeline completed successfully in ${runtime}s"
    
    # cleanup temp files
    rm -f /tmp/backup_hashes.txt
}

# === ENTRY POINT ===
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    # verify running as expected user
    if [[ "$(id -un)" != "postgres" ]] && [[ "$EUID" -ne 0 ]]; then
        log_warn "not running as postgres user or root"
    fi
    
    # set resource limits
    ulimit -v $((MAX_MEMORY_GB * 1024 * 1024))  # virtual memory limit
    
    # execute main pipeline with error handling
    if ! main "$@"; then
        local runtime=$(($(date +%s) - ${start_time:-$(date +%s)}))
        emit_metrics 1 "$runtime"
        log_fatal "pipeline failed"
    fi
fi