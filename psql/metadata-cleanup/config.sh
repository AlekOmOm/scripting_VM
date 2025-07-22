#!/bin/bash
#
# configuration and utilities for postgres cleanup pipeline
# version: 0.4 – 2025-07-22
#

# === RUNTIME CONFIGURATION ===
readonly CLEANUP_VERSION="0.4"
readonly CLEANUP_DATE="2025-07-22"

# database configuration
readonly DEFAULT_MAIN_HOST="localhost"
readonly DEFAULT_MAIN_PORT="5432"
readonly DEFAULT_MAIN_DB="postgres"
readonly DEFAULT_ARCHIVE_DB="postgres_archive"
readonly DEFAULT_SCHEMA="backtest"

# temporal window configuration (days)
readonly DEFAULT_RECENT_DAYS=7
readonly DEFAULT_BACKUP_DAYS=14
readonly DEFAULT_PURGE_DAYS=14

# resource constraints
readonly DEFAULT_MAX_RUNTIME_MINUTES=5
readonly DEFAULT_MAX_MEMORY_GB=1
readonly DEFAULT_BATCH_SIZE=10000

# logging configuration
readonly LOG_DIR="/var/log"
readonly LOG_FILE="postgres-cleanup.log"
readonly LOG_FORMAT_ISO8601="%Y-%m-%dT%H:%M:%S%z"

# metrics configuration
readonly METRICS_DIR="/var/lib/node_exporter/textfile_collector"
readonly METRICS_FILE="postgres_cleanup.prom"

# === ENVIRONMENT RESOLUTION ===
get_config() {
    local var_name="$1"
    local default_value="$2"
    
    # precedence: environment -> config file -> default
    local value
    value="${!var_name:-}"
    
    if [[ -z "$value" ]] && [[ -f "$HOME/.postgres-cleanup.conf" ]]; then
        value=$(grep "^${var_name}=" "$HOME/.postgres-cleanup.conf" 2>/dev/null | cut -d'=' -f2- || true)
    fi
    
    echo "${value:-$default_value}"
}

# === UTILITY FUNCTIONS ===
timestamp_iso8601() {
    date +"$LOG_FORMAT_ISO8601"
}

format_duration() {
    local seconds="$1"
    
    if [[ "$seconds" -lt 60 ]]; then
        echo "${seconds}s"
    elif [[ "$seconds" -lt 3600 ]]; then
        echo "$((seconds / 60))m$((seconds % 60))s"
    else
        echo "$((seconds / 3600))h$(((seconds % 3600) / 60))m$((seconds % 60))s"
    fi
}

human_readable_bytes() {
    local bytes="$1"
    local units=("B" "KB" "MB" "GB" "TB")
    local unit_index=0
    local size="$bytes"
    
    while [[ "$size" -gt 1024 ]] && [[ "$unit_index" -lt 4 ]]; do
        size=$((size / 1024))
        unit_index=$((unit_index + 1))
    done
    
    echo "${size}${units[$unit_index]}"
}

# === DATABASE CONNECTION TESTING ===
test_db_connection() {
    local host="$1"
    local port="$2" 
    local database="$3"
    local timeout="${4:-5}"
    
    timeout "$timeout" psql -h "$host" -p "$port" -d "$database" -c "SELECT 1;" >/dev/null 2>&1
}

get_db_size() {
    local host="$1"
    local port="$2"
    local database="$3"
    
    psql -h "$host" -p "$port" -d "$database" -t -A -c "SELECT pg_database_size('$database');" 2>/dev/null || echo "0"
}

# === LOCK MANAGEMENT ===
acquire_lock() {
    local lock_name="$1"
    local lock_file="/tmp/postgres-cleanup-${lock_name}.lock"
    
    # use flock for atomic locking
    exec 200>"$lock_file"
    
    if ! flock -n 200; then
        echo "ERROR: another instance is already running (lock: $lock_name)" >&2
        return 1
    fi
    
    echo $$ >&200
    echo "$lock_file"
}

release_lock() {
    local lock_file="$1"
    
    if [[ -n "$lock_file" ]] && [[ -f "$lock_file" ]]; then
        rm -f "$lock_file"
        exec 200>&-
    fi
}

# === VALIDATION HELPERS ===
validate_positive_integer() {
    local value="$1"
    local name="$2"
    
    if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: $name must be positive integer, got: $value" >&2
        return 1
    fi
}

validate_database_name() {
    local name="$1"
    
    # postgresql identifier rules: alphanumeric + underscore, max 63 chars
    if ! [[ "$name" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]] || [[ ${#name} -gt 63 ]]; then
        echo "ERROR: invalid database name: $name" >&2
        return 1
    fi
}

validate_schema_name() {
    local name="$1"
    
    if ! [[ "$name" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]] || [[ ${#name} -gt 63 ]]; then
        echo "ERROR: invalid schema name: $name" >&2
        return 1
    fi
}

# === POSTGRESQL UTILITY FUNCTIONS ===
get_table_row_count() {
    local host="$1"
    local port="$2"
    local database="$3"
    local schema="$4"
    local table="$5"
    local where_clause="${6:-TRUE}"
    
    psql -h "$host" -p "$port" -d "$database" -t -A -c "
        SELECT COUNT(*) 
        FROM ${schema}.${table} 
        WHERE ${where_clause};
    " 2>/dev/null || echo "0"
}

check_table_exists() {
    local host="$1"
    local port="$2"
    local database="$3"
    local schema="$4"
    local table="$5"
    
    local exists
    exists=$(psql -h "$host" -p "$port" -d "$database" -t -A -c "
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = '$schema' 
          AND table_name = '$table';
    " 2>/dev/null || echo "0")
    
    [[ "$exists" -eq 1 ]]
}

get_database_timezone() {
    local host="$1"
    local port="$2" 
    local database="$3"
    
    psql -h "$host" -p "$port" -d "$database" -t -A -c "SHOW timezone;" 2>/dev/null || echo "UTC"
}

# === CLEANUP HELPERS ===
cleanup_temp_files() {
    local temp_prefix="postgres-cleanup"
    
    find /tmp -name "${temp_prefix}*" -type f -mtime +1 -delete 2>/dev/null || true
}

rotate_log_file() {
    local log_path="$1" 
    local max_size_mb="${2:-100}"
    
    if [[ -f "$log_path" ]] && [[ $(stat -f%z "$log_path" 2>/dev/null || stat -c%s "$log_path" 2>/dev/null || echo 0) -gt $((max_size_mb * 1024 * 1024)) ]]; then
        mv "$log_path" "${log_path}.old"
        touch "$log_path"
        chmod 644 "$log_path"
    fi
}

# === ENVIRONMENT VALIDATION ===
validate_environment() {
    local errors=0
    
    # check required commands
    local required_commands=("psql" "pg_dump" "timeout" "flock" "logger")
    for cmd in "${required_commands[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            echo "ERROR: required command not found: $cmd" >&2
            errors=$((errors + 1))
        fi
    done
    
    # check write permissions for log directory
    if [[ ! -w "$(dirname "$LOG_DIR/$LOG_FILE")" ]]; then
        echo "ERROR: no write permission for log directory: $LOG_DIR" >&2
        errors=$((errors + 1))
    fi
    
    # check ulimit settings
    local memory_limit_kb
    memory_limit_kb=$(ulimit -v)
    if [[ "$memory_limit_kb" != "unlimited" ]] && [[ "$memory_limit_kb" -lt $((DEFAULT_MAX_MEMORY_GB * 1024 * 1024)) ]]; then
        echo "WARNING: memory limit may be too restrictive: ${memory_limit_kb}KB" >&2
    fi
    
    return "$errors"
}

# === INITIALIZATION ===
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "postgres cleanup pipeline utilities v$CLEANUP_VERSION"
    echo "source this file to use utility functions"
    
    # demonstrate configuration resolution
    echo ""
    echo "configuration (resolved):"
    echo "  main database: $(get_config 'MAIN_DB_NAME' "$DEFAULT_MAIN_DB")"
    echo "  archive database: $(get_config 'ARCHIVE_DB_NAME' "$DEFAULT_ARCHIVE_DB")"
    echo "  schema: $(get_config 'SCHEMA' "$DEFAULT_SCHEMA")"
    echo "  recent window: $(get_config 'RECENT_DAYS' "$DEFAULT_RECENT_DAYS") days"
    
    # validate environment
    echo ""
    echo "environment validation:"
    if validate_environment; then
        echo "  ✓ all checks passed"
    else
        echo "  ✗ validation failed"
        exit 1
    fi
fi