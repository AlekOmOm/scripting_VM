#!/bin/bash
#
# test suite for postgres metadata cleanup pipeline
# version: 0.4 – 2025-07-22
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly TEST_DB="postgres_test"
readonly TEST_ARCHIVE_DB="postgres_test_archive"
readonly TEST_SCHEMA="backtest_test"

# test data configuration  
readonly TEST_ROWS_RECENT=100
readonly TEST_ROWS_BACKUP=50
readonly TEST_ROWS_OBSOLETE=25

log_test() {
    echo "$(date -Iseconds) [TEST] $*"
}

setup_test_environment() {
    log_test "setting up test environment"
    
    # create test databases
    dropdb --if-exists "$TEST_DB" || true
    dropdb --if-exists "$TEST_ARCHIVE_DB" || true
    
    createdb "$TEST_DB"
    createdb "$TEST_ARCHIVE_DB"
    
    # create test schema and tables in both databases
    for db in "$TEST_DB" "$TEST_ARCHIVE_DB"; do
        psql -d "$db" << EOF
CREATE SCHEMA $TEST_SCHEMA;

CREATE TABLE $TEST_SCHEMA.metadata (
    hash VARCHAR(64) PRIMARY KEY,
    created TIMESTAMP DEFAULT NOW(),
    data JSONB
);

CREATE TABLE $TEST_SCHEMA.signals (
    id SERIAL PRIMARY KEY,
    hash VARCHAR(64) REFERENCES $TEST_SCHEMA.metadata(hash) ON DELETE CASCADE,
    signal_type VARCHAR(50),
    value DECIMAL,
    created TIMESTAMP DEFAULT NOW()
);

CREATE TABLE $TEST_SCHEMA.fills (
    id SERIAL PRIMARY KEY, 
    hash VARCHAR(64) REFERENCES $TEST_SCHEMA.metadata(hash) ON DELETE CASCADE,
    quantity INTEGER,
    price DECIMAL,
    created TIMESTAMP DEFAULT NOW()
);
EOF
    done
    
    log_test "test databases created"
}

generate_test_data() {
    log_test "generating test data"
    
    # insert recent data (last 7 days)
    psql -d "$TEST_DB" << EOF
INSERT INTO $TEST_SCHEMA.metadata (hash, created, data)
SELECT 
    'recent_' || generate_series || '_hash',
    NOW() - INTERVAL '3 days' + (random() * INTERVAL '6 days'),
    '{"type": "recent"}'::jsonb
FROM generate_series(1, $TEST_ROWS_RECENT);

-- add signals and fills for recent data
INSERT INTO $TEST_SCHEMA.signals (hash, signal_type, value, created)
SELECT 
    hash,
    'test_signal',
    random() * 100,
    created
FROM $TEST_SCHEMA.metadata 
WHERE hash LIKE 'recent_%';

INSERT INTO $TEST_SCHEMA.fills (hash, quantity, price, created)
SELECT 
    hash,
    (random() * 1000)::integer,
    random() * 50,
    created  
FROM $TEST_SCHEMA.metadata
WHERE hash LIKE 'recent_%';
EOF

    # insert backup window data (7-14 days ago)
    psql -d "$TEST_DB" << EOF
INSERT INTO $TEST_SCHEMA.metadata (hash, created, data)
SELECT 
    'backup_' || generate_series || '_hash',
    NOW() - INTERVAL '10 days' + (random() * INTERVAL '6 days'),
    '{"type": "backup"}'::jsonb
FROM generate_series(1, $TEST_ROWS_BACKUP);

-- add signals and fills for backup data
INSERT INTO $TEST_SCHEMA.signals (hash, signal_type, value, created)
SELECT 
    hash,
    'test_signal',
    random() * 100,
    created
FROM $TEST_SCHEMA.metadata 
WHERE hash LIKE 'backup_%';

INSERT INTO $TEST_SCHEMA.fills (hash, quantity, price, created)  
SELECT 
    hash,
    (random() * 1000)::integer,
    random() * 50,
    created
FROM $TEST_SCHEMA.metadata
WHERE hash LIKE 'backup_%';
EOF

    # insert obsolete data (> 14 days ago)
    psql -d "$TEST_DB" << EOF
INSERT INTO $TEST_SCHEMA.metadata (hash, created, data)
SELECT 
    'obsolete_' || generate_series || '_hash', 
    NOW() - INTERVAL '20 days' + (random() * INTERVAL '5 days'),
    '{"type": "obsolete"}'::jsonb
FROM generate_series(1, $TEST_ROWS_OBSOLETE);

-- add signals and fills for obsolete data
INSERT INTO $TEST_SCHEMA.signals (hash, signal_type, value, created)
SELECT 
    hash,
    'test_signal',
    random() * 100,
    created
FROM $TEST_SCHEMA.metadata 
WHERE hash LIKE 'obsolete_%';

INSERT INTO $TEST_SCHEMA.fills (hash, quantity, price, created)
SELECT 
    hash,
    (random() * 1000)::integer, 
    random() * 50,
    created
FROM $TEST_SCHEMA.metadata
WHERE hash LIKE 'obsolete_%';
EOF

    log_test "test data generated"
}

verify_initial_state() {
    log_test "verifying initial test state"
    
    local metadata_count signals_count fills_count
    
    metadata_count=$(psql -t -A -d "$TEST_DB" -c "SELECT COUNT(*) FROM $TEST_SCHEMA.metadata;")
    signals_count=$(psql -t -A -d "$TEST_DB" -c "SELECT COUNT(*) FROM $TEST_SCHEMA.signals;")
    fills_count=$(psql -t -A -d "$TEST_DB" -c "SELECT COUNT(*) FROM $TEST_SCHEMA.fills;")
    
    local expected_total=$((TEST_ROWS_RECENT + TEST_ROWS_BACKUP + TEST_ROWS_OBSOLETE))
    
    log_test "initial counts - metadata: $metadata_count, signals: $signals_count, fills: $fills_count"
    
    if [[ "$metadata_count" -ne "$expected_total" ]]; then
        echo "ERROR: unexpected metadata count: $metadata_count != $expected_total" >&2
        return 1
    fi
    
    log_test "initial state verified"
}

run_cleanup_pipeline() {
    log_test "running cleanup pipeline with test configuration"
    
    # modify main.sh environment for testing
    export PGHOST="localhost"
    export PGPORT="5432" 
    export MAIN_DB_NAME="$TEST_DB"
    export ARCHIVE_DB_NAME="$TEST_ARCHIVE_DB"
    export SCHEMA="$TEST_SCHEMA"
    
    # run the main pipeline
    if ! "$SCRIPT_DIR/main.sh"; then
        log_test "ERROR: pipeline execution failed"
        return 1
    fi
    
    log_test "pipeline execution completed"
}

verify_cleanup_results() {
    log_test "verifying cleanup results"
    
    # check main database - should only have recent data
    local main_metadata main_signals main_fills
    main_metadata=$(psql -t -A -d "$TEST_DB" -c "SELECT COUNT(*) FROM $TEST_SCHEMA.metadata;")
    main_signals=$(psql -t -A -d "$TEST_DB" -c "SELECT COUNT(*) FROM $TEST_SCHEMA.signals;")  
    main_fills=$(psql -t -A -d "$TEST_DB" -c "SELECT COUNT(*) FROM $TEST_SCHEMA.fills;")
    
    # check archive database - should have backup + obsolete data
    local archive_metadata archive_signals archive_fills
    archive_metadata=$(psql -t -A -d "$TEST_ARCHIVE_DB" -c "SELECT COUNT(*) FROM $TEST_SCHEMA.metadata;")
    archive_signals=$(psql -t -A -d "$TEST_ARCHIVE_DB" -c "SELECT COUNT(*) FROM $TEST_SCHEMA.signals;")
    archive_fills=$(psql -t -A -d "$TEST_ARCHIVE_DB" -c "SELECT COUNT(*) FROM $TEST_SCHEMA.fills;")
    
    log_test "post-cleanup counts:"
    log_test "  main db - metadata: $main_metadata, signals: $main_signals, fills: $main_fills"  
    log_test "  archive db - metadata: $archive_metadata, signals: $archive_signals, fills: $archive_fills"
    
    # verify main database has only recent data
    if [[ "$main_metadata" -ne "$TEST_ROWS_RECENT" ]]; then
        echo "ERROR: main database metadata count incorrect: $main_metadata != $TEST_ROWS_RECENT" >&2
        return 1
    fi
    
    # verify archive has backup data (obsolete data should be purged)
    if [[ "$archive_metadata" -ne "$TEST_ROWS_BACKUP" ]]; then
        echo "ERROR: archive database metadata count incorrect: $archive_metadata != $TEST_ROWS_BACKUP" >&2  
        return 1
    fi
    
    log_test "cleanup results verified successfully"
}

cleanup_test_environment() {
    log_test "cleaning up test environment"
    
    dropdb --if-exists "$TEST_DB" || true
    dropdb --if-exists "$TEST_ARCHIVE_DB" || true
    
    log_test "test environment cleaned up"
}

run_all_tests() {
    local start_time
    start_time=$(date +%s)
    
    log_test "postgres cleanup pipeline test suite starting"
    
    # ensure cleanup on exit
    trap cleanup_test_environment EXIT
    
    setup_test_environment
    generate_test_data  
    verify_initial_state
    run_cleanup_pipeline
    verify_cleanup_results
    
    local runtime=$(($(date +%s) - start_time))
    log_test "all tests passed successfully in ${runtime}s"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    run_all_tests "$@"
fi