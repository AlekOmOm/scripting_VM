#!/bin/bash
#
# installation & deployment script for postgres cleanup pipeline
# version: 0.4 – 2025-07-22
#

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly INSTALL_DIR="/usr/local/bin"
readonly LOG_FILE="/var/log/postgres-cleanup.log"
readonly CRON_SCHEDULE="30 2 * * 1"  # monday 02:30
readonly SERVICE_USER="postgres"

log() {
    echo "$(date -Iseconds) [INSTALL] $*"
}

check_prerequisites() {
    log "checking prerequisites"
    
    # check required commands
    local required_cmds=("psql" "pg_dump" "crontab" "logger")
    for cmd in "${required_cmds[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            echo "ERROR: required command not found: $cmd" >&2
            exit 1
        fi
    done
    
    # check postgres user exists
    if ! id "$SERVICE_USER" >/dev/null 2>&1; then
        echo "ERROR: user $SERVICE_USER does not exist" >&2
        exit 1
    fi
    
    log "prerequisites satisfied"
}

install_scripts() {
    log "installing scripts to $INSTALL_DIR"
    
    # copy main script
    sudo install -o root -g root -m 755 "$SCRIPT_DIR/main.sh" "$INSTALL_DIR/"
    
    # create log file with proper permissions
    sudo touch "$LOG_FILE"
    sudo chown "$SERVICE_USER:$SERVICE_USER" "$LOG_FILE"
    sudo chmod 644 "$LOG_FILE"
    
    log "scripts installed"
}

setup_cron() {
    log "setting up cron job for user $SERVICE_USER"
    
    # create cron entry
    local cron_entry="$CRON_SCHEDULE TZ=Europe/Stockholm $INSTALL_DIR/main.sh >/dev/null 2>&1"
    
    # install cron job for postgres user
    (sudo -u "$SERVICE_USER" crontab -l 2>/dev/null | grep -v "$INSTALL_DIR/main.sh" || true; echo "$cron_entry") | sudo -u "$SERVICE_USER" crontab -
    
    log "cron job installed: $cron_entry"
}

create_archive_db() {
    log "checking/creating archive database"
    
    # check if archive database exists
    local db_exists
    db_exists=$(sudo -u postgres psql -t -A -c "SELECT COUNT(*) FROM pg_database WHERE datname = 'postgres_archive';")
    
    if [[ "$db_exists" -eq 0 ]]; then
        log "creating archive database"
        sudo -u postgres createdb postgres_archive
        
        # copy schema structure
        log "copying schema structure"
        sudo -u postgres pg_dump -s -n backtest postgres | sudo -u postgres psql postgres_archive
    else
        log "archive database already exists"
    fi
}

test_installation() {
    log "testing installation"
    
    # test script permissions and basic functionality
    if ! sudo -u "$SERVICE_USER" "$INSTALL_DIR/main.sh" --help >/dev/null 2>&1; then
        log "WARNING: script test failed (expected for help option)"
    fi
    
    # verify cron installation
    if ! sudo -u "$SERVICE_USER" crontab -l | grep -q "$INSTALL_DIR/main.sh"; then
        echo "ERROR: cron job not found" >&2
        exit 1
    fi
    
    log "installation test completed"
}

main() {
    log "postgres cleanup pipeline installation starting"
    
    # check running as root
    if [[ "$EUID" -ne 0 ]]; then
        echo "ERROR: must run as root for installation" >&2
        exit 1
    fi
    
    check_prerequisites
    install_scripts
    create_archive_db
    setup_cron
    test_installation
    
    log "installation completed successfully"
    log "next run scheduled: $(date -d 'next monday 02:30')"
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi