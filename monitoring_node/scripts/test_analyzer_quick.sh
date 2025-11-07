#!/bin/bash

#
# Quick Test Script - Verify Analyzer Works with Existing Data
# Run this on monitoringnode after copying the analyzer files
#

set -e

echo "========================================"
echo "Storage Health Analyzer - Quick Test"
echo "========================================"
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Paths
DATA_DIR="/home/admin/monitor_data"
MONITOR_DIR="/home/admin/monitoring_node"

echo -e "${BLUE}Environment:${NC}"
echo "  Hostname: $(hostname)"
echo "  User: $(whoami)"
echo "  Python: $(python3 --version)"
echo ""

# Check 1: Data directory
echo -e "${BLUE}Checking data directory...${NC}"
if [ -d "$DATA_DIR" ]; then
    echo -e "${GREEN}✓ Data directory exists: $DATA_DIR${NC}"
    
    # Show clients
    echo ""
    echo "Client directories:"
    for client_dir in "$DATA_DIR"/*; do
        if [ -d "$client_dir" ]; then
            client_name=$(basename "$client_dir")
            file_count=$(find "$client_dir" -name "*.json" -type f | wc -l)
            latest=$(find "$client_dir" -name "*.json" -type f -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
            
            echo "  ├─ $client_name: $file_count files"
            if [ -n "$latest" ]; then
                age_seconds=$(( $(date +%s) - $(stat -c %Y "$latest") ))
                age_minutes=$(( age_seconds / 60 ))
                echo "     └─ Latest: $(basename "$latest") (${age_minutes}m ago)"
            fi
        fi
    done
else
    echo -e "${RED}✗ Data directory not found: $DATA_DIR${NC}"
    echo "  Clients haven't sent data yet, or directory doesn't exist"
    exit 1
fi

# Check 2: Monitoring directory
echo ""
echo -e "${BLUE}Checking monitoring_node directory...${NC}"
if [ -d "$MONITOR_DIR" ]; then
    echo -e "${GREEN}✓ Monitoring directory exists: $MONITOR_DIR${NC}"
    
    # Check for required files
    required_files=(
        "scripts/analyze_storage_health.py"
        "config/analyzer_config.json"
        "config/thresholds.json"
        "utils.py"
        "data_validator.py"
        "alert_handler.py"
    )
    
    echo ""
    echo "Required files:"
    all_present=true
    for file in "${required_files[@]}"; do
        if [ -f "$MONITOR_DIR/$file" ]; then
            echo -e "  ${GREEN}✓${NC} $file"
        else
            echo -e "  ${RED}✗${NC} $file (MISSING)"
            all_present=false
        fi
    done
    
    if [ "$all_present" = false ]; then
        echo ""
        echo -e "${RED}Some required files are missing. Run setup_monitoring.sh first.${NC}"
        exit 1
    fi
else
    echo -e "${RED}✗ Monitoring directory not found: $MONITOR_DIR${NC}"
    echo "  Run setup_monitoring.sh first"
    exit 1
fi

# Check 3: Test analyzer
echo ""
echo -e "${BLUE}Testing analyzer with existing data...${NC}"
echo ""

cd "$MONITOR_DIR"

# Run analyzer
echo -e "${YELLOW}Running: python3 scripts/analyze_storage_health.py${NC}"
echo ""

python3 scripts/analyze_storage_health.py

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ Analyzer ran successfully!${NC}"
    
    # Show logs
    if [ -f "logs/analyzer.log" ]; then
        echo ""
        echo -e "${BLUE}Last 15 lines from analyzer log:${NC}"
        echo "─────────────────────────────────────────"
        tail -15 logs/analyzer.log
        echo "─────────────────────────────────────────"
    fi
    
    # Show reports
    echo ""
    echo -e "${BLUE}Generated reports:${NC}"
    if [ -d "reports" ] && [ "$(ls -A reports 2>/dev/null)" ]; then
        ls -lth reports/ | head -10
    else
        echo "  No reports generated (check logs for details)"
    fi
    
    # Show archive
    echo ""
    echo -e "${BLUE}Archived files:${NC}"
    if [ -d "logs/archive" ] && [ "$(ls -A logs/archive 2>/dev/null)" ]; then
        archive_count=$(find logs/archive -name "*.json" -type f | wc -l)
        echo "  $archive_count files archived"
    else
        echo "  No files archived yet"
    fi
    
else
    echo ""
    echo -e "${RED}✗ Analyzer failed!${NC}"
    echo ""
    echo -e "${YELLOW}Check the log for details:${NC}"
    if [ -f "logs/analyzer.log" ]; then
        tail -30 logs/analyzer.log
    else
        echo "  Log file not found: $MONITOR_DIR/logs/analyzer.log"
    fi
    exit 1
fi

# Summary
echo ""
echo "========================================"
echo -e "${GREEN}✓ Test completed successfully!${NC}"
echo "========================================"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Review the log: tail -f $MONITOR_DIR/logs/analyzer.log"
echo "  2. Check reports: cat $MONITOR_DIR/reports/summary_*.json | jq ."
echo "  3. Set up automatic execution: run setup_monitoring.sh"
echo ""
echo -e "${BLUE}Monitor ongoing:${NC}"
echo "  # Watch for new client data"
echo "  watch -n 5 'find $DATA_DIR -name \"*.json\" -mmin -15'"
echo ""
echo "  # Watch analyzer logs"
echo "  tail -f $MONITOR_DIR/logs/analyzer.log"
echo ""
