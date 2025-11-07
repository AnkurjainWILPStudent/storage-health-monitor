#!/bin/bash

#
# Test Script - Verify Storage Health Monitor Setup
# Can be run on either client or monitoring VM
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo "========================================"
echo "Storage Health Monitor - System Tester"
echo "========================================"
echo ""

# Detect VM type
if [ -d "client_node" ]; then
    VM_TYPE="client"
    BASE_DIR="client_node"
elif [ -d "monitoring_node" ]; then
    VM_TYPE="monitoring"
    BASE_DIR="monitoring_node"
else
    echo -e "${RED}Error: Cannot detect VM type${NC}"
    echo "Run this script from the storage-health-monitor directory"
    exit 1
fi

echo -e "${BLUE}Detected VM type: $VM_TYPE${NC}"
echo ""

# Test counter
PASSED=0
FAILED=0

# Test function
test_item() {
    local description="$1"
    local command="$2"
    
    echo -n "Testing: $description ... "
    
    if eval "$command" > /dev/null 2>&1; then
        echo -e "${GREEN}PASS${NC}"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        ((FAILED++))
        return 1
    fi
}

# Test function with output
test_item_output() {
    local description="$1"
    local command="$2"
    
    echo -n "Testing: $description ... "
    
    output=$(eval "$command" 2>&1)
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}PASS${NC}"
        echo "  → $output"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}FAIL${NC}"
        echo "  → $output"
        ((FAILED++))
        return 1
    fi
}

# Common tests for both VMs
echo "=== System Tests ==="
test_item "Python 3 installed" "which python3"
test_item "SSH client available" "which ssh"
test_item_output "Python version" "python3 --version"
test_item_output "Hostname" "hostname"
test_item_output "IP addresses" "hostname -I"

echo ""
echo "=== Network Tests ==="
test_item "Internet connectivity" "ping -c 1 8.8.8.8"
test_item "DNS resolution" "ping -c 1 google.com"

# Client-specific tests
if [ "$VM_TYPE" == "client" ]; then
    echo ""
    echo "=== Client-Specific Tests ==="
    
    test_item "psutil module installed" "python3 -c 'import psutil'"
    test_item "smartctl installed" "which smartctl"
    test_item "lsblk installed" "which lsblk"
    test_item "SSH key exists" "test -f ~/.ssh/id_ed25519"
    test_item "Config file exists" "test -f $BASE_DIR/config.json"
    test_item "disk_monitor.py exists" "test -f $BASE_DIR/disk_monitor.py"
    test_item "utils.py exists" "test -f $BASE_DIR/utils.py"
    test_item "Logs directory exists" "test -d $BASE_DIR/logs"
    
    # Test SSH connection to monitoring node
    if [ -f "$BASE_DIR/config.json" ]; then
        MONITOR_IP=$(python3 -c "import json; print(json.load(open('$BASE_DIR/config.json'))['monitoring_node_ip'])" 2>/dev/null)
        MONITOR_USER=$(python3 -c "import json; print(json.load(open('$BASE_DIR/config.json'))['monitoring_node_user'])" 2>/dev/null)
        
        if [ -n "$MONITOR_IP" ] && [ -n "$MONITOR_USER" ]; then
            echo ""
            echo "=== Monitoring Node Connection Tests ==="
            test_item "Ping monitoring node ($MONITOR_IP)" "ping -c 1 -W 2 $MONITOR_IP"
            test_item "SSH to monitoring node" "ssh -o BatchMode=yes -o ConnectTimeout=5 -i ~/.ssh/id_ed25519 $MONITOR_USER@$MONITOR_IP 'exit 0'"
        fi
    fi
    
    # Check cron or systemd
    echo ""
    echo "=== Scheduler Tests ==="
    if crontab -l 2>/dev/null | grep -q "disk_monitor.py"; then
        echo -e "${GREEN}Cron job configured${NC}"
        crontab -l | grep disk_monitor.py
    else
        echo -e "${YELLOW}No cron job found${NC}"
    fi
    
    if systemctl list-unit-files 2>/dev/null | grep -q "disk-monitor.timer"; then
        echo -e "${GREEN}Systemd timer configured${NC}"
        systemctl status disk-monitor.timer --no-pager 2>&1 | head -5
    else
        echo -e "${YELLOW}No systemd timer found${NC}"
    fi
fi

# Monitoring-specific tests
if [ "$VM_TYPE" == "monitoring" ]; then
    echo ""
    echo "=== Monitoring-Specific Tests ==="
    
    test_item "SSH server running" "systemctl is-active ssh"
    test_item "Config directory exists" "test -d $BASE_DIR/config"
    test_item "Scripts directory exists" "test -d $BASE_DIR/scripts"
    test_item "analyzer_config.json exists" "test -f $BASE_DIR/config/analyzer_config.json"
    test_item "thresholds.json exists" "test -f $BASE_DIR/config/thresholds.json"
    test_item "analyze_storage_health.py exists" "test -f $BASE_DIR/scripts/analyze_storage_health.py"
    test_item "utils.py exists" "test -f $BASE_DIR/utils.py"
    test_item "data_validator.py exists" "test -f $BASE_DIR/data_validator.py"
    test_item "alert_handler.py exists" "test -f $BASE_DIR/alert_handler.py"
    test_item "Logs directory exists" "test -d $BASE_DIR/logs"
    test_item "Reports directory exists" "test -d $BASE_DIR/reports"
    
    # Check data directory
    if [ -f "$BASE_DIR/config/analyzer_config.json" ]; then
        DATA_DIR=$(python3 -c "import json; print(json.load(open('$BASE_DIR/config/analyzer_config.json'))['data_directory'])" 2>/dev/null)
        if [ -n "$DATA_DIR" ]; then
            test_item "Data directory exists ($DATA_DIR)" "test -d $DATA_DIR"
            
            # Count files
            FILE_COUNT=$(find "$DATA_DIR" -name "*.json" 2>/dev/null | wc -l)
            echo -e "${BLUE}Data files found: $FILE_COUNT${NC}"
        fi
    fi
    
    # Check systemd
    echo ""
    echo "=== Scheduler Tests ==="
    if systemctl list-unit-files 2>/dev/null | grep -q "storage-analyzer.timer"; then
        echo -e "${GREEN}Systemd timer configured${NC}"
        systemctl status storage-analyzer.timer --no-pager 2>&1 | head -5
    else
        echo -e "${YELLOW}No systemd timer found${NC}"
    fi
fi

# Summary
echo ""
echo "========================================"
echo "Test Summary"
echo "========================================"
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed! System is properly configured.${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠ Some tests failed. Review the output above.${NC}"
    exit 1
fi
