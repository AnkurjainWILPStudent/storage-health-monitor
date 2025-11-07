#!/bin/bash

#
# Monitoring Node Setup Script - Simplified for Existing Environment
# Run this on the monitoring VM (monitoringnode)
# Assumes: Python3, SSH, and all packages already installed
#

set -e

echo "=========================================="
echo "Storage Health Monitor - Monitoring Setup"
echo "=========================================="
echo ""
echo "VM: monitoringnode (192.168.56.10)"
echo "Clients: clientnode (192.168.56.11), clientnode2 (192.168.56.12)"
echo ""

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Base directory
MONITOR_DIR="/home/admin/monitoring_node"
DATA_DIR="/home/admin/monitor_data"

echo -e "${BLUE}Step 1: Verifying existing setup...${NC}"
echo ""

# Check if running as admin user
CURRENT_USER=$(whoami)
if [ "$CURRENT_USER" != "admin" ]; then
    echo -e "${YELLOW}Warning: Expected to run as 'admin' user, running as '$CURRENT_USER'${NC}"
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Check data directory exists and has client data
if [ -d "$DATA_DIR" ]; then
    echo -e "${GREEN}✓ Data directory exists: $DATA_DIR${NC}"
    
    # Count existing files
    CLIENT_COUNT=$(find "$DATA_DIR" -type d -maxdepth 1 -mindepth 1 | wc -l)
    JSON_COUNT=$(find "$DATA_DIR" -name "*.json" -type f | wc -l)
    
    echo -e "${GREEN}  Found $CLIENT_COUNT client directories${NC}"
    echo -e "${GREEN}  Found $JSON_COUNT JSON files${NC}"
    
    if [ $JSON_COUNT -eq 0 ]; then
        echo -e "${YELLOW}  Warning: No JSON files found. Clients may not have sent data yet.${NC}"
    fi
else
    echo -e "${RED}✗ Data directory not found: $DATA_DIR${NC}"
    echo -e "${YELLOW}  This should have been created by client SCP transfers.${NC}"
    read -p "Create it now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        mkdir -p "$DATA_DIR"
        chmod 750 "$DATA_DIR"
        echo -e "${GREEN}  Created $DATA_DIR${NC}"
    else
        echo -e "${RED}Cannot proceed without data directory${NC}"
        exit 1
    fi
fi

# Check Python version
echo ""
PYTHON_VERSION=$(python3 --version 2>&1 | grep -oP '\d+\.\d+' | head -1)
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$PYTHON_MAJOR" -ge 3 ] && [ "$PYTHON_MINOR" -ge 8 ]; then
    echo -e "${GREEN}✓ Python $PYTHON_VERSION (OK)${NC}"
else
    echo -e "${RED}✗ Python 3.8+ required, found $PYTHON_VERSION${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}Step 2: Creating monitoring_node directory structure...${NC}"
echo ""

# Create base directory
mkdir -p "$MONITOR_DIR"
cd "$MONITOR_DIR"

# Create subdirectories
mkdir -p config
mkdir -p logs
mkdir -p logs/archive
mkdir -p reports
mkdir -p scripts

echo -e "${GREEN}✓ Created directory structure:${NC}"
echo "  $MONITOR_DIR/"
echo "  ├── config/"
echo "  ├── logs/"
echo "  ├── logs/archive/"
echo "  ├── reports/"
echo "  └── scripts/"

echo ""
echo -e "${BLUE}Step 3: Copying monitoring scripts and configs...${NC}"
echo ""

# Get the source directory (where this script is running from)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SOURCE_MONITOR_DIR="$(dirname "$SCRIPT_DIR")"

# Copy scripts
if [ -f "$SOURCE_MONITOR_DIR/scripts/analyze_storage_health.py" ]; then
    cp "$SOURCE_MONITOR_DIR/scripts/analyze_storage_health.py" "$MONITOR_DIR/scripts/"
    chmod +x "$MONITOR_DIR/scripts/analyze_storage_health.py"
    echo -e "${GREEN}✓ Copied analyze_storage_health.py${NC}"
else
    echo -e "${RED}✗ Source script not found: $SOURCE_MONITOR_DIR/scripts/analyze_storage_health.py${NC}"
    exit 1
fi

# Copy supporting Python modules
for module in utils.py data_validator.py alert_handler.py; do
    if [ -f "$SOURCE_MONITOR_DIR/$module" ]; then
        cp "$SOURCE_MONITOR_DIR/$module" "$MONITOR_DIR/"
        echo -e "${GREEN}✓ Copied $module${NC}"
    else
        echo -e "${RED}✗ Module not found: $SOURCE_MONITOR_DIR/$module${NC}"
        exit 1
    fi
done

# Copy configs
for config in analyzer_config.json thresholds.json; do
    if [ -f "$SOURCE_MONITOR_DIR/config/$config" ]; then
        cp "$SOURCE_MONITOR_DIR/config/$config" "$MONITOR_DIR/config/"
        echo -e "${GREEN}✓ Copied config/$config${NC}"
    else
        echo -e "${RED}✗ Config not found: $SOURCE_MONITOR_DIR/config/$config${NC}"
        exit 1
    fi
done

echo ""
echo -e "${BLUE}Step 4: Testing the analyzer with existing data...${NC}"
echo ""

cd "$MONITOR_DIR"

# Run analyzer once
echo -e "${YELLOW}Running analyzer test...${NC}"
python3 scripts/analyze_storage_health.py

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ Analyzer test successful!${NC}"
    
    # Show results
    if [ -f "logs/analyzer.log" ]; then
        echo ""
        echo -e "${BLUE}Last 10 lines of analyzer log:${NC}"
        tail -10 logs/analyzer.log
    fi
    
    echo ""
    echo -e "${BLUE}Generated reports:${NC}"
    ls -lh reports/ 2>/dev/null || echo "  No reports yet (may be normal if no valid data)"
    
else
    echo ""
    echo -e "${RED}✗ Analyzer test failed!${NC}"
    echo -e "${YELLOW}Check logs for details: $MONITOR_DIR/logs/analyzer.log${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}Step 5: Setting up automatic execution with systemd...${NC}"
echo ""

# Create systemd service file
sudo tee /etc/systemd/system/storage-analyzer.service > /dev/null <<EOF
[Unit]
Description=Storage Health Analyzer
After=network.target

[Service]
Type=oneshot
User=admin
WorkingDirectory=$MONITOR_DIR
ExecStart=/usr/bin/python3 $MONITOR_DIR/scripts/analyze_storage_health.py
StandardOutput=append:$MONITOR_DIR/logs/analyzer.log
StandardError=append:$MONITOR_DIR/logs/analyzer.log

[Install]
WantedBy=multi-user.target
EOF

echo -e "${GREEN}✓ Created systemd service${NC}"

# Create systemd timer file (runs every 10 minutes)
sudo tee /etc/systemd/system/storage-analyzer.timer > /dev/null <<EOF
[Unit]
Description=Storage Health Analyzer Timer
Requires=storage-analyzer.service

[Timer]
OnBootSec=2min
OnUnitActiveSec=10min

[Install]
WantedBy=timers.target
EOF

echo -e "${GREEN}✓ Created systemd timer (runs every 10 minutes)${NC}"

# Reload systemd and enable timer
sudo systemctl daemon-reload
sudo systemctl enable storage-analyzer.timer
sudo systemctl start storage-analyzer.timer

echo -e "${GREEN}✓ Enabled and started systemd timer${NC}"

# Show status
echo ""
echo -e "${BLUE}Timer status:${NC}"
sudo systemctl status storage-analyzer.timer --no-pager -l | head -15

echo ""
echo "=========================================="
echo -e "${GREEN}✓ Monitoring node setup complete!${NC}"
echo "=========================================="
echo ""
echo -e "${BLUE}Configuration Summary:${NC}"
echo "  Data directory:    $DATA_DIR"
echo "  Scripts directory: $MONITOR_DIR/scripts"
echo "  Logs directory:    $MONITOR_DIR/logs"
echo "  Reports directory: $MONITOR_DIR/reports"
echo "  Config directory:  $MONITOR_DIR/config"
echo ""
echo -e "${BLUE}Analyzer Schedule:${NC}"
echo "  Runs automatically every 10 minutes via systemd timer"
echo "  First run: 2 minutes after boot"
echo ""
echo -e "${BLUE}Useful Commands:${NC}"
echo "  # Check timer status"
echo "  sudo systemctl status storage-analyzer.timer"
echo ""
echo "  # Check last run status"
echo "  sudo systemctl status storage-analyzer.service"
echo ""
echo "  # View analyzer logs"
echo "  tail -f $MONITOR_DIR/logs/analyzer.log"
echo ""
echo "  # Run analyzer manually"
echo "  cd $MONITOR_DIR && python3 scripts/analyze_storage_health.py"
echo ""
echo "  # View latest report"
echo "  ls -lt $MONITOR_DIR/reports/ | head -5"
echo ""
echo "  # Watch for new client data"
echo "  watch -n 5 'find $DATA_DIR -name \"*.json\" -mmin -15'"
echo ""
echo -e "${GREEN}The analyzer is now running automatically!${NC}"
echo ""
