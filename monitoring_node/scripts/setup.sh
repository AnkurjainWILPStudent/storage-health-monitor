#!/bin/bash
#
# Storage Health Monitor - Setup Script
# Sets up the monitoring node environment for production Linux deployment
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
INSTALL_DIR="/opt/storage-health-monitor"
LOG_DIR="/var/log/storage-health-monitor"
DATA_DIR="/home/admin/monitor_data"
SYSTEMD_DIR="/etc/systemd/system"
SERVICE_USER="admin"

echo -e "${GREEN}Storage Health Monitor - Setup Script${NC}"
echo "========================================"
echo ""

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo -e "${RED}Error: This script must be run as root${NC}"
   exit 1
fi

echo "Installation directory: $INSTALL_DIR"
echo "Log directory: $LOG_DIR"
echo "Data directory: $DATA_DIR"
echo "Service user: $SERVICE_USER"
echo ""

# Create directories
echo -e "${YELLOW}Creating directories...${NC}"
mkdir -p "$LOG_DIR"
mkdir -p "$LOG_DIR/archive"
mkdir -p "$LOG_DIR/reports"
mkdir -p "$DATA_DIR"
mkdir -p "$INSTALL_DIR"

# Set permissions
echo -e "${YELLOW}Setting permissions...${NC}"
chown -R $SERVICE_USER:$SERVICE_USER "$LOG_DIR"
chown -R $SERVICE_USER:$SERVICE_USER "$DATA_DIR"
chown -R $SERVICE_USER:$SERVICE_USER "$INSTALL_DIR"
chmod 755 "$LOG_DIR"
chmod 755 "$DATA_DIR"
chmod 755 "$INSTALL_DIR"

# Copy application files
echo -e "${YELLOW}Copying application files...${NC}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

rsync -av --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' \
    "$PROJECT_ROOT/" "$INSTALL_DIR/"

# Make scripts executable
chmod +x "$INSTALL_DIR/monitoring_node/scripts/analyze_storage_health.py"
chmod +x "$INSTALL_DIR/monitoring_node/scripts/run_analyzer.sh"

# Install Python dependencies if requirements.txt exists
if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    pip3 install -r "$PROJECT_ROOT/requirements.txt"
fi

# Update config file paths
echo -e "${YELLOW}Updating configuration...${NC}"
CONFIG_FILE="$INSTALL_DIR/monitoring_node/config/analyzer_config.json"
if [ -f "$CONFIG_FILE" ]; then
    # Update paths in config (this is a simple sed replacement, adjust as needed)
    sed -i "s|/home/admin/monitor_data|$DATA_DIR|g" "$CONFIG_FILE"
    sed -i "s|/var/log/storage-health-monitor|$LOG_DIR|g" "$CONFIG_FILE"
fi

# Install systemd service
echo -e "${YELLOW}Installing systemd service...${NC}"
cp "$INSTALL_DIR/monitoring_node/scripts/storage-analyzer.service" "$SYSTEMD_DIR/"
cp "$INSTALL_DIR/monitoring_node/scripts/storage-analyzer.timer" "$SYSTEMD_DIR/"

# Update service file with actual installation path
sed -i "s|/opt/storage-health-monitor|$INSTALL_DIR|g" "$SYSTEMD_DIR/storage-analyzer.service"
sed -i "s|User=admin|User=$SERVICE_USER|g" "$SYSTEMD_DIR/storage-analyzer.service"
sed -i "s|Group=admin|Group=$SERVICE_USER|g" "$SYSTEMD_DIR/storage-analyzer.service"

# Reload systemd
systemctl daemon-reload

echo ""
echo -e "${GREEN}Setup completed successfully!${NC}"
echo ""
echo "Next steps:"
echo "1. Review configuration files:"
echo "   - $INSTALL_DIR/monitoring_node/config/analyzer_config.json"
echo "   - $INSTALL_DIR/monitoring_node/config/thresholds.json"
echo ""
echo "2. Start and enable the service:"
echo "   sudo systemctl start storage-analyzer.timer"
echo "   sudo systemctl enable storage-analyzer.timer"
echo ""
echo "3. Check service status:"
echo "   sudo systemctl status storage-analyzer.timer"
echo "   sudo systemctl status storage-analyzer.service"
echo ""
echo "4. View logs:"
echo "   sudo journalctl -u storage-analyzer.service -f"
echo "   tail -f $LOG_DIR/analyzer.log"
echo ""
echo "5. Test the analyzer manually:"
echo "   sudo -u $SERVICE_USER $INSTALL_DIR/monitoring_node/scripts/run_analyzer.sh"
echo ""
