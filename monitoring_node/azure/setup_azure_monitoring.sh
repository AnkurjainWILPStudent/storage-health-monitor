#!/bin/bash
# Azure Storage Monitoring - Setup Script
# Deploys Azure monitoring to monitoring node

set -e

echo "=========================================="
echo "Azure Storage Monitoring - Setup"
echo "=========================================="

# Configuration
INSTALL_DIR="/home/admin/monitoring_node/azure"
SCRIPTS_DIR="$INSTALL_DIR/scripts"
REPORTS_DIR="/home/admin/azure_reports"
LOG_DIR="/home/admin/monitoring_node/logs"
SYSTEMD_DIR="/etc/systemd/system"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# Check if running as root for systemd setup
if [ "$EUID" -ne 0 ] && [ "$1" != "--skip-systemd" ]; then
    warn "Not running as root. Systemd services will not be installed."
    warn "Run with sudo to install systemd services, or use --skip-systemd"
    read -p "Continue without systemd? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
    SKIP_SYSTEMD=true
else
    SKIP_SYSTEMD=false
fi

# Step 1: Check prerequisites
info "Checking prerequisites..."

if ! command -v python3 &> /dev/null; then
    error "Python 3 not found. Please install Python 3.8+"
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
info "Python version: $PYTHON_VERSION"

# Step 2: Install Azure SDK
info "Installing Azure SDK..."
python3 -m pip install --upgrade azure-identity azure-storage-blob || error "Failed to install Azure SDK"
info "✓ Azure SDK installed"

# Step 3: Create directories
info "Creating directories..."
mkdir -p "$REPORTS_DIR" || error "Failed to create reports directory"
mkdir -p "$LOG_DIR" || warn "Log directory already exists"
info "✓ Directories created"

# Step 4: Check configuration
info "Checking configuration..."
if [ ! -f "$INSTALL_DIR/azure_config.json" ]; then
    error "azure_config.json not found in $INSTALL_DIR"
fi

# Validate config (check for placeholders)
if grep -q "YOUR_TENANT_ID" "$INSTALL_DIR/azure_config.json"; then
    error "azure_config.json contains placeholder values. Please update with real credentials."
fi

info "✓ Configuration file found"

# Step 5: Make scripts executable
info "Making scripts executable..."
chmod +x "$SCRIPTS_DIR"/*.py || warn "Failed to set execute permissions"
info "✓ Scripts are executable"

# Step 6: Test Azure connection
info "Testing Azure connection..."
python3 "$SCRIPTS_DIR/test_azure_connection.py" --config "$INSTALL_DIR/azure_config.json"
if [ $? -ne 0 ]; then
    error "Azure connection test failed. Please check credentials."
fi
info "✓ Azure connection successful"

# Step 7: Create systemd services (if not skipped)
if [ "$SKIP_SYSTEMD" = false ]; then
    info "Creating systemd services..."
    
    # Finance monitoring service (every 5 minutes)
    cat > "$SYSTEMD_DIR/azure-finance-monitor.service" << 'EOF'
[Unit]
Description=Azure Finance Storage Monitor
After=network.target

[Service]
Type=oneshot
User=admin
WorkingDirectory=/home/admin/monitoring_node/azure
ExecStart=/usr/bin/python3 /home/admin/monitoring_node/azure/scripts/monitor_azure_storage.py --config /home/admin/monitoring_node/azure/azure_config.json
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

    # Finance monitoring timer
    cat > "$SYSTEMD_DIR/azure-finance-monitor.timer" << 'EOF'
[Unit]
Description=Azure Finance Storage Monitor Timer
Requires=azure-finance-monitor.service

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
AccuracySec=1s

[Install]
WantedBy=timers.target
EOF

    # Marketing monitoring service (every 10 minutes)
    cat > "$SYSTEMD_DIR/azure-marketing-monitor.service" << 'EOF'
[Unit]
Description=Azure Marketing Storage Monitor
After=network.target

[Service]
Type=oneshot
User=admin
WorkingDirectory=/home/admin/monitoring_node/azure
ExecStart=/usr/bin/python3 /home/admin/monitoring_node/azure/scripts/monitor_azure_storage.py --config /home/admin/monitoring_node/azure/azure_config.json
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

    # Marketing monitoring timer
    cat > "$SYSTEMD_DIR/azure-marketing-monitor.timer" << 'EOF'
[Unit]
Description=Azure Marketing Storage Monitor Timer
Requires=azure-marketing-monitor.service

[Timer]
OnBootSec=3min
OnUnitActiveSec=10min
AccuracySec=1s

[Install]
WantedBy=timers.target
EOF

    # Reload systemd
    systemctl daemon-reload
    
    # Enable and start timers
    systemctl enable azure-finance-monitor.timer
    systemctl enable azure-marketing-monitor.timer
    systemctl start azure-finance-monitor.timer
    systemctl start azure-marketing-monitor.timer
    
    info "✓ Systemd services created and started"
    
    # Show timer status
    echo ""
    info "Timer Status:"
    systemctl list-timers azure-* --no-pager
else
    info "Skipping systemd service creation"
fi

# Step 8: Summary
echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Config file: $INSTALL_DIR/azure_config.json"
echo "  Scripts: $SCRIPTS_DIR/"
echo "  Reports: $REPORTS_DIR/"
echo "  Logs: $LOG_DIR/azure_monitor.log"
echo ""

if [ "$SKIP_SYSTEMD" = false ]; then
    echo "Monitoring Schedule:"
    echo "  Finance: Every 5 minutes"
    echo "  Marketing: Every 10 minutes"
    echo ""
    echo "Systemd Commands:"
    echo "  View logs: sudo journalctl -u azure-finance-monitor -f"
    echo "  Check status: sudo systemctl status azure-finance-monitor.timer"
    echo "  Stop monitoring: sudo systemctl stop azure-finance-monitor.timer"
    echo "  Start monitoring: sudo systemctl start azure-finance-monitor.timer"
else
    echo "Manual Testing:"
    echo "  python3 $SCRIPTS_DIR/monitor_azure_storage.py"
fi

echo ""
echo "Next Steps:"
echo "  1. Monitor logs: tail -f $LOG_DIR/azure_monitor.log"
echo "  2. Check reports: ls -lh $REPORTS_DIR/"
echo "  3. Upload test data: python3 $SCRIPTS_DIR/upload_test_data.py --help"
echo ""
echo "=========================================="
