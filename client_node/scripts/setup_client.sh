#!/bin/bash

#
# Client Node Setup Script for VirtualBox VMs
# Run this on user1 and user2 VMs
#

set -e

echo "====================================="
echo "Storage Health Monitor - Client Setup"
echo "====================================="

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CLIENT_DIR="$(dirname "$SCRIPT_DIR")"

echo -e "${GREEN}Client directory: $CLIENT_DIR${NC}"

# Check if running as root
if [ "$EUID" -eq 0 ]; then
   echo -e "${RED}Please do not run as root. The script will ask for sudo when needed.${NC}"
   exit 1
fi

# Step 1: Install system dependencies
echo ""
echo "Step 1: Installing system dependencies..."
sudo apt-get update
sudo apt-get install -y python3 python3-pip smartmontools lsblk openssh-client

# Step 2: Install Python dependencies
echo ""
echo "Step 2: Installing Python dependencies..."
pip3 install --user -r "$CLIENT_DIR/requirements.txt"

# Step 3: Create log directory
echo ""
echo "Step 3: Creating log directory..."
mkdir -p "$CLIENT_DIR/logs"
chmod 755 "$CLIENT_DIR/logs"

# Step 4: Configure config.json
echo ""
echo "Step 4: Configuring client..."

# Check if config.json exists
if [ ! -f "$CLIENT_DIR/config.json" ]; then
    echo -e "${YELLOW}config.json not found. Creating from template...${NC}"
    
    # Prompt for monitoring node details
    read -p "Enter monitoring node IP address [192.168.56.10]: " MONITOR_IP
    MONITOR_IP=${MONITOR_IP:-192.168.56.10}
    
    read -p "Enter monitoring node username [admin]: " MONITOR_USER
    MONITOR_USER=${MONITOR_USER:-admin}
    
    read -p "Enter monitoring node data directory [/home/admin/monitor_data]: " MONITOR_DIR
    MONITOR_DIR=${MONITOR_DIR:-/home/admin/monitor_data}
    
    # Get current user
    CURRENT_USER=$(whoami)
    
    # Create config.json
    cat > "$CLIENT_DIR/config.json" <<EOF
{
  "monitoring_node_ip": "$MONITOR_IP",
  "monitoring_node_user": "$MONITOR_USER",
  "monitoring_node_receive_dir": "$MONITOR_DIR",
  "port": 22,
  "interval_minutes": 5,
  "log_path": "$CLIENT_DIR/logs/client_log.log",
  "use_internal_ip_for_monitoring": false,
  "keep_local_copy": false
}
EOF
    echo -e "${GREEN}Created config.json${NC}"
else
    echo -e "${GREEN}config.json already exists${NC}"
fi

# Step 5: Setup SSH key
echo ""
echo "Step 5: Setting up SSH key..."

SSH_KEY="$HOME/.ssh/id_ed25519"
if [ ! -f "$SSH_KEY" ]; then
    echo -e "${YELLOW}SSH key not found. Generating...${NC}"
    ssh-keygen -t ed25519 -C "client-to-monitoring" -f "$SSH_KEY" -N ""
    echo -e "${GREEN}SSH key generated${NC}"
    
    echo ""
    echo -e "${YELLOW}IMPORTANT: Copy the SSH key to monitoring node:${NC}"
    MONITOR_IP=$(grep -oP '"monitoring_node_ip":\s*"\K[^"]+' "$CLIENT_DIR/config.json")
    MONITOR_USER=$(grep -oP '"monitoring_node_user":\s*"\K[^"]+' "$CLIENT_DIR/config.json")
    echo "Run: ssh-copy-id -i $SSH_KEY.pub $MONITOR_USER@$MONITOR_IP"
else
    echo -e "${GREEN}SSH key already exists at $SSH_KEY${NC}"
fi

# Step 6: Test the client
echo ""
echo "Step 6: Testing client..."
echo -e "${YELLOW}Running disk_monitor.py (requires sudo)...${NC}"

cd "$CLIENT_DIR"
sudo python3 disk_monitor.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}Test run successful!${NC}"
    echo "Check logs: tail -f $CLIENT_DIR/logs/client_log.log"
else
    echo -e "${RED}Test run failed. Check logs for details.${NC}"
fi

# Step 7: Setup cron job
echo ""
read -p "Do you want to set up automatic monitoring with cron? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    CRON_JOB="*/5 * * * * cd $CLIENT_DIR && sudo /usr/bin/python3 disk_monitor.py >> /tmp/disk_monitor_cron.log 2>&1"
    
    # Check if cron job already exists
    if crontab -l 2>/dev/null | grep -q "disk_monitor.py"; then
        echo -e "${YELLOW}Cron job already exists${NC}"
    else
        (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
        echo -e "${GREEN}Cron job added successfully${NC}"
        echo "The disk monitor will run every 5 minutes"
    fi
fi

# Step 8: Setup systemd (alternative to cron)
echo ""
read -p "Do you want to set up systemd service instead of cron? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Creating systemd service..."
    
    sudo tee /etc/systemd/system/disk-monitor.service > /dev/null <<EOF
[Unit]
Description=Storage Health Monitor - Disk Monitor Client
After=network.target

[Service]
Type=oneshot
User=root
WorkingDirectory=$CLIENT_DIR
ExecStart=/usr/bin/python3 $CLIENT_DIR/disk_monitor.py
StandardOutput=append:/tmp/disk_monitor.log
StandardError=append:/tmp/disk_monitor.log

[Install]
WantedBy=multi-user.target
EOF

    sudo tee /etc/systemd/system/disk-monitor.timer > /dev/null <<EOF
[Unit]
Description=Storage Health Monitor - Disk Monitor Timer
Requires=disk-monitor.service

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable disk-monitor.timer
    sudo systemctl start disk-monitor.timer
    
    echo -e "${GREEN}Systemd service and timer created and started${NC}"
    echo "Check status: sudo systemctl status disk-monitor.timer"
fi

# Final summary
echo ""
echo "====================================="
echo -e "${GREEN}Client setup complete!${NC}"
echo "====================================="
echo ""
echo "Next steps:"
echo "1. Copy SSH key to monitoring node (if not done yet):"
MONITOR_IP=$(grep -oP '"monitoring_node_ip":\s*"\K[^"]+' "$CLIENT_DIR/config.json")
MONITOR_USER=$(grep -oP '"monitoring_node_user":\s*"\K[^"]+' "$CLIENT_DIR/config.json")
echo "   ssh-copy-id -i $SSH_KEY.pub $MONITOR_USER@$MONITOR_IP"
echo ""
echo "2. Test SSH connection:"
echo "   ssh -i $SSH_KEY $MONITOR_USER@$MONITOR_IP 'echo Connection OK'"
echo ""
echo "3. Monitor logs:"
echo "   tail -f $CLIENT_DIR/logs/client_log.log"
echo ""
echo "4. Manual test run:"
echo "   cd $CLIENT_DIR && sudo python3 disk_monitor.py"
echo ""
