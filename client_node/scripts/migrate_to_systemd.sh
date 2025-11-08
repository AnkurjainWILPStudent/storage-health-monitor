#!/bin/bash

# Quick script to remove cron jobs and switch to systemd timer
# Run this on BOTH clientnode and clientnode2 VMs

set -e

echo "=========================================="
echo "Migrating from Cron to Systemd Timer"
echo "=========================================="
echo

# Check if running as the correct user (not root)
if [ "$EUID" -eq 0 ]; then
   echo "Error: Do not run as root. Run as regular user (user1)"
   exit 1
fi

# Remove cron jobs
echo "Step 1: Removing existing cron jobs..."
crontab -l > /tmp/crontab.backup 2>/dev/null || true
if grep -q "disk_monitor.py" /tmp/crontab.backup 2>/dev/null; then
    echo "Found existing disk_monitor cron jobs. Removing..."
    crontab -l | grep -v "disk_monitor.py" | crontab - || crontab -r
    echo "✓ Cron jobs removed (backup saved to /tmp/crontab.backup)"
else
    echo "✓ No disk_monitor cron jobs found"
fi

echo
echo "Step 2: Setting up systemd timer..."
cd ~/project-root/client_node
bash scripts/setup_client.sh

echo
echo "=========================================="
echo "Migration Complete!"
echo "=========================================="
echo
echo "Verification:"
echo "  systemctl status disk-monitor.timer"
echo "  systemctl list-timers disk-monitor.timer"
echo
echo "View logs:"
echo "  journalctl -u disk-monitor.service -f"
echo
