# VirtualBox VM Setup Guide

## Overview
This guide covers setting up a 3-VM storage health monitoring system in VirtualBox:
- **1 Monitoring Node VM**: Collects and analyzes storage reports
- **2 Client VMs (user1, user2)**: Generate storage health reports and send to monitoring node

## Architecture

```
┌─────────────────┐       SSH/SCP        ┌──────────────────┐
│   Client VM     │─────────────────────▶│  Monitoring VM   │
│    (user1)      │   JSON Reports       │                  │
└─────────────────┘                      │  - Analyzer      │
                                         │  - Alerting      │
┌─────────────────┐       SSH/SCP        │  - Logs/Reports  │
│   Client VM     │─────────────────────▶│                  │
│    (user2)      │   JSON Reports       └──────────────────┘
└─────────────────┘
```

## Prerequisites

- VirtualBox installed on your host machine
- 3 Linux VMs (Ubuntu 20.04+ or similar) with:
  - Network configured (NAT + Host-Only or Bridged)
  - SSH server installed and running
  - Python 3.8+ installed
  - `lsblk` and `smartctl` available on client VMs
  - SSH key-based authentication configured

## VM Network Configuration

### Option 1: Host-Only Network (Recommended for Testing)
1. Create a Host-Only network in VirtualBox: File → Host Network Manager → Create
2. Configure each VM with two adapters:
   - **Adapter 1**: NAT (for internet access)
   - **Adapter 2**: Host-Only (for internal communication)

### Option 2: Bridged Network
- Single adapter in Bridged mode (VMs get IPs from your local network)

### Example IP Configuration
```
Monitoring VM:  192.168.56.10 (hostname: monitoring-node)
Client VM 1:    192.168.56.11 (hostname: user1)
Client VM 2:    192.168.56.12 (hostname: user2)
```

**Update `/etc/hosts` on all VMs:**
```bash
sudo nano /etc/hosts
```
Add:
```
192.168.56.10   monitoring-node
192.168.56.11   user1
192.168.56.12   user2
```

## Step 1: SSH Key Setup

### On Each Client VM (user1, user2):

1. Generate SSH key pair:
```bash
ssh-keygen -t ed25519 -C "client-to-monitoring" -f ~/.ssh/id_ed25519 -N ""
```

2. Copy public key to monitoring node:
```bash
ssh-copy-id -i ~/.ssh/id_ed25519.pub admin@192.168.56.10
```
Replace `admin` with the actual username on monitoring node.

3. Test connection:
```bash
ssh -i ~/.ssh/id_ed25519 admin@192.168.56.10 "echo 'Connection successful'"
```

## Step 2: Deploy Client Node (user1 and user2)

### On each client VM:

1. Clone/copy the repository:
```bash
cd ~
mkdir -p storage-health-monitor
cd storage-health-monitor
```

2. Copy the `client_node` directory contents to the VM

3. Install Python dependencies:
```bash
cd ~/storage-health-monitor/client_node
pip3 install -r requirements.txt
# Or system-wide:
sudo apt-get update
sudo apt-get install -y python3-pip python3-psutil
```

4. Install `smartmontools`:
```bash
sudo apt-get install -y smartmontools
```

5. Configure the client:
```bash
nano config.json
```

Update with your monitoring node details:
```json
{
  "monitoring_node_ip": "192.168.56.10",
  "monitoring_node_user": "admin",
  "monitoring_node_receive_dir": "/home/admin/monitor_data",
  "port": 22,
  "interval_minutes": 5,
  "log_path": "/home/user/storage-health-monitor/client_node/logs/client_log.log",
  "use_internal_ip_for_monitoring": false,
  "keep_local_copy": false
}
```

6. Test the disk monitor manually:
```bash
sudo python3 disk_monitor.py
```
Check logs:
```bash
tail -f logs/client_log.log
```

7. Set up cron job for automatic monitoring:
```bash
crontab -e
```
Add:
```cron
# Run disk monitor every 5 minutes
*/5 * * * * cd /home/user/storage-health-monitor/client_node && sudo /usr/bin/python3 disk_monitor.py >> /tmp/disk_monitor_cron.log 2>&1
```

Or use systemd (see `client_node/scripts/setup_systemd.sh`)

## Step 3: Deploy Monitoring Node

### On monitoring VM:

1. Clone/copy the repository:
```bash
cd ~
mkdir -p storage-health-monitor
cd storage-health-monitor
```

2. Copy the `monitoring_node` directory contents to the VM

3. Create data receive directory:
```bash
mkdir -p ~/monitor_data
chmod 750 ~/monitor_data
```

4. Install Python dependencies:
```bash
cd ~/storage-health-monitor/monitoring_node
pip3 install -r requirements.txt
```

5. Configure the analyzer:
```bash
nano config/analyzer_config.json
```

Update paths as needed:
```json
{
  "data_directory": "/home/admin/monitor_data",
  "log_path": "/home/admin/storage-health-monitor/monitoring_node/logs/analyzer.log",
  "output_directory": "/home/admin/storage-health-monitor/monitoring_node/reports",
  ...
}
```

6. Configure thresholds:
```bash
nano config/thresholds.json
```
Adjust warning/critical levels as needed.

7. Test the analyzer manually:
```bash
cd ~/storage-health-monitor/monitoring_node
python3 scripts/analyze_storage_health.py
```

Check logs:
```bash
tail -f logs/analyzer.log
```

8. Set up systemd service (automated):
```bash
cd ~/storage-health-monitor/monitoring_node/scripts
sudo bash setup.sh
```

Or manually:
```bash
sudo cp scripts/storage-analyzer.service /etc/systemd/system/
sudo cp scripts/storage-analyzer.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable storage-analyzer.timer
sudo systemctl start storage-analyzer.timer
```

9. Check service status:
```bash
sudo systemctl status storage-analyzer.timer
sudo systemctl status storage-analyzer.service
```

## Step 4: Testing the Complete System

### Test 1: Manual Client Run

On **user1**:
```bash
cd ~/storage-health-monitor/client_node
sudo python3 disk_monitor.py
```

Check if file appears on **monitoring node**:
```bash
ls -lR ~/monitor_data/
```
You should see: `~/monitor_data/user1/user1_<timestamp>.json`

### Test 2: Analyzer Processing

On **monitoring node**:
```bash
cd ~/storage-health-monitor/monitoring_node
python3 scripts/analyze_storage_health.py
```

Check output:
```bash
cat logs/analyzer.log
ls -l reports/
```

### Test 3: End-to-End Flow

1. Wait for cron job to run on clients (or trigger manually)
2. Check monitoring node receives files:
```bash
watch -n 5 'find ~/monitor_data -name "*.json" -mmin -10'
```
3. Check analyzer processes them:
```bash
tail -f ~/storage-health-monitor/monitoring_node/logs/analyzer.log
```

## Step 5: Monitoring and Troubleshooting

### Check Client Logs
```bash
# On user1 or user2
tail -f ~/storage-health-monitor/client_node/logs/client_log.log
```

### Check Analyzer Logs
```bash
# On monitoring node
tail -f ~/storage-health-monitor/monitoring_node/logs/analyzer.log
```

### Check Network Connectivity
```bash
# From client to monitoring
ping -c 3 192.168.56.10
ssh -i ~/.ssh/id_ed25519 admin@192.168.56.10 "echo 'SSH OK'"
```

### Common Issues

**Issue**: SCP fails with "Permission denied"
- **Solution**: Ensure SSH keys are properly configured and `~/.ssh/authorized_keys` on monitoring node has correct permissions (600)

**Issue**: "smartctl: command not found"
- **Solution**: Install smartmontools: `sudo apt-get install smartmontools`

**Issue**: Analyzer doesn't process files
- **Solution**: Check `data_directory` path in `analyzer_config.json` matches where clients upload files

**Issue**: No files in monitor_data
- **Solution**: Check client logs, verify network connectivity, check SSH key authentication

## File Structure on VMs

### Client VMs (user1, user2):
```
~/storage-health-monitor/
├── client_node/
│   ├── disk_monitor.py
│   ├── utils.py
│   ├── config.json
│   ├── requirements.txt
│   ├── logs/
│   │   └── client_log.log
│   └── scripts/
│       └── setup_systemd.sh
└── .ssh/
    └── id_ed25519
```

### Monitoring VM:
```
~/storage-health-monitor/
├── monitoring_node/
│   ├── scripts/
│   │   ├── analyze_storage_health.py
│   │   ├── run_analyzer.sh
│   │   └── setup.sh
│   ├── config/
│   │   ├── analyzer_config.json
│   │   └── thresholds.json
│   ├── utils.py
│   ├── data_validator.py
│   ├── alert_handler.py
│   ├── requirements.txt
│   ├── logs/
│   │   └── analyzer.log
│   └── reports/
│       └── summary_*.json
└── monitor_data/
    ├── user1/
    │   └── user1_<timestamp>.json
    └── user2/
        └── user2_<timestamp>.json
```

## Advanced Configuration

### Change Collection Interval

**Client side** - Edit crontab:
```bash
crontab -e
# Change from */5 to */10 for 10-minute intervals
```

**Monitoring side** - Edit timer:
```bash
sudo nano /etc/systemd/system/storage-analyzer.timer
# Change OnUnitActiveSec value
sudo systemctl daemon-reload
sudo systemctl restart storage-analyzer.timer
```

### Enable Email Alerts

Edit `monitoring_node/config/analyzer_config.json`:
```json
{
  "alert_config": {
    "email": {
      "smtp_server": "smtp.gmail.com",
      "smtp_port": 587,
      "use_tls": true,
      "sender": "your-email@gmail.com",
      "recipients": ["admin@example.com"]
    }
  }
}
```

Update `config/thresholds.json`:
```json
{
  "alerts": {
    "enable_email": true,
    "enable_console": true
  }
}
```

### Custom Thresholds

Edit `monitoring_node/config/thresholds.json` to adjust warning/critical levels for:
- Disk usage percentage
- Inode usage
- SMART attributes
- Data freshness

## Backup and Recovery

### Backup Configuration
```bash
# On each VM
tar -czf ~/storage-monitor-backup-$(date +%Y%m%d).tar.gz \
  ~/storage-health-monitor/client_node/config.json \
  ~/storage-health-monitor/monitoring_node/config/
```

### View Historical Data
```bash
# On monitoring node
cd ~/storage-health-monitor/monitoring_node
ls -lh logs/archive/  # Archived JSON reports
ls -lh reports/       # Analysis summaries
```

## Performance Tips

1. **Limit SMART checks**: SMART checks can be slow. Consider running them less frequently:
   - Modify `disk_monitor.py` to skip SMART or cache results
   
2. **Archive old data**: The analyzer automatically archives processed files. Adjust retention:
   ```json
   "retention_days": 7
   ```

3. **Reduce logging**: Change log level in production:
   ```python
   logger.setLevel(logging.WARNING)
   ```

## Security Considerations

1. **SSH Keys**: Use strong key types (ed25519) and protect private keys
2. **File Permissions**: Ensure logs and data directories have proper permissions (750/640)
3. **Firewall**: Only allow SSH (port 22) between client and monitoring nodes
4. **Sudo Access**: Client script needs sudo for SMART checks - review security implications

## Next Steps

- Set up centralized log aggregation (e.g., rsyslog, ELK stack)
- Add Grafana dashboards for visualization
- Implement webhook alerts to Slack/Discord
- Add more metrics (network I/O, temperature sensors)
- Create automated backup scripts

## Support

Check logs first:
- Client: `~/storage-health-monitor/client_node/logs/client_log.log`
- Monitor: `~/storage-health-monitor/monitoring_node/logs/analyzer.log`

Review the main README.md for detailed API and module documentation.
