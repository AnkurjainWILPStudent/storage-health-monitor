# Quick Reference - VirtualBox VM Commands

## Client VMs (user1, user2)

### Initial Setup
```bash
# Run the setup script
cd ~/storage-health-monitor/client_node/scripts
chmod +x setup_client.sh
./setup_client.sh

# Copy SSH key to monitoring node (replace with actual IP)
ssh-copy-id -i ~/.ssh/id_ed25519.pub admin@192.168.56.10
```

### Manual Operations
```bash
# Run disk monitor manually
cd ~/storage-health-monitor/client_node
sudo python3 disk_monitor.py

# Check logs
tail -f ~/storage-health-monitor/client_node/logs/client_log.log

# View cron jobs
crontab -l

# Test SSH connection to monitoring node
ssh -i ~/.ssh/id_ed25519 admin@192.168.56.10 "echo 'Connection OK'"
```

### Systemd Control (if using systemd)
```bash
# Check timer status
sudo systemctl status disk-monitor.timer

# Check service status
sudo systemctl status disk-monitor.service

# View logs
sudo journalctl -u disk-monitor.service -f

# Restart timer
sudo systemctl restart disk-monitor.timer

# Stop monitoring
sudo systemctl stop disk-monitor.timer

# Start monitoring
sudo systemctl start disk-monitor.timer
```

### Troubleshooting
```bash
# Check network connectivity
ping -c 3 192.168.56.10

# Test SSH
ssh -v -i ~/.ssh/id_ed25519 admin@192.168.56.10

# Check disk space
df -h

# Check SMART status
sudo smartctl -H /dev/sda

# View cron execution log
tail -f /tmp/disk_monitor_cron.log

# Check if psutil is installed
python3 -c "import psutil; print(psutil.__version__)"
```

---

## Monitoring VM

### Initial Setup
```bash
# Run the setup script
cd ~/storage-health-monitor/monitoring_node/scripts
chmod +x setup_monitoring.sh
./setup_monitoring.sh
```

### Manual Operations
```bash
# Run analyzer manually
cd ~/storage-health-monitor/monitoring_node
python3 scripts/analyze_storage_health.py

# Check logs
tail -f ~/storage-health-monitor/monitoring_node/logs/analyzer.log

# View received data
ls -lR ~/monitor_data/

# View reports
ls -l ~/storage-health-monitor/monitoring_node/reports/
cat ~/storage-health-monitor/monitoring_node/reports/summary_*.json | jq .

# Watch for new data files
watch -n 5 'find ~/monitor_data -name "*.json" -mmin -10'
```

### Systemd Control
```bash
# Check timer status
sudo systemctl status storage-analyzer.timer

# Check service status
sudo systemctl status storage-analyzer.service

# View logs
sudo journalctl -u storage-analyzer.service -f
sudo journalctl -u storage-analyzer.timer -f

# Restart timer
sudo systemctl restart storage-analyzer.timer

# Stop analyzer
sudo systemctl stop storage-analyzer.timer

# Start analyzer
sudo systemctl start storage-analyzer.timer

# Run service manually (one-shot)
sudo systemctl start storage-analyzer.service
```

### Configuration
```bash
# Edit analyzer config
nano ~/storage-health-monitor/monitoring_node/config/analyzer_config.json

# Edit thresholds
nano ~/storage-health-monitor/monitoring_node/config/thresholds.json

# Reload systemd after config changes
sudo systemctl daemon-reload
```

### Data Management
```bash
# Count received files
find ~/monitor_data -name "*.json" | wc -l

# Find recent files (last 30 minutes)
find ~/monitor_data -name "*.json" -mmin -30

# View a specific client's latest report
ls -lt ~/monitor_data/user1/*.json | head -1 | xargs cat | jq .

# Clean old archive files (older than 30 days)
find ~/storage-health-monitor/monitoring_node/logs/archive -name "*.json" -mtime +30 -delete

# Check disk usage
du -sh ~/monitor_data
du -sh ~/storage-health-monitor/monitoring_node/logs/archive
```

### Troubleshooting
```bash
# Check if analyzer is running
ps aux | grep analyze_storage_health.py

# Test JSON file manually
python3 -c "import json; print(json.load(open('~/monitor_data/user1/file.json')))"

# Check SSH server status
sudo systemctl status ssh

# View authorized SSH keys
cat ~/.ssh/authorized_keys

# Check Python version
python3 --version

# Test analyzer with verbose output
cd ~/storage-health-monitor/monitoring_node
python3 -u scripts/analyze_storage_health.py 2>&1 | tee test.log

# Monitor system resources
htop
# or
top
```

---

## Network Debugging (All VMs)

### Check IP Addresses
```bash
# Show all IP addresses
ip addr show

# Show specific interface
ip addr show eth1

# Test connectivity between VMs
ping -c 3 192.168.56.10  # monitoring node
ping -c 3 192.168.56.11  # user1
ping -c 3 192.168.56.12  # user2
```

### SSH Debugging
```bash
# Test SSH with verbose output
ssh -vvv -i ~/.ssh/id_ed25519 admin@192.168.56.10

# Check SSH key permissions
ls -la ~/.ssh/
# id_ed25519 should be 600
# id_ed25519.pub should be 644
chmod 600 ~/.ssh/id_ed25519
chmod 644 ~/.ssh/id_ed25519.pub

# Test SCP
echo "test" > /tmp/test.txt
scp -i ~/.ssh/id_ed25519 /tmp/test.txt admin@192.168.56.10:/tmp/
```

### Firewall
```bash
# Check if firewall is active
sudo ufw status

# Allow SSH (if firewall is enabled)
sudo ufw allow 22/tcp
```

---

## Monitoring & Verification

### End-to-End Test
```bash
# On client (user1):
sudo python3 ~/storage-health-monitor/client_node/disk_monitor.py

# On monitoring node:
# Wait ~10 seconds, then check:
find ~/monitor_data/user1 -name "*.json" -mmin -1

# Should show a file with recent timestamp
# Then run analyzer:
cd ~/storage-health-monitor/monitoring_node
python3 scripts/analyze_storage_health.py

# Check the report:
ls -lt ~/storage-health-monitor/monitoring_node/reports/ | head -5
```

### View Logs in Real-Time
```bash
# Terminal 1 - Monitor client logs
tail -f ~/storage-health-monitor/client_node/logs/client_log.log

# Terminal 2 - Monitor analyzer logs  
tail -f ~/storage-health-monitor/monitoring_node/logs/analyzer.log

# Terminal 3 - Watch for new files
watch -n 2 'ls -lht ~/monitor_data/*/  | head -20'
```

### Performance Monitoring
```bash
# Check system load
uptime

# Check disk I/O
iostat -x 1

# Check network traffic
sudo iftop -i eth1

# Check process CPU/memory
ps aux | grep python3
```

---

## Configuration Examples

### Change Collection Interval to 10 Minutes

**Client (cron method):**
```bash
crontab -e
# Change from */5 to */10
```

**Client (systemd method):**
```bash
sudo nano /etc/systemd/system/disk-monitor.timer
# Change OnUnitActiveSec=5min to OnUnitActiveSec=10min
sudo systemctl daemon-reload
sudo systemctl restart disk-monitor.timer
```

**Monitoring (systemd):**
```bash
sudo nano /etc/systemd/system/storage-analyzer.timer
# Change OnUnitActiveSec=10min to OnUnitActiveSec=10min
sudo systemctl daemon-reload
sudo systemctl restart storage-analyzer.timer
```

### Enable Email Alerts

Edit `~/storage-health-monitor/monitoring_node/config/analyzer_config.json`:
```json
{
  "alert_config": {
    "email": {
      "smtp_server": "smtp.gmail.com",
      "smtp_port": 587,
      "use_tls": true,
      "sender": "your-email@gmail.com",
      "recipients": ["admin@example.com"],
      "subject_prefix": "[Storage Alert]"
    }
  }
}
```

Edit `~/storage-health-monitor/monitoring_node/config/thresholds.json`:
```json
{
  "alerts": {
    "enable_email": true
  }
}
```

---

## Useful One-Liners

```bash
# Count files per client
find ~/monitor_data -name "*.json" | sed 's|.*/\([^/]*\)/[^/]*$|\1|' | sort | uniq -c

# Show latest report from each client
for client in ~/monitor_data/*/; do echo "=== $client ==="; ls -t "$client"*.json 2>/dev/null | head -1; done

# Check if data is flowing (last 5 minutes)
find ~/monitor_data -name "*.json" -mmin -5 -exec echo "Recent: {}" \;

# Archive old reports manually
find ~/monitor_data -name "*.json" -mtime +7 -exec mv {} ~/storage-health-monitor/monitoring_node/logs/archive/ \;

# Pretty-print a JSON report
cat ~/monitor_data/user1/user1_*.json | python3 -m json.tool

# Extract disk usage from all reports
find ~/monitor_data -name "*.json" -exec sh -c 'echo "{}:"; grep -o "\"percent\": [0-9.]*" {} | head -1' \;

# Check smart status across all reports
grep -r "\"status\":" ~/monitor_data/ | grep -v "OK"
```

---

## Backup & Restore

### Backup Configuration
```bash
# Backup client config
tar -czf ~/backup-client-$(date +%Y%m%d).tar.gz \
  ~/storage-health-monitor/client_node/config.json \
  ~/storage-health-monitor/client_node/logs/

# Backup monitoring config
tar -czf ~/backup-monitoring-$(date +%Y%m%d).tar.gz \
  ~/storage-health-monitor/monitoring_node/config/ \
  ~/storage-health-monitor/monitoring_node/logs/ \
  ~/monitor_data/
```

### Restore Configuration
```bash
# Restore client
tar -xzf ~/backup-client-20251106.tar.gz -C ~

# Restore monitoring
tar -xzf ~/backup-monitoring-20251106.tar.gz -C ~
```

---

## Shutdown/Cleanup

### Stop All Services
```bash
# On clients
sudo systemctl stop disk-monitor.timer
crontab -r  # Remove cron jobs

# On monitoring node
sudo systemctl stop storage-analyzer.timer
```

### Clean All Data
```bash
# On monitoring node
rm -rf ~/monitor_data/*
rm -rf ~/storage-health-monitor/monitoring_node/logs/archive/*
rm -rf ~/storage-health-monitor/monitoring_node/reports/*
rm -f ~/storage-health-monitor/monitoring_node/logs/*.log
```

### Remove Services
```bash
# Client
sudo systemctl stop disk-monitor.timer
sudo systemctl disable disk-monitor.timer
sudo rm /etc/systemd/system/disk-monitor.*

# Monitoring
sudo systemctl stop storage-analyzer.timer
sudo systemctl disable storage-analyzer.timer
sudo rm /etc/systemd/system/storage-analyzer.*

# Reload systemd
sudo systemctl daemon-reload
```
