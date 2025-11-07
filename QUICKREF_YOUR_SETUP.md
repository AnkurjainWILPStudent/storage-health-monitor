# Quick Reference - Your VirtualBox Environment

## Your VM Configuration

```
Monitoring Node: monitoringnode (192.168.56.10) - User: admin
Client Node 1:   clientnode     (192.168.56.11)
Client Node 2:   clientnode2    (192.168.56.12)

Data Directory:  /home/admin/monitor_data/{clientnode|clientnode2}/
Monitor Base:    /home/admin/monitoring_node/
```

## Setup (One-Time on Monitoring Node)

```bash
# 1. Copy monitoring_node files to the VM
# From your development machine, copy the monitoring_node directory

# 2. SSH to monitoringnode
ssh admin@192.168.56.10

# 3. Run setup script
cd /path/to/monitoring_node/scripts
chmod +x setup_monitoring.sh
./setup_monitoring.sh

# This will:
# - Create /home/admin/monitoring_node/
# - Copy all scripts and configs
# - Test the analyzer with existing data
# - Set up systemd timer (runs every 10 minutes)
```

## Manual Operations

### Run Analyzer Once
```bash
# On monitoringnode
cd /home/admin/monitoring_node
python3 scripts/analyze_storage_health.py
```

### View Logs
```bash
# Live log tail
tail -f /home/admin/monitoring_node/logs/analyzer.log

# Last 50 lines
tail -50 /home/admin/monitoring_node/logs/analyzer.log

# Search for errors
grep -i error /home/admin/monitoring_node/logs/analyzer.log

# Search for alerts
grep -i alert /home/admin/monitoring_node/logs/analyzer.log
```

### View Reports
```bash
# List reports
ls -lt /home/admin/monitoring_node/reports/

# View latest report (with jq)
cat /home/admin/monitoring_node/reports/summary_*.json | jq .

# View latest report (without jq)
cat /home/admin/monitoring_node/reports/summary_*.json | python3 -m json.tool
```

### Check Client Data
```bash
# List all client files
ls -lR /home/admin/monitor_data/

# Count files per client
echo "clientnode: $(find /home/admin/monitor_data/clientnode -name '*.json' 2>/dev/null | wc -l) files"
echo "clientnode2: $(find /home/admin/monitor_data/clientnode2 -name '*.json' 2>/dev/null | wc -l) files"

# Find recent files (last 15 minutes)
find /home/admin/monitor_data -name "*.json" -mmin -15

# Watch for new files (updates every 5 seconds)
watch -n 5 'find /home/admin/monitor_data -name "*.json" -mmin -10 -ls'
```

### Check Archived Data
```bash
# List archived files
ls -lh /home/admin/monitoring_node/logs/archive/

# Count archived files
find /home/admin/monitoring_node/logs/archive -name "*.json" | wc -l

# Find old archives (>30 days)
find /home/admin/monitoring_node/logs/archive -name "*.json" -mtime +30
```

## Systemd Control

### Check Status
```bash
# Check if timer is running
sudo systemctl status storage-analyzer.timer

# Check last analyzer run
sudo systemctl status storage-analyzer.service

# View timer schedule
systemctl list-timers storage-analyzer.timer
```

### Start/Stop/Restart
```bash
# Start timer
sudo systemctl start storage-analyzer.timer

# Stop timer
sudo systemctl stop storage-analyzer.timer

# Restart timer
sudo systemctl restart storage-analyzer.timer

# Trigger immediate run (without waiting for timer)
sudo systemctl start storage-analyzer.service
```

### View Logs
```bash
# View systemd logs
sudo journalctl -u storage-analyzer.service -f

# View last 50 lines
sudo journalctl -u storage-analyzer.service -n 50

# View logs since today
sudo journalctl -u storage-analyzer.service --since today
```

### Modify Schedule
```bash
# Edit timer (change 10min interval)
sudo nano /etc/systemd/system/storage-analyzer.timer

# After editing, reload
sudo systemctl daemon-reload
sudo systemctl restart storage-analyzer.timer
```

## Configuration

### Analyzer Config
```bash
# Edit main config
nano /home/admin/monitoring_node/config/analyzer_config.json

# Key settings:
# - data_directory: /home/admin/monitor_data
# - log_path: /home/admin/monitoring_node/logs/analyzer.log
# - output_directory: /home/admin/monitoring_node/reports
# - archive_directory: /home/admin/monitoring_node/logs/archive
# - retention_days: 30
```

### Thresholds
```bash
# Edit thresholds
nano /home/admin/monitoring_node/config/thresholds.json

# Key thresholds:
# - disk_usage: warning 75%, critical 90%
# - data_freshness: max_age_minutes 15
# - smart: reallocated_sectors, pending_sectors
```

## Monitoring & Troubleshooting

### Real-Time Monitoring (3 terminals)
```bash
# Terminal 1: Watch analyzer logs
tail -f /home/admin/monitoring_node/logs/analyzer.log

# Terminal 2: Watch for new client files
watch -n 5 'find /home/admin/monitor_data -name "*.json" -mmin -15 -ls'

# Terminal 3: Watch reports directory
watch -n 10 'ls -lt /home/admin/monitoring_node/reports/ | head -10'
```

### Common Issues

**No files being processed:**
```bash
# Check if files exist
ls -lR /home/admin/monitor_data/

# Check file pattern in config
grep file_pattern /home/admin/monitoring_node/config/analyzer_config.json
# Should be: "*/*.json" (to match clientnode/*.json pattern)
```

**Analyzer not running automatically:**
```bash
# Check timer status
sudo systemctl status storage-analyzer.timer

# Check if enabled
sudo systemctl is-enabled storage-analyzer.timer

# If not enabled
sudo systemctl enable storage-analyzer.timer
sudo systemctl start storage-analyzer.timer
```

**Python errors:**
```bash
# Check Python path
which python3

# Check if script is executable
ls -l /home/admin/monitoring_node/scripts/analyze_storage_health.py

# Run with explicit python3
python3 /home/admin/monitoring_node/scripts/analyze_storage_health.py
```

**Permission errors:**
```bash
# Check directory permissions
ls -ld /home/admin/monitoring_node
ls -ld /home/admin/monitor_data

# Fix if needed
chmod 755 /home/admin/monitoring_node
chmod 755 /home/admin/monitoring_node/logs
chmod 755 /home/admin/monitoring_node/reports
```

## Verification Commands

### Quick Health Check
```bash
# Run this to verify everything is working
cd /home/admin/monitoring_node/scripts
chmod +x test_analyzer_quick.sh
./test_analyzer_quick.sh
```

### End-to-End Test
```bash
# 1. Check clients are sending data
find /home/admin/monitor_data -name "*.json" -mmin -10

# 2. Run analyzer manually
cd /home/admin/monitoring_node
python3 scripts/analyze_storage_health.py

# 3. Check log for errors
tail -20 logs/analyzer.log

# 4. Verify report was created
ls -lt reports/ | head -3

# 5. Check systemd timer
sudo systemctl status storage-analyzer.timer
```

## Useful One-Liners

```bash
# Count total JSON files
find /home/admin/monitor_data -name "*.json" | wc -l

# Show latest file from each client
for client in /home/admin/monitor_data/*; do 
    echo "=== $(basename $client) ==="; 
    ls -t "$client"/*.json 2>/dev/null | head -1; 
done

# Check disk usage percentage from latest reports
find /home/admin/monitor_data -name "*.json" -mmin -30 -exec sh -c 'echo "{}:"; grep -o "\"percent\": [0-9.]*" {} | head -3' \;

# View summary of all reports
for report in /home/admin/monitoring_node/reports/summary_*.json; do
    echo "=== $(basename $report) ==="
    cat "$report" | python3 -m json.tool | grep -A 3 summary
done

# Check SMART status across all recent files
find /home/admin/monitor_data -name "*.json" -mmin -60 -exec sh -c 'echo "{}:"; grep -o "\"status\": \"[^\"]*\"" {} | head -1' \;

# Find files that haven't been processed (not in archive)
comm -23 <(find /home/admin/monitor_data -name "*.json" | sort) <(find /home/admin/monitoring_node/logs/archive -name "*.json" -exec basename {} \; | sort)
```

## Performance Tuning

### Change Analysis Interval
```bash
# Edit timer
sudo nano /etc/systemd/system/storage-analyzer.timer

# Change OnUnitActiveSec=10min to desired interval (e.g., 5min, 15min)

# Reload
sudo systemctl daemon-reload
sudo systemctl restart storage-analyzer.timer
```

### Archive Cleanup
```bash
# Manual cleanup of old archives (>30 days)
find /home/admin/monitoring_node/logs/archive -name "*.json" -mtime +30 -delete

# Cleanup old reports (>7 days)
find /home/admin/monitoring_node/reports -name "*.json" -mtime +7 -delete
```

## Backup & Maintenance

### Backup Configuration
```bash
# Backup configs and recent data
tar -czf ~/backup-monitoring-$(date +%Y%m%d).tar.gz \
    /home/admin/monitoring_node/config/ \
    /home/admin/monitoring_node/logs/analyzer.log \
    /home/admin/monitoring_node/reports/

# Backup received data
tar -czf ~/backup-client-data-$(date +%Y%m%d).tar.gz \
    /home/admin/monitor_data/
```

### Check Disk Usage
```bash
# Check monitoring node disk usage
du -sh /home/admin/monitoring_node/*
du -sh /home/admin/monitor_data/*

# Detailed breakdown
du -h /home/admin/monitoring_node/logs/archive | tail -1
du -h /home/admin/monitor_data | tail -1
```

## Emergency Commands

### Stop Everything
```bash
sudo systemctl stop storage-analyzer.timer
sudo systemctl stop storage-analyzer.service
```

### Clear All Data (CAREFUL!)
```bash
# Stop analyzer first
sudo systemctl stop storage-analyzer.timer

# Clear archives (keeps source data)
rm -rf /home/admin/monitoring_node/logs/archive/*

# Clear reports
rm -f /home/admin/monitoring_node/reports/*

# Clear logs
> /home/admin/monitoring_node/logs/analyzer.log
```

### Reset to Fresh State
```bash
# Stop services
sudo systemctl stop storage-analyzer.timer
sudo systemctl disable storage-analyzer.timer

# Remove systemd files
sudo rm /etc/systemd/system/storage-analyzer.*
sudo systemctl daemon-reload

# Remove monitoring directory
rm -rf /home/admin/monitoring_node

# Now re-run setup_monitoring.sh
```

## Contact & Help

- Check logs first: `tail -f /home/admin/monitoring_node/logs/analyzer.log`
- Run quick test: `/home/admin/monitoring_node/scripts/test_analyzer_quick.sh`
- Review config: `cat /home/admin/monitoring_node/config/analyzer_config.json`
