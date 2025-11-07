# Deployment Guide - Your Specific Setup

## Your Environment

```
✓ Monitoring VM:  monitoringnode (192.168.56.10) - User: admin
✓ Client VM 1:    clientnode     (192.168.56.11)
✓ Client VM 2:    clientnode2    (192.168.56.12)

✓ Network:        All VMs can communicate
✓ SSH:            Key-based authentication working
✓ Clients:        Already sending JSON data to /home/admin/monitor_data/
✓ Dependencies:   Python3, psutil, smartmontools installed on all VMs
```

## What You Need to Deploy

You only need to set up the **monitoring node analyzer** since clients are already working.

## Step-by-Step Deployment

### Step 1: Copy Files to Monitoring Node

From your development machine (Windows):

```powershell
# Option A: Using SCP (if you have it on Windows)
scp -r monitoring_node admin@192.168.56.10:/tmp/

# Option B: Using WinSCP or similar GUI tool
# - Connect to 192.168.56.10 as admin
# - Upload the entire monitoring_node folder to /tmp/

# Option C: Share folder and copy via VM
# - Set up shared folder in VirtualBox
# - Mount in VM and copy files
```

### Step 2: SSH to Monitoring Node

```bash
ssh admin@192.168.56.10
```

### Step 3: Run Setup Script

```bash
# Go to the scripts directory
cd /tmp/monitoring_node/scripts

# Make setup script executable
chmod +x setup_monitoring.sh

# Run setup
./setup_monitoring.sh
```

**What the script does:**
1. ✓ Verifies your existing setup (data directory, Python version)
2. ✓ Creates `/home/admin/monitoring_node/` directory structure
3. ✓ Copies all scripts, configs, and modules
4. ✓ Tests the analyzer with your existing client data
5. ✓ Sets up systemd timer to run every 10 minutes
6. ✓ Starts the automated analysis

**Expected output:**
```
==========================================
Storage Health Monitor - Monitoring Setup
==========================================

VM: monitoringnode (192.168.56.10)
Clients: clientnode (192.168.56.11), clientnode2 (192.168.56.12)

Step 1: Verifying existing setup...

✓ Data directory exists: /home/admin/monitor_data
  Found 2 client directories
  Found XX JSON files

✓ Python 3.X (OK)

Step 2: Creating monitoring_node directory structure...

✓ Created directory structure:
  /home/admin/monitoring_node/
  ├── config/
  ├── logs/
  ├── logs/archive/
  ├── reports/
  └── scripts/

Step 3: Copying monitoring scripts and configs...

✓ Copied analyze_storage_health.py
✓ Copied utils.py
✓ Copied data_validator.py
✓ Copied alert_handler.py
✓ Copied config/analyzer_config.json
✓ Copied config/thresholds.json

Step 4: Testing the analyzer with existing data...

Running analyzer test...

✓ Analyzer test successful!

Step 5: Setting up automatic execution with systemd...

✓ Created systemd service
✓ Created systemd timer (runs every 10 minutes)
✓ Enabled and started systemd timer

Timer status:
● storage-analyzer.timer - Storage Health Analyzer Timer
   Loaded: loaded
   Active: active (waiting)

==========================================
✓ Monitoring node setup complete!
==========================================
```

### Step 4: Verify It's Working

```bash
# Check that analyzer ran
tail -20 /home/admin/monitoring_node/logs/analyzer.log

# Check for reports
ls -lt /home/admin/monitoring_node/reports/

# Check systemd timer
sudo systemctl status storage-analyzer.timer
```

### Step 5: Monitor Real-Time

Open 2-3 terminals to monitor:

**Terminal 1 - Analyzer logs:**
```bash
ssh admin@192.168.56.10
tail -f /home/admin/monitoring_node/logs/analyzer.log
```

**Terminal 2 - Incoming data:**
```bash
ssh admin@192.168.56.10
watch -n 5 'find /home/admin/monitor_data -name "*.json" -mmin -15 -ls'
```

**Terminal 3 - Reports:**
```bash
ssh admin@192.168.56.10
watch -n 30 'ls -lt /home/admin/monitoring_node/reports/ | head -5'
```

## Verification Checklist

After deployment, verify:

- [ ] Setup script completed without errors
- [ ] `/home/admin/monitoring_node/` directory exists
- [ ] Analyzer log file created: `/home/admin/monitoring_node/logs/analyzer.log`
- [ ] At least one report created: `ls /home/admin/monitoring_node/reports/`
- [ ] Systemd timer is active: `sudo systemctl is-active storage-analyzer.timer`
- [ ] No errors in logs: `grep -i error /home/admin/monitoring_node/logs/analyzer.log`

## Expected Behavior

### Automatic Operation

- **Every 10 minutes**: Systemd timer triggers the analyzer
- **Analyzer**:
  1. Scans `/home/admin/monitor_data/clientnode/` and `/home/admin/monitor_data/clientnode2/`
  2. Validates each JSON file
  3. Checks disk usage, SMART status against thresholds
  4. Generates alerts if thresholds exceeded
  5. Creates summary report in `/home/admin/monitoring_node/reports/`
  6. Archives processed files to `/home/admin/monitoring_node/logs/archive/`

### Directory Structure After Setup

```
/home/admin/
├── monitor_data/                    # Clients send here (already exists)
│   ├── clientnode/
│   │   └── clientnode_*.json
│   └── clientnode2/
│       └── clientnode2_*.json
│
└── monitoring_node/                 # NEW - Analyzer lives here
    ├── scripts/
    │   ├── analyze_storage_health.py
    │   └── test_analyzer_quick.sh
    ├── config/
    │   ├── analyzer_config.json
    │   └── thresholds.json
    ├── logs/
    │   ├── analyzer.log
    │   └── archive/
    │       └── *.json (processed files)
    ├── reports/
    │   └── summary_*.json
    ├── utils.py
    ├── data_validator.py
    └── alert_handler.py
```

## Troubleshooting

### Issue: Setup script fails

**Check:**
```bash
# Verify you're on the monitoring node
hostname  # Should show: monitoringnode

# Verify you're the admin user
whoami    # Should show: admin

# Check if data directory exists
ls -ld /home/admin/monitor_data

# Check Python version
python3 --version  # Should be 3.8+
```

### Issue: No files being processed

**Check:**
```bash
# Are files present?
find /home/admin/monitor_data -name "*.json" | head -10

# Check the file pattern in config
grep file_pattern /home/admin/monitoring_node/config/analyzer_config.json
# Should be: "*/*.json"

# Try running analyzer manually
cd /home/admin/monitoring_node
python3 scripts/analyze_storage_health.py
```

### Issue: Timer not running

**Fix:**
```bash
# Check status
sudo systemctl status storage-analyzer.timer

# If inactive, start it
sudo systemctl start storage-analyzer.timer

# If disabled, enable it
sudo systemctl enable storage-analyzer.timer
sudo systemctl start storage-analyzer.timer

# Verify
sudo systemctl is-active storage-analyzer.timer
```

### Issue: Python errors

**Check:**
```bash
# Run with full error output
cd /home/admin/monitoring_node
python3 -u scripts/analyze_storage_health.py

# Check imports
python3 -c "import sys; print(sys.version)"
python3 -c "import json, os, pathlib; print('OK')"

# Check file paths in config
cat config/analyzer_config.json
```

## Quick Commands Reference

### Manual Operations
```bash
# Run analyzer once
cd /home/admin/monitoring_node && python3 scripts/analyze_storage_health.py

# View last 50 log lines
tail -50 /home/admin/monitoring_node/logs/analyzer.log

# View latest report
ls -t /home/admin/monitoring_node/reports/*.json | head -1 | xargs cat | python3 -m json.tool
```

### Systemd Operations
```bash
# Check timer
sudo systemctl status storage-analyzer.timer

# Check service
sudo systemctl status storage-analyzer.service

# Trigger immediate run
sudo systemctl start storage-analyzer.service

# Stop automatic runs
sudo systemctl stop storage-analyzer.timer

# Start automatic runs
sudo systemctl start storage-analyzer.timer

# View systemd logs
sudo journalctl -u storage-analyzer.service -n 50
```

### Monitoring
```bash
# Watch logs live
tail -f /home/admin/monitoring_node/logs/analyzer.log

# Watch for new client files
watch -n 5 'find /home/admin/monitor_data -name "*.json" -mmin -15'

# Count files
echo "Total JSON files: $(find /home/admin/monitor_data -name '*.json' | wc -l)"
echo "Archived files: $(find /home/admin/monitoring_node/logs/archive -name '*.json' | wc -l)"
```

## Configuration Tuning

### Change Analysis Interval

Edit the timer to run more or less frequently:

```bash
sudo nano /etc/systemd/system/storage-analyzer.timer

# Change this line:
OnUnitActiveSec=10min

# To (example):
OnUnitActiveSec=5min   # Run every 5 minutes
OnUnitActiveSec=15min  # Run every 15 minutes
OnUnitActiveSec=1h     # Run every hour

# Save and reload
sudo systemctl daemon-reload
sudo systemctl restart storage-analyzer.timer
```

### Adjust Thresholds

```bash
nano /home/admin/monitoring_node/config/thresholds.json

# Example changes:
# - disk_usage: warning 80%, critical 95%
# - data_freshness: max_age_minutes 30
```

After changing config, restart the timer:
```bash
sudo systemctl restart storage-analyzer.timer
```

## Maintenance

### View Summary Statistics

```bash
# From latest report
cat /home/admin/monitoring_node/reports/summary_*.json | python3 -m json.tool | grep -A 10 summary
```

### Backup

```bash
# Backup configs and logs
tar -czf ~/monitoring-backup-$(date +%Y%m%d).tar.gz \
    /home/admin/monitoring_node/config \
    /home/admin/monitoring_node/logs/analyzer.log

# Backup reports
tar -czf ~/monitoring-reports-$(date +%Y%m%d).tar.gz \
    /home/admin/monitoring_node/reports
```

### Cleanup Old Archives

```bash
# Clean archives older than 30 days
find /home/admin/monitoring_node/logs/archive -name "*.json" -mtime +30 -delete

# Clean old reports
find /home/admin/monitoring_node/reports -name "*.json" -mtime +7 -delete
```

## Success Indicators

You'll know it's working when:

1. ✓ Systemd timer shows "active (waiting)"
2. ✓ Analyzer log shows regular runs every 10 minutes
3. ✓ Reports directory has recent summary_*.json files
4. ✓ Archive directory has processed client JSON files
5. ✓ No repeated errors in logs

## Next Steps After Deployment

1. **Monitor for 24 hours** to ensure stability
2. **Review thresholds** and adjust based on your actual disk usage
3. **Set up email alerts** (optional) - edit `analyzer_config.json`
4. **Create dashboard** (optional) - parse JSON reports into graphs
5. **Document any custom changes** for your team

## Getting Help

**Check logs first:**
```bash
tail -100 /home/admin/monitoring_node/logs/analyzer.log
```

**Run quick test:**
```bash
/home/admin/monitoring_node/scripts/test_analyzer_quick.sh
```

**Common issues documented in:**
- QUICKREF_YOUR_SETUP.md - Detailed command reference for your setup

---

**Ready to deploy? Run the setup script on monitoringnode and you're done!**
