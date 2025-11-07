# Storage Health Analyzer - Monitoring Node

## Overview

The Storage Health Analyzer is a production-ready monitoring system that reads client JSON reports, validates data integrity, checks metrics against configurable thresholds, and generates alerts when issues are detected.

## Features

- ✅ **Comprehensive Validation**: Schema validation, data integrity checks, and freshness verification
- ✅ **Threshold Monitoring**: Disk usage, inode usage, system metrics (CPU, memory, load), and SMART disk health
- ✅ **Multi-Channel Alerts**: Console, log file, email, and webhook notifications
- ✅ **Alert Cooldown**: Prevents alert spam with configurable cooldown periods
- ✅ **Data Archival**: Automatic archival of processed files with retention policies
- ✅ **Production Ready**: Comprehensive error handling, logging, and monitoring
- ✅ **Systemd Integration**: Timer-based execution with automatic recovery
- ✅ **Resource Limits**: CPU and memory limits for production safety

## Architecture

```
monitoring_node/
├── config/
│   ├── analyzer_config.json      # Main analyzer configuration
│   └── thresholds.json            # Alert thresholds
├── scripts/
│   ├── analyze_storage_health.py # Main analyzer script
│   ├── run_analyzer.sh            # Execution wrapper
│   ├── setup.sh                   # Installation script
│   ├── storage-analyzer.service   # Systemd service
│   └── storage-analyzer.timer     # Systemd timer
├── utils.py                       # Utility functions
├── data_validator.py              # Data validation module
└── alert_handler.py               # Alert delivery module
```

## Installation (Linux)

### Prerequisites

- Python 3.7+
- Root/sudo access
- systemd (for automated execution)

### Quick Setup

```bash
# 1. Clone or copy the project to your Linux server
cd /path/to/storage-health-monitor

# 2. Run the setup script as root
sudo ./monitoring_node/scripts/setup.sh

# 3. Edit configuration files as needed
sudo nano /opt/storage-health-monitor/monitoring_node/config/analyzer_config.json
sudo nano /opt/storage-health-monitor/monitoring_node/config/thresholds.json

# 4. Start and enable the service
sudo systemctl start storage-analyzer.timer
sudo systemctl enable storage-analyzer.timer

# 5. Verify it's running
sudo systemctl status storage-analyzer.timer
```

### Manual Installation

```bash
# Create directories
sudo mkdir -p /var/log/storage-health-monitor/{archive,reports}
sudo mkdir -p /home/admin/monitor_data
sudo mkdir -p /opt/storage-health-monitor

# Copy files
sudo cp -r monitoring_node /opt/storage-health-monitor/

# Set permissions
sudo chown -R admin:admin /var/log/storage-health-monitor
sudo chown -R admin:admin /home/admin/monitor_data
sudo chown -R admin:admin /opt/storage-health-monitor

# Install systemd files
sudo cp monitoring_node/scripts/storage-analyzer.service /etc/systemd/system/
sudo cp monitoring_node/scripts/storage-analyzer.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

## Configuration

### Analyzer Configuration (`analyzer_config.json`)

```json
{
  "data_directory": "/home/admin/monitor_data",
  "log_path": "/var/log/storage-health-monitor/analyzer.log",
  "output_directory": "/var/log/storage-health-monitor/reports",
  "thresholds_file": "config/thresholds.json",
  "analysis": {
    "scan_interval_seconds": 60,
    "file_pattern": "*.json",
    "archive_processed": true,
    "archive_directory": "/var/log/storage-health-monitor/archive",
    "retention_days": 30
  }
}
```

### Threshold Configuration (`thresholds.json`)

```json
{
  "disk_usage": {
    "warning_percent": 75,
    "critical_percent": 90
  },
  "inode_usage": {
    "warning_percent": 80,
    "critical_percent": 95
  },
  "system": {
    "cpu_usage_warning": 80,
    "cpu_usage_critical": 95,
    "memory_usage_warning": 85,
    "memory_usage_critical": 95
  },
  "alerts": {
    "alert_cooldown_minutes": 30
  }
}
```

## Usage

### Manual Execution

```bash
# Run once
sudo -u admin /opt/storage-health-monitor/monitoring_node/scripts/run_analyzer.sh

# Run with verbose output
sudo -u admin /opt/storage-health-monitor/monitoring_node/scripts/run_analyzer.sh --verbose

# Run directly with custom config
cd /opt/storage-health-monitor
python3 monitoring_node/scripts/analyze_storage_health.py \
    --config monitoring_node/config/analyzer_config.json \
    --thresholds monitoring_node/config/thresholds.json \
    --verbose
```

### Systemd Service Management

```bash
# Start the timer (runs every 5 minutes)
sudo systemctl start storage-analyzer.timer

# Enable at boot
sudo systemctl enable storage-analyzer.timer

# Stop the timer
sudo systemctl stop storage-analyzer.timer

# Check status
sudo systemctl status storage-analyzer.timer
sudo systemctl status storage-analyzer.service

# View logs
sudo journalctl -u storage-analyzer.service -f
sudo journalctl -u storage-analyzer.service --since "1 hour ago"
```

### Monitoring

```bash
# View analyzer logs
tail -f /var/log/storage-health-monitor/analyzer.log

# View recent analysis reports
ls -ltr /var/log/storage-health-monitor/reports/

# Check a specific report
cat /var/log/storage-health-monitor/reports/analysis_report_20250106_120000.json | jq .

# Check archived client data
ls -ltr /var/log/storage-health-monitor/archive/

# Monitor systemd timer
systemctl list-timers storage-analyzer.timer
```

## Alert Configuration

### Email Alerts

Edit `analyzer_config.json`:

```json
{
  "alerts": {
    "enable_email": true,
    "enable_webhook": false,
    "enable_console": true,
    "enable_log": true
  },
  "alert_config": {
    "email": {
      "smtp_server": "smtp.gmail.com",
      "smtp_port": 587,
      "use_tls": true,
      "sender": "storage-monitor@yourdomain.com",
      "recipients": ["admin@yourdomain.com", "ops@yourdomain.com"],
      "username": "your-smtp-username",
      "password": "your-smtp-password"
    }
  }
}
```

### Webhook Alerts

```json
{
  "alerts": {
    "enable_webhook": true
  },
  "alert_config": {
    "webhook": {
      "url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
      "timeout_seconds": 10,
      "retry_count": 3
    }
  }
}
```

## Client Data Format

The analyzer expects JSON files in the following format:

```json
{
  "hostname": "server01",
  "timestamp": "2025-11-06T10:30:00Z",
  "disks": [
    {
      "device": "/dev/sda1",
      "mount_point": "/",
      "filesystem": "ext4",
      "total_bytes": 107374182400,
      "used_bytes": 85899345920,
      "available_bytes": 21474836480,
      "usage_percent": 80.0,
      "inodes_total": 6553600,
      "inodes_used": 524288,
      "inodes_free": 6029312
    }
  ],
  "system": {
    "cpu_usage_percent": 45.2,
    "memory_usage_percent": 72.8,
    "load_average": [2.5, 2.1, 1.8]
  },
  "smart": {
    "/dev/sda": {
      "reallocated_sectors": 0,
      "current_pending_sectors": 0,
      "offline_uncorrectable": 0
    }
  }
}
```

## Troubleshooting

### Check Service Status

```bash
# Check if timer is active
systemctl is-active storage-analyzer.timer

# Check if timer is enabled
systemctl is-enabled storage-analyzer.timer

# View timer schedule
systemctl list-timers storage-analyzer.timer

# Check last execution
systemctl status storage-analyzer.service
```

### Common Issues

**No files processed:**
- Check that client JSON files exist in `/home/admin/monitor_data`
- Verify file permissions: `ls -la /home/admin/monitor_data`
- Check the `file_pattern` in `analyzer_config.json`

**Validation errors:**
- Review the client JSON format
- Check logs: `grep "Validation failed" /var/log/storage-health-monitor/analyzer.log`
- Run with `--verbose` flag to see detailed errors

**Permission errors:**
- Ensure directories are owned by the service user:
  ```bash
  sudo chown -R admin:admin /var/log/storage-health-monitor
  sudo chown -R admin:admin /home/admin/monitor_data
  ```

**Systemd service not running:**
- Check journalctl: `sudo journalctl -u storage-analyzer.service -n 50`
- Verify Python path: `which python3`
- Test manually: `sudo -u admin /opt/storage-health-monitor/monitoring_node/scripts/run_analyzer.sh`

### Debug Mode

```bash
# Run with verbose logging
python3 /opt/storage-health-monitor/monitoring_node/scripts/analyze_storage_health.py \
    --config /opt/storage-health-monitor/monitoring_node/config/analyzer_config.json \
    --thresholds /opt/storage-health-monitor/monitoring_node/config/thresholds.json \
    --verbose
```

## Performance Tuning

### Adjust Timer Frequency

Edit `/etc/systemd/system/storage-analyzer.timer`:

```ini
[Timer]
# Run every 10 minutes instead of 5
OnCalendar=*:0/10
```

Then reload: `sudo systemctl daemon-reload`

### Resource Limits

Edit `/etc/systemd/system/storage-analyzer.service`:

```ini
[Service]
# Increase memory limit
MemoryLimit=1G

# Increase CPU quota
CPUQuota=100%
```

### Archive Retention

Edit `analyzer_config.json`:

```json
{
  "analysis": {
    "retention_days": 60
  }
}
```

## Security Considerations

1. **File Permissions**: Ensure log and data directories are only accessible by the service user
2. **SMTP Credentials**: Store email passwords securely, consider using environment variables or secrets management
3. **Network Access**: If using webhooks, ensure proper firewall rules
4. **Log Rotation**: Configure logrotate for analyzer logs:

```bash
# /etc/logrotate.d/storage-analyzer
/var/log/storage-health-monitor/analyzer.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    create 0644 admin admin
}
```

## Maintenance

### Regular Tasks

- **Weekly**: Review alerts and adjust thresholds
- **Monthly**: Check disk space for logs and archives
- **Quarterly**: Review and update alert configuration

### Backup

```bash
# Backup configuration
tar -czf storage-monitor-config-backup.tar.gz \
    /opt/storage-health-monitor/monitoring_node/config/

# Backup recent reports
tar -czf storage-monitor-reports-backup.tar.gz \
    /var/log/storage-health-monitor/reports/
```

## Support

For issues, questions, or contributions:
- Check logs: `/var/log/storage-health-monitor/analyzer.log`
- Review systemd logs: `journalctl -u storage-analyzer.service`
- Test manually with `--verbose` flag

## License

[Specify your license here]
