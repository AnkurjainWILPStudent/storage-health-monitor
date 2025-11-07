
# Storage Health Monitor

Production-ready distributed storage health monitoring system for VirtualBox VMs. Monitors disk usage, SMART status, and system health across multiple Linux client nodes with centralized analysis and **email alerting**.

## 🚀 Quick Start

**Before deployment, you need:**

1. **Gmail Account** for sending alerts:
   - Gmail address (e.g., `storage-alerts@gmail.com`)
   - Gmail App Password (16 characters) - [Generate here](https://myaccount.google.com/apppasswords)
   - Recipient email(s) for receiving alerts

2. **Your VM Information**:
   - Monitoring VM SSH IP: `192.168.1.13` (already configured)
   - Client VMs: `clientnode` (192.168.1.14), `clientnode2` (192.168.1.15)
   - Clients already sending data to `/home/admin/monitor_data/`

3. **Deployment Method**:
   - Push this project to GitHub
   - Pull on monitoring VM (192.168.1.13)
   - Configure email using the provided scripts

**→ See [EMAIL_SETUP_GUIDE.md](EMAIL_SETUP_GUIDE.md) for detailed email configuration**  
**→ See [GITHUB_WORKFLOW.md](GITHUB_WORKFLOW.md) for deployment via GitHub**

## 🎯 Overview

This system monitors storage health across multiple client machines and provides centralized analysis on a monitoring node:

- **Client Nodes**: Collect disk metrics (usage, SMART status, I/O) and send JSON reports via SSH/SCP
- **Monitoring Node**: Receives reports, validates data, checks thresholds, generates alerts and summaries

**Perfect for**: VirtualBox lab environments, small-scale deployments, educational projects, and proof-of-concept testing.

## 🏗️ Architecture

```
┌─────────────────────┐                    ┌──────────────────────┐
│   Client VM (user1) │                    │   Monitoring VM      │
│                     │                    │                      │
│  ┌───────────────┐  │    SSH/SCP        │  ┌────────────────┐  │
│  │ disk_monitor  │──┼───── JSON ────────┼─▶│   Analyzer     │  │
│  │     .py       │  │    Reports        │  │                │  │
│  └───────────────┘  │                    │  │ • Validation   │  │
│   • Disk usage     │                    │  │ • Thresholds   │  │
│   • SMART status   │                    │  │ • Alerts       │  │
│   • Sends reports  │                    │  │ • Reports      │  │
└─────────────────────┘                    │  └────────────────┘  │
                                           │                      │
┌─────────────────────┐    SSH/SCP        │  ┌────────────────┐  │
│   Client VM (user2) │                    │  │  Alert Handler │  │
│                     │                    │  │                │  │
│  ┌───────────────┐  │    JSON           │  │ • Email        │  │
│  │ disk_monitor  │──┼────────────────────┼─▶│ • Webhook      │  │
│  │     .py       │  │    Reports        │  │ • Logs         │  │
│  └───────────────┘  │                    │  └────────────────┘  │
└─────────────────────┘                    └──────────────────────┘
```

## ✨ Features

### Client Node Features
- **Comprehensive Metrics**: Disk usage, partition info, filesystem types
- **SMART Monitoring**: Hardware health checks using smartctl
- **Automatic Reporting**: Scheduled via cron or systemd timers
- **Secure Transfer**: SSH key-based authentication with SCP
- **Configurable**: JSON-based configuration
- **Logging**: Detailed operation logs

### Monitoring Node Features
- **Data Validation**: Schema validation and integrity checks
- **Threshold Monitoring**: Configurable warning/critical levels (75% warning, 90% critical)
- **Multi-level Alerts**: **Email (Gmail)**, webhook, console, and log alerts
- **Email Notifications**: HTML-formatted alerts sent when thresholds exceeded
  - WARNING: Disk > 75%, Memory > 80%, SMART errors > 10
  - CRITICAL: Disk > 90%, Memory > 95%, SMART errors > 50
- **Automatic Archival**: Archive processed files with retention policies
- **Summary Reports**: JSON-formatted analysis summaries
- **Production-Ready**: Error handling, logging, systemd integration

## 📋 Prerequisites

### All VMs
- Linux OS (Ubuntu 20.04+, Debian 10+, or similar)
- Python 3.8+
- SSH server (monitoring node) and client (all nodes)
- Network connectivity between VMs

### Client VMs
- `psutil` Python library
- `smartmontools` package
- `lsblk` utility (usually pre-installed)
- SSH key pair for authentication

### Monitoring VM
- Standard Python library only (no extra dependencies)
- Sufficient disk space for logs and reports

## 🚀 Quick Start

### For VirtualBox Lab Setup

**This is designed for a 3-VM VirtualBox environment:**
- 1 Monitoring Node VM
- 2 Client VMs (user1, user2)

See **[VIRTUALBOX_SETUP.md](VIRTUALBOX_SETUP.md)** for complete step-by-step instructions.

### Quick Setup Summary

**1. On each Client VM (user1, user2):**
```bash
cd ~/storage-health-monitor/client_node/scripts
chmod +x setup_client.sh
./setup_client.sh
```

**2. On Monitoring VM:**
```bash
cd ~/storage-health-monitor/monitoring_node/scripts
chmod +x setup_monitoring.sh
./setup_monitoring.sh
```

**3. Copy SSH keys from clients to monitoring node:**
```bash
# Run on each client
ssh-copy-id -i ~/.ssh/id_ed25519.pub admin@192.168.56.10
```

**4. Test the system:**
```bash
# On client: send a report
sudo python3 ~/storage-health-monitor/client_node/disk_monitor.py

# On monitoring: run analyzer
python3 ~/storage-health-monitor/monitoring_node/scripts/analyze_storage_health.py
```

## 📁 Project Structure

```
storage-health-monitor/
├── client_node/                    # Deploy on client VMs
│   ├── disk_monitor.py            # Main monitoring script
│   ├── utils.py                   # Utility functions
│   ├── config.json                # Client configuration
│   ├── requirements.txt           # Python dependencies
│   ├── logs/                      # Log files
│   └── scripts/
│       └── setup_client.sh        # Automated setup script
│
├── monitoring_node/                # Deploy on monitoring VM
│   ├── scripts/
│   │   ├── analyze_storage_health.py  # Main analyzer
│   │   ├── setup_monitoring.sh        # Automated setup
│   │   ├── run_analyzer.sh            # Manual run script
│   │   ├── storage-analyzer.service   # Systemd service
│   │   └── storage-analyzer.timer     # Systemd timer
│   ├── config/
│   │   ├── analyzer_config.json       # Analyzer configuration
│   │   └── thresholds.json            # Alert thresholds
│   ├── utils.py                       # Utility functions
│   ├── data_validator.py              # Data validation
│   ├── alert_handler.py               # Alert management
│   ├── requirements.txt               # Python dependencies
│   ├── logs/                          # Log files
│   └── reports/                       # Analysis reports
│
├── VIRTUALBOX_SETUP.md            # Complete VM setup guide
├── QUICKREF.md                    # Command reference
├── test_setup.sh                  # System verification script
└── README.md                      # This file
```

## ⚙️ Configuration

### Client Configuration (`client_node/config.json`)

```json
{
  "monitoring_node_ip": "192.168.56.10",
  "monitoring_node_user": "admin",
  "monitoring_node_receive_dir": "/home/admin/monitor_data",
  "port": 22,
  "interval_minutes": 5,
  "log_path": "/home/user/storage-health-monitor/client_node/logs/client_log.log",
  "keep_local_copy": false
}
```

### Monitoring Configuration (`monitoring_node/config/analyzer_config.json`)

```json
{
  "data_directory": "/home/admin/monitor_data",
  "log_path": "/var/log/storage-health-monitor/analyzer.log",
  "output_directory": "/var/log/storage-health-monitor/reports",
  "analysis": {
    "file_pattern": "*.json",
    "archive_processed": true,
    "archive_directory": "/var/log/storage-health-monitor/archive",
    "retention_days": 30
  }
}
```

### Thresholds Configuration (`monitoring_node/config/thresholds.json`)

```json
{
  "disk_usage": {
    "warning_percent": 75,
    "critical_percent": 90
  },
  "smart": {
    "reallocated_sectors_warning": 5,
    "reallocated_sectors_critical": 50
  },
  "data_freshness": {
    "max_age_minutes": 15
  }
}
```

## 🔍 Testing & Verification

### Test Your Setup

Run the system tester on each VM:

```bash
cd ~/storage-health-monitor
chmod +x test_setup.sh
./test_setup.sh
```

This validates:
- System dependencies
- Network connectivity
- SSH configuration
- File permissions
- Service status

### Manual Testing

**End-to-end test:**

```bash
# 1. On client VM
sudo python3 ~/storage-health-monitor/client_node/disk_monitor.py

# 2. On monitoring VM - verify file received
ls -l ~/monitor_data/user1/

# 3. Run analyzer
cd ~/storage-health-monitor/monitoring_node
python3 scripts/analyze_storage_health.py

# 4. Check results
cat logs/analyzer.log
ls -l reports/
```

## 📊 Monitoring & Operations

### View Logs

**Client:**
```bash
tail -f ~/storage-health-monitor/client_node/logs/client_log.log
```

**Monitoring:**
```bash
tail -f ~/storage-health-monitor/monitoring_node/logs/analyzer.log
```

### Check Service Status

**Client (systemd):**
```bash
sudo systemctl status disk-monitor.timer
```

**Monitoring (systemd):**
```bash
sudo systemctl status storage-analyzer.timer
```

### View Reports

**Latest summary:**
```bash
ls -lt ~/storage-health-monitor/monitoring_node/reports/ | head -5
cat ~/storage-health-monitor/monitoring_node/reports/summary_*.json | jq .
```

### Monitor Data Flow

**Watch for new files:**
```bash
watch -n 5 'find ~/monitor_data -name "*.json" -mmin -10'
```

## 🔧 Troubleshooting

### Common Issues

**1. Client can't connect to monitoring node**
```bash
# Test connectivity
ping 192.168.56.10

# Test SSH
ssh -i ~/.ssh/id_ed25519 admin@192.168.56.10 "echo OK"

# Check SSH key permissions
chmod 600 ~/.ssh/id_ed25519
```

**2. SMART checks fail**
```bash
# Install smartmontools
sudo apt-get install smartmontools

# Test manually
sudo smartctl -H /dev/sda
```

**3. No data on monitoring node**
```bash
# Check client logs
tail ~/storage-health-monitor/client_node/logs/client_log.log

# Verify data directory path matches config
grep data_directory ~/storage-health-monitor/monitoring_node/config/analyzer_config.json
```

**4. Analyzer not processing files**
```bash
# Check analyzer logs
tail ~/storage-health-monitor/monitoring_node/logs/analyzer.log

# Run manually with Python to see errors
cd ~/storage-health-monitor/monitoring_node
python3 -u scripts/analyze_storage_health.py
```

See **[QUICKREF.md](QUICKREF.md)** for more troubleshooting commands.

## 📖 Documentation

- **[VIRTUALBOX_SETUP.md](VIRTUALBOX_SETUP.md)** - Complete VirtualBox VM setup guide
- **[QUICKREF.md](QUICKREF.md)** - Quick reference for common commands
- **[monitoring_node/README.md](monitoring_node/README.md)** - Detailed monitoring node documentation

## 🔐 Security Considerations

1. **SSH Keys**: Use ed25519 keys, protect private keys with proper permissions (600)
2. **File Permissions**: Logs and data directories should be 750/640
3. **Network**: Use Host-Only or internal networks in VirtualBox
4. **Sudo**: Client script requires sudo for SMART checks - audit carefully
5. **Data Validation**: All client data is validated before processing

## 🚦 Performance Tips

1. **Adjust Collection Interval**: Change from 5 to 10+ minutes for less frequent checks
2. **Limit SMART Checks**: SMART operations are slow; consider reducing frequency
3. **Archive Old Data**: Configure retention policies to prevent disk fill
4. **Reduce Log Verbosity**: Use WARNING level in production

## 🔮 Future Enhancements

- Grafana dashboards for visualization
- Prometheus metrics export
- Slack/Discord webhook alerts
- Web UI for configuration
- Temperature and fan speed monitoring
- Network I/O metrics
- Centralized log aggregation (ELK stack)

## 📝 Example Data Flow

**1. Client collects data:**
```json
{
  "collected_at": "2025-11-06T10:30:00Z",
  "host": "user1",
  "disk_partitions": [
    {
      "device": "/dev/sda1",
      "mountpoint": "/",
      "percent": 45.2
    }
  ],
  "smart": [
    {
      "device": "/dev/sda",
      "status": "OK"
    }
  ]
}
```

**2. Client sends to monitoring node via SCP:**
```
/home/admin/monitor_data/user1/user1_2025-11-06T10:30:00Z.json
```

**3. Analyzer processes and generates report:**
```json
{
  "timestamp": "2025-11-06T10:30:15Z",
  "summary": {
    "total_files": 2,
    "issues_found": 0,
    "alerts_sent": 0
  },
  "clients": [
    {
      "hostname": "user1",
      "status": "healthy",
      "disk_usage_max": 45.2,
      "smart_status": "all_ok"
    }
  ]
}
```

## 🤝 Contributing

This is a VirtualBox lab project. Feel free to:
- Add new metrics (temperature, network I/O)
- Improve alerting mechanisms
- Add visualization dashboards
- Enhance documentation

## 📄 License

This project is for educational and testing purposes. Use in production environments at your own risk.

## 🆘 Support

1. **Check logs first** - Most issues are visible in logs
2. **Run test_setup.sh** - Validates your configuration
3. **Review VIRTUALBOX_SETUP.md** - Step-by-step setup instructions
4. **Check QUICKREF.md** - Common commands and solutions

## 🏁 Getting Started Checklist

- [ ] All 3 VMs created and running Linux
- [ ] Network configured (VMs can ping each other)
- [ ] SSH server installed on monitoring VM
- [ ] Python 3.8+ on all VMs
- [ ] Repository cloned/copied to all VMs
- [ ] Run `setup_client.sh` on each client
- [ ] Run `setup_monitoring.sh` on monitoring node
- [ ] SSH keys copied from clients to monitoring node
- [ ] Test end-to-end with manual runs
- [ ] Run `test_setup.sh` on all VMs
- [ ] Verify automatic scheduling (cron/systemd)
- [ ] Monitor logs for 24 hours to ensure stability

**Ready to begin?** Start with **[VIRTUALBOX_SETUP.md](VIRTUALBOX_SETUP.md)**!
