# Azure Storage Monitoring System

Complete setup and usage guide for Azure blob storage monitoring with alerting.

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Azure Portal Setup](#azure-portal-setup)
4. [Installation](#installation)
5. [Configuration](#configuration)
6. [Testing](#testing)
7. [Deployment](#deployment)
8. [Monitoring & Alerts](#monitoring--alerts)
9. [Troubleshooting](#troubleshooting)

---

## Overview

This system monitors Azure blob storage containers across multiple storage accounts with different SLAs:

- **Finance Storage** (Mission-Critical)
  - Containers: invoices, transactions, financial-reports, audit-logs
  - Schedule: Every 5 minutes
  - Alert Thresholds: WARNING @ 60%, CRITICAL @ 70%

- **Marketing Storage** (Standard)
  - Containers: campaigns, analytics, media-assets, customer-data
  - Schedule: Every 10 minutes
  - Alert Thresholds: WARNING @ 80%, CRITICAL @ 90%

### Features
- ✅ Auto-discovery of containers
- ✅ Real-time usage monitoring
- ✅ Email alerts (separate per account)
- ✅ CSV reports (separate per account)
- ✅ Container accessibility checks
- ✅ Blob count and size tracking
- ✅ 15-minute alert cooldown
- ✅ Integration with existing VM monitoring

---

## Prerequisites

### System Requirements
- Ubuntu-based Linux (monitoring node)
- Python 3.8 or higher
- Internet connectivity
- systemd (for automated monitoring)

### Azure Requirements
- Azure account with active subscription
- Global Administrator or Contributor role
- Service Principal with Storage Blob Data Reader role

### Python Packages
```bash
pip3 install azure-identity azure-storage-blob
```

---

## Azure Portal Setup

**IMPORTANT:** Complete this section BEFORE installation!

Follow the detailed guide: [AZURE_SETUP_GUIDE.md](./AZURE_SETUP_GUIDE.md)

### Quick Checklist
- [ ] Created Finance storage account
- [ ] Created Marketing storage account
- [ ] Created 4 containers in each account
- [ ] Registered Service Principal (App Registration)
- [ ] Assigned "Storage Blob Data Reader" role
- [ ] Collected all 8 credentials:
  - [ ] Tenant ID
  - [ ] Client ID (Application ID)
  - [ ] Client Secret
  - [ ] Subscription ID
  - [ ] Finance storage account name
  - [ ] Finance connection string
  - [ ] Marketing storage account name
  - [ ] Marketing connection string

---

## Installation

### Step 1: Navigate to Azure Monitoring Directory

On your **monitoring node** (192.168.1.13):

```bash
cd /home/admin/monitoring_node/azure
```

### Step 2: Update Configuration

Edit `azure_config.json` with your Azure credentials:

```bash
nano azure_config.json
```

Replace all `YOUR_*` and `PASTE_*` placeholders with real values from Azure Portal.

**Security Note:** Never commit `azure_config.json` to public repositories!

### Step 3: Run Setup Script

```bash
# Make setup script executable
chmod +x setup_azure_monitoring.sh

# Run with sudo to install systemd services
sudo ./setup_azure_monitoring.sh

# Or run without systemd (manual execution only)
./setup_azure_monitoring.sh --skip-systemd
```

The setup script will:
1. Install Azure SDK packages
2. Create required directories
3. Validate configuration
4. Test Azure connection
5. Create systemd services and timers
6. Start monitoring

---

## Configuration

### Main Configuration File: `azure_config.json`

```json
{
  "azure_credentials": {
    "tenant_id": "your-tenant-id",
    "client_id": "your-client-id",
    "client_secret": "your-client-secret",
    "subscription_id": "your-subscription-id"
  },
  "storage_accounts": {
    "finance": {
      "account_name": "financestorageacct",
      "connection_string": "DefaultEndpointsProtocol=https;...",
      "monitoring": {
        "enabled": true,
        "schedule_minutes": 5,
        "auto_discover_containers": true,
        "thresholds": {
          "usage_warning_percent": 60,
          "usage_critical_percent": 70
        }
      }
    },
    "marketing": {
      "account_name": "marketingstorageacct",
      "connection_string": "DefaultEndpointsProtocol=https;...",
      "monitoring": {
        "enabled": true,
        "schedule_minutes": 10,
        "auto_discover_containers": true,
        "thresholds": {
          "usage_warning_percent": 80,
          "usage_critical_percent": 90
        }
      }
    }
  }
}
```

### Email Configuration

Shares email settings from main monitoring system: `/home/admin/monitoring_node/config/analyzer_config.json`

Emails are sent separately per account with distinct subject prefixes:
- `[Azure Finance Alert]` - Finance storage alerts
- `[Azure Marketing Alert]` - Marketing storage alerts

---

## Testing

### Test 1: Validate Azure Connection

```bash
cd /home/admin/monitoring_node/azure
python3 scripts/test_azure_connection.py
```

**Expected Output:**
```
======================================================================
Azure Storage Connection Test
======================================================================

Testing Azure Service Principal Credentials
======================================================================
✓ tenant_id: Configured
✓ client_id: Configured
✓ client_secret: Configured
✓ subscription_id: Configured

✓ Azure credentials initialized successfully

----------------------------------------------------------------------
Testing Storage Account: finance
----------------------------------------------------------------------
✓ Successfully connected to finance
✓ Discovered 4 containers:
  • invoices
  • transactions
  • financial-reports
  • audit-logs

  Testing read access to 'invoices'...
  ✓ Read access confirmed
  ✓ Found 0 blobs (0.00 KB)

======================================================================
✓ All tests passed! Azure monitoring is ready.
======================================================================
```

### Test 2: Run Manual Monitor

```bash
python3 scripts/monitor_azure_storage.py
```

Check for:
- CSV reports in `/home/admin/azure_reports/`
- Log entries in `/home/admin/monitoring_node/logs/azure_monitor.log`

### Test 3: Upload Test Data

Upload small test files:
```bash
python3 scripts/upload_test_data.py --account finance --container invoices --count 5 --size 10
```

This uploads 5 files of 10 MB each to the `invoices` container.

**WARNING:** Large uploads may consume Azure credits!

---

## Deployment

### Systemd Services

Two separate monitoring services run automatically:

#### Finance Monitor (Every 5 Minutes)
```bash
# Check status
sudo systemctl status azure-finance-monitor.timer

# View logs
sudo journalctl -u azure-finance-monitor -f

# Stop monitoring
sudo systemctl stop azure-finance-monitor.timer

# Start monitoring
sudo systemctl start azure-finance-monitor.timer
```

#### Marketing Monitor (Every 10 Minutes)
```bash
# Check status
sudo systemctl status azure-marketing-monitor.timer

# View logs
sudo journalctl -u azure-marketing-monitor -f

# Stop monitoring
sudo systemctl stop azure-marketing-monitor.timer

# Start monitoring
sudo systemctl start azure-marketing-monitor.timer
```

### View All Azure Timers
```bash
sudo systemctl list-timers azure-* --all
```

### Manual Execution (Testing)

Run monitoring manually without systemd:
```bash
cd /home/admin/monitoring_node/azure
python3 scripts/monitor_azure_storage.py
```

---

## Monitoring & Alerts

### Metrics Collected

For each container:
- **Container Name**
- **Blob Count** - Number of files
- **Used Capacity** - Total size of all blobs (GB)
- **Total Capacity** - Estimated account capacity (GB)
- **Usage Percentage** - Used/Total × 100
- **Status** - Healthy, Not Found, Error
- **Accessibility** - Yes/No
- **Last Modified** - Container last update timestamp
- **Alert Level** - OK, WARNING, CRITICAL

### Alert Triggers

#### Finance Storage
| Condition | Level | Threshold |
|-----------|-------|-----------|
| Container inaccessible | CRITICAL | N/A |
| Usage ≥ 70% | CRITICAL | 70% |
| Usage ≥ 60% | WARNING | 60% |

#### Marketing Storage
| Condition | Level | Threshold |
|-----------|-------|-----------|
| Container inaccessible | CRITICAL | N/A |
| Usage ≥ 90% | CRITICAL | 90% |
| Usage ≥ 80% | WARNING | 80% |

### Email Alert Format

**Subject:** `[Azure Finance Alert] CRITICAL - container_usage - invoices`

**Body:**
```
Azure Finance Storage container_usage - invoices

Azure container 'finance/invoices' usage high: 72.5%

Details:
container: invoices
usage_percent: 72.50
blob_count: 150
used: 3.62 GB
total: 5.00 TB
status: Healthy

Timestamp: 2024-01-15 14:30:45 UTC
```

### CSV Reports

Reports saved to: `/home/admin/azure_reports/`

**Naming:** `{account}_storage_report_{timestamp}.csv`

Example: `finance_storage_report_20240115_143045.csv`

**CSV Columns:**
- timestamp
- account_name
- container_name
- blob_count
- used_gb
- total_gb
- usage_percent
- status
- is_accessible
- last_modified
- alert_level

### Viewing Reports

```bash
# List all reports
ls -lh /home/admin/azure_reports/

# View latest finance report
cat /home/admin/azure_reports/finance_storage_report_*.csv | tail -20

# Count reports by account
ls /home/admin/azure_reports/ | grep finance | wc -l
```

### Log Monitoring

```bash
# Live tail of Azure monitoring logs
tail -f /home/admin/monitoring_node/logs/azure_monitor.log

# Search for CRITICAL alerts
grep CRITICAL /home/admin/monitoring_node/logs/azure_monitor.log

# View today's monitoring activity
grep "$(date +%Y-%m-%d)" /home/admin/monitoring_node/logs/azure_monitor.log
```

---

## Troubleshooting

### Issue: "Azure SDK not installed"

**Solution:**
```bash
pip3 install --upgrade azure-identity azure-storage-blob
```

### Issue: "Credential initialization failed"

**Possible Causes:**
1. Incorrect Tenant ID, Client ID, or Client Secret
2. Service Principal not created
3. Network connectivity issues

**Solution:**
1. Verify credentials in Azure Portal
2. Run test script: `python3 scripts/test_azure_connection.py`
3. Check Azure AD > App Registrations

### Issue: "Container not found"

**Possible Causes:**
1. Container name mismatch
2. Service Principal lacks permissions
3. Wrong storage account

**Solution:**
1. List containers: Use Azure Portal Storage Browser
2. Verify role assignment: "Storage Blob Data Reader"
3. Check connection string matches account

### Issue: "No email alerts received"

**Possible Causes:**
1. Email not configured in main analyzer config
2. Alert cooldown active (15 minutes)
3. Thresholds not exceeded

**Solution:**
1. Check `/home/admin/monitoring_node/config/analyzer_config.json`
2. Wait 15 minutes between same alerts
3. Upload test data to trigger thresholds

### Issue: "Permission denied" errors

**Solution:**
```bash
# Fix ownership
sudo chown -R admin:admin /home/admin/monitoring_node/azure
sudo chown -R admin:admin /home/admin/azure_reports

# Fix permissions
chmod +x /home/admin/monitoring_node/azure/scripts/*.py
```

### Issue: Systemd service not starting

**Debug commands:**
```bash
# Check service status
sudo systemctl status azure-finance-monitor.service

# View detailed logs
sudo journalctl -xe -u azure-finance-monitor

# Manually test script
python3 /home/admin/monitoring_node/azure/scripts/monitor_azure_storage.py

# Reload systemd after changes
sudo systemctl daemon-reload
sudo systemctl restart azure-finance-monitor.timer
```

### Issue: High Azure costs

**Monitor your usage:**
1. Azure Portal > Cost Management
2. Check transaction counts (list operations cost money)
3. Consider increasing monitoring intervals

**Cost-saving tips:**
- Increase schedule intervals (5min → 10min)
- Disable auto-discovery, specify containers manually
- Use Azure free tier limits wisely

---

## Architecture

### Directory Structure
```
/home/admin/monitoring_node/azure/
├── AZURE_SETUP_GUIDE.md          # Azure Portal setup guide
├── README.md                     # This file
├── azure_config.json             # Main configuration (SECRETS!)
├── azure_config.template.json    # Template with docs
├── setup_azure_monitoring.sh     # Installation script
└── scripts/
    ├── monitor_azure_storage.py  # Main monitoring script
    ├── test_azure_connection.py  # Connection validator
    └── upload_test_data.py       # Test data uploader

/home/admin/azure_reports/        # CSV reports directory
├── finance_storage_report_*.csv
└── marketing_storage_report_*.csv

/home/admin/monitoring_node/logs/
└── azure_monitor.log             # Monitoring logs

/etc/systemd/system/
├── azure-finance-monitor.service
├── azure-finance-monitor.timer
├── azure-marketing-monitor.service
└── azure-marketing-monitor.timer
```

### Integration with VM Monitoring

- **Shared:** Email alerts system, logging utilities, alert cooldown
- **Separate:** Reports directory, log file, systemd services
- **Independent:** Monitoring schedules, thresholds, configurations

---

## Best Practices

### Security
- Never commit `azure_config.json` to Git
- Rotate client secrets regularly (every 6-12 months)
- Use least-privilege access (Storage Blob Data Reader only)
- Store connection strings securely

### Monitoring
- Review logs weekly for errors
- Archive old CSV reports monthly
- Test email alerts quarterly
- Verify Service Principal expiration dates

### Maintenance
- Update Azure SDK packages monthly
- Monitor Azure free tier usage limits
- Review and adjust thresholds based on actual usage
- Clean up test data after testing

---

## Support

### Documentation
- [Azure Storage Documentation](https://docs.microsoft.com/azure/storage/)
- [Azure SDK for Python](https://github.com/Azure/azure-sdk-for-python)
- [systemd Timer Units](https://www.freedesktop.org/software/systemd/man/systemd.timer.html)

### Project Files
- Main project: `/home/admin/monitoring_node/`
- VM monitoring: `/home/admin/monitoring_node/scripts/`
- Configuration: `/home/admin/monitoring_node/config/`

### Quick Reference Commands

```bash
# Test connection
python3 scripts/test_azure_connection.py

# Run monitoring manually
python3 scripts/monitor_azure_storage.py

# Upload test data (5 files, 10 MB each)
python3 scripts/upload_test_data.py --account finance --container invoices --count 5 --size 10

# Check systemd status
sudo systemctl list-timers azure-*

# View live logs
tail -f /home/admin/monitoring_node/logs/azure_monitor.log

# View email alerts
grep "Sending alert" /home/admin/monitoring_node/logs/azure_monitor.log
```

---

**Last Updated:** January 2024  
**Version:** 1.0.0  
**Author:** Storage Health Monitoring System
