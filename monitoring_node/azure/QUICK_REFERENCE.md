# Azure Storage Monitoring - Quick Reference Card

## 🚀 Quick Start (3 Commands)

```bash
# 1. Test connection
python3 scripts/test_azure_connection.py

# 2. Deploy monitoring
sudo ./setup_azure_monitoring.sh

# 3. Check status
sudo systemctl list-timers azure-*
```

---

## 📋 Essential Files

| File | Purpose | Action Required |
|------|---------|-----------------|
| `DEPLOYMENT_GUIDE.md` | Step-by-step deployment | READ FIRST |
| `AZURE_SETUP_GUIDE.md` | Azure Portal setup | Follow for credentials |
| `azure_config.json` | Main configuration | UPDATE with credentials |
| `README.md` | Full documentation | Reference |

---

## 🔑 Credentials Needed (8 Total)

From Azure Portal:
- [ ] Tenant ID
- [ ] Client ID (Application ID)
- [ ] Client Secret
- [ ] Subscription ID
- [ ] Finance storage account name
- [ ] Finance connection string
- [ ] Marketing storage account name
- [ ] Marketing connection string

---

## ⚙️ Configuration Summary

### Finance (Mission-Critical)
- **Interval:** 5 minutes
- **WARNING:** 60% usage
- **CRITICAL:** 70% usage
- **Containers:** invoices, transactions, financial-reports, audit-logs

### Marketing (Standard)
- **Interval:** 10 minutes
- **WARNING:** 80% usage
- **CRITICAL:** 90% usage
- **Containers:** campaigns, analytics, media-assets, customer-data

---

## 🛠️ Common Commands

### Testing
```bash
# Validate Azure connection
python3 scripts/test_azure_connection.py

# Run monitoring manually
python3 scripts/monitor_azure_storage.py

# Upload test data (5 files, 10 MB each)
python3 scripts/upload_test_data.py --account finance --container invoices --count 5 --size 10
```

### Systemd Management
```bash
# Check timer status
sudo systemctl list-timers azure-*

# View service logs
sudo journalctl -u azure-finance-monitor -f

# Restart timer
sudo systemctl restart azure-finance-monitor.timer

# Stop monitoring
sudo systemctl stop azure-finance-monitor.timer
```

### Monitoring
```bash
# Live log tail
tail -f /home/admin/monitoring_node/logs/azure_monitor.log

# List reports
ls -lh /home/admin/azure_reports/

# View latest finance report
cat /home/admin/azure_reports/finance_storage_report_*.csv | tail -20

# Count alerts sent
grep "Sending alert" /home/admin/monitoring_node/logs/azure_monitor.log | wc -l
```

---

## 🔍 Troubleshooting (One-Liners)

### Issue: "Azure SDK not installed"
```bash
pip3 install --upgrade azure-identity azure-storage-blob
```

### Issue: "Permission denied"
```bash
chmod +x /home/admin/monitoring_node/azure/scripts/*.py && chmod +x /home/admin/monitoring_node/azure/*.sh
```

### Issue: "Service not starting"
```bash
sudo systemctl daemon-reload && sudo systemctl restart azure-finance-monitor.timer && sudo systemctl status azure-finance-monitor.timer
```

### Issue: "Connection test failed"
```bash
# Check credentials in azure_config.json - no placeholder values!
grep "YOUR_" /home/admin/monitoring_node/azure/azure_config.json
# If this returns anything, you have placeholders to replace
```

---

## 📊 File Locations

| Resource | Path |
|----------|------|
| **Scripts** | `/home/admin/monitoring_node/azure/scripts/` |
| **Configuration** | `/home/admin/monitoring_node/azure/azure_config.json` |
| **CSV Reports** | `/home/admin/azure_reports/` |
| **Logs** | `/home/admin/monitoring_node/logs/azure_monitor.log` |
| **Systemd Services** | `/etc/systemd/system/azure-*-monitor.*` |

---

## ✅ Deployment Checklist

### Before Deployment
- [ ] Azure Portal setup completed (see AZURE_SETUP_GUIDE.md)
- [ ] All 8 credentials collected
- [ ] azure_config.json updated (no placeholders)
- [ ] Files transferred to monitoring node

### During Deployment
- [ ] Azure SDK installed: `pip3 install azure-identity azure-storage-blob`
- [ ] Connection test passed: `python3 scripts/test_azure_connection.py`
- [ ] Setup script executed: `sudo ./setup_azure_monitoring.sh`
- [ ] Systemd timers active: `sudo systemctl list-timers azure-*`

### After Deployment
- [ ] CSV reports generating in /home/admin/azure_reports/
- [ ] Logs show successful monitoring
- [ ] No errors in systemd status
- [ ] Email alerts received (if thresholds exceeded)

---

## 🎯 Success Indicators

Monitor is working when you see:

✅ **Systemd Timers Active**
```bash
$ sudo systemctl list-timers azure-*
NEXT                        LEFT         LAST PASSED UNIT
Mon 2024-01-15 14:35:00 UTC 3min 25s left n/a  n/a    azure-finance-monitor.timer
Mon 2024-01-15 14:40:00 UTC 8min 25s left n/a  n/a    azure-marketing-monitor.timer
```

✅ **Logs Show Success**
```
[INFO] Starting Azure Storage monitoring
[INFO] Monitoring storage account: finance
[INFO] Discovered 4 containers in finance
[INFO] CSV report saved: /home/admin/azure_reports/finance_storage_report_20240115_143045.csv
```

✅ **Reports Generated**
```bash
$ ls -lh /home/admin/azure_reports/
-rw-r--r-- 1 admin admin 1.2K Jan 15 14:30 finance_storage_report_20240115_143045.csv
-rw-r--r-- 1 admin admin 1.1K Jan 15 14:40 marketing_storage_report_20240115_144015.csv
```

---

## 🚨 Common Mistakes

❌ **Placeholder values in azure_config.json**
- Fix: Replace ALL `YOUR_*` and `PASTE_*` with real values from Azure

❌ **Connection string has line breaks**
- Fix: Must be single line starting with "DefaultEndpointsProtocol=https"

❌ **Service Principal missing role**
- Fix: Assign "Storage Blob Data Reader" role in Azure Portal

❌ **Scripts not executable**
- Fix: `chmod +x /home/admin/monitoring_node/azure/scripts/*.py`

❌ **Wrong Python version**
- Fix: Use Python 3.8+ (`python3 --version`)

---

## 📧 Email Alert Format

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

---

## 📈 Monitoring Metrics

| Metric | Description | Source |
|--------|-------------|--------|
| **blob_count** | Number of files in container | Azure API |
| **used_gb** | Total size of all blobs (GB) | Azure API |
| **total_gb** | Estimated capacity (GB) | Calculated (5TB default) |
| **usage_percent** | (Used / Total) × 100 | Calculated |
| **status** | Healthy / Not Found / Error | Azure API |
| **is_accessible** | Yes / No | Azure API |
| **alert_level** | OK / WARNING / CRITICAL | Threshold comparison |

---

## 🔄 Update Workflow

To modify thresholds:

1. Edit configuration:
   ```bash
   nano /home/admin/monitoring_node/azure/azure_config.json
   ```

2. Change values:
   ```json
   "thresholds": {
     "usage_warning_percent": 60,
     "usage_critical_percent": 70
   }
   ```

3. No restart needed - changes take effect on next monitoring cycle

---

## 💰 Cost Management

**Free Tier Limits (be aware):**
- Storage: First 5 GB free
- Transactions: 20,000 read operations/month free
- Monitoring reads: ~8,640/day (Finance 5min) + ~4,320/day (Marketing 10min)

**Cost-Saving Tips:**
- Increase monitoring intervals (5min → 10min)
- Disable auto-discovery, specify containers manually
- Monitor free tier usage in Azure Cost Management

---

## 📞 Emergency Commands

### Stop All Azure Monitoring
```bash
sudo systemctl stop azure-finance-monitor.timer
sudo systemctl stop azure-marketing-monitor.timer
```

### Start All Azure Monitoring
```bash
sudo systemctl start azure-finance-monitor.timer
sudo systemctl start azure-marketing-monitor.timer
```

### View All Errors (Last Hour)
```bash
sudo journalctl -u azure-finance-monitor --since "1 hour ago" | grep -i error
```

### Clear Old Reports (Keep Last 7 Days)
```bash
find /home/admin/azure_reports/ -name "*.csv" -mtime +7 -delete
```

---

## 🎓 Learning Resources

- [Azure Storage Documentation](https://docs.microsoft.com/azure/storage/)
- [Azure SDK for Python](https://github.com/Azure/azure-sdk-for-python)
- [Service Principal Guide](https://docs.microsoft.com/azure/active-directory/develop/howto-create-service-principal-portal)

---

## 🆘 Getting Help

1. Check logs: `tail -f /home/admin/monitoring_node/logs/azure_monitor.log`
2. Run test: `python3 scripts/test_azure_connection.py`
3. Read troubleshooting: `README.md` section 9
4. Check systemd: `sudo systemctl status azure-finance-monitor.service`

---

**Quick Reference Version:** 1.0  
**Last Updated:** January 2024  
**Print this page for easy reference!**
