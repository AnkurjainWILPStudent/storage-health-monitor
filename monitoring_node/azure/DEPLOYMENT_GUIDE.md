# Azure Monitoring - Quick Deployment Guide

## Current Status: ✅ Code Ready, ⏳ Azure Setup Pending

All Azure monitoring scripts and configuration files have been created. Now you need to complete the Azure Portal setup and deploy to your monitoring node.

---

## 📋 Step-by-Step Deployment

### Phase 1: Azure Portal Setup (Do This First!)

**Time Required:** ~30-45 minutes

1. **Open Azure Portal:** https://portal.azure.com
2. **Follow the detailed guide:** `AZURE_SETUP_GUIDE.md`
3. **Create Resources:**
   - Finance storage account
   - Marketing storage account
   - 8 containers total (4 in each)
   - Service Principal (App Registration)
   - Role assignments

4. **Collect Credentials:** Keep these handy for next phase
   - ✏️ Tenant ID: _______________________
   - ✏️ Client ID: _______________________
   - ✏️ Client Secret: _______________________
   - ✏️ Subscription ID: _______________________
   - ✏️ Finance storage name: _______________________
   - ✏️ Finance connection string: _______________________
   - ✏️ Marketing storage name: _______________________
   - ✏️ Marketing connection string: _______________________

---

### Phase 2: Transfer Files to Monitoring Node

**From your Windows machine:**

Open PowerShell and navigate to project directory:
```powershell
cd C:\Users\ankur\Desktop\MISC\projects\storage-health-monitor
```

**Option A: Using Git (Recommended)**
```powershell
# Commit and push changes
git add monitoring_node/azure/*
git commit -m "Add Azure storage monitoring system"
git push origin main

# Then on monitoring node (SSH):
cd /home/admin/monitoring_node
git pull origin main
```

**Option B: Using SCP (Direct Transfer)**
```powershell
# Transfer entire azure directory
scp -r monitoring_node/azure admin@192.168.1.13:/home/admin/monitoring_node/

# Make scripts executable on monitoring node
ssh admin@192.168.1.13 "chmod +x /home/admin/monitoring_node/azure/*.sh"
ssh admin@192.168.1.13 "chmod +x /home/admin/monitoring_node/azure/scripts/*.py"
```

**Option C: Using VS Code Remote SSH**
```
1. Open VS Code
2. Connect to monitoring node (192.168.1.13)
3. Navigate to /home/admin/monitoring_node/
4. Copy azure folder from local to remote
```

---

### Phase 3: Configure Credentials

**On monitoring node (SSH):**

```bash
# Navigate to azure directory
cd /home/admin/monitoring_node/azure

# Edit configuration file
nano azure_config.json

# Replace ALL placeholder values:
# - YOUR_TENANT_ID → your actual tenant ID
# - YOUR_CLIENT_ID → your actual client ID
# - YOUR_CLIENT_SECRET → your actual client secret
# - YOUR_SUBSCRIPTION_ID → your actual subscription ID
# - YOUR_FINANCE_STORAGE_NAME → your finance storage account name
# - PASTE_FINANCE_CONNECTION_STRING_HERE → your finance connection string
# - YOUR_MARKETING_STORAGE_NAME → your marketing storage account name
# - PASTE_MARKETING_CONNECTION_STRING_HERE → your marketing connection string

# Save and exit (Ctrl+O, Enter, Ctrl+X)
```

**Important:** Make sure there are NO placeholder values left!

---

### Phase 4: Install and Test

**On monitoring node:**

```bash
# 1. Install Azure SDK
pip3 install --upgrade azure-identity azure-storage-blob

# 2. Test connection
cd /home/admin/monitoring_node/azure
python3 scripts/test_azure_connection.py

# Expected: ✓ All tests passed! Azure monitoring is ready.
```

If test fails, review error messages and check:
- Credentials are correct (no typos)
- Service Principal has "Storage Blob Data Reader" role
- Network connectivity to Azure

---

### Phase 5: Deploy Systemd Services

**On monitoring node:**

```bash
# Run setup script with sudo (installs systemd services)
cd /home/admin/monitoring_node/azure
sudo ./setup_azure_monitoring.sh

# The script will:
# ✓ Install dependencies
# ✓ Create directories
# ✓ Validate configuration
# ✓ Test Azure connection
# ✓ Create systemd timers
# ✓ Start monitoring
```

**Verify deployment:**
```bash
# Check timer status
sudo systemctl list-timers azure-*

# Should show:
# azure-finance-monitor.timer   - Next run in ~5 min
# azure-marketing-monitor.timer - Next run in ~10 min
```

---

### Phase 6: Verify Monitoring

**Wait 5-10 minutes, then check:**

```bash
# 1. Check logs
tail -f /home/admin/monitoring_node/logs/azure_monitor.log

# Should see:
# [INFO] Starting Azure Storage monitoring
# [INFO] Monitoring storage account: finance
# [INFO] Discovered X containers in finance
# [INFO] CSV report saved: /home/admin/azure_reports/finance_storage_report_*.csv

# 2. Check CSV reports
ls -lh /home/admin/azure_reports/

# Should see:
# finance_storage_report_YYYYMMDD_HHMMSS.csv
# marketing_storage_report_YYYYMMDD_HHMMSS.csv

# 3. View latest report
cat /home/admin/azure_reports/finance_storage_report_*.csv | tail -20
```

---

### Phase 7: Test Alerts

**Upload test data to trigger alerts:**

```bash
# Upload 5 small test files to finance/invoices
cd /home/admin/monitoring_node/azure
python3 scripts/upload_test_data.py --account finance --container invoices --count 5 --size 10

# This uploads 50 MB total (won't trigger alerts yet)

# Wait for next monitoring cycle (5 minutes for finance)
# Check email for alerts
```

**To trigger actual alerts:**
- You'll need to upload enough data to exceed 60% (warning) or 70% (critical)
- For free tier with 5TB estimated capacity, that's ~3TB!
- Better approach: Lower thresholds in `azure_config.json` for testing

**Testing with lower thresholds:**
```bash
# Edit config temporarily
nano azure_config.json

# Change finance thresholds to:
# "usage_warning_percent": 0.01,   # 0.01% (very low)
# "usage_critical_percent": 0.02,  # 0.02%

# Restart monitoring
sudo systemctl restart azure-finance-monitor.timer

# Upload small test data
python3 scripts/upload_test_data.py --account finance --container invoices --count 5 --size 10

# Wait 5 minutes, check email for alerts
```

**Remember to restore thresholds after testing!**

---

## 🎯 Success Criteria

Your Azure monitoring is fully operational when:

- ✅ `test_azure_connection.py` shows all green checkmarks
- ✅ Systemd timers are active and running
- ✅ CSV reports generate every 5/10 minutes
- ✅ Logs show successful monitoring cycles
- ✅ Email alerts arrive when thresholds exceeded
- ✅ No errors in logs or systemd status

---

## 📊 Monitoring Schedule Summary

| Account | Interval | Warning | Critical | Containers |
|---------|----------|---------|----------|------------|
| Finance | 5 min | 60% | 70% | invoices, transactions, financial-reports, audit-logs |
| Marketing | 10 min | 80% | 90% | campaigns, analytics, media-assets, customer-data |

---

## 🔧 Troubleshooting Quick Fixes

### "Module not found: azure"
```bash
pip3 install --upgrade azure-identity azure-storage-blob
```

### "Permission denied"
```bash
chmod +x /home/admin/monitoring_node/azure/scripts/*.py
chmod +x /home/admin/monitoring_node/azure/setup_azure_monitoring.sh
```

### "Connection string invalid"
- Check for line breaks in connection string
- Must be single line starting with "DefaultEndpointsProtocol=https"
- Copy again from Azure Portal

### "Service Principal authentication failed"
- Verify Tenant ID matches Azure AD tenant
- Verify Client ID matches App Registration
- Verify Client Secret hasn't expired
- Check role assignment: Storage Blob Data Reader

### "No email alerts"
- Check `/home/admin/monitoring_node/config/analyzer_config.json` for email config
- Test email separately with VM monitoring
- Check alert cooldown (15 minutes)
- Verify thresholds are being exceeded

---

## 📚 Documentation Files

- **AZURE_SETUP_GUIDE.md** - Detailed Azure Portal walkthrough
- **README.md** - Complete system documentation
- **azure_config.template.json** - Configuration reference
- **DEPLOYMENT_GUIDE.md** - This file

---

## 🚀 Next Actions for You

1. [ ] Complete Azure Portal setup using `AZURE_SETUP_GUIDE.md`
2. [ ] Collect all 8 credential values
3. [ ] Transfer files to monitoring node
4. [ ] Update `azure_config.json` with real credentials
5. [ ] Run `test_azure_connection.py`
6. [ ] Execute `setup_azure_monitoring.sh`
7. [ ] Verify monitoring is working
8. [ ] Test alerts with sample data

---

## ⏱️ Timeline Estimate

- **Azure Portal Setup:** 30-45 minutes (first time)
- **File Transfer:** 5 minutes
- **Configuration:** 10 minutes
- **Installation:** 5 minutes
- **Testing:** 15 minutes
- **Total:** ~1-2 hours

---

## 💡 Pro Tips

1. **Keep credentials secure:** Never commit `azure_config.json` to Git
2. **Use Git ignore:** Add `azure_config.json` to `.gitignore`
3. **Backup configuration:** Keep encrypted backup of credentials
4. **Monitor costs:** Check Azure Cost Management regularly
5. **Test thoroughly:** Use low thresholds for initial testing
6. **Document changes:** Update README if you modify thresholds

---

## ✅ Deployment Checklist

### Pre-Deployment
- [ ] Azure Portal setup completed
- [ ] All 8 credentials collected
- [ ] Files transferred to monitoring node
- [ ] `azure_config.json` updated with real values
- [ ] No placeholder values remaining

### Deployment
- [ ] Azure SDK installed
- [ ] Connection test passed
- [ ] Setup script executed successfully
- [ ] Systemd timers active

### Post-Deployment
- [ ] CSV reports generating
- [ ] Logs show successful monitoring
- [ ] No errors in systemd status
- [ ] Email alerts tested and working

---

**Ready to start?** Begin with `AZURE_SETUP_GUIDE.md` and follow the steps! 🚀
