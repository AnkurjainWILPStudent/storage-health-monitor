# Deployment Checklist

Use this checklist to deploy the Storage Health Monitor to your VMs with email alerts.

## ✅ Pre-Deployment (On Windows Host)

### 1. Prepare Gmail for Alerts

- [ ] Choose Gmail account for sending alerts
- [ ] Enable 2-Step Verification on Gmail account
  - URL: https://myaccount.google.com/security
- [ ] Generate App Password
  - URL: https://myaccount.google.com/apppasswords
  - App name: "Storage Health Monitor"
  - **Save the 16-character password** (format: xxxx xxxx xxxx xxxx)
- [ ] Note recipient email address(es) for alerts

**Your Information:**
```
Gmail Address: ___________________________________
App Password:  
Recipients:    ___________________________________
```

### 2. Push to GitHub

- [ ] Review that .gitignore is present (protects .env files)
- [ ] Initialize git repository (if not done)
  ```powershell
  cd C:\Users\ankur\Desktop\MISC\projects\storage-health-monitor
  git init
  git add .
  git commit -m "Initial commit: Storage Health Monitor"
  ```
- [ ] Create GitHub repository
  - Suggested name: `storage-health-monitor`
- [ ] Add remote and push
  ```powershell
  git remote add origin https://github.com/YOUR-USERNAME/storage-health-monitor.git
  git branch -M main
  git push -u origin main
  ```

## ✅ Deployment (On Monitoring Node)

### 3. SSH to Monitoring Node

```bash
ssh admin@192.168.1.13
```

### 4. Clone Repository

- [ ] Clone from GitHub
  ```bash
  cd /home/admin
  git clone https://github.com/YOUR-USERNAME/storage-health-monitor.git
  cd storage-health-monitor
  ```

### 5. Configure Email

**Option A: Using configure_email.py (Recommended)**

- [ ] Run configuration script
  ```bash
  cd monitoring_node/scripts
  chmod +x configure_email.py
  python3 configure_email.py
  ```
- [ ] Enter Gmail address when prompted
- [ ] Enter App Password when prompted (paste the 16-char password)
- [ ] Enter recipient emails (comma-separated if multiple)

**Option B: Using .env file**

- [ ] Create .env file
  ```bash
  cd /home/admin/storage-health-monitor
  cp .env.template .env
  nano .env
  ```
- [ ] Add credentials:
  ```
  EMAIL_SENDER=your-email@gmail.com
  EMAIL_PASSWORD=xxxxxxxxxxxx
  EMAIL_RECIPIENTS=admin@example.com
  ```
- [ ] Run configuration
  ```bash
  cd monitoring_node/scripts
  python3 configure_email.py
  ```

### 6. Test Email Alerts

- [ ] Run test script
  ```bash
  cd /home/admin/storage-health-monitor/monitoring_node/scripts
  python3 test_email_alert.py
  ```
- [ ] Verify output shows successful send
- [ ] **Check your email inbox** - you should receive 2 emails:
  - WARNING alert (simulated 80% disk usage)
  - CRITICAL alert (simulated 95% disk usage)
- [ ] If emails not received, check spam folder

**Troubleshooting:**
- If "Authentication failed": Double-check App Password (not regular Gmail password)
- If "Connection failed": Check `ping smtp.gmail.com` and `nc -zv smtp.gmail.com 587`
- See EMAIL_SETUP_GUIDE.md for more troubleshooting

### 7. Run Setup Script

- [ ] Make setup script executable
  ```bash
  cd /home/admin/storage-health-monitor/monitoring_node/scripts
  chmod +x setup_monitoring.sh
  ```
- [ ] Run setup
  ```bash
  ./setup_monitoring.sh
  ```
- [ ] Verify directories created:
  ```bash
  ls -ld /home/admin/monitoring_node
  ls -ld /home/admin/monitoring_node/{scripts,config,logs,reports}
  ```

### 8. Verify Client Data

- [ ] Check that client nodes are sending data
  ```bash
  ls -lh /home/admin/monitor_data/clientnode/
  ls -lh /home/admin/monitor_data/clientnode2/
  ```
- [ ] Should see JSON files with timestamps (e.g., `storage_report_20240115_143000.json`)

### 9. Start Monitoring Service

- [ ] Enable and start systemd timer
  ```bash
  sudo systemctl daemon-reload
  sudo systemctl enable storage-analyzer.timer
  sudo systemctl start storage-analyzer.timer
  ```
- [ ] Check status
  ```bash
  sudo systemctl status storage-analyzer.timer
  sudo systemctl status storage-analyzer.service
  ```

### 10. Verify Analysis

- [ ] Run analyzer manually (first time)
  ```bash
  cd /home/admin/monitoring_node/scripts
  python3 analyze_storage_health.py
  ```
- [ ] Check for analysis reports
  ```bash
  ls -lh /home/admin/monitoring_node/reports/
  ```
- [ ] Check logs for errors
  ```bash
  tail -f /home/admin/monitoring_node/logs/analyzer.log
  ```

## ✅ Post-Deployment Verification

### 11. Monitor Logs

- [ ] Watch analyzer logs for 10 minutes
  ```bash
  tail -f /home/admin/monitoring_node/logs/analyzer.log
  ```
- [ ] Verify it runs every 10 minutes (check timestamps)
- [ ] Look for "Analysis completed successfully" messages

### 12. Check Reports

- [ ] View latest report
  ```bash
  cat /home/admin/monitoring_node/reports/summary_*.json | jq .
  ```
- [ ] Verify both clients appear (clientnode, clientnode2)
- [ ] Check severity levels are being calculated

### 13. Test Alert Triggers

**Option A: Simulate high disk usage on client**

On client node (192.168.1.14 or .15):
```bash
# Create large file to exceed threshold
dd if=/dev/zero of=/tmp/testfile bs=1G count=10
```

Wait 10 minutes for next analysis, then check:
```bash
# On monitoring node
tail -20 /home/admin/monitoring_node/logs/analyzer.log
# Should see WARNING or CRITICAL alert triggered
```

**Don't forget to delete test file:**
```bash
rm /tmp/testfile
```

**Option B: Lower threshold temporarily**

```bash
# On monitoring node
nano /home/admin/monitoring_node/config/thresholds.json
# Change disk warning to 10% temporarily
# Wait for next analysis cycle
# Change back to 75% after testing
```

### 14. Verify Emails Received

- [ ] If alert triggered, check inbox for real alert email
- [ ] Verify email format is HTML with proper details
- [ ] Check that severity level is correct (WARNING/CRITICAL)

## ✅ Ongoing Maintenance

### Daily/Weekly Tasks

- [ ] Check `/home/admin/monitoring_node/logs/analyzer.log` for errors
- [ ] Verify systemd timer is running: `systemctl status storage-analyzer.timer`
- [ ] Check inbox for any alerts received

### Monthly Tasks

- [ ] Review threshold settings in `thresholds.json`
- [ ] Check disk space on monitoring node: `df -h /home/admin`
- [ ] Clean old archived reports if needed

### When Making Changes

- [ ] Edit files on Windows host
- [ ] Commit and push to GitHub
  ```powershell
  git add .
  git commit -m "Description of changes"
  git push origin main
  ```
- [ ] Pull on monitoring node
  ```bash
  ssh admin@192.168.1.13
  cd /home/admin/storage-health-monitor
  git pull origin main
  ```
- [ ] Restart service if needed
  ```bash
  sudo systemctl restart storage-analyzer.timer
  ```

## 📚 Reference Documentation

After deployment, refer to these guides:

- **GITHUB_WORKFLOW.md** - Git workflow, updating code, protecting secrets
- **EMAIL_SETUP_GUIDE.md** - Email configuration, troubleshooting, Gmail limits
- **DEPLOYMENT_YOUR_SETUP.md** - Detailed deployment instructions for your environment
- **QUICKREF_YOUR_SETUP.md** - Quick reference commands and tips

## 🎯 Success Criteria

Your deployment is successful when:

✅ Test emails (WARNING and CRITICAL) received in inbox  
✅ Analyzer runs every 10 minutes automatically  
✅ Logs show "Analysis completed successfully"  
✅ Reports generated in `/home/admin/monitoring_node/reports/`  
✅ Both client nodes (clientnode, clientnode2) appear in reports  
✅ Real alert email received when threshold actually exceeded  
✅ No errors in `/home/admin/monitoring_node/logs/analyzer.log`  

## 🔧 Troubleshooting Quick Reference

| Issue | Check | Solution |
|-------|-------|----------|
| No test emails | Spam folder | Mark as "Not Spam" |
| Authentication failed | App Password | Regenerate at myaccount.google.com/apppasswords |
| Service not running | `systemctl status` | `sudo systemctl start storage-analyzer.timer` |
| No reports generated | Analyzer logs | Run manually: `python3 analyze_storage_health.py` |
| Missing client data | `/home/admin/monitor_data/` | Check client nodes are sending reports |
| Emails not on alerts | `thresholds.json` | Verify `enable_email: true` |

## ✉️ Support

If you encounter issues:

1. Check logs: `tail -100 /home/admin/monitoring_node/logs/analyzer.log`
2. Run analyzer manually to see output: `python3 analyze_storage_health.py`
3. Verify config: `cat /home/admin/monitoring_node/config/analyzer_config.json | jq .email`
4. Test email independently: `python3 test_email_alert.py`

**Your Setup:**
- Monitoring Node: admin@192.168.1.13
- Client Node 1: clientnode (192.168.1.14)
- Client Node 2: clientnode2 (192.168.1.15)
- Data Location: /home/admin/monitor_data/
- Analyzer Location: /home/admin/monitoring_node/

---

**Ready to deploy? Start with Step 1 above! 🚀**
