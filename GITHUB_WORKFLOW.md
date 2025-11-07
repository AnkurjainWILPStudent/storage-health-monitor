# GitHub Workflow Guide

## Using Git with Your VMs

Since your VMs can't access the VirtualBox network 192.168.56.x from outside, but you SSH using 192.168.1.x, you can use GitHub as the deployment method.

## Setup Process

### 1. Initial GitHub Setup (One Time)

**On your Windows host machine:**

```powershell
# Navigate to project directory
cd C:\Users\ankur\Desktop\MISC\projects\storage-health-monitor

# Initialize git (if not already done)
git init

# Add all files
git add .

# Commit
git commit -m "Initial commit: Storage Health Monitor with email alerts"

# Create repository on GitHub (via web or CLI)
# Then add remote and push
git remote add origin https://github.com/AnkurjainWILPStudent/storage-health-monitor.git
git branch -M main
git push -u origin main
```

### 2. Deploy to Monitoring Node VM

**SSH to monitoring node:**

```bash
ssh admin@192.168.1.13
```

**Clone the repository:**

```bash
# Clone from GitHub
cd /home/admin
git clone https://github.com/AnkurjainWILPStudent/storage-health-monitor.git

# Or if you prefer a specific directory name
git clone https://github.com/AnkurjainWILPStudent/storage-health-monitor.git monitoring_node_repo
cd monitoring_node_repo
```

**Set up email configuration:**

```bash
# Copy environment template
cp .env.template .env

# Edit with your credentials
nano .env
```

Add your actual values:
```bash
EMAIL_SENDER=your-actual-email@gmail.com
EMAIL_PASSWORD=your-gmail-app-password
EMAIL_RECIPIENTS=admin@example.com
```

**Configure email in analyzer:**

```bash
cd monitoring_node/scripts
chmod +x configure_email.py
python3 configure_email.py
```

This will:
- Read from .env file (if present)
- Or prompt you interactively
- Update analyzer_config.json with credentials

**Run setup:**

```bash
chmod +x setup_monitoring.sh
./setup_monitoring.sh
```

**Test email alerts:**

```bash
python3 test_email_alert.py
```

You should receive 2 test emails (WARNING and CRITICAL).

### 3. Update When You Make Changes

**On Windows (after making changes):**

```powershell
cd C:\Users\ankur\Desktop\MISC\projects\storage-health-monitor

# Stage changes
git add .

# Commit
git commit -m "Description of your changes"

# Push to GitHub
git push origin main
```

**On Monitoring Node VM:**

```bash
ssh admin@192.168.1.13
cd /home/admin/storage-health-monitor  # or monitoring_node_repo

# Pull latest changes
git pull origin main

# If you updated configs, restart the analyzer
sudo systemctl restart storage-analyzer.timer
```

## Important: Protecting Sensitive Data

### What's Safe to Commit (already configured in .gitignore)

✅ **DO commit:**
- Scripts (.py files)
- Configuration templates (.env.template)
- Documentation (.md files)  
- Empty config files with placeholders

❌ **DON'T commit:**
- `.env` file (contains passwords)
- `analyzer_config.json` if it has real passwords
- Log files (*.log)
- Data files (monitor_data/, reports/)
- SSH keys

### Handling analyzer_config.json

The config file has your email password. Two options:

**Option 1: Use environment variables (Recommended)**

Modify analyzer to read from environment:
```bash
export EMAIL_SENDER="your@gmail.com"
export EMAIL_PASSWORD="your-app-password"
python3 scripts/analyze_storage_health.py
```

**Option 2: Keep config local (Current)**

The current analyzer_config.json in git has placeholders. On your VM:
```bash
# After git pull, re-run configuration
cd /home/admin/storage-health-monitor/monitoring_node/scripts
python3 configure_email.py
```

This updates the local config without committing passwords to git.

## Workflow Examples

### Example 1: Update Thresholds

**On Windows:**

```powershell
# Edit thresholds
code monitoring_node\config\thresholds.json

# Change disk warning from 75% to 80%
# Save file

# Commit and push
git add monitoring_node/config/thresholds.json
git commit -m "Increased disk warning threshold to 80%"
git push origin main
```

**On Monitoring Node:**

```bash
ssh admin@192.168.1.13
cd /home/admin/storage-health-monitor
git pull origin main

# Restart analyzer to pick up changes
sudo systemctl restart storage-analyzer.timer
```

### Example 2: Fix a Bug in Analyzer

**On Windows:**

```powershell
# Edit the Python script
code monitoring_node\scripts\analyze_storage_health.py

# Make your changes
# Save

# Commit and push
git add monitoring_node/scripts/analyze_storage_health.py
git commit -m "Fixed bug in disk usage calculation"
git push origin main
```

**On Monitoring Node:**

```bash
ssh admin@192.168.1.13
cd /home/admin/storage-health-monitor
git pull origin main

# Restart to apply changes
sudo systemctl restart storage-analyzer.service
```

### Example 3: Update Documentation

**On Windows:**

```powershell
# Edit docs
code README.md

# Commit and push
git add README.md
git commit -m "Updated deployment instructions"
git push origin main
```

**On Monitoring Node:**

```bash
# Pull latest docs
cd /home/admin/storage-health-monitor
git pull origin main

# No restart needed for docs
```

## Git Commands Reference

### Daily Operations

```bash
# Check status
git status

# See what changed
git diff

# Pull latest changes
git pull origin main

# Stage all changes
git add .

# Commit with message
git commit -m "Your message here"

# Push to GitHub
git push origin main

# View commit history
git log --oneline -10
```

### Useful Commands

```bash
# Discard local changes to a file
git checkout -- filename

# Undo last commit (keep changes)
git reset --soft HEAD~1

# See what will be pulled
git fetch origin
git log HEAD..origin/main --oneline

# Create a branch for testing
git checkout -b test-feature
git checkout main  # switch back
```

## Best Practices

### 1. Commit Messages

Good commit messages:
```
✓ "Added email alerting for critical disk usage"
✓ "Fixed bug in SMART status parsing"
✓ "Updated thresholds: disk warning 75% → 80%"
```

Bad commit messages:
```
✗ "update"
✗ "fix"
✗ "changes"
```

### 2. Before Pushing

```bash
# Always check what you're committing
git status
git diff

# Make sure .env is NOT staged
git status | grep .env  # Should not appear

# Review logs are not included
git status | grep -E '\.log|monitor_data|reports'  # Should not appear
```

### 3. Regular Commits

```bash
# Commit often with logical changes
git add monitoring_node/alert_handler.py
git commit -m "Enhanced email alert formatting"

git add monitoring_node/config/thresholds.json
git commit -m "Adjusted SMART error thresholds"

# Then push both
git push origin main
```

## Troubleshooting

### Issue: git pull fails with conflicts

```bash
# See what conflicts
git status

# If you want to keep remote version
git checkout --theirs filename
git add filename

# If you want to keep local version
git checkout --ours filename
git add filename

# Complete the merge
git commit
```

### Issue: Accidentally committed .env

```bash
# Remove from git (keeps local file)
git rm --cached .env

# Add to .gitignore (already there)
echo ".env" >> .gitignore

# Commit the removal
git add .gitignore
git commit -m "Removed .env from repository"
git push origin main

# If already pushed, you may need to remove from history
# (more complex - ask for help if needed)
```

### Issue: Can't push - authentication failed

```bash
# Use GitHub personal access token
# Settings → Developer settings → Personal access tokens
# Generate token with "repo" scope

# When prompted for password, use the token instead
git push origin main
Username: AnkurjainWILPStudent
Password: [paste token here]

# Or configure git credential helper
git config --global credential.helper store
```

## VM Network Reference

Your VMs have two IP sets:

**Internal Network (VirtualBox Host-Only - 192.168.56.x):**
- Used by VMs to communicate with each other
- Used in client_node/config.json
- Not accessible from your Windows host

**SSH Network (Bridged - 192.168.1.x):**
- monitoringnode: 192.168.1.13
- clientnode: 192.168.1.14
- clientnode2: 192.168.1.15
- Used for SSH access from Windows host

**GitHub Workflow:**
```
Windows Host ──git push──> GitHub ──git pull──> Monitoring Node (192.168.1.13)
     ↓                                                   ↓
  Make changes                                    Apply changes
  Commit & push                                   Pull & restart
```

## Summary

1. ✅ Develop on Windows, push to GitHub
2. ✅ Pull on monitoring node (SSH via 192.168.1.13)
3. ✅ Never commit passwords (.env, configured analyzer_config.json)
4. ✅ Use `configure_email.py` to set up secrets on VM
5. ✅ Restart services after pulling changes
6. ✅ Test email alerts after configuration

**You're all set to use GitHub for deploying to your VMs!**
