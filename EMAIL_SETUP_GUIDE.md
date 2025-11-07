# Email Alert Setup Guide

## Prerequisites

You need a Gmail account to send alert emails. The analyzer will send emails when:
- **WARNING**: Disk usage exceeds 75%
- **CRITICAL**: Disk usage exceeds 90%

## Step 1: Generate Gmail App Password

Google requires an "App Password" for applications to send emails (your regular Gmail password won't work).

### Generate App Password:

1. **Enable 2-Step Verification** (if not already enabled):
   - Go to: https://myaccount.google.com/security
   - Click "2-Step Verification" under "Signing in to Google"
   - Follow the setup wizard

2. **Create App Password**:
   - Go to: https://myaccount.google.com/apppasswords
   - Or: Google Account → Security → 2-Step Verification → App passwords
   - Select app: "Mail" or "Other (Custom name)"
   - Enter name: "Storage Health Monitor"
   - Click "Generate"
   - **Copy the 16-character password** (format: xxxx xxxx xxxx xxxx)

3. **Save the password** - you won't see it again!

## Step 2: Configure Email on Monitoring Node

### Option A: Using configure_email.py (Recommended)

**SSH to monitoring node:**

```bash
ssh admin@192.168.1.13
cd /home/admin/storage-health-monitor/monitoring_node/scripts
```

**Method 1: Interactive prompts**

```bash
python3 configure_email.py
```

You'll be prompted for:
- Gmail address: `your-email@gmail.com`
- App Password: `xxxxxxxxxxxx` (paste the 16-char password)
- Recipient emails: `admin@example.com,alerts@example.com` (comma-separated)

**Method 2: Using environment variables**

```bash
# Create .env file in project root
cd /home/admin/storage-health-monitor
nano .env
```

Add:
```bash
EMAIL_SENDER=your-email@gmail.com
EMAIL_PASSWORD=xxxxxxxxxxxx
EMAIL_RECIPIENTS=admin@example.com,alerts@example.com
```

Save and exit (Ctrl+X, Y, Enter)

```bash
# Run configuration
cd monitoring_node/scripts
python3 configure_email.py
```

This reads from .env automatically.

### Option B: Manual Configuration

Edit the config file directly:

```bash
ssh admin@192.168.1.13
cd /home/admin/storage-health-monitor/monitoring_node/config
nano analyzer_config.json
```

Update the email section:

```json
"email": {
  "enabled": true,
  "smtp_server": "smtp.gmail.com",
  "smtp_port": 587,
  "use_tls": true,
  "sender_email": "your-email@gmail.com",
  "sender_password": "xxxxxxxxxxxx",
  "recipient_emails": [
    "admin@example.com",
    "alerts@example.com"
  ],
  "send_on_warning": true,
  "send_on_critical": true
}
```

Save and exit.

## Step 3: Test Email Alerts

```bash
ssh admin@192.168.1.13
cd /home/admin/storage-health-monitor/monitoring_node/scripts
python3 test_email_alert.py
```

**Expected output:**

```
Loading analyzer config...
Email alerts enabled: True
SMTP Server: smtp.gmail.com:587
Sender: your-email@gmail.com
Recipients: ['admin@example.com']

Sending test WARNING alert...
✓ WARNING alert sent successfully

Sending test CRITICAL alert...
✓ CRITICAL alert sent successfully

Check your inbox at: admin@example.com
```

**Check your email** - you should receive 2 test emails:
1. **WARNING Alert**: Disk usage simulation at 80%
2. **CRITICAL Alert**: Disk usage simulation at 95%

## Troubleshooting

### Error: "Authentication failed"

**Cause:** Wrong password or App Password not used

**Solution:**
1. Make sure you're using the **App Password**, not your regular Gmail password
2. Copy the password again from Google (no spaces)
3. If using .env, check for extra spaces: `EMAIL_PASSWORD=yourpassword` (no spaces around =)

### Error: "SMTP connection failed"

**Cause:** Network issues or blocked port

**Solution:**
1. Check internet connection: `ping smtp.gmail.com`
2. Verify port 587 is open: `nc -zv smtp.gmail.com 587`
3. Try alternative port 465 with SSL in config (less common)

### Error: "Recipient address rejected"

**Cause:** Invalid recipient email format

**Solution:**
1. Check email addresses in config
2. Make sure format is: `name@domain.com`
3. For multiple recipients: `["email1@domain.com", "email2@domain.com"]`

### Email not received

**Possible causes:**

1. **Check spam folder** - Gmail may mark alerts as spam initially
2. **Verify recipient email** - typo in address?
3. **Check Gmail "Sent" folder** - was it sent?
4. **Gmail sending limits** - free Gmail accounts limited to 500 emails/day

### Error: "Email not enabled in config"

**Cause:** Email alerts disabled in thresholds.json

**Solution:**
```bash
nano /home/admin/storage-health-monitor/monitoring_node/config/thresholds.json
```

Change:
```json
"alerts": {
  "enable_email": true,  // Make sure this is true
  ...
}
```

## Email Alert Details

### What triggers an email?

Based on `thresholds.json`:

**WARNING Level:**
- Disk usage > 75%
- Memory usage > 80%
- SMART errors > 10
- Failed disks detected

**CRITICAL Level:**
- Disk usage > 90%
- Memory usage > 95%
- SMART errors > 50
- Multiple failed disks

### Email format

**Subject:**
```
[WARNING] Storage Health Alert - clientnode
[CRITICAL] Storage Health Alert - clientnode2
```

**Body (HTML formatted):**
```
Storage Health Alert

Client: clientnode
Severity: CRITICAL
Timestamp: 2024-01-15 14:30:00

Issues Detected:
• Disk /dev/sda1 usage: 92.5% (Critical)
• Memory usage: 85.2% (Warning)

Details:
{
  "disk_usage": 92.5,
  "memory_usage": 85.2,
  ...
}
```

### Customizing email behavior

Edit `analyzer_config.json`:

```json
"email": {
  "send_on_warning": true,   // Send for WARNING level
  "send_on_critical": true,  // Send for CRITICAL level
  ...
}
```

To only get critical alerts:
```json
"send_on_warning": false,
"send_on_critical": true
```

## Multiple Recipients

Add multiple admins to receive alerts:

**In configure_email.py:**
```
Recipients (comma-separated): admin@example.com,backup-admin@example.com,alerts@example.com
```

**In analyzer_config.json:**
```json
"recipient_emails": [
  "admin@example.com",
  "backup-admin@example.com",
  "alerts@example.com"
]
```

## Security Best Practices

### 1. Never Commit Passwords

✅ **Safe:**
```bash
# Using .env file (in .gitignore)
EMAIL_PASSWORD=yourapppassword
```

❌ **Unsafe:**
```bash
# Hardcoding in scripts or committing to git
git add analyzer_config.json  # Contains password!
git commit -m "Added config"  # DON'T DO THIS
```

### 2. Protect .env File

```bash
# Set proper permissions
chmod 600 /home/admin/storage-health-monitor/.env

# Only readable by owner (admin)
ls -l .env
# Output: -rw------- 1 admin admin 150 Jan 15 14:30 .env
```

### 3. Use Different Passwords

- Don't reuse your personal Gmail password
- Create a dedicated Gmail account for monitoring (optional but recommended)
- Example: `storage-alerts@gmail.com`

### 4. Rotate App Passwords

Periodically regenerate:
1. Revoke old App Password in Google Account
2. Generate new App Password
3. Update .env file
4. Re-run configure_email.py

## Gmail Account Limits

**Free Gmail Account:**
- 500 emails per day
- 500 recipients per day

**If you exceed limits:**
1. Use workspace Gmail (2000/day limit)
2. Reduce alert frequency
3. Use webhook alerts instead for high-frequency monitoring

**Example calculation:**
- 2 client nodes
- Check every 10 minutes
- Only critical alerts

Worst case (both nodes always critical):
- 24 hours / 10 minutes = 144 checks
- 144 checks × 2 nodes = 288 emails/day
- Still under 500 limit ✓

## Summary Checklist

- [ ] Enable 2-Step Verification on Gmail
- [ ] Generate App Password
- [ ] SSH to monitoring node (192.168.1.13)
- [ ] Create .env file or run configure_email.py
- [ ] Enter Gmail address, App Password, recipients
- [ ] Run test_email_alert.py
- [ ] Verify 2 test emails received
- [ ] Check spam folder if emails missing
- [ ] Secure .env file permissions (chmod 600)
- [ ] Start analyzer service

**You're all set for email alerts!**

## Questions?

If test emails work but real alerts don't:
1. Check analyzer logs: `cat /home/admin/monitoring_node/logs/analyzer.log`
2. Verify client data is being received: `ls -l /home/admin/monitor_data/clientnode/`
3. Check if thresholds are actually exceeded
4. Run analyzer manually to see output: `python3 analyze_storage_health.py`
