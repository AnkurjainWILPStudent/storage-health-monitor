# Required Input Summary

## 📧 What You Need to Provide

To complete the email alert setup, you'll need the following information:

### 1. Gmail Account Details

**Gmail Address** (for sending alerts):
```
Example: storage-alerts@gmail.com
Your Email: _______________________________
```

**Gmail App Password** (16 characters):
- ⚠️ This is NOT your regular Gmail password
- You must generate this from: https://myaccount.google.com/apppasswords
- Format: `xxxx xxxx xxxx xxxx` (4 groups of 4 characters)
```
Your App Password: ____ ____ ____ ____
```

**Recipient Email(s)** (who receives alerts):
```
Example: admin@example.com
Your Recipients: _______________________________
```
*Note: For multiple recipients, separate with commas: `admin@example.com,backup@example.com`*

---

## 🔧 How to Get Gmail App Password

### Prerequisites
1. You must have 2-Step Verification enabled on your Gmail account
2. You need access to your Google Account settings

### Step-by-Step Instructions

1. **Go to Google Account Security**
   - URL: https://myaccount.google.com/security
   - Sign in with your Gmail account

2. **Enable 2-Step Verification** (if not already enabled)
   - Scroll to "Signing in to Google"
   - Click "2-Step Verification"
   - Follow the setup wizard (you'll need your phone)

3. **Generate App Password**
   - Go to: https://myaccount.google.com/apppasswords
   - Or: Google Account → Security → 2-Step Verification → App passwords (at bottom)
   
4. **Create Password**
   - Select app: "Mail" or "Other (Custom name)"
   - Enter name: "Storage Health Monitor"
   - Click "Generate"

5. **Copy the Password**
   - You'll see a 16-character password like: `abcd efgh ijkl mnop`
   - **Copy it immediately** - you won't see it again!
   - Paste it somewhere safe (you'll use it during deployment)

---

## 📝 Configuration Examples

### Example 1: Single Administrator

```bash
EMAIL_SENDER=monitoring@gmail.com
EMAIL_PASSWORD=abcdefghijklmnop
EMAIL_RECIPIENTS=admin@company.com
```

### Example 2: Multiple Recipients

```bash
EMAIL_SENDER=storage-alerts@gmail.com
EMAIL_PASSWORD=wxyzabcd12345678
EMAIL_RECIPIENTS=admin@company.com,backup-admin@company.com,alerts@company.com
```

### Example 3: Personal Gmail for Testing

```bash
EMAIL_SENDER=your.personal@gmail.com
EMAIL_PASSWORD=yourapppassword
EMAIL_RECIPIENTS=your.personal@gmail.com
```
*Note: You can send and receive on the same email for testing*

---

## 🚀 When Will You Use This Information?

During deployment, you'll enter these values in **one of two ways**:

### Option 1: Interactive Configuration (Easier)

When you run `configure_email.py`, it will prompt you:

```bash
cd /home/admin/storage-health-monitor/monitoring_node/scripts
python3 configure_email.py

# You'll see:
Enter sender email address: storage-alerts@gmail.com
Enter sender email password (App Password): [paste 16-char password]
Enter recipient email addresses (comma-separated): admin@company.com
```

### Option 2: .env File (More Secure)

Create a `.env` file with your credentials:

```bash
cd /home/admin/storage-health-monitor
nano .env
```

Add your information:
```
EMAIL_SENDER=your-email@gmail.com
EMAIL_PASSWORD=your-app-password
EMAIL_RECIPIENTS=recipient@example.com
```

Then run:
```bash
cd monitoring_node/scripts
python3 configure_email.py
```

It will automatically read from `.env`.

---

## ✅ Verification

After configuration, you'll test with:

```bash
python3 test_email_alert.py
```

This sends 2 test emails:
1. **WARNING alert** - simulated 80% disk usage
2. **CRITICAL alert** - simulated 95% disk usage

**Success looks like:**
- Both emails arrive in your inbox within 1-2 minutes
- Emails are HTML-formatted with colored severity levels
- Subject shows [WARNING] or [CRITICAL] with client hostname

**If emails don't arrive:**
- Check spam folder (mark as "Not Spam")
- Verify App Password is correct (16 characters, no spaces)
- Confirm 2-Step Verification is enabled
- See EMAIL_SETUP_GUIDE.md for troubleshooting

---

## 🔒 Security Notes

### DO:
✅ Use App Password (not your regular Gmail password)  
✅ Keep App Password in `.env` file (protected by `.gitignore`)  
✅ Use `chmod 600 .env` to restrict file permissions  
✅ Consider creating a dedicated Gmail account for monitoring  

### DON'T:
❌ Never commit `.env` file to git  
❌ Never hardcode passwords in scripts  
❌ Don't share App Password in chat/email  
❌ Don't reuse App Password for other applications  

---

## 📊 Alert Thresholds (What Triggers Emails)

Once configured, emails will be sent automatically when:

### WARNING Level (Yellow):
- Disk usage > **75%**
- Memory usage > **80%**
- SMART errors > **10**
- Any failed disk detected

### CRITICAL Level (Red):
- Disk usage > **90%**
- Memory usage > **95%**
- SMART errors > **50**
- Multiple failed disks

*These thresholds can be adjusted in `monitoring_node/config/thresholds.json`*

---

## 📧 Email Preview

### Subject Line:
```
[WARNING] Storage Health Alert - clientnode
[CRITICAL] Storage Health Alert - clientnode2
```

### Email Body (HTML):
```
Storage Health Alert

Client: clientnode
Severity: CRITICAL
Timestamp: 2024-01-15 14:30:00

Issues Detected:
• Disk /dev/sda1 usage: 92.5% (Critical)
• Memory usage: 85.2% (Warning)

[Detailed JSON data follows...]
```

---

## 🎯 Next Steps

1. **Generate App Password** (if not done already)
2. **Fill in your details** at the top of this document
3. **Follow DEPLOYMENT_CHECKLIST.md** for complete deployment
4. **Configure email** using one of the two options above
5. **Run test_email_alert.py** to verify
6. **Done!** Emails will automatically send when thresholds exceeded

---

## 📚 Additional Resources

- **DEPLOYMENT_CHECKLIST.md** - Complete deployment steps
- **EMAIL_SETUP_GUIDE.md** - Detailed email configuration and troubleshooting
- **GITHUB_WORKFLOW.md** - How to use GitHub for deployment
- **README.md** - Project overview and features

---

## ❓ Quick FAQ

**Q: Can I use a different email provider (not Gmail)?**  
A: Currently optimized for Gmail. Other providers need different SMTP settings.

**Q: Will this work with Gmail's free tier?**  
A: Yes! Free Gmail allows 500 emails/day, more than enough for typical monitoring.

**Q: Do I need a separate Gmail account?**  
A: No, but recommended for security. You can use your personal Gmail for testing.

**Q: What if I don't have 2-Step Verification?**  
A: You must enable it to generate App Passwords. It's quick: https://myaccount.google.com/security

**Q: Can I change recipients later?**  
A: Yes! Just re-run `configure_email.py` or edit `.env` and re-run.

**Q: Are passwords stored securely?**  
A: Yes, in `.env` file which is in `.gitignore` (never committed to git).

---

**Ready to proceed? Start with generating your Gmail App Password above! 🚀**
