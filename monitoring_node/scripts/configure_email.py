#!/usr/bin/env python3
"""
Configure email alerts from environment variables or interactive input.
Run this on the monitoring node to set up email alerting.
"""

import json
import os
import sys
import getpass
from pathlib import Path


def load_config(config_path):
    """Load analyzer configuration."""
    with open(config_path, 'r') as f:
        return json.load(f)


def save_config(config_path, config):
    """Save analyzer configuration."""
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)
    print(f"✓ Configuration saved to: {config_path}")


def get_env_or_prompt(env_var, prompt, is_password=False, default=None):
    """Get value from environment variable or prompt user."""
    value = os.environ.get(env_var)
    
    if value:
        if is_password:
            print(f"✓ Using {env_var} from environment")
        else:
            print(f"✓ Using {env_var} from environment: {value}")
        return value
    
    if is_password:
        value = getpass.getpass(prompt)
    else:
        if default:
            prompt = f"{prompt} [{default}]"
        value = input(f"{prompt}: ").strip()
        if not value and default:
            value = default
    
    return value


def main():
    print("=" * 70)
    print("Storage Health Monitor - Email Alert Configuration")
    print("=" * 70)
    print()
    
    # Find config file
    script_dir = Path(__file__).parent
    config_path = script_dir.parent / "config" / "analyzer_config.json"
    
    if not config_path.exists():
        print(f"❌ Config file not found: {config_path}")
        print("Make sure you're running this from the monitoring_node/scripts directory")
        sys.exit(1)
    
    print(f"Config file: {config_path}")
    print()
    
    # Load current config
    config = load_config(config_path)
    
    print("Email Configuration Setup")
    print("-" * 70)
    print()
    print("You can either:")
    print("  1. Set environment variables (EMAIL_SENDER, EMAIL_PASSWORD, EMAIL_RECIPIENTS)")
    print("  2. Enter values interactively")
    print()
    
    # Get email configuration
    sender_email = get_env_or_prompt(
        'EMAIL_SENDER',
        'Gmail address (sender)',
        default=config.get('alert_config', {}).get('email', {}).get('sender_email')
    )
    
    if not sender_email or sender_email == 'your-email@gmail.com':
        print("❌ Valid Gmail address required")
        sys.exit(1)
    
    print()
    print("Gmail App Password Setup:")
    print("  1. Go to: https://myaccount.google.com/apppasswords")
    print("  2. Select 'Mail' and generate a password")
    print("  3. Copy the 16-character password")
    print()
    
    sender_password = get_env_or_prompt(
        'EMAIL_PASSWORD',
        'Gmail App Password (16 chars)',
        is_password=True
    )
    
    if not sender_password or len(sender_password) < 10:
        print("❌ Valid Gmail App Password required (at least 10 characters)")
        sys.exit(1)
    
    recipients_str = get_env_or_prompt(
        'EMAIL_RECIPIENTS',
        'Recipient email(s) (comma-separated)',
        default=','.join(config.get('alert_config', {}).get('email', {}).get('recipient_emails', []))
    )
    
    recipient_emails = [email.strip() for email in recipients_str.split(',') if email.strip()]
    
    if not recipient_emails:
        print("❌ At least one recipient email required")
        sys.exit(1)
    
    # Update configuration
    if 'alert_config' not in config:
        config['alert_config'] = {}
    
    if 'email' not in config['alert_config']:
        config['alert_config']['email'] = {}
    
    config['alert_config']['email'].update({
        'enabled': True,
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'use_tls': True,
        'use_ssl': False,
        'sender_email': sender_email,
        'sender_password': sender_password,
        'recipient_emails': recipient_emails,
        'subject_prefix': '[Storage Alert]',
        'send_on_warning': True,
        'send_on_critical': True
    })
    
    # Save configuration
    save_config(config_path, config)
    
    print()
    print("=" * 70)
    print("✓ Email alerts configured successfully!")
    print("=" * 70)
    print()
    print("Configuration:")
    print(f"  Sender:     {sender_email}")
    print(f"  Recipients: {', '.join(recipient_emails)}")
    print(f"  SMTP:       smtp.gmail.com:587 (TLS)")
    print()
    print("Next steps:")
    print("  1. Test email alerts:")
    print(f"     python3 {script_dir}/test_email_alert.py")
    print()
    print("  2. Run the analyzer:")
    print("     python3 ../scripts/analyze_storage_health.py")
    print()
    print("⚠️  IMPORTANT: Do NOT commit analyzer_config.json with passwords!")
    print("   Use .env file for secrets (already in .gitignore)")
    print()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        sys.exit(1)
