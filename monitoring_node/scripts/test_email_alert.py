#!/usr/bin/env python3
"""
Test email alert functionality.
Sends a test WARNING and CRITICAL alert to verify configuration.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from alert_handler import AlertHandler, AlertLevel
from utils import load_config
import logging

def main():
    print("=" * 70)
    print("Storage Health Monitor - Email Alert Test")
    print("=" * 70)
    print()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # Load configuration
    config_dir = Path(__file__).parent.parent / "config"
    config_path = config_dir / "analyzer_config.json"
    thresholds_path = config_dir / "thresholds.json"
    
    if not config_path.exists():
        print(f"❌ Config not found: {config_path}")
        print("Run configure_email.py first")
        sys.exit(1)
    
    config = load_config(str(config_path))
    thresholds = load_config(str(thresholds_path))
    
    # Check email configuration
    email_config = config.get('alert_config', {}).get('email', {})
    if not email_config.get('enabled'):
        print("❌ Email alerts are not enabled in config")
        print("Run configure_email.py to set up email alerts")
        sys.exit(1)
    
    sender = email_config.get('sender_email')
    recipients = email_config.get('recipient_emails', [])
    
    print("Email Configuration:")
    print(f"  Sender:     {sender}")
    print(f"  Recipients: {', '.join(recipients)}")
    print()
    
    # Create alert handler
    alert_handler = AlertHandler(thresholds, logger)
    
    print("Sending test alerts...")
    print()
    
    # Test WARNING alert
    print("1. Sending WARNING test alert...")
    success_warning = alert_handler.send_alert(
        level=AlertLevel.WARNING,
        title="Test WARNING Alert",
        message="This is a test WARNING alert from Storage Health Monitor. If you receive this email, WARNING alerts are working correctly.",
        hostname="monitoringnode",
        details={
            'test_type': 'WARNING level',
            'disk_usage': '78%',
            'threshold': '75% (warning)',
            'timestamp': 'Test run'
        }
    )
    
    if success_warning:
        print("✓ WARNING alert sent")
    else:
        print("❌ WARNING alert failed (check logs)")
    
    print()
    
    # Test CRITICAL alert
    print("2. Sending CRITICAL test alert...")
    success_critical = alert_handler.send_alert(
        level=AlertLevel.CRITICAL,
        title="Test CRITICAL Alert",
        message="This is a test CRITICAL alert from Storage Health Monitor. If you receive this email, CRITICAL alerts are working correctly.",
        hostname="monitoringnode",
        details={
            'test_type': 'CRITICAL level',
            'disk_usage': '95%',
            'threshold': '90% (critical)',
            'timestamp': 'Test run',
            'action_required': 'This is a test - no action needed'
        }
    )
    
    if success_critical:
        print("✓ CRITICAL alert sent")
    else:
        print("❌ CRITICAL alert failed (check logs)")
    
    print()
    print("=" * 70)
    
    if success_warning and success_critical:
        print("✓ Email alert test completed successfully!")
        print()
        print("Check your inbox for:")
        for recipient in recipients:
            print(f"  • {recipient}")
        print()
        print("You should receive 2 test emails:")
        print("  1. [Storage Alert] WARNING: Test WARNING Alert")
        print("  2. [Storage Alert] CRITICAL: Test CRITICAL Alert")
    else:
        print("❌ Some alerts failed to send")
        print()
        print("Troubleshooting:")
        print("  1. Check sender_email and sender_password in analyzer_config.json")
        print("  2. Verify Gmail App Password (not regular password)")
        print("  3. Check recipient_emails are correct")
        print("  4. Review logs for detailed error messages")
    
    print("=" * 70)
    print()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
