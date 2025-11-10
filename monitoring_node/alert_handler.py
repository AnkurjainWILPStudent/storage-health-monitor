#!/usr/bin/env python3
"""
Alert handler for storage health monitoring.
Manages alert generation, cooldowns, and delivery via multiple channels.
"""

import os
import json
import logging
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pathlib import Path


class AlertLevel:
    """Alert severity levels."""
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertHandler:
    """Handles alert generation and delivery for storage health issues."""
    
    def __init__(self, config: Dict[str, Any], logger: Optional[logging.Logger] = None):
        """
        Initialize alert handler.
        
        Args:
            config: Alert configuration dictionary
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.alert_history = {}
        # Fixed: Use alert_config path instead of alerts
        self.cooldown_minutes = config.get('alert_config', {}).get('cooldown_minutes', 30)
        
        # Load alert history if exists
        self._load_alert_history()
    
    def send_alert(
        self,
        level: str,
        title: str,
        message: str,
        hostname: str,
        details: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send alert through configured channels.
        
        Args:
            level: Alert level (INFO, WARNING, CRITICAL)
            title: Alert title
            message: Alert message
            hostname: Hostname of affected system
            details: Additional details dictionary
            
        Returns:
            True if alert was sent, False if suppressed by cooldown
        """
        # Check cooldown
        alert_key = f"{hostname}:{title}"
        if not self._should_send_alert(alert_key, level):
            self.logger.debug(f"Alert suppressed by cooldown: {alert_key}")
            return False
        
        # Record alert
        self._record_alert(alert_key, level)
        
        # Build alert payload
        alert_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': level,
            'title': title,
            'message': message,
            'hostname': hostname,
            'details': details or {}
        }
        
        # Send through enabled channels
        alert_config = self.config.get('alert_config', {})
        success = False
        
        # Console alerts
        if alert_config.get('console', {}).get('enabled', True):
            self._send_console_alert(alert_data)
            success = True
        
        # Log alerts
        if alert_config.get('log', {}).get('enabled', True):
            self._send_log_alert(alert_data)
            success = True
        
        # Email alerts
        if alert_config.get('email', {}).get('enabled', False):
            try:
                self._send_email_alert(alert_data)
                success = True
            except Exception as e:
                self.logger.error(f"Failed to send email alert: {e}")
        
        # Webhook alerts
        if alert_config.get('webhook', {}).get('enabled', False):
            try:
                self._send_webhook_alert(alert_data)
                success = True
            except Exception as e:
                self.logger.error(f"Failed to send webhook alert: {e}")
        
        return success
    
    def _should_send_alert(self, alert_key: str, level: str) -> bool:
        """Check if alert should be sent based on cooldown."""
        # Always send CRITICAL alerts
        if level == AlertLevel.CRITICAL:
            return True
        
        if alert_key not in self.alert_history:
            return True
        
        last_sent = self.alert_history[alert_key]['last_sent']
        cooldown = timedelta(minutes=self.cooldown_minutes)
        
        return datetime.utcnow() - last_sent > cooldown
    
    def _record_alert(self, alert_key: str, level: str) -> None:
        """Record alert in history."""
        self.alert_history[alert_key] = {
            'last_sent': datetime.utcnow(),
            'level': level,
            'count': self.alert_history.get(alert_key, {}).get('count', 0) + 1
        }
        self._save_alert_history()
    
    def _send_console_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send alert to console."""
        level = alert_data['level']
        title = alert_data['title']
        message = alert_data['message']
        hostname = alert_data['hostname']
        
        banner = "=" * 80
        print(f"\n{banner}")
        print(f"[{level}] {title}")
        print(f"Host: {hostname}")
        print(f"Time: {alert_data['timestamp']}")
        print(f"{banner}")
        print(message)
        if alert_data.get('details'):
            print("\nDetails:")
            for key, value in alert_data['details'].items():
                print(f"  {key}: {value}")
        print(f"{banner}\n")
    
    def _send_log_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send alert to log file."""
        level = alert_data['level']
        message = f"[{alert_data['hostname']}] {alert_data['title']}: {alert_data['message']}"
        
        if level == AlertLevel.CRITICAL:
            self.logger.critical(message)
        elif level == AlertLevel.WARNING:
            self.logger.warning(message)
        else:
            self.logger.info(message)
        
        if alert_data.get('details'):
            self.logger.info(f"Alert details: {json.dumps(alert_data['details'])}")
    
    def _send_email_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send alert via email using Gmail SMTP."""
        email_config = self.config.get('alert_config', {}).get('email', {})
        
        # Check if email alerts are enabled
        if not email_config.get('enabled', False):
            self.logger.debug("Email alerts are disabled")
            return
        
        # Check for required configuration
        sender_email = email_config.get('sender_email')
        sender_password = email_config.get('sender_password')
        recipient_emails = email_config.get('recipient_emails', [])
        
        if not sender_email or not sender_password:
            self.logger.warning("Email sender credentials not configured")
            return
        
        if not recipient_emails:
            self.logger.warning("No email recipients configured")
            return
        
        # Check alert level filtering
        level = alert_data['level']
        if level == AlertLevel.WARNING and not email_config.get('send_on_warning', True):
            self.logger.debug("WARNING level alerts disabled in config")
            return
        if level == AlertLevel.CRITICAL and not email_config.get('send_on_critical', True):
            self.logger.debug("CRITICAL level alerts disabled in config")
            return
        
        # Build email
        subject_prefix = email_config.get('subject_prefix', '[Storage Alert]')
        subject = f"{subject_prefix} {alert_data['level']}: {alert_data['title']}"
        
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipient_emails)
        msg['Subject'] = subject
        
        # Build plain text email body (no HTML, no styling)
        body_text = f"""Storage Health Alert

Level: {alert_data['level']}
Hostname: {alert_data['hostname']}
Time: {alert_data['timestamp']}

Message:
{alert_data['message']}

---
This is an automated alert from Storage Health Monitor.
Monitoring Node: monitoringnode (192.168.1.13)
"""
        
        # Attach plain text only
        msg.attach(MIMEText(body_text, 'plain'))
        
        # Send email via Gmail SMTP
        smtp_server = email_config.get('smtp_server', 'smtp.gmail.com')
        smtp_port = email_config.get('smtp_port', 587)
        use_tls = email_config.get('use_tls', True)
        use_ssl = email_config.get('use_ssl', False)
        
        try:
            if use_ssl:
                # Use SMTP_SSL for port 465
                server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            else:
                server = smtplib.SMTP(smtp_server, smtp_port)
                if use_tls:
                    server.starttls()
            
            # Login with sender credentials
            server.login(sender_email, sender_password)
            
            # Send email
            server.send_message(msg)
            server.quit()
            
            self.logger.info(f"Email alert sent to {len(recipient_emails)} recipients: {', '.join(recipient_emails)}")
            
        except smtplib.SMTPAuthenticationError:
            self.logger.error("Email authentication failed. Check sender_email and sender_password (use Gmail App Password)")
        except smtplib.SMTPException as e:
            self.logger.error(f"SMTP error sending email: {e}")
        except Exception as e:
            self.logger.error(f"Failed to send email alert: {e}", exc_info=True)
    
    def _send_webhook_alert(self, alert_data: Dict[str, Any]) -> None:
        """Send alert via webhook."""
        import urllib.request
        import urllib.error
        
        webhook_config = self.config.get('alert_config', {}).get('webhook', {})
        url = webhook_config.get('url')
        
        if not url:
            self.logger.warning("No webhook URL configured")
            return
        
        timeout = webhook_config.get('timeout_seconds', 10)
        retry_count = webhook_config.get('retry_count', 3)
        
        payload = json.dumps(alert_data).encode('utf-8')
        
        for attempt in range(retry_count):
            try:
                req = urllib.request.Request(
                    url,
                    data=payload,
                    headers={'Content-Type': 'application/json'}
                )
                
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    if response.status == 200:
                        self.logger.info("Webhook alert sent successfully")
                        return
                    else:
                        self.logger.warning(f"Webhook returned status {response.status}")
            
            except urllib.error.URLError as e:
                self.logger.warning(f"Webhook attempt {attempt + 1} failed: {e}")
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
        
        self.logger.error("Failed to send webhook alert after all retries")
    
    def _load_alert_history(self) -> None:
        """Load alert history from file."""
        history_file = Path('/tmp/storage-monitor-alert-history.json')
        
        if not history_file.exists():
            return
        
        try:
            with open(history_file, 'r') as f:
                data = json.load(f)
            
            # Convert timestamp strings back to datetime
            for key, value in data.items():
                if 'last_sent' in value:
                    value['last_sent'] = datetime.fromisoformat(value['last_sent'])
            
            self.alert_history = data
        except Exception as e:
            self.logger.warning(f"Failed to load alert history: {e}")
    
    def _save_alert_history(self) -> None:
        """Save alert history to file."""
        history_file = Path('/tmp/storage-monitor-alert-history.json')
        
        try:
            # Convert datetime to string for JSON serialization
            data = {}
            for key, value in self.alert_history.items():
                data[key] = {
                    'last_sent': value['last_sent'].isoformat(),
                    'level': value['level'],
                    'count': value['count']
                }
            
            with open(history_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.warning(f"Failed to save alert history: {e}")
    
    def generate_summary_report(self, analysis_results: List[Dict[str, Any]]) -> str:
        """
        Generate a summary report from analysis results.
        
        Args:
            analysis_results: List of analysis result dictionaries
            
        Returns:
            Summary report string
        """
        total_hosts = len(analysis_results)
        hosts_with_issues = sum(1 for r in analysis_results if r.get('issues'))
        total_issues = sum(len(r.get('issues', [])) for r in analysis_results)
        
        critical_issues = sum(
            1 for r in analysis_results
            for issue in r.get('issues', [])
            if issue.get('level') == AlertLevel.CRITICAL
        )
        
        report = f"""
Storage Health Monitoring Summary
Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}

Total Hosts Monitored: {total_hosts}
Hosts with Issues: {hosts_with_issues}
Total Issues Found: {total_issues}
Critical Issues: {critical_issues}

"""
        
        if critical_issues > 0:
            report += "CRITICAL ISSUES:\n"
            for result in analysis_results:
                hostname = result.get('hostname', 'unknown')
                for issue in result.get('issues', []):
                    if issue.get('level') == AlertLevel.CRITICAL:
                        report += f"  [{hostname}] {issue.get('message')}\n"
        
        return report
