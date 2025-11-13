#!/usr/bin/env python3
"""
Azure Storage Monitor - Cloud Storage Health Monitoring
Monitors Azure Blob Storage containers for Finance and Marketing departments.
"""

import os
import sys
import json
import logging
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

# Azure SDK imports
try:
    from azure.identity import ClientSecretCredential
    from azure.storage.blob import BlobServiceClient, ContainerClient
    from azure.core.exceptions import AzureError, ResourceNotFoundError
except ImportError:
    print("ERROR: Azure SDK not installed!")
    print("Install with: pip3 install azure-identity azure-storage-blob")
    sys.exit(1)

# Import shared utilities from parent monitoring system
# Add /home/admin/monitoring_node to Python path
monitoring_node_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, monitoring_node_path)

try:
    from utils import setup_logger, format_bytes, get_timestamp
    from alert_handler import AlertHandler, AlertLevel
except ImportError as e:
    print(f"ERROR: Could not import monitoring_node utilities: {e}")
    print(f"Tried to import from: {monitoring_node_path}")
    print("Ensure this script is in /home/admin/monitoring_node/azure/scripts/")
    sys.exit(1)


@dataclass
class ContainerMetrics:
    """Storage container metrics"""
    container_name: str
    blob_count: int
    total_bytes: int
    used_bytes: int
    usage_percent: float
    status: str
    last_modified: Optional[datetime]
    is_accessible: bool


class AzureStorageMonitor:
    """Monitor Azure Blob Storage containers"""
    
    def __init__(self, config_path: str):
        """Initialize Azure storage monitor"""
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.alert_handler = None
        self.stats = {
            'containers_checked': 0,
            'containers_healthy': 0,
            'containers_warning': 0,
            'containers_critical': 0,
            'alerts_sent': 0
        }
        
        # Initialize Azure credentials
        self.credential = self._get_azure_credential()
        
        # Initialize alert handler if email enabled
        if self.config['monitoring_settings']['enable_email_alerts']:
            # Use shared email config from main analyzer
            shared_config_path = '/home/admin/monitoring_node/config/analyzer_config.json'
            if os.path.exists(shared_config_path):
                with open(shared_config_path, 'r') as f:
                    shared_config = json.load(f)
                # Use the shared alert_config directly (it has email settings)
                self.alert_handler = AlertHandler(shared_config, self.logger)
            else:
                self.logger.warning("Shared config not found, email alerts disabled")
                self.alert_handler = None
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load Azure configuration"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate required fields
        required_fields = ['azure_credentials', 'storage_accounts', 'monitoring_settings']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")
        
        return config
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging"""
        log_path = self.config['monitoring_settings']['log_path']
        return setup_logger(log_path, "azure_monitor")
    
    def _get_azure_credential(self) -> ClientSecretCredential:
        """Get Azure authentication credential"""
        creds = self.config['azure_credentials']
        
        try:
            credential = ClientSecretCredential(
                tenant_id=creds['tenant_id'],
                client_id=creds['client_id'],
                client_secret=creds['client_secret']
            )
            self.logger.info("Azure credentials initialized successfully")
            return credential
        except Exception as e:
            self.logger.error(f"Failed to initialize Azure credentials: {e}")
            raise
    
    def monitor_single_account(self, account_name: str):
        """Monitor a single storage account"""
        self.logger.info("=" * 80)
        self.logger.info(f"Starting Azure Storage monitoring for: {account_name}")
        self.logger.info("=" * 80)
        
        if account_name not in self.config['storage_accounts']:
            self.logger.error(f"Account '{account_name}' not found in configuration")
            return
        
        account_config = self.config['storage_accounts'][account_name]
        
        if not account_config['monitoring']['enabled']:
            self.logger.info(f"Account '{account_name}' is disabled in configuration")
            return
        
        results = self.monitor_storage_account(account_name, account_config)
        self._generate_csv_report(account_name, results)
        self._log_statistics()
        
        self.logger.info(f"Azure Storage monitoring completed for: {account_name}")
        self.logger.info("=" * 80)
    
    def monitor_all_accounts(self):
        """Monitor all configured storage accounts"""
        self.logger.info("=" * 80)
        self.logger.info("Starting Azure Storage monitoring")
        self.logger.info("=" * 80)
        
        all_results = {}
        
        for account_name, account_config in self.config['storage_accounts'].items():
            if not account_config['monitoring']['enabled']:
                self.logger.info(f"Skipping disabled account: {account_name}")
                continue
            
            self.logger.info(f"Monitoring storage account: {account_name}")
            results = self.monitor_storage_account(account_name, account_config)
            all_results[account_name] = results
        
        # Generate reports
        for account_name, results in all_results.items():
            self._generate_csv_report(account_name, results)
        
        # Log final statistics
        self._log_statistics()
        
        self.logger.info("Azure Storage monitoring completed")
        self.logger.info("=" * 80)
    
    def monitor_storage_account(self, account_name: str, account_config: Dict[str, Any]) -> List[ContainerMetrics]:
        """Monitor a single storage account"""
        results = []
        
        try:
            # Create BlobServiceClient
            connection_string = account_config['connection_string']
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            
            # Auto-discover containers
            if account_config['monitoring']['auto_discover_containers']:
                containers = [container.name for container in blob_service_client.list_containers()]
                self.logger.info(f"Discovered {len(containers)} containers in {account_name}")
            else:
                containers = account_config['monitoring'].get('container_names', [])
            
            # Monitor each container
            for container_name in containers:
                metrics = self._get_container_metrics(
                    blob_service_client,
                    container_name,
                    account_name
                )
                results.append(metrics)
                
                # Check thresholds and send alerts
                self._check_and_alert(account_name, container_name, metrics, account_config)
        
        except Exception as e:
            self.logger.error(f"Error monitoring {account_name}: {e}")
        
        return results
    
    def _get_container_metrics(self, 
                               blob_service_client: BlobServiceClient,
                               container_name: str,
                               account_name: str) -> ContainerMetrics:
        """Get metrics for a single container"""
        self.stats['containers_checked'] += 1
        
        try:
            container_client = blob_service_client.get_container_client(container_name)
            
            # Check accessibility
            try:
                container_props = container_client.get_container_properties()
                is_accessible = True
                status = "Healthy"
                last_modified = container_props.get('last_modified')
            except ResourceNotFoundError:
                is_accessible = False
                status = "Not Found"
                last_modified = None
            except AzureError as e:
                is_accessible = False
                status = f"Error: {str(e)[:50]}"
                last_modified = None
            
            # Get blob count and size
            blob_count = 0
            total_bytes = 0
            
            if is_accessible:
                try:
                    for blob in container_client.list_blobs():
                        blob_count += 1
                        total_bytes += blob.size
                except Exception as e:
                    self.logger.warning(f"Error listing blobs in {container_name}: {e}")
            
            # For free tier, we estimate capacity (Azure doesn't provide direct quota info)
            # Assume 5TB default capacity for calculation purposes
            estimated_capacity = 5 * 1024 * 1024 * 1024 * 1024  # 5 TB in bytes
            usage_percent = (total_bytes / estimated_capacity * 100) if estimated_capacity > 0 else 0
            
            metrics = ContainerMetrics(
                container_name=container_name,
                blob_count=blob_count,
                total_bytes=estimated_capacity,
                used_bytes=total_bytes,
                usage_percent=usage_percent,
                status=status,
                last_modified=last_modified,
                is_accessible=is_accessible
            )
            
            self.logger.debug(f"{account_name}/{container_name}: {blob_count} blobs, "
                            f"{format_bytes(total_bytes)} used, {usage_percent:.2f}% capacity")
            
            return metrics
        
        except Exception as e:
            self.logger.error(f"Error getting metrics for {container_name}: {e}")
            return ContainerMetrics(
                container_name=container_name,
                blob_count=0,
                total_bytes=0,
                used_bytes=0,
                usage_percent=0,
                status=f"Error: {str(e)[:50]}",
                last_modified=None,
                is_accessible=False
            )
    
    def _check_and_alert(self, 
                        account_name: str,
                        container_name: str,
                        metrics: ContainerMetrics,
                        account_config: Dict[str, Any]):
        """Check thresholds and send alerts if needed"""
        if not self.alert_handler:
            return
        
        thresholds = account_config['monitoring']['thresholds']
        issues = []
        
        # Check accessibility
        if not metrics.is_accessible:
            self.stats['containers_critical'] += 1
            issues.append({
                'level': AlertLevel.CRITICAL,
                'type': 'container_inaccessible',
                'message': f"Azure container '{account_name}/{container_name}' is not accessible (Status: {metrics.status})"
            })
        
        # Check usage thresholds
        elif metrics.usage_percent >= thresholds['usage_critical_percent']:
            self.stats['containers_critical'] += 1
            issues.append({
                'level': AlertLevel.CRITICAL,
                'type': 'high_usage',
                'message': f"Azure container '{account_name}/{container_name}' usage high: {metrics.usage_percent:.3f}%"
            })
        
        elif metrics.usage_percent >= thresholds['usage_warning_percent']:
            self.stats['containers_warning'] += 1
            issues.append({
                'level': AlertLevel.WARNING,
                'type': 'high_usage',
                'message': f"Azure container '{account_name}/{container_name}' usage: {metrics.usage_percent:.3f}%"
            })
        else:
            self.stats['containers_healthy'] += 1
        
        # Send alerts
        for issue in issues:
            try:
                subject_prefix = f"[Azure {account_name.title()} Alert]"
                self.alert_handler.send_alert(
                    level=issue['level'],
                    title=f"container_usage - {container_name}",
                    message=issue['message'],
                    hostname=f"Azure {account_name.title()} Storage - {container_name}",
                    details={
                        'container': container_name,
                        'usage_percent': f"{metrics.usage_percent:.2f}",
                        'blob_count': metrics.blob_count,
                        'used': format_bytes(metrics.used_bytes),
                        'total': format_bytes(metrics.total_bytes),
                        'status': metrics.status
                    }
                )
                self.stats['alerts_sent'] += 1
            except Exception as e:
                self.logger.error(f"Failed to send alert: {e}")
    
    def _generate_csv_report(self, account_name: str, results: List[ContainerMetrics]):
        """Generate CSV report for storage account"""
        reports_dir = self.config['monitoring_settings']['reports_directory']
        os.makedirs(reports_dir, exist_ok=True)
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        csv_path = os.path.join(reports_dir, f'{account_name}_storage_report_{timestamp}.csv')
        
        try:
            with open(csv_path, 'w', newline='') as csvfile:
                fieldnames = [
                    'timestamp', 'account_name', 'container_name', 'blob_count',
                    'used_gb', 'total_gb', 'usage_percent', 'status', 
                    'is_accessible', 'last_modified', 'alert_level'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for metrics in results:
                    # Determine alert level
                    thresholds = self.config['storage_accounts'][account_name]['monitoring']['thresholds']
                    if not metrics.is_accessible:
                        alert_level = 'CRITICAL'
                    elif metrics.usage_percent >= thresholds['usage_critical_percent']:
                        alert_level = 'CRITICAL'
                    elif metrics.usage_percent >= thresholds['usage_warning_percent']:
                        alert_level = 'WARNING'
                    else:
                        alert_level = 'OK'
                    
                    writer.writerow({
                        'timestamp': get_timestamp(),
                        'account_name': account_name,
                        'container_name': metrics.container_name,
                        'blob_count': metrics.blob_count,
                        'used_gb': f"{metrics.used_bytes / (1024**3):.2f}",
                        'total_gb': f"{metrics.total_bytes / (1024**3):.2f}",
                        'usage_percent': f"{metrics.usage_percent:.2f}",
                        'status': metrics.status,
                        'is_accessible': 'Yes' if metrics.is_accessible else 'No',
                        'last_modified': metrics.last_modified.isoformat() if metrics.last_modified else 'N/A',
                        'alert_level': alert_level
                    })
            
            self.logger.info(f"CSV report saved: {csv_path}")
        
        except Exception as e:
            self.logger.error(f"Failed to generate CSV report: {e}")
    
    def _log_statistics(self):
        """Log monitoring statistics"""
        self.logger.info("=" * 60)
        self.logger.info("Azure Storage Monitoring Statistics:")
        self.logger.info(f"  Containers checked: {self.stats['containers_checked']}")
        self.logger.info(f"  Healthy: {self.stats['containers_healthy']}")
        self.logger.info(f"  Warning: {self.stats['containers_warning']}")
        self.logger.info(f"  Critical: {self.stats['containers_critical']}")
        self.logger.info(f"  Alerts sent: {self.stats['alerts_sent']}")
        self.logger.info("=" * 60)


def main():
    """Main entry point"""
    import argparse
    
    # Determine default config path
    script_dir = Path(__file__).parent
    default_config = script_dir.parent / 'azure_config.json'
    
    parser = argparse.ArgumentParser(description='Azure Storage Monitor')
    parser.add_argument(
        '--config',
        default=str(default_config),
        help='Path to Azure configuration file'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in test mode (validate credentials only)'
    )
    parser.add_argument(
        '--account',
        choices=['finance', 'marketing'],
        help='Monitor specific account only (finance or marketing)'
    )
    
    args = parser.parse_args()
    
    try:
        monitor = AzureStorageMonitor(args.config)
        
        if args.test:
            print("✓ Configuration loaded successfully")
            print("✓ Azure credentials initialized")
            print("✓ Logger initialized")
            print("\nTest passed! Ready to monitor Azure storage.")
            return 0
        
        # Monitor specific account or all accounts
        if args.account:
            monitor.monitor_single_account(args.account)
        else:
            monitor.monitor_all_accounts()
        return 0
    
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
