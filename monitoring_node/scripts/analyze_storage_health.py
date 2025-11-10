#!/usr/bin/env python3

"""

Storage Health Analyzer - Main Script
Reads client JSON reports, validates data, checks against thresholds, and generates alerts.
Production-ready with comprehensive error handling, logging, and monitoring.
"""

import os
import sys
import json
import signal
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import from utils (works both in dev and deployed structure)
try:
    from monitoring_node.utils import (
        setup_logger,
        load_config,
        load_thresholds,
        write_json_atomic,
        get_timestamp,
        get_file_age_minutes,
        archive_file,
        cleanup_old_files,
        ensure_directory,
        format_bytes,
        format_percentage
    )
except ModuleNotFoundError:
    # Fallback for deployed structure where utils.py is in parent directory
    from utils import (
        setup_logger,
        load_config,
        load_thresholds,
        write_json_atomic,
        get_timestamp,
        get_file_age_minutes,
        archive_file,
        cleanup_old_files,
        ensure_directory,
        format_bytes,
        format_percentage
    )

try:
    from monitoring_node.data_validator import DataValidator
    from monitoring_node.alert_handler import AlertHandler, AlertLevel
except ModuleNotFoundError:
    # Fallback for deployed structure
    from data_validator import DataValidator
    from alert_handler import AlertHandler, AlertLevel


class StorageHealthAnalyzer:
    """Main analyzer class for storage health monitoring."""
    
    def __init__(self, config_path: str, thresholds_path: str):
        """
        Initialize the analyzer.
        
        Args:
            config_path: Path to analyzer configuration file
            thresholds_path: Path to thresholds configuration file
        """
        # Load configurations
        self.config = load_config(config_path)
        self.thresholds = load_thresholds(thresholds_path)
        
        # Setup logger
        log_path = self.config.get('log_path', '/var/log/storage-health-monitor/analyzer.log')
        self.logger = setup_logger(log_path, "storage_analyzer")
        
        # Initialize components
        self.validator = DataValidator(self.logger)
        self.alert_handler = AlertHandler(self.config, self.logger)
        
        # Statistics
        self.stats = {
            'files_processed': 0,
            'files_validated': 0,
            'files_failed': 0,
            'alerts_sent': 0,
            'issues_found': 0
        }
        
        # Ensure required directories exist
        self._ensure_directories()
        
        self.logger.info("Storage Health Analyzer initialized")
        self.logger.info(f"Data directory: {self.config.get('data_directory')}")
        self.logger.info(f"Output directory: {self.config.get('output_directory')}")
    
    def _ensure_directories(self) -> None:
        """Ensure all required directories exist."""
        dirs = [
            self.config.get('data_directory'),
            self.config.get('output_directory'),
            self.config.get('analysis', {}).get('archive_directory'),
            os.path.dirname(self.config.get('log_path', ''))
        ]
        
        for directory in dirs:
            if directory:
                ensure_directory(directory)
    
    def run(self) -> None:
        """Run the analyzer main loop."""
        self.logger.info("Starting storage health analysis")
        
        try:
            # Find and process all JSON files
            data_dir = self.config.get('data_directory')
            file_pattern = self.config.get('analysis', {}).get('file_pattern', '*.json')
            
            json_files = list(Path(data_dir).glob(file_pattern))
            self.logger.info(f"Found {len(json_files)} JSON files to process")
            
            if not json_files:
                self.logger.warning(f"No files to process in {data_dir}")
                return
            
            analysis_results = []
            
            # Process each file
            for file_path in json_files:
                try:
                    result = self._process_file(str(file_path))
                    if result:
                        analysis_results.append(result)
                except Exception as e:
                    self.logger.error(f"Error processing {file_path}: {e}", exc_info=True)
                    self.stats['files_failed'] += 1
            
            # Generate summary report
            self._generate_summary_report(analysis_results)
            
            # Cleanup old files
            self._cleanup_old_data()
            
            # Log statistics
            self._log_statistics()
            
            self.logger.info("Storage health analysis completed")
        
        except Exception as e:
            self.logger.critical(f"Fatal error in analyzer: {e}", exc_info=True)
            raise
    
    def _process_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Process a single client report file.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Analysis result dictionary or None if processing failed
        """
        self.logger.debug(f"Processing file: {file_path}")
        self.stats['files_processed'] += 1
        
        # Check file age
        age_minutes = get_file_age_minutes(file_path)
        max_age = self.thresholds.get('data_freshness', {}).get('max_age_minutes', 15)
        
        if age_minutes > max_age:
            self.logger.warning(
                f"File {file_path} is stale (age: {age_minutes:.1f} minutes)"
            )
        
        # Validate file
        is_valid, data, errors, warnings = self.validator.validate_file(file_path)
        
        if not is_valid:
            self.logger.error(f"Validation failed for {file_path}")
            for error in errors:
                self.logger.error(f"  - {error}")
            self.stats['files_failed'] += 1
            return None
        
        self.stats['files_validated'] += 1
        
        # Log warnings
        for warning in warnings:
            self.logger.warning(f"  - {warning}")
        
        # Analyze data against thresholds
        result = self._analyze_data(data, file_path)
        
        # Archive processed file
        if self.config.get('analysis', {}).get('archive_processed', True):
            archive_dir = self.config.get('analysis', {}).get('archive_directory')
            if archive_dir:
                try:
                    archive_file(file_path, archive_dir)
                    self.logger.debug(f"Archived {file_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to archive {file_path}: {e}")
        
        return result
    
    def _analyze_data(self, data: Dict[str, Any], source_file: str) -> Dict[str, Any]:
        """
        Analyze validated data against thresholds.
        
        Args:
            data: Validated client data
            source_file: Source file path
            
        Returns:
            Analysis result dictionary
        """
        hostname = data.get('hostname', 'unknown')
        timestamp = data.get('timestamp', 'unknown')
        
        self.logger.info(f"Analyzing data from {hostname} (timestamp: {timestamp})")
        
        result = {
            'hostname': hostname,
            'timestamp': timestamp,
            'source_file': source_file,
            'analyzed_at': get_timestamp(),
            'issues': []
        }
        
        # Analyze disk usage
        # Deduplicate disks by device to avoid multiple alerts for same physical disk
        seen_devices = {}
        for disk in data.get('disks', []):
            device = disk.get('device', 'unknown')
            # Keep only the root mount (/) or first occurrence of each device
            if device not in seen_devices:
                seen_devices[device] = disk
            elif disk.get('mount_point') == '/':
                # Prefer root mount point if available
                seen_devices[device] = disk
        
        # Check thresholds for unique devices only
        for disk in seen_devices.values():
            disk_issues = self._check_disk_thresholds(disk, hostname)
            result['issues'].extend(disk_issues)
        
        # Analyze system metrics if available
        if 'system' in data:
            system_issues = self._check_system_thresholds(data['system'], hostname)
            result['issues'].extend(system_issues)
        
        # Analyze SMART data if available
        if 'smart' in data:
            smart_issues = self._check_smart_thresholds(data['smart'], hostname)
            result['issues'].extend(smart_issues)
        
        # Send alerts for issues found
        for issue in result['issues']:
            self._send_issue_alert(issue, hostname)
        
        # Update statistics
        self.stats['issues_found'] += len(result['issues'])
        
        return result
    
    def _check_disk_thresholds(self, disk: Dict[str, Any], hostname: str) -> List[Dict[str, Any]]:
        """Check disk usage against thresholds."""
        issues = []
        device = disk.get('device', 'unknown')
        mount_point = disk.get('mount_point', 'unknown')
        usage_percent = disk.get('usage_percent', 0)
        
        disk_thresholds = self.thresholds.get('disk_usage', {})
        warning_threshold = disk_thresholds.get('warning_percent', 75)
        critical_threshold = disk_thresholds.get('critical_percent', 90)
        
        # Check disk usage
        if usage_percent >= critical_threshold:
            issues.append({
                'level': AlertLevel.CRITICAL,
                'type': 'disk_usage',
                'device': device,
                'mount_point': mount_point,
                'message': f"Critical disk usage on {device} ({mount_point}): {format_percentage(usage_percent)}",
                'details': {
                    'usage_percent': usage_percent,
                    'threshold': critical_threshold,
                    'total': format_bytes(disk.get('total_bytes', 0)),
                    'used': format_bytes(disk.get('used_bytes', 0)),
                    'available': format_bytes(disk.get('available_bytes', 0))
                }
            })
        elif usage_percent >= warning_threshold:
            issues.append({
                'level': AlertLevel.WARNING,
                'type': 'disk_usage',
                'device': device,
                'mount_point': mount_point,
                'message': f"High disk usage on {device} ({mount_point}): {format_percentage(usage_percent)}",
                'details': {
                    'usage_percent': usage_percent,
                    'threshold': warning_threshold,
                    'total': format_bytes(disk.get('total_bytes', 0)),
                    'used': format_bytes(disk.get('used_bytes', 0)),
                    'available': format_bytes(disk.get('available_bytes', 0))
                }
            })
        
        # Check inode usage if available
        if 'inodes_total' in disk and disk['inodes_total'] > 0:
            inodes_used = disk.get('inodes_used', 0)
            inodes_total = disk['inodes_total']
            inode_usage_percent = (inodes_used / inodes_total) * 100
            
            inode_thresholds = self.thresholds.get('inode_usage', {})
            inode_warning = inode_thresholds.get('warning_percent', 80)
            inode_critical = inode_thresholds.get('critical_percent', 95)
            
            if inode_usage_percent >= inode_critical:
                issues.append({
                    'level': AlertLevel.CRITICAL,
                    'type': 'inode_usage',
                    'device': device,
                    'mount_point': mount_point,
                    'message': f"Critical inode usage on {device}: {format_percentage(inode_usage_percent)}",
                    'details': {
                        'usage_percent': inode_usage_percent,
                        'inodes_used': inodes_used,
                        'inodes_total': inodes_total
                    }
                })
            elif inode_usage_percent >= inode_warning:
                issues.append({
                    'level': AlertLevel.WARNING,
                    'type': 'inode_usage',
                    'device': device,
                    'mount_point': mount_point,
                    'message': f"High inode usage on {device}: {format_percentage(inode_usage_percent)}",
                    'details': {
                        'usage_percent': inode_usage_percent,
                        'inodes_used': inodes_used,
                        'inodes_total': inodes_total
                    }
                })
        
        return issues
    
    def _check_system_thresholds(self, system: Dict[str, Any], hostname: str) -> List[Dict[str, Any]]:
        """Check system metrics against thresholds."""
        issues = []
        system_thresholds = self.thresholds.get('system', {})
        
        # Check CPU usage
        cpu_usage = system.get('cpu_usage_percent')
        if cpu_usage is not None:
            cpu_critical = system_thresholds.get('cpu_usage_critical', 95)
            cpu_warning = system_thresholds.get('cpu_usage_warning', 80)
            
            if cpu_usage >= cpu_critical:
                issues.append({
                    'level': AlertLevel.CRITICAL,
                    'type': 'cpu_usage',
                    'message': f"Critical CPU usage: {format_percentage(cpu_usage)}",
                    'details': {'usage_percent': cpu_usage, 'threshold': cpu_critical}
                })
            elif cpu_usage >= cpu_warning:
                issues.append({
                    'level': AlertLevel.WARNING,
                    'type': 'cpu_usage',
                    'message': f"High CPU usage: {format_percentage(cpu_usage)}",
                    'details': {'usage_percent': cpu_usage, 'threshold': cpu_warning}
                })
        
        # Check memory usage
        mem_usage = system.get('memory_usage_percent')
        if mem_usage is not None:
            mem_critical = system_thresholds.get('memory_usage_critical', 95)
            mem_warning = system_thresholds.get('memory_usage_warning', 85)
            
            if mem_usage >= mem_critical:
                issues.append({
                    'level': AlertLevel.CRITICAL,
                    'type': 'memory_usage',
                    'message': f"Critical memory usage: {format_percentage(mem_usage)}",
                    'details': {'usage_percent': mem_usage, 'threshold': mem_critical}
                })
            elif mem_usage >= mem_warning:
                issues.append({
                    'level': AlertLevel.WARNING,
                    'type': 'memory_usage',
                    'message': f"High memory usage: {format_percentage(mem_usage)}",
                    'details': {'usage_percent': mem_usage, 'threshold': mem_warning}
                })
        
        # Check load average
        load_avg = system.get('load_average')
        if load_avg and len(load_avg) >= 1:
            load_1min = load_avg[0]
            load_critical = system_thresholds.get('load_average_critical', 8.0)
            load_warning = system_thresholds.get('load_average_warning', 4.0)
            
            if load_1min >= load_critical:
                issues.append({
                    'level': AlertLevel.CRITICAL,
                    'type': 'load_average',
                    'message': f"Critical load average: {load_1min:.2f}",
                    'details': {'load_1min': load_1min, 'threshold': load_critical}
                })
            elif load_1min >= load_warning:
                issues.append({
                    'level': AlertLevel.WARNING,
                    'type': 'load_average',
                    'message': f"High load average: {load_1min:.2f}",
                    'details': {'load_1min': load_1min, 'threshold': load_warning}
                })
        
        return issues
    
    def _check_smart_thresholds(self, smart_data: Any, hostname: str) -> List[Dict[str, Any]]:
        """Check SMART metrics against thresholds."""
        issues = []
        smart_thresholds = self.thresholds.get('smart', {})
        
        # Handle both dict and list formats
        if isinstance(smart_data, list):
            # Client sends list of smart entries: [{"device": "/dev/sda", ...}]
            smart_entries = smart_data
        elif isinstance(smart_data, dict):
            # Dict format: {"/dev/sda": {...}}
            smart_entries = [{"device": device, **attrs} for device, attrs in smart_data.items()]
        else:
            return issues
        
        for entry in smart_entries:
            if not isinstance(entry, dict):
                continue
            
            device = entry.get('device', 'unknown')
            
            # Skip if SMART check failed or not available
            if entry.get('status') in ['ERROR', 'SKIPPED']:
                self.logger.debug(f"Skipping SMART analysis for {device}: {entry.get('status')}")
                continue
            
            # Get attributes (may be in 'attributes' key or directly in entry)
            attrs = entry.get('attributes', entry)
            
            # Check reallocated sectors
            reallocated = attrs.get('reallocated_sectors', 0)
            if reallocated >= smart_thresholds.get('reallocated_sectors_critical', 50):
                issues.append({
                    'level': AlertLevel.CRITICAL,
                    'type': 'smart_reallocated_sectors',
                    'device': device,
                    'message': f"Critical: {device} has {reallocated} reallocated sectors",
                    'details': {'count': reallocated}
                })
            elif reallocated >= smart_thresholds.get('reallocated_sectors_warning', 5):
                issues.append({
                    'level': AlertLevel.WARNING,
                    'type': 'smart_reallocated_sectors',
                    'device': device,
                    'message': f"Warning: {device} has {reallocated} reallocated sectors",
                    'details': {'count': reallocated}
                })
            
            # Check pending sectors
            pending = attrs.get('current_pending_sectors', 0)
            if pending >= smart_thresholds.get('current_pending_sectors_critical', 10):
                issues.append({
                    'level': AlertLevel.CRITICAL,
                    'type': 'smart_pending_sectors',
                    'device': device,
                    'message': f"Critical: {device} has {pending} pending sectors",
                    'details': {'count': pending}
                })
            elif pending >= smart_thresholds.get('current_pending_sectors_warning', 1):
                issues.append({
                    'level': AlertLevel.WARNING,
                    'type': 'smart_pending_sectors',
                    'device': device,
                    'message': f"Warning: {device} has {pending} pending sectors",
                    'details': {'count': pending}
                })
            
            # Check offline uncorrectable
            uncorrectable = attrs.get('offline_uncorrectable', 0)
            if uncorrectable >= smart_thresholds.get('offline_uncorrectable_critical', 5):
                issues.append({
                    'level': AlertLevel.CRITICAL,
                    'type': 'smart_uncorrectable',
                    'device': device,
                    'message': f"Critical: {device} has {uncorrectable} uncorrectable sectors",
                    'details': {'count': uncorrectable}
                })
            elif uncorrectable >= smart_thresholds.get('offline_uncorrectable_warning', 1):
                issues.append({
                    'level': AlertLevel.WARNING,
                    'type': 'smart_uncorrectable',
                    'device': device,
                    'message': f"Warning: {device} has {uncorrectable} uncorrectable sectors",
                    'details': {'count': uncorrectable}
                })
        
        return issues
    
    def _send_issue_alert(self, issue: Dict[str, Any], hostname: str) -> None:
        """Send alert for an issue."""
        level = issue.get('level', AlertLevel.INFO)
        message = issue.get('message', 'Unknown issue')
        title = f"{issue.get('type', 'unknown')} - {hostname}"
        details = issue.get('details', {})
        
        try:
            if self.alert_handler.send_alert(level, title, message, hostname, details):
                self.stats['alerts_sent'] += 1
        except Exception as e:
            self.logger.error(f"Failed to send alert: {e}")
    
    def _generate_summary_report(self, results: List[Dict[str, Any]]) -> None:
        """Generate and save summary report in CSV format only."""
        output_dir = self.config.get('output_directory')
        if not output_dir:
            return
        
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        report_path_csv = os.path.join(output_dir, f'analysis_report_{timestamp}.csv')
        
        try:
            # Save CSV report only
            self._write_csv_report(report_path_csv, results)
            self.logger.info(f"CSV report saved to {report_path_csv}")
        except Exception as e:
            self.logger.error(f"Failed to save CSV report: {e}")
    
    def _write_csv_report(self, csv_path: str, results: List[Dict[str, Any]]) -> None:
        """Write results to CSV file."""
        import csv
        
        with open(csv_path, 'w', newline='') as csvfile:
            fieldnames = [
                'timestamp', 'hostname', 'issue_level', 'issue_type', 
                'device', 'mount_point', 'message', 'usage_percent', 
                'threshold', 'total_gb', 'used_gb', 'available_gb'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in results:
                hostname = result.get('hostname', 'unknown')
                timestamp = result.get('timestamp', '')
                
                # If no issues, write one row showing OK status
                if not result.get('issues'):
                    writer.writerow({
                        'timestamp': timestamp,
                        'hostname': hostname,
                        'issue_level': 'OK',
                        'issue_type': 'none',
                        'device': '',
                        'mount_point': '',
                        'message': 'No issues detected',
                        'usage_percent': '',
                        'threshold': '',
                        'total_gb': '',
                        'used_gb': '',
                        'available_gb': ''
                    })
                else:
                    # Write one row per issue
                    for issue in result.get('issues', []):
                        details = issue.get('details', {})
                        writer.writerow({
                            'timestamp': timestamp,
                            'hostname': hostname,
                            'issue_level': issue.get('level', ''),
                            'issue_type': issue.get('type', ''),
                            'device': issue.get('device', ''),
                            'mount_point': issue.get('mount_point', ''),
                            'message': issue.get('message', ''),
                            'usage_percent': details.get('usage_percent', ''),
                            'threshold': details.get('threshold', ''),
                            'total_gb': details.get('total', ''),
                            'used_gb': details.get('used', ''),
                            'available_gb': details.get('available', '')
                        })
    
    def _cleanup_old_data(self) -> None:
        """Clean up old archived files."""
        archive_dir = self.config.get('analysis', {}).get('archive_directory')
        retention_days = self.config.get('analysis', {}).get('retention_days', 30)
        
        if archive_dir and os.path.exists(archive_dir):
            deleted = cleanup_old_files(archive_dir, retention_days, '*.json')
            if deleted > 0:
                self.logger.info(f"Cleaned up {deleted} old files from archive")
        
        # Cleanup old reports
        output_dir = self.config.get('output_directory')
        if output_dir and os.path.exists(output_dir):
            deleted = cleanup_old_files(output_dir, retention_days, 'analysis_report_*.json')
            if deleted > 0:
                self.logger.info(f"Cleaned up {deleted} old reports")
    
    def _log_statistics(self) -> None:
        """Log analysis statistics."""
        self.logger.info("=" * 60)
        self.logger.info("Analysis Statistics:")
        self.logger.info(f"  Files processed: {self.stats['files_processed']}")
        self.logger.info(f"  Files validated: {self.stats['files_validated']}")
        self.logger.info(f"  Files failed: {self.stats['files_failed']}")
        self.logger.info(f"  Issues found: {self.stats['issues_found']}")
        self.logger.info(f"  Alerts sent: {self.stats['alerts_sent']}")
        self.logger.info("=" * 60)


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    print(f"\nReceived signal {signum}, shutting down gracefully...")
    sys.exit(0)


def main():
    """Main entry point."""
    # Determine default config paths based on script location
    script_dir = Path(__file__).parent
    default_config_dir = script_dir.parent / 'config'
    
    parser = argparse.ArgumentParser(
        description='Storage Health Analyzer - Monitor and analyze client storage reports'
    )
    parser.add_argument(
        '--config',
        default=str(default_config_dir / 'analyzer_config.json'),
        help='Path to analyzer configuration file'
    )
    parser.add_argument(
        '--thresholds',
        default=str(default_config_dir / 'thresholds.json'),
        help='Path to thresholds configuration file'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        analyzer = StorageHealthAnalyzer(args.config, args.thresholds)
        
        if args.verbose:
            analyzer.logger.setLevel(10)  # DEBUG level
        
        analyzer.run()
        return 0
    
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
        return 130
    
    except Exception as e:
        print(f"Fatal error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
