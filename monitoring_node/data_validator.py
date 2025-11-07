#!/usr/bin/env python3
"""
Data validator for storage health monitoring.
Validates client data structure, data types, and value ranges.
"""

import json
import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class DataValidator:
    """Validates storage health monitoring data from clients."""
    
    # Required top-level fields
    REQUIRED_FIELDS = [
        'hostname',
        'timestamp',
        'disks'
    ]
    
    # Required fields for each disk entry
    REQUIRED_DISK_FIELDS = [
        'device',
        'mount_point',
        'filesystem',
        'total_bytes',
        'used_bytes',
        'available_bytes',
        'usage_percent'
    ]
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize validator.
        
        Args:
            logger: Optional logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.errors = []
        self.warnings = []
    
    def _normalize_client_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize client data to expected format.
        Handles different field naming conventions from various clients.
        """
        normalized = data.copy()
        
        # Map 'host' to 'hostname'
        if 'host' in normalized and 'hostname' not in normalized:
            normalized['hostname'] = normalized.pop('host')
        
        # Map 'collected_at' to 'timestamp'
        if 'collected_at' in normalized and 'timestamp' not in normalized:
            normalized['timestamp'] = normalized.pop('collected_at')
        
        # Map 'disk_partitions' to 'disks'
        if 'disk_partitions' in normalized and 'disks' not in normalized:
            disk_partitions = normalized.pop('disk_partitions')
            
            # Normalize each disk entry
            normalized_disks = []
            for disk in disk_partitions:
                normalized_disk = {
                    'device': disk.get('device', ''),
                    'mount_point': disk.get('mountpoint', disk.get('mount_point', '')),
                    'filesystem': disk.get('fstype', disk.get('filesystem', '')),
                    'total_bytes': disk.get('total', disk.get('total_bytes', 0)),
                    'used_bytes': disk.get('used', disk.get('used_bytes', 0)),
                    'available_bytes': disk.get('free', disk.get('available_bytes', 0)),
                    'usage_percent': disk.get('percent', disk.get('usage_percent', 0.0))
                }
                
                # Copy optional fields if present
                if 'opts' in disk:
                    normalized_disk['opts'] = disk['opts']
                if 'inodes_total' in disk:
                    normalized_disk['inodes_total'] = disk['inodes_total']
                if 'inodes_used' in disk:
                    normalized_disk['inodes_used'] = disk['inodes_used']
                
                normalized_disks.append(normalized_disk)
            
            normalized['disks'] = normalized_disks
        
        return normalized
    
    def validate_report(self, data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str], List[str]]:
        """
        Validate a complete client report.
        
        Args:
            data: Client report data
            
        Returns:
            Tuple of (is_valid, normalized_data, errors, warnings)
        """
        self.errors = []
        self.warnings = []
        
        try:
            # Normalize client data to expected format
            normalized_data = self._normalize_client_data(data)
            
            # Validate structure
            self._validate_structure(normalized_data)
            
            # Validate field types and values
            self._validate_metadata(normalized_data)
            self._validate_disks(normalized_data.get('disks', []))
            
            # Optional fields validation
            if 'system' in normalized_data:
                self._validate_system_info(normalized_data['system'])
            
            if 'smart' in normalized_data:
                self._validate_smart_data(normalized_data['smart'])
            
        except ValidationError as e:
            self.errors.append(str(e))
            normalized_data = data  # Return original data if normalization failed
        except Exception as e:
            self.errors.append(f"Unexpected validation error: {str(e)}")
            normalized_data = data  # Return original data if validation failed
        
        is_valid = len(self.errors) == 0
        return is_valid, normalized_data, self.errors, self.warnings
    
    def _validate_structure(self, data: Dict[str, Any]) -> None:
        """Validate basic structure and required fields."""
        if not isinstance(data, dict):
            raise ValidationError("Data must be a dictionary")
        
        missing_fields = [field for field in self.REQUIRED_FIELDS if field not in data]
        if missing_fields:
            raise ValidationError(f"Missing required fields: {', '.join(missing_fields)}")
    
    def _validate_metadata(self, data: Dict[str, Any]) -> None:
        """Validate metadata fields."""
        # Validate hostname
        hostname = data.get('hostname')
        if not isinstance(hostname, str) or not hostname.strip():
            self.errors.append("Invalid hostname: must be non-empty string")
        
        # Validate timestamp
        timestamp = data.get('timestamp')
        if not isinstance(timestamp, str):
            self.errors.append("Invalid timestamp: must be string")
        else:
            try:
                self._parse_timestamp(timestamp)
            except Exception:
                self.errors.append(f"Invalid timestamp format: {timestamp}")
        
        # Check data freshness
        try:
            ts = self._parse_timestamp(timestamp)
            age_minutes = (datetime.utcnow() - ts).total_seconds() / 60
            if age_minutes > 60:
                self.warnings.append(f"Data is {age_minutes:.1f} minutes old")
        except Exception:
            pass
    
    def _validate_disks(self, disks: List[Dict[str, Any]]) -> None:
        """Validate disk entries."""
        if not isinstance(disks, list):
            raise ValidationError("'disks' must be a list")
        
        if len(disks) == 0:
            self.warnings.append("No disk data provided")
            return
        
        for idx, disk in enumerate(disks):
            self._validate_disk_entry(disk, idx)
    
    def _validate_disk_entry(self, disk: Dict[str, Any], index: int) -> None:
        """Validate individual disk entry."""
        if not isinstance(disk, dict):
            self.errors.append(f"Disk entry {index} is not a dictionary")
            return
        
        # Check required fields
        missing = [field for field in self.REQUIRED_DISK_FIELDS if field not in disk]
        if missing:
            self.errors.append(
                f"Disk {index} missing fields: {', '.join(missing)}"
            )
        
        # Validate device name
        device = disk.get('device', '')
        if not isinstance(device, str) or not device.strip():
            self.errors.append(f"Disk {index}: invalid device name")
        
        # Validate mount point
        mount_point = disk.get('mount_point', '')
        if not isinstance(mount_point, str) or not mount_point.strip():
            self.errors.append(f"Disk {index}: invalid mount_point")
        
        # Validate filesystem
        filesystem = disk.get('filesystem', '')
        if not isinstance(filesystem, str) or not filesystem.strip():
            self.warnings.append(f"Disk {index}: missing or invalid filesystem")
        
        # Validate numeric values
        self._validate_disk_metrics(disk, index)
    
    def _validate_disk_metrics(self, disk: Dict[str, Any], index: int) -> None:
        """Validate disk metric values."""
        # Validate byte values
        for field in ['total_bytes', 'used_bytes', 'available_bytes']:
            value = disk.get(field)
            if not isinstance(value, (int, float)) or value < 0:
                self.errors.append(
                    f"Disk {index}: {field} must be non-negative number"
                )
        
        # Validate usage percentage
        usage_percent = disk.get('usage_percent')
        if not isinstance(usage_percent, (int, float)):
            self.errors.append(
                f"Disk {index}: usage_percent must be a number"
            )
        elif not 0 <= usage_percent <= 100:
            self.errors.append(
                f"Disk {index}: usage_percent must be between 0 and 100"
            )
        
        # Validate consistency
        total = disk.get('total_bytes', 0)
        used = disk.get('used_bytes', 0)
        available = disk.get('available_bytes', 0)
        
        if total > 0 and (used + available) > total * 1.1:  # 10% tolerance
            self.warnings.append(
                f"Disk {index}: used + available exceeds total (possible filesystem overhead)"
            )
        
        # Validate inode data if present
        if 'inodes_total' in disk:
            self._validate_inode_metrics(disk, index)
    
    def _validate_inode_metrics(self, disk: Dict[str, Any], index: int) -> None:
        """Validate inode metrics."""
        inodes_total = disk.get('inodes_total')
        inodes_used = disk.get('inodes_used')
        inodes_free = disk.get('inodes_free')
        
        if not isinstance(inodes_total, int) or inodes_total < 0:
            self.errors.append(f"Disk {index}: invalid inodes_total")
        
        if not isinstance(inodes_used, int) or inodes_used < 0:
            self.errors.append(f"Disk {index}: invalid inodes_used")
        
        if inodes_total > 0 and inodes_used > inodes_total:
            self.errors.append(f"Disk {index}: inodes_used exceeds inodes_total")
    
    def _validate_system_info(self, system: Dict[str, Any]) -> None:
        """Validate system information."""
        if not isinstance(system, dict):
            self.warnings.append("System info is not a dictionary")
            return
        
        # Validate CPU usage
        if 'cpu_usage_percent' in system:
            cpu = system['cpu_usage_percent']
            if not isinstance(cpu, (int, float)) or not 0 <= cpu <= 100:
                self.warnings.append("Invalid cpu_usage_percent")
        
        # Validate memory usage
        if 'memory_usage_percent' in system:
            mem = system['memory_usage_percent']
            if not isinstance(mem, (int, float)) or not 0 <= mem <= 100:
                self.warnings.append("Invalid memory_usage_percent")
        
        # Validate load average
        if 'load_average' in system:
            load = system['load_average']
            if not isinstance(load, (list, tuple)) or len(load) != 3:
                self.warnings.append("load_average should be array of 3 values")
    
    def _validate_smart_data(self, smart: Dict[str, Any]) -> None:
        """Validate SMART disk data."""
        if not isinstance(smart, dict):
            self.warnings.append("SMART data is not a dictionary")
            return
        
        for device, attrs in smart.items():
            if not isinstance(attrs, dict):
                self.warnings.append(f"SMART data for {device} is not a dictionary")
                continue
            
            # Validate critical SMART attributes
            critical_attrs = [
                'reallocated_sectors',
                'current_pending_sectors',
                'offline_uncorrectable'
            ]
            
            for attr in critical_attrs:
                if attr in attrs:
                    value = attrs[attr]
                    if not isinstance(value, int) or value < 0:
                        self.warnings.append(
                            f"SMART {device}: invalid {attr} value"
                        )
    
    @staticmethod
    def _parse_timestamp(timestamp_str: str) -> datetime:
        """Parse ISO timestamp string."""
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1]
        return datetime.fromisoformat(timestamp_str)
    
    def validate_file(self, file_path: str) -> Tuple[bool, Dict[str, Any], List[str], List[str]]:
        """
        Validate a JSON file.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Tuple of (is_valid, data, errors, warnings)
        """
        self.errors = []
        self.warnings = []
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
        except FileNotFoundError:
            self.errors.append(f"File not found: {file_path}")
            return False, {}, self.errors, self.warnings
        except json.JSONDecodeError as e:
            self.errors.append(f"Invalid JSON: {str(e)}")
            return False, {}, self.errors, self.warnings
        except Exception as e:
            self.errors.append(f"Error reading file: {str(e)}")
            return False, {}, self.errors, self.warnings
        
        is_valid, normalized_data, errors, warnings = self.validate_report(data)
        return is_valid, normalized_data, errors, warnings
