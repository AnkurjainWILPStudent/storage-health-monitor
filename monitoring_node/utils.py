#!/usr/bin/env python3
"""
Utility functions for the storage health monitoring node.
Provides logging, configuration management, file operations, and validation helpers.
"""

import logging
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import hashlib


def setup_logger(
    log_path: str,
    logger_name: str = "storage_analyzer",
    level: int = logging.INFO,
    max_bytes: int = 10485760,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Set up a rotating file logger with console output.
    
    Args:
        log_path: Path to the log file
        logger_name: Name of the logger
        level: Logging level (default: INFO)
        max_bytes: Maximum size of log file before rotation
        backup_count: Number of backup files to keep
        
    Returns:
        Configured logger instance
    """
    # Create log directory if it doesn't exist
    log_dir = os.path.dirname(log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers - clear any existing handlers first
    if logger.handlers:
        logger.handlers.clear()
    
    # Prevent propagation to root logger to avoid duplicate console output
    logger.propagate = False
    
    # File handler with rotation
    from logging.handlers import RotatingFileHandler
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    file_handler.setLevel(level)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load JSON configuration file with validation.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Invalid JSON in configuration file {config_path}: {e.msg}",
            e.doc,
            e.pos
        )


def load_thresholds(thresholds_path: str) -> Dict[str, Any]:
    """
    Load threshold configuration with validation.
    
    Args:
        thresholds_path: Path to the thresholds file
        
    Returns:
        Thresholds dictionary
    """
    return load_config(thresholds_path)


def write_json_atomic(path: str, data: Dict[str, Any], indent: int = 2) -> None:
    """
    Write JSON data to file atomically to prevent corruption.
    
    Args:
        path: Target file path
        data: Data to write
        indent: JSON indentation level
    """
    tmp_path = f"{path}.tmp"
    try:
        with open(tmp_path, 'w') as f:
            json.dump(data, f, indent=indent, default=str)
        os.replace(tmp_path, path)
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise e


def get_timestamp() -> str:
    """
    Get current UTC timestamp in ISO 8601 format.
    
    Returns:
        ISO format timestamp string
    """
    return datetime.utcnow().isoformat() + "Z"


def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse ISO 8601 timestamp string to datetime object.
    
    Args:
        timestamp_str: ISO format timestamp string
        
    Returns:
        datetime object
    """
    # Remove 'Z' suffix if present
    if timestamp_str.endswith('Z'):
        timestamp_str = timestamp_str[:-1]
    return datetime.fromisoformat(timestamp_str)


def ensure_directory(path: str) -> None:
    """
    Ensure directory exists, create if necessary.
    
    Args:
        path: Directory path
    """
    os.makedirs(path, exist_ok=True)


def get_file_age_minutes(file_path: str) -> float:
    """
    Get the age of a file in minutes based on modification time.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Age in minutes
    """
    if not os.path.exists(file_path):
        return float('inf')
    
    mtime = os.path.getmtime(file_path)
    age_seconds = datetime.now().timestamp() - mtime
    return age_seconds / 60.0


def calculate_file_hash(file_path: str, algorithm: str = 'sha256') -> str:
    """
    Calculate hash of a file for integrity checking.
    
    Args:
        file_path: Path to the file
        algorithm: Hash algorithm (default: sha256)
        
    Returns:
        Hex digest of the file hash
    """
    hash_func = hashlib.new(algorithm)
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def validate_json_structure(
    data: Dict[str, Any],
    required_fields: list,
    logger: Optional[logging.Logger] = None
) -> tuple[bool, list]:
    """
    Validate that JSON data contains required fields.
    
    Args:
        data: JSON data to validate
        required_fields: List of required field names (supports nested with dot notation)
        logger: Optional logger for error messages
        
    Returns:
        Tuple of (is_valid, list_of_missing_fields)
    """
    missing_fields = []
    
    for field in required_fields:
        if '.' in field:
            # Handle nested fields
            parts = field.split('.')
            current = data
            try:
                for part in parts:
                    current = current[part]
            except (KeyError, TypeError):
                missing_fields.append(field)
                if logger:
                    logger.warning(f"Missing required field: {field}")
        else:
            if field not in data:
                missing_fields.append(field)
                if logger:
                    logger.warning(f"Missing required field: {field}")
    
    return len(missing_fields) == 0, missing_fields


def archive_file(source_path: str, archive_dir: str, add_timestamp: bool = True) -> str:
    """
    Archive a file by moving it to an archive directory.
    
    Args:
        source_path: Path to the source file
        archive_dir: Archive directory path
        add_timestamp: Whether to add timestamp to archived filename
        
    Returns:
        Path to archived file
    """
    ensure_directory(archive_dir)
    
    basename = os.path.basename(source_path)
    if add_timestamp:
        name, ext = os.path.splitext(basename)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        basename = f"{name}_{timestamp}{ext}"
    
    dest_path = os.path.join(archive_dir, basename)
    os.rename(source_path, dest_path)
    return dest_path


def cleanup_old_files(directory: str, max_age_days: int, pattern: str = "*") -> int:
    """
    Clean up files older than specified days.
    
    Args:
        directory: Directory to clean
        max_age_days: Maximum age in days
        pattern: File pattern to match
        
    Returns:
        Number of files deleted
    """
    if not os.path.exists(directory):
        return 0
    
    deleted_count = 0
    cutoff_time = datetime.now().timestamp() - (max_age_days * 86400)
    
    for file_path in Path(directory).glob(pattern):
        if file_path.is_file():
            if os.path.getmtime(file_path) < cutoff_time:
                try:
                    os.remove(file_path)
                    deleted_count += 1
                except Exception:
                    pass  # Ignore errors during cleanup
    
    return deleted_count


def format_bytes(bytes_value: int) -> str:
    """
    Format bytes to human-readable string.
    
    Args:
        bytes_value: Number of bytes
        
    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} PB"


def format_percentage(value: float, decimal_places: int = 2) -> str:
    """
    Format a value as percentage.
    
    Args:
        value: Value to format (0-100)
        decimal_places: Number of decimal places
        
    Returns:
        Formatted percentage string
    """
    return f"{value:.{decimal_places}f}%"
