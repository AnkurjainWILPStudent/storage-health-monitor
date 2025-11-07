#!/usr/bin/env python3
"""
Quick test script to verify analyzer functionality.
Creates a sample data file and runs the analyzer against it.
"""

import os
import sys
import json
import shutil
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def create_test_data_directory():
    """Create a temporary test data directory."""
    test_dir = Path(__file__).parent.parent / "test_data_temp"
    test_dir.mkdir(exist_ok=True)
    return str(test_dir)

def create_sample_data(output_dir: str):
    """Create sample client data files."""
    # Sample 1: Normal disk usage
    sample1 = {
        "hostname": "test-server-01",
        "timestamp": "2025-11-06T14:30:00Z",
        "disks": [
            {
                "device": "/dev/sda1",
                "mount_point": "/",
                "filesystem": "ext4",
                "total_bytes": 107374182400,
                "used_bytes": 53687091200,
                "available_bytes": 53687091200,
                "usage_percent": 50.0,
                "inodes_total": 6553600,
                "inodes_used": 524288,
                "inodes_free": 6029312
            }
        ],
        "system": {
            "cpu_usage_percent": 35.0,
            "memory_usage_percent": 60.0,
            "load_average": [1.5, 1.2, 1.0]
        }
    }
    
    # Sample 2: High disk usage (warning)
    sample2 = {
        "hostname": "test-server-02",
        "timestamp": "2025-11-06T14:30:00Z",
        "disks": [
            {
                "device": "/dev/sda1",
                "mount_point": "/",
                "filesystem": "ext4",
                "total_bytes": 107374182400,
                "used_bytes": 85899345920,
                "available_bytes": 21474836480,
                "usage_percent": 80.0
            }
        ]
    }
    
    # Sample 3: Critical disk usage
    sample3 = {
        "hostname": "test-server-03",
        "timestamp": "2025-11-06T14:30:00Z",
        "disks": [
            {
                "device": "/dev/sda1",
                "mount_point": "/",
                "filesystem": "ext4",
                "total_bytes": 107374182400,
                "used_bytes": 96636764160,
                "available_bytes": 10737418240,
                "usage_percent": 90.0
            }
        ],
        "system": {
            "cpu_usage_percent": 95.0,
            "memory_usage_percent": 90.0,
            "load_average": [8.5, 7.2, 6.0]
        }
    }
    
    samples = [
        ("server01_report.json", sample1),
        ("server02_report.json", sample2),
        ("server03_report.json", sample3)
    ]
    
    for filename, data in samples:
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Created: {filepath}")

def update_test_config(test_data_dir: str):
    """Create a test configuration that uses the test data directory."""
    config_dir = Path(__file__).parent.parent / "config"
    test_config = {
        "data_directory": test_data_dir,
        "log_path": os.path.join(test_data_dir, "test_analyzer.log"),
        "output_directory": os.path.join(test_data_dir, "reports"),
        "thresholds_file": str(config_dir / "thresholds.json"),
        "alert_config": {
            "email": {"smtp_server": "", "recipients": []},
            "webhook": {"url": ""}
        },
        "analysis": {
            "file_pattern": "*.json",
            "archive_processed": False,
            "retention_days": 30
        },
        "alerts": {
            "enable_email": False,
            "enable_webhook": False,
            "enable_console": True,
            "enable_log": True
        }
    }
    
    test_config_path = os.path.join(test_data_dir, "test_config.json")
    with open(test_config_path, 'w') as f:
        json.dump(test_config, f, indent=2)
    
    return test_config_path

def run_test():
    """Run the test."""
    print("=" * 60)
    print("Storage Health Analyzer - Quick Test")
    print("=" * 60)
    print()
    
    # Create test environment
    print("Setting up test environment...")
    test_dir = create_test_data_directory()
    reports_dir = os.path.join(test_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    
    print(f"Test directory: {test_dir}")
    print()
    
    # Create sample data
    print("Creating sample data files...")
    create_sample_data(test_dir)
    print()
    
    # Create test config
    print("Creating test configuration...")
    test_config_path = update_test_config(test_dir)
    print(f"Test config: {test_config_path}")
    print()
    
    # Run the analyzer
    print("Running analyzer...")
    print("-" * 60)
    
    from monitoring_node.scripts.analyze_storage_health import StorageHealthAnalyzer
    from monitoring_node.config import thresholds
    
    config_dir = Path(__file__).parent.parent / "config"
    thresholds_path = str(config_dir / "thresholds.json")
    
    try:
        analyzer = StorageHealthAnalyzer(test_config_path, thresholds_path)
        analyzer.run()
        
        print("-" * 60)
        print()
        print("Test completed successfully!")
        print()
        print("Results:")
        print(f"  - Log file: {os.path.join(test_dir, 'test_analyzer.log')}")
        print(f"  - Reports: {reports_dir}")
        print()
        
        # Show generated report
        report_files = list(Path(reports_dir).glob("*.json"))
        if report_files:
            print(f"Generated report: {report_files[0]}")
            with open(report_files[0], 'r') as f:
                report = json.load(f)
            
            print("\nSummary:")
            summary = report.get('summary', {})
            print(f"  Total hosts: {summary.get('total_hosts', 0)}")
            print(f"  Hosts with issues: {summary.get('hosts_with_issues', 0)}")
            print(f"  Total issues: {summary.get('total_issues', 0)}")
            print(f"  Critical issues: {summary.get('critical_issues', 0)}")
        
        # Cleanup option
        print()
        response = input("Clean up test directory? (y/n): ")
        if response.lower() == 'y':
            shutil.rmtree(test_dir)
            print("Test directory removed.")
        else:
            print(f"Test data preserved at: {test_dir}")
        
        return 0
    
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(run_test())
