#!/usr/bin/env python3
"""
Azure Test Data Upload - Upload test files to simulate storage usage
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

try:
    from azure.storage.blob import BlobServiceClient
    from azure.core.exceptions import AzureError
except ImportError:
    print("❌ Azure SDK not installed!")
    print("   Install with: pip3 install azure-identity azure-storage-blob")
    sys.exit(1)


class TestDataUploader:
    """Upload test data to Azure storage containers"""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config not found: {config_path}")
        
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def generate_test_file(self, size_mb: float) -> bytes:
        """Generate test file data"""
        size_bytes = int(size_mb * 1024 * 1024)
        # Generate random-ish data (repeating pattern is fine for testing)
        pattern = b"TEST_DATA_" * 100
        full_data = pattern * (size_bytes // len(pattern) + 1)
        return full_data[:size_bytes]
    
    def upload_to_container(self,
                           account_name: str,
                           container_name: str,
                           file_count: int,
                           file_size_mb: float):
        """Upload test files to a container"""
        print(f"\n{'=' * 70}")
        print(f"Uploading to {account_name}/{container_name}")
        print(f"{'=' * 70}")
        
        try:
            # Get connection string
            account_config = self.config['storage_accounts'][account_name]
            connection_string = account_config['connection_string']
            
            # Create BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(container_name)
            
            # Check if container exists
            try:
                container_client.get_container_properties()
            except AzureError:
                print(f"❌ Container '{container_name}' not found")
                return False
            
            # Generate and upload files
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            test_data = self.generate_test_file(file_size_mb)
            
            print(f"\nUploading {file_count} files ({file_size_mb} MB each)...")
            
            for i in range(file_count):
                blob_name = f"test_data_{timestamp}_{i + 1:03d}.bin"
                blob_client = container_client.get_blob_client(blob_name)
                
                try:
                    blob_client.upload_blob(test_data, overwrite=True)
                    print(f"  ✓ Uploaded: {blob_name}")
                except Exception as e:
                    print(f"  ❌ Failed to upload {blob_name}: {e}")
            
            # Calculate total upload
            total_mb = file_count * file_size_mb
            print(f"\n✓ Upload complete: {file_count} files, {total_mb:.2f} MB total")
            
            # Show current container stats
            blob_count = 0
            total_size = 0
            for blob in container_client.list_blobs():
                blob_count += 1
                total_size += blob.size
            
            total_gb = total_size / (1024 ** 3)
            print(f"\nContainer stats after upload:")
            print(f"  Total blobs: {blob_count}")
            print(f"  Total size: {total_gb:.3f} GB")
            
            return True
        
        except Exception as e:
            print(f"❌ Upload failed: {e}")
            return False
    
    def simulate_high_usage(self, account_name: str, target_percent: float):
        """
        Simulate high storage usage by uploading files
        
        Note: For Azure free tier, we estimate capacity. This uploads files
        to trigger percentage-based alerts.
        """
        print(f"\n{'=' * 70}")
        print(f"Simulating {target_percent}% usage for {account_name}")
        print(f"{'=' * 70}")
        
        try:
            account_config = self.config['storage_accounts'][account_name]
            connection_string = account_config['connection_string']
            
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            
            # Get all containers
            containers = [c.name for c in blob_service_client.list_containers()]
            
            if not containers:
                print(f"❌ No containers found in {account_name}")
                return False
            
            # Upload to first container
            target_container = containers[0]
            
            # Calculate upload size
            # Assuming 5TB capacity estimate, upload enough to hit target percentage
            estimated_capacity_gb = 5 * 1024  # 5 TB
            target_gb = estimated_capacity_gb * (target_percent / 100)
            
            # Upload in 100MB chunks
            chunk_size_mb = 100
            file_count = int(target_gb * 1024 / chunk_size_mb)
            
            print(f"\nTarget: {target_percent}% of estimated {estimated_capacity_gb / 1024:.1f} TB capacity")
            print(f"Uploading ~{target_gb:.2f} GB to '{target_container}'")
            print(f"This will take {file_count} files of {chunk_size_mb} MB each")
            
            confirm = input("\nProceed with upload? (yes/no): ")
            if confirm.lower() != 'yes':
                print("Upload cancelled")
                return False
            
            return self.upload_to_container(
                account_name,
                target_container,
                file_count,
                chunk_size_mb
            )
        
        except Exception as e:
            print(f"❌ Simulation failed: {e}")
            return False


def main():
    """Main entry point"""
    # Default config path
    script_dir = Path(__file__).parent
    default_config = script_dir.parent / 'azure_config.json'
    
    parser = argparse.ArgumentParser(
        description='Upload test data to Azure storage containers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload 10 files (5 MB each) to finance/invoices
  python upload_test_data.py --account finance --container invoices --count 10 --size 5

  # Simulate 75% usage (WARNING: uploads large amount of data)
  python upload_test_data.py --account finance --simulate 75
        """
    )
    
    parser.add_argument(
        '--config',
        default=str(default_config),
        help='Path to Azure configuration file'
    )
    parser.add_argument(
        '--account',
        required=True,
        choices=['finance', 'marketing'],
        help='Storage account name'
    )
    parser.add_argument(
        '--container',
        help='Container name to upload to'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=5,
        help='Number of files to upload (default: 5)'
    )
    parser.add_argument(
        '--size',
        type=float,
        default=10.0,
        help='Size of each file in MB (default: 10.0)'
    )
    parser.add_argument(
        '--simulate',
        type=float,
        metavar='PERCENT',
        help='Simulate storage usage at PERCENT%% (e.g., 75 for 75%%)'
    )
    
    args = parser.parse_args()
    
    try:
        uploader = TestDataUploader(args.config)
        
        if args.simulate:
            # Simulate high usage
            success = uploader.simulate_high_usage(args.account, args.simulate)
        else:
            # Upload specific files
            if not args.container:
                print("❌ --container required when not using --simulate")
                return 1
            
            success = uploader.upload_to_container(
                args.account,
                args.container,
                args.count,
                args.size
            )
        
        return 0 if success else 1
    
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        return 1
    except KeyError as e:
        print(f"\n❌ Invalid account name: {e}")
        print("   Available accounts: finance, marketing")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
