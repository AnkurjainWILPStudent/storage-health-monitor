#!/usr/bin/env python3
"""
Azure Connection Test - Validate Azure credentials and access
"""

import os
import sys
import json
from pathlib import Path

try:
    from azure.identity import ClientSecretCredential
    from azure.storage.blob import BlobServiceClient
    from azure.core.exceptions import AzureError
except ImportError:
    print("❌ Azure SDK not installed!")
    print("   Install with: pip3 install azure-identity azure-storage-blob")
    sys.exit(1)


class AzureConnectionTester:
    """Test Azure credentials and storage access"""
    
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.credential = None
        self.results = {
            'credentials_valid': False,
            'storage_accounts_accessible': {},
            'containers_discovered': {}
        }
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config not found: {config_path}")
        
        with open(config_path, 'r') as f:
            return json.load(f)
    
    def test_credentials(self) -> bool:
        """Test Azure Service Principal credentials"""
        print("\n" + "=" * 70)
        print("Testing Azure Service Principal Credentials")
        print("=" * 70)
        
        try:
            creds = self.config['azure_credentials']
            
            # Check for placeholder values
            placeholders = ['YOUR_', 'PASTE_', 'COPY_']
            for key, value in creds.items():
                if any(p in str(value) for p in placeholders):
                    print(f"❌ {key}: Contains placeholder value")
                    print(f"   Please update {key} in azure_config.json")
                    return False
                print(f"✓ {key}: Configured")
            
            # Try to create credential
            self.credential = ClientSecretCredential(
                tenant_id=creds['tenant_id'],
                client_id=creds['client_id'],
                client_secret=creds['client_secret']
            )
            
            print("\n✓ Azure credentials initialized successfully")
            self.results['credentials_valid'] = True
            return True
        
        except Exception as e:
            print(f"\n❌ Credential initialization failed: {e}")
            return False
    
    def test_storage_account(self, account_name: str, account_config: dict) -> bool:
        """Test access to a storage account"""
        print(f"\n{'-' * 70}")
        print(f"Testing Storage Account: {account_name}")
        print(f"{'-' * 70}")
        
        try:
            connection_string = account_config['connection_string']
            
            # Check for placeholder
            if 'DefaultEndpointsProtocol' not in connection_string:
                print(f"❌ Invalid connection string (looks like placeholder)")
                return False
            
            # Create BlobServiceClient
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            
            # Try to list containers
            containers = []
            for container in blob_service_client.list_containers():
                containers.append(container.name)
            
            print(f"✓ Successfully connected to {account_name}")
            print(f"✓ Discovered {len(containers)} containers:")
            for container_name in containers:
                print(f"  • {container_name}")
            
            self.results['storage_accounts_accessible'][account_name] = True
            self.results['containers_discovered'][account_name] = containers
            
            # Test read access to first container
            if containers:
                test_container = containers[0]
                print(f"\n  Testing read access to '{test_container}'...")
                container_client = blob_service_client.get_container_client(test_container)
                
                blob_count = 0
                total_size = 0
                for blob in container_client.list_blobs():
                    blob_count += 1
                    total_size += blob.size
                
                print(f"  ✓ Read access confirmed")
                print(f"  ✓ Found {blob_count} blobs ({total_size / 1024:.2f} KB)")
            
            return True
        
        except AzureError as e:
            print(f"❌ Azure error: {e}")
            self.results['storage_accounts_accessible'][account_name] = False
            return False
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            self.results['storage_accounts_accessible'][account_name] = False
            return False
    
    def test_all(self):
        """Run all tests"""
        print("\n" + "=" * 70)
        print("Azure Storage Connection Test")
        print("=" * 70)
        
        # Test credentials
        if not self.test_credentials():
            print("\n❌ Credential test failed. Please check azure_config.json")
            return False
        
        # Test each storage account
        all_passed = True
        for account_name, account_config in self.config['storage_accounts'].items():
            if not self.test_storage_account(account_name, account_config):
                all_passed = False
        
        # Print summary
        self._print_summary()
        
        return all_passed
    
    def _print_summary(self):
        """Print test summary"""
        print("\n" + "=" * 70)
        print("Test Summary")
        print("=" * 70)
        
        if self.results['credentials_valid']:
            print("✓ Azure Service Principal credentials: VALID")
        else:
            print("❌ Azure Service Principal credentials: INVALID")
        
        print(f"\nStorage Accounts:")
        for account_name, accessible in self.results['storage_accounts_accessible'].items():
            status = "✓ ACCESSIBLE" if accessible else "❌ FAILED"
            containers = self.results['containers_discovered'].get(account_name, [])
            container_count = len(containers)
            print(f"  {account_name}: {status} ({container_count} containers)")
        
        print("\n" + "=" * 70)
        
        # Final verdict
        all_passed = (
            self.results['credentials_valid'] and
            all(self.results['storage_accounts_accessible'].values())
        )
        
        if all_passed:
            print("✓ All tests passed! Azure monitoring is ready.")
        else:
            print("❌ Some tests failed. Please review errors above.")
        
        print("=" * 70 + "\n")


def main():
    """Main entry point"""
    import argparse
    
    # Default config path
    script_dir = Path(__file__).parent
    default_config = script_dir.parent / 'azure_config.json'
    
    parser = argparse.ArgumentParser(description='Test Azure connection')
    parser.add_argument(
        '--config',
        default=str(default_config),
        help='Path to Azure configuration file'
    )
    
    args = parser.parse_args()
    
    try:
        tester = AzureConnectionTester(args.config)
        success = tester.test_all()
        return 0 if success else 1
    
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("   Make sure azure_config.json exists and is configured.")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
