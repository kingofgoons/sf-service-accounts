#!/usr/bin/env python3
"""
Test script for ETL Service Account connectivity and basic operations
This script verifies:
1. RSA key authentication works
2. Service account can create tables
3. Service account can insert and query data
"""

import sys
import json
from pathlib import Path
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector


# Path to the config file (relative to this script)
CONFIG_PATH = Path(__file__).parent / 'config.json'

# Path to the private key file (relative to this script)
PRIVATE_KEY_PATH = Path(__file__).parent.parent / 'rsa_key.p8'


def load_config(config_path):
    """
    Load Snowflake configuration from JSON file
    
    Args:
        config_path: Path to the config.json file
        
    Returns:
        Dictionary with Snowflake connection parameters
    """
    if not config_path.exists():
        print(f"Error: Config file not found at {config_path}")
        print("\nPlease create config.json from config.template.json:")
        print(f"  cp {config_path.parent / 'config.template.json'} {config_path}")
        print("  Then update the values in config.json with your Snowflake account details")
        sys.exit(1)
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate required fields
        required_fields = ['account', 'user', 'warehouse', 'database', 'schema', 'role']
        missing_fields = [field for field in required_fields if field not in config]
        
        if missing_fields:
            print(f"Error: Missing required fields in config.json: {', '.join(missing_fields)}")
            sys.exit(1)
        
        return config
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)


def load_private_key(key_path):
    """
    Load and decode the private key for RSA authentication
    
    Args:
        key_path: Path to the private key file
        
    Returns:
        Encoded private key bytes
    """
    with open(key_path, 'rb') as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,  # No password since we used -nocrypt
            backend=default_backend()
        )
    
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    return pkb


def test_connection(config):
    """
    Test the Snowflake connection with RSA key authentication
    
    Args:
        config: Dictionary with Snowflake connection parameters
    """
    print("=" * 60)
    print("ETL Service Account Connection Test")
    print("=" * 60)
    
    # Load the private key
    print("\n1. Loading RSA private key...")
    try:
        private_key_bytes = load_private_key(PRIVATE_KEY_PATH)
        print("   ✓ Private key loaded successfully")
    except Exception as e:
        print(f"   ✗ Error loading private key: {e}")
        return False
    
    # Establish connection
    print("\n2. Connecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(
            account=config['account'],
            user=config['user'],
            private_key=private_key_bytes,
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config['schema'],
            role=config['role']
        )
        print("   ✓ Connected successfully!")
        print(f"   Account: {config['account']}")
        print(f"   User: {config['user']}")
        print(f"   Role: {config['role']}")
    except Exception as e:
        print(f"   ✗ Connection failed: {e}")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Test 3: Create a test table
        print("\n3. Creating test table...")
        try:
            cursor.execute("""
                CREATE OR REPLACE TABLE test_etl_connection (
                    id INTEGER,
                    name VARCHAR(100),
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    description VARCHAR(500)
                )
            """)
            print("   ✓ Table 'test_etl_connection' created successfully")
        except Exception as e:
            print(f"   ✗ Error creating table: {e}")
            return False
        
        # Test 4: Insert test data
        print("\n4. Inserting test data...")
        test_data = [
            (1, 'Test Record 1', 'First test record for ETL service account'),
            (2, 'Test Record 2', 'Second test record with RSA authentication'),
            (3, 'Test Record 3', 'Third test record to verify INSERT permissions'),
            (4, 'Test Record 4', 'Fourth test record for validation'),
            (5, 'Test Record 5', 'Fifth and final test record')
        ]
        
        try:
            for record in test_data:
                cursor.execute(
                    "INSERT INTO test_etl_connection (id, name, description) VALUES (%s, %s, %s)",
                    record
                )
            print(f"   ✓ Inserted {len(test_data)} test records")
        except Exception as e:
            print(f"   ✗ Error inserting data: {e}")
            return False
        
        # Test 5: Query the data back
        print("\n5. Querying test data...")
        try:
            cursor.execute("SELECT * FROM test_etl_connection ORDER BY id")
            results = cursor.fetchall()
            print(f"   ✓ Query successful! Retrieved {len(results)} records")
            print("\n   Sample data:")
            print("   " + "-" * 56)
            print(f"   {'ID':<5} {'Name':<20} {'Description':<30}")
            print("   " + "-" * 56)
            for row in results[:3]:  # Show first 3 records
                print(f"   {row[0]:<5} {row[1]:<20} {row[3][:30]:<30}")
            if len(results) > 3:
                print(f"   ... and {len(results) - 3} more record(s)")
        except Exception as e:
            print(f"   ✗ Error querying data: {e}")
            return False
        
        # Test 6: Verify row count
        print("\n6. Verifying data integrity...")
        try:
            cursor.execute("SELECT COUNT(*) FROM test_etl_connection")
            count = cursor.fetchone()[0]
            print(f"   ✓ Total records in table: {count}")
            assert count == len(test_data), "Row count mismatch!"
        except Exception as e:
            print(f"   ✗ Error verifying data: {e}")
            return False
        
        # Optional: Clean up (comment out if you want to keep the test table)
        print("\n7. Cleaning up test table...")
        try:
            cursor.execute("DROP TABLE IF EXISTS test_etl_connection")
            print("   ✓ Test table dropped")
        except Exception as e:
            print(f"   ⚠ Warning: Could not drop test table: {e}")
        
        cursor.close()
        
    finally:
        conn.close()
        print("\n8. Connection closed")
    
    print("\n" + "=" * 60)
    print("✓ All tests passed successfully!")
    print("=" * 60)
    return True


if __name__ == '__main__':
    # Load configuration from config.json
    print("Loading configuration from config.json...")
    config = load_config(CONFIG_PATH)
    
    # Check if private key file exists
    if not PRIVATE_KEY_PATH.exists():
        print(f"Error: Private key file not found at {PRIVATE_KEY_PATH}")
        print("Please ensure rsa_key.p8 exists in the parent directory")
        sys.exit(1)
    
    # Check if account is configured
    if config['account'] == 'YOUR_ACCOUNT_IDENTIFIER':
        print("Error: Please update the 'account' value in config.json")
        print("Replace 'YOUR_ACCOUNT_IDENTIFIER' with your actual Snowflake account identifier")
        sys.exit(1)
    
    print("✓ Configuration loaded successfully\n")
    
    # Run the test
    success = test_connection(config)
    sys.exit(0 if success else 1)

