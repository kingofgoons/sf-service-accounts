# ETL Service Account Client-Side Testing

This directory contains the client-side test script to verify the ETL service account can connect to Snowflake using RSA key authentication.

## Setup

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Create configuration file:**
   Copy the template and update with your Snowflake account details:
   ```bash
   cp config.template.json config.json
   ```
   
   Then edit `config.json` with your actual values:
   ```json
   {
     "account": "xy12345.us-east-1",
     "user": "ETL_SERVICE_USER",
     "warehouse": "ETL_WH",
     "database": "ETL_DB",
     "schema": "ETL_SCHEMA",
     "role": "ETL_SERVICE_ROLE"
   }
   ```
   
   **⚠️ Important:** `config.json` is excluded from git via `.gitignore` to protect your account information.

3. **Ensure RSA keys are set up:**
   - The private key (`rsa_key.p8`) should be in the parent directory
   - The public key should be configured in Snowflake (see `../snowflake-side/create.service.user.sql`)

## Running the Test

Execute the test script:
```bash
python test_service_account.py
```

## What the Test Does

The script performs the following operations:

1. ✅ Loads the RSA private key from `../rsa_key.p8`
2. ✅ Connects to Snowflake using RSA key authentication
3. ✅ Creates a test table (`test_etl_connection`)
4. ✅ Inserts 5 test records
5. ✅ Queries the data back
6. ✅ Verifies data integrity (row count)
7. ✅ Cleans up by dropping the test table

## Expected Output

If successful, you should see output like:
```
============================================================
ETL Service Account Connection Test
============================================================

1. Loading RSA private key...
   ✓ Private key loaded successfully

2. Connecting to Snowflake...
   ✓ Connected successfully!
   Account: xy12345.us-east-1
   User: ETL_SERVICE_USER
   Role: ETL_SERVICE_ROLE

3. Creating test table...
   ✓ Table 'test_etl_connection' created successfully

4. Inserting test data...
   ✓ Inserted 5 test records

5. Querying test data...
   ✓ Query successful! Retrieved 5 records

6. Verifying data integrity...
   ✓ Total records in table: 5

7. Cleaning up test table...
   ✓ Test table dropped

8. Connection closed

============================================================
✓ All tests passed successfully!
============================================================
```

## Troubleshooting

- **"Config file not found"**: Run `cp config.template.json config.json` and edit with your details
- **"Private key file not found"**: Ensure `rsa_key.p8` exists in the parent directory
- **"Connection failed"**: Verify your account identifier and that the public key is set in Snowflake
- **"Permission denied"**: Ensure the service user has the correct role and grants (run the SQL script in `../snowflake-side/`)

## Security Notes

The following files are **NOT** tracked in git (via `.gitignore`):
- `config.json` - Contains your Snowflake account information
- `../rsa_key.p8` - Your private RSA key (never commit this!)

The following files **ARE** tracked in git:
- `config.template.json` - Template showing config structure (no sensitive data)
- `test_service_account.py` - The test script itself
- `requirements.txt` - Python dependencies

