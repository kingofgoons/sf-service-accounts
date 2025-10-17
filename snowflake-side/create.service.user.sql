-- =====================================================
-- SERVICE USER SETUP FOR ETL WITH RSA KEY AUTHENTICATION
-- =====================================================
-- Run this script as ACCOUNTADMIN or with appropriate privileges

-- Step 1: Use SYSADMIN to create database, schema, and warehouse
USE ROLE SYSADMIN;

-- Create database and schema for ETL operations
CREATE DATABASE IF NOT EXISTS ETL_DB;
CREATE SCHEMA IF NOT EXISTS ETL_DB.ETL_SCHEMA;

-- Create warehouse for ETL operations BEFORE creating user
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for ETL service account operations';

-- Step 2: Use SECURITYADMIN to create role
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS ETL_SERVICE_ROLE
  COMMENT = 'Role for ETL service account operations';

-- Step 3: Use USERADMIN to create the service user
USE ROLE USERADMIN;

-- TYPE = SERVICE designates this as a service account (not a person)
CREATE USER IF NOT EXISTS ETL_SERVICE_USER
  TYPE = SERVICE
  DEFAULT_WAREHOUSE = ETL_WH
  DEFAULT_NAMESPACE = ETL_DB.ETL_SCHEMA
  DEFAULT_ROLE = ETL_SERVICE_ROLE
  COMMENT = 'Service account for ETL operations with RSA key authentication';

-- Step 4: Use SECURITYADMIN to grant roles and privileges
USE ROLE SECURITYADMIN;

-- Grant the role to the service user
GRANT ROLE ETL_SERVICE_ROLE TO USER ETL_SERVICE_USER;

-- Grant database and schema privileges
GRANT USAGE ON DATABASE ETL_DB TO ROLE ETL_SERVICE_ROLE;
GRANT USAGE ON SCHEMA ETL_DB.ETL_SCHEMA TO ROLE ETL_SERVICE_ROLE;

-- Grant privileges needed for ETL operations
GRANT CREATE TABLE ON SCHEMA ETL_DB.ETL_SCHEMA TO ROLE ETL_SERVICE_ROLE;
GRANT CREATE VIEW ON SCHEMA ETL_DB.ETL_SCHEMA TO ROLE ETL_SERVICE_ROLE;
GRANT CREATE STAGE ON SCHEMA ETL_DB.ETL_SCHEMA TO ROLE ETL_SERVICE_ROLE;
GRANT CREATE FILE FORMAT ON SCHEMA ETL_DB.ETL_SCHEMA TO ROLE ETL_SERVICE_ROLE;
GRANT CREATE PIPE ON SCHEMA ETL_DB.ETL_SCHEMA TO ROLE ETL_SERVICE_ROLE;

-- Grant privileges on all existing and future tables in the schema
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA ETL_DB.ETL_SCHEMA TO ROLE ETL_SERVICE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA ETL_DB.ETL_SCHEMA TO ROLE ETL_SERVICE_ROLE;

-- Grant warehouse privileges
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE ETL_SERVICE_ROLE;
GRANT OPERATE ON WAREHOUSE ETL_WH TO ROLE ETL_SERVICE_ROLE;

-- Step 5: Use USERADMIN to configure RSA public key authentication
USE ROLE USERADMIN;
-- Replace <RSA_PUBLIC_KEY> with the actual public key (without headers/footers)
-- To generate RSA key pair on client side:
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
-- Then copy the content between -----BEGIN PUBLIC KEY----- and -----END PUBLIC KEY-----

ALTER USER ETL_SERVICE_USER SET RSA_PUBLIC_KEY = '<RSA_PUBLIC_KEY>';

-- Optional: Set a second RSA public key for key rotation
-- ALTER USER ETL_SERVICE_USER SET RSA_PUBLIC_KEY_2 = '<RSA_PUBLIC_KEY_2>';

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================

-- Verify the user was created
SHOW USERS LIKE 'ETL_SERVICE_USER';

-- Verify the role and grants
SHOW GRANTS TO ROLE ETL_SERVICE_ROLE;

-- Verify the user's RSA public key is set
DESC USER ETL_SERVICE_USER;

-- =====================================================
-- NOTES
-- =====================================================
-- 1. Replace <RSA_PUBLIC_KEY> with your actual public key value
-- 2. The public key should be a single line without BEGIN/END headers
-- 3. Adjust warehouse size, database/schema names as needed
-- 4. The service user will authenticate using the private key from client side
-- 5. Password authentication is disabled by default for SERVICE type users

