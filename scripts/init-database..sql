-- Drop and recreate DataWarehouse database

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'datawarehouse') THEN
        PERFORM pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = 'datawarehouse';

        EXECUTE 'DROP DATABASE datawarehouse';
    END IF;
END $$;

-- Create DataWarehouse Database and Schemas
create database DataWarehouse;
create schema bronze;
create schema silver;
create schema gold;