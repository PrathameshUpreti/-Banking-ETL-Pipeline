
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.email import EmailOperator


default_args={
    'owner':'Prathamesh Upreti',
    'depends_on_past':False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
dag= DAG(
    'BAanking_ETL_Pipeline',
    default_args=default_args,
    description='Automated Banking ETL Pipeline - S3 to Snowflake',
    schedule_interval='@daily',
    catchup=False,
    tags=['Banking','etl','snowflake','s3']
)

S3_BUCKET = 'banking-bucket-etl'
SNOWFLAKE_CONN_ID = 'Snowflake_connection'
SNOWFLAKE_DATABASE = 'Bank'
SNOWFLAKE_SCHEMA = 'bank'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_STAGE ='banking_stage'


check_customers_file=S3KeySensor(
    task_id='check_cutomers_for_files_in_S3',
    bucket_name=S3_BUCKET,
    bucket_key='customers.csv',
    aws_conn_id='aws_conn',
    timeout=300,
    poke_interval=30,
    dag=dag

)
check_accounts_file=S3KeySensor(
    task_id='check_accounts_for_files_in_S3',
    bucket_name=S3_BUCKET,
    bucket_key='accounts.csv',
    aws_conn_id='aws_conn',
    timeout=300,
    poke_interval=30,
    dag=dag

)
check_transactions_file=S3KeySensor(
    task_id='check_transaction_for_files_in_S3',
    bucket_name=S3_BUCKET,
    bucket_key='transactions.csv',
    aws_conn_id='aws_conn',
    timeout=300,
    poke_interval=30,
    dag=dag

)
check_loans_file=S3KeySensor(
    task_id='check_loan_for_files_in_S3',
    bucket_name=S3_BUCKET,
    bucket_key='loans.csv',
    aws_conn_id='aws_conn',
    timeout=300,
    poke_interval=30,
    dag=dag

)
list_s3_files = S3ListOperator(
    task_id='list_s3_files',
    bucket='banking-bucket-etl',
    aws_conn_id='aws_conn',
    dag=dag,
)
create_stage = SQLExecuteQueryOperator(
    task_id='create_stage',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    CREATE STAGE IF NOT EXISTS banking_stage
    URL='s3://banking-bucket-etl'
    storage_integration=S3_IntegrationS;
    """,
    dag=dag,
)
verify_stage = SQLExecuteQueryOperator(
    task_id='verify_stage_and_list_s3_files',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    LIST @banking_stage;
    """,
    dag=dag,
)




create_staging_table= SQLExecuteQueryOperator(
    task_id='create_database_schema',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
        USE DATABASE {SNOWFLAKE_DATABASE};
        USE SCHEMA {SNOWFLAKE_SCHEMA};
        USE WAREHOUSE {SNOWFLAKE_WAREHOUSE};
        CREATE TABLE IF NOT EXISTS CUSTOMERS(
          customer_id INT,
          name STRING,
          dob DATE,
          city STRING,
          account_type STRING,
          join_date DATE
        );
        CREATE TABLE IF NOT EXISTS ACCOUNTS(
          account_id INT,
          customer_id INT,
          branch STRING,
          balance FLOAT,
          status STRING
        );

        CREATE TABLE IF NOT EXISTS TRANSACTIONS(
          txn_id INT,
          account_id INT,
          txn_date DATE,
          txn_type STRING,
          amount FLOAT
        );
        CREATE TABLE IF NOT EXISTS LOAN(
          loan_id INT,
          customer_id INT,
          loan_type STRING,
          amount FLOAT,
          start_date DATE,
          status STRING
        
        );
        """,
        dag=dag
)

truncate_tables = SQLExecuteQueryOperator(
    task_id='truncate_staging_tables',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
   
    TRUNCATE TABLE IF EXISTS CUSTOMERS;
    TRUNCATE TABLE IF EXISTS ACCOUNTS;
    TRUNCATE TABLE IF EXISTS TRANSACTIONS;
    TRUNCATE TABLE IF EXISTS LOAN;
    """,
    dag=dag,
)

load_customers = SQLExecuteQueryOperator(
    task_id='load_customers_from_s3',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    COPY INTO CUSTOMERS FROM @banking_stage/customers.csv
    file_format=(type=csv field_delimiter=',' skip_header=1)
    on_error='continue';
    
    -- Log the load
    SELECT 'CUSTOMERS' AS table_name, COUNT(*) AS records_loaded FROM CUSTOMERS;
    """,
    dag=dag,
)

# Task: Load Accounts Data
load_accounts = SQLExecuteQueryOperator(
    task_id='load_accounts_from_s3',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    COPY INTO ACCOUNTS FROM @banking_stage/accounts.csv
    file_format=(type=csv field_delimiter=',' skip_header=1)
    on_error='continue';
    
    -- Log the load
    SELECT 'ACCOUNTS' AS table_name, COUNT(*) AS records_loaded FROM ACCOUNTS;
    """,
    dag=dag,
)

# Task: Load Transactions Data
load_transactions = SQLExecuteQueryOperator(
    task_id='load_transactions_from_s3',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    COPY INTO TRANSACTIONS FROM @banking_stage/transactions.csv
    file_format=(type=csv field_delimiter=',' skip_header=1)
    on_error='continue';
    
    -- Log the load
    SELECT 'TRANSACTIONS' AS table_name, COUNT(*) AS records_loaded FROM TRANSACTIONS;
    """,
    dag=dag,
)

# Task: Load Loan Data
load_loans = SQLExecuteQueryOperator(
    task_id='load_loans_from_s3',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    COPY INTO LOAN FROM @banking_stage/loans.csv
    file_format=(type=csv field_delimiter=',' skip_header=1)
    on_error='continue';
    
    -- Log the load
    SELECT 'LOAN' AS table_name, COUNT(*) AS records_loaded FROM LOAN;
    """,
    dag=dag,
)

data_quality_check = SQLExecuteQueryOperator(
    task_id='data_quality_check',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    CREATE OR REPLACE TABLE etl_null_report AS
    SELECT 'CUSTOMERS_NULL_CHECK' AS check_type, COUNT(*) AS failed_records
    FROM CUSTOMERS
    WHERE customer_id IS NULL OR name IS NULL OR dob IS NULL
    UNION ALL
    SELECT 'CUSTOMERS_DUPLICATES', COUNT(*)
    FROM (
    SELECT customer_id FROM CUSTOMERS GROUP BY customer_id HAVING COUNT(*) > 1)
    UNION ALL
    SELECT 'ACCOUNTS_NULL_CHECK', COUNT(*)
    FROM ACCOUNTS
    WHERE account_id IS NULL OR customer_id IS NULL OR balance IS NULL
    UNION ALL
    SELECT 'TRANSACTION_AMOUNT_VALIDATION', COUNT(*)
    FROM TRANSACTIONS
    WHERE amount <= 0
    UNION ALL
    SELECT 'LOAN_AMOUNT_VALIDATION', COUNT(*)
    FROM LOAN
    WHERE amount <= 0;


    """,
    dag=dag

)
create_customer_account = SQLExecuteQueryOperator(
    task_id='create_customer_account',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
USE DATABASE {SNOWFLAKE_DATABASE};
USE SCHEMA {SNOWFLAKE_SCHEMA};
CREATE OR REPLACE TABLE customer_account AS
SELECT c.name ,a.account_id, c.city , a.balance
FROM customers  as c
Join accounts as a
on c.customer_id=a.customer_id;
    """,
    dag=dag
)


create_transaction_summary = SQLExecuteQueryOperator(
    task_id='create_transaction_summary',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
USE DATABASE {SNOWFLAKE_DATABASE};
USE SCHEMA {SNOWFLAKE_SCHEMA};
CREATE OR REPLACE TABLE customer_transaction_summary AS
SELECT ca.name,
SUM(CASE WHEN UPPER(TRIM(t.txn_type)) = 'CREDIT' THEN TO_NUMBER(t.amount) ELSE 0 END) AS total_credit,
SUM(CASE WHEN UPPER(TRIM(t.txn_type)) = 'DEBIT' THEN TO_NUMBER(t.amount) ELSE 0 END) AS total_debit
FROM customer_account as ca
join transactions as t
on ca.account_id=t.account_id
group by ca.name

""",
dag=dag
)
create_loan_balance_ratio = SQLExecuteQueryOperator(
    task_id='create_loan_balance_ratio',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
USE DATABASE {SNOWFLAKE_DATABASE};
USE SCHEMA {SNOWFLAKE_SCHEMA};
CREATE OR REPLACE TABLE loan_to_balance_ratio AS
SELECT c.name, i.loan_type, i.amount as loan_amount, a.BALANCE,
(loan_amount/a.BALANCE) as loan_balanace_ratio

from customers as c
join accounts a on c.customer_id=a.customer_id
join loan i on c.customer_id=i.customer_id


""",
dag=dag
)
create_risk_profile = SQLExecuteQueryOperator(
    task_id='create_risk_profile',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
USE DATABASE {SNOWFLAKE_DATABASE};
USE SCHEMA {SNOWFLAKE_SCHEMA};
CREATE OR REPLACE TABLE customer_risk_profile AS
SELECT c.customer_id , c.name, sum(a.balance) as total_balance, sum(i.amount) as total_loan,SUM(i.amount)/NULLIF(SUM(a.balance), 0) as loan_balance_ratio,
CASE WHEN SUM(i.amount)/NULLIF(SUM(a.balance),0)>0.5 THEN 'YES' ELSE 'NO ' END AS Over_Laveraged_customer
from customers as c
LEFT JOIN accounts as a
ON c.customer_id=a.customer_id
LEFT JOIN LOAN as i
ON c.customer_id = i.customer_id

GROUP BY c.customer_id , c.name
""",
dag=dag
)

create_loan_aging = SQLExecuteQueryOperator(
    task_id='create_loan_aging',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
USE DATABASE {SNOWFLAKE_DATABASE};
USE SCHEMA {SNOWFLAKE_SCHEMA};
CREATE OR REPLACE TABLE active_loan_aging AS
SELECT loan_id,start_date,status,loan_type,amount,
DATEDIFF('month',start_date, CURRENT_DATE) AS loan_age_month
from loan
WHERE status='Active'

""",
dag=dag
)
create_customer_segmentation = SQLExecuteQueryOperator(
    task_id='create_customer_segmentation',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    
    CREATE OR REPLACE TABLE customer_segmentation AS
    SELECT c.customer_id, c.name, SUM(a.balance) AS total_balance,
    CASE 
    WHEN SUM(a.balance) < 15000 THEN 'Normal'
    WHEN SUM(a.balance) BETWEEN 15000 AND 50000 THEN 'Moderate'
    ELSE 'Premium'
    END AS CUSTOMER_SEGMENT
    FROM customers AS c
    JOIN accounts AS a ON c.customer_id = a.customer_id
    WHERE a.status = 'Active'
    GROUP BY c.customer_id, c.name;
    """,
    dag=dag,
)
create_risky_transactions = SQLExecuteQueryOperator(
    task_id='create_risky_transactions',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    
    
    CREATE OR REPLACE TABLE risky_transactions AS
    SELECT txn_id, account_id, amount, txn_date, txn_type,
    CASE WHEN amount > 30000 THEN 'RISKY' ELSE 'SAFE' END AS FLAG_TRANSACTION
    FROM transactions;
    """,
    dag=dag,
)
create_customer_loan_summary = SQLExecuteQueryOperator(
    task_id='create_customer_loan_summary',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    
    CREATE OR REPLACE TABLE customer_loan_summary AS
    SELECT c.name, c.customer_id, 
    COUNT(l.loan_id) AS Active_loan_count, 
    SUM(l.amount) AS total_amount
    FROM customers c 
    JOIN loan l ON c.customer_id = l.customer_id
    WHERE l.status = 'Active'
    GROUP BY c.name, c.customer_id;
    """,
    dag=dag,
)
create_city_stats = SQLExecuteQueryOperator(
    task_id='create_city_stats',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    
    
    CREATE OR REPLACE TABLE city_account_stats AS
    SELECT c.city, COUNT(a.account_id) AS total_account, 
    SUM(a.balance) AS Total_Balance, 
    AVG(a.balance) AS avg_balance_per_acc
    FROM customers c 
    JOIN accounts a ON c.customer_id = a.customer_id
    WHERE a.status = 'Active'
    GROUP BY c.city
    ORDER BY Total_Balance DESC;
    """,
    dag=dag,
)
create_customer_account_summary = SQLExecuteQueryOperator(
    task_id='create_customer_account_summary',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    
    CREATE OR REPLACE TABLE customer_account_summary AS
    SELECT c.customer_id, c.name, COUNT(a.account_id) AS Total_acc
    FROM customers AS c
    JOIN accounts AS a ON c.customer_id = a.customer_id
    WHERE status = 'Active'
    GROUP BY c.customer_id, c.name
    ORDER BY total_acc DESC;
    """,
    dag=dag,
)
create_high_value_customers = SQLExecuteQueryOperator(
    task_id='create_high_value_customers',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    
    
    CREATE OR REPLACE TABLE high_value_customers AS
    SELECT c.customer_id, c.name,
    COALESCE(SUM(DISTINCT a.balance), 0) AS total_balance,
    COALESCE(SUM(DISTINCT l.amount), 0) AS total_loan_exposure,
    CASE WHEN COALESCE(SUM(DISTINCT a.balance), 0) > 100000
         OR COALESCE(SUM(DISTINCT l.amount), 0) > 30000
         THEN 'HIGH VALUE' ELSE 'REGULAR' END AS CUSTOMER_VALUES
    FROM customers c
    LEFT JOIN accounts AS a ON c.customer_id = a.customer_id AND a.status = 'Active'
    LEFT JOIN loan AS l ON c.customer_id = l.customer_id AND l.status = 'Active'
    GROUP BY c.customer_id, c.name
    ORDER BY total_balance DESC, total_loan_exposure DESC;
    """,
    dag=dag,
)

create_loan_type_summary = SQLExecuteQueryOperator(
    task_id='create_loan_type_summary',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    
    CREATE OR REPLACE TABLE loan_type_summary AS
    SELECT loan_type, SUM(amount) AS total_loan_amount,
    AVG(amount) AS average_loan_amount,
    COUNT(loan_id) AS total_loan
    FROM LOAN
    WHERE status = 'Active'
    GROUP BY loan_type
    ORDER BY total_loan_amount DESC;
    """,
    dag=dag,
)
create_etl_summary = SQLExecuteQueryOperator(
    task_id='create_etl_summary',
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
    USE DATABASE {SNOWFLAKE_DATABASE};
    USE SCHEMA {SNOWFLAKE_SCHEMA};
    
    CREATE OR REPLACE TABLE etl_execution_summary AS
    SELECT 
        'ETL Process Completed Successfully' AS status,
        CURRENT_TIMESTAMP() AS completed_at,
        (SELECT COUNT(*) FROM CUSTOMERS) AS customers_loaded,
        (SELECT COUNT(*) FROM ACCOUNTS) AS accounts_loaded,
        (SELECT COUNT(*) FROM TRANSACTIONS) AS transactions_loaded,
        (SELECT COUNT(*) FROM LOAN) AS loans_loaded,
        (SELECT COUNT(*) FROM customer_account) AS customer_account_records,
        (SELECT COUNT(*) FROM customer_transaction_summary) AS transaction_summary_records,
        (SELECT COUNT(*) FROM customer_risk_profile) AS risk_profile_records,
        (SELECT COUNT(*) FROM customer_segmentation) AS segmentation_records,
        (SELECT COUNT(*) FROM high_value_customers) AS high_value_customer_records,
        (SELECT COUNT(*) FROM risky_transactions) AS risky_transaction_records,
        (SELECT COUNT(*) FROM etl_null_report) AS etl_null_report_records;
    
    -- Display summary
    SELECT * FROM etl_execution_summary;
    """,
    dag=dag,
)
[check_customers_file, check_accounts_file, check_transactions_file, check_loans_file] >> list_s3_files

# After S3 validation, create Snowflake infrastructure
list_s3_files >> create_stage >> verify_stage >> create_staging_table >> truncate_tables

# Data loading in parallel (after truncation)
truncate_tables >> [load_customers, load_accounts, load_transactions, load_loans]

# Data quality check after all loads complete
[load_customers, load_accounts, load_transactions, load_loans] >> data_quality_check

# Transformation tasks - create base tables
data_quality_check >> create_customer_account

# Dependent transformations
create_customer_account >> create_transaction_summary
create_customer_account >> create_loan_balance_ratio

# Risk and analytical tables (parallel execution)
data_quality_check >> [
    create_risk_profile,
    create_loan_aging,
    create_customer_segmentation,
    create_risky_transactions,
    create_customer_loan_summary,
    create_city_stats,
    create_customer_account_summary,
    create_high_value_customers,
    create_loan_type_summary
]

# Final summary after all transformations
[
    create_transaction_summary,
    create_loan_balance_ratio,
    create_risk_profile,
    create_loan_aging,
    create_customer_segmentation,
    create_risky_transactions,
    create_customer_loan_summary,
    create_city_stats,
    create_customer_account_summary,
    create_high_value_customers,
    create_loan_type_summary
] >> create_etl_summary 






