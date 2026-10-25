-- RAW_PATH points to the headerless CSV directory uploaded by run.sh.
CREATE DATABASE IF NOT EXISTS banking_data;
USE banking_data;

-- Stage CSV fields as strings before casting into ORC.
CREATE EXTERNAL TABLE IF NOT EXISTS client_info_raw (
  age STRING, job STRING, marital STRING, education STRING, `default` STRING,
  balance STRING, housing STRING, loan STRING, contact STRING, day STRING,
  month STRING, duration STRING, campaign STRING, pdays STRING, previous STRING,
  poutcome STRING, y STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=';', 'quoteChar'='"')
STORED AS TEXTFILE
LOCATION '${hivevar:RAW_PATH}'
TBLPROPERTIES ('skip.header.line.count'='0');

-- Update the staging location for the selected run.
ALTER TABLE client_info_raw SET TBLPROPERTIES ('skip.header.line.count'='0');
ALTER TABLE client_info_raw SET LOCATION '${hivevar:RAW_PATH}';

CREATE TABLE IF NOT EXISTS client_info (
  age INT, job STRING, marital STRING, education STRING, `default` STRING,
  balance BIGINT, housing STRING, loan STRING, contact STRING, day INT,
  month STRING, duration INT, campaign INT, pdays INT, previous INT,
  poutcome STRING, y STRING
) STORED AS ORC;

-- Refresh the derived table without appending duplicate records.
INSERT OVERWRITE TABLE client_info
SELECT CAST(age AS INT), lower(trim(job)), lower(trim(marital)), lower(trim(education)),
       lower(trim(`default`)), CAST(balance AS BIGINT), lower(trim(housing)),
       lower(trim(loan)), lower(trim(contact)), CAST(day AS INT), lower(trim(month)),
       CAST(duration AS INT), CAST(campaign AS INT), CAST(pdays AS INT),
       CAST(previous AS INT), lower(trim(poutcome)), lower(trim(y))
FROM client_info_raw;

SELECT 'H01 ingestion verification' AS question;
SELECT COUNT(*) AS typed_rows, COUNT(age) AS nonnull_age, COUNT(balance) AS nonnull_balance
FROM client_info;
