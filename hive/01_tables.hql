-- Run with beeline --hivevar RAW_PATH=/user/you/banking/raw/RUN_ID -f hive/01_tables.hql
-- RAW_PATH must contain only the semicolon-delimited bank.csv uploaded by run_hadoop.sh.
CREATE DATABASE IF NOT EXISTS banking_data;
USE banking_data;

-- OpenCSVSerde is used only for string staging. Quoted numeric fields must be cast
-- into a separate typed table; declaring CSV numeric types directly is not portable.
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
TBLPROPERTIES ('skip.header.line.count'='1');

-- Repoint only the external staging metadata on subsequent runs; raw files are preserved.
ALTER TABLE client_info_raw SET LOCATION '${hivevar:RAW_PATH}';

CREATE TABLE IF NOT EXISTS client_info (
  age INT, job STRING, marital STRING, education STRING, `default` STRING,
  balance BIGINT, housing STRING, loan STRING, contact STRING, day INT,
  month STRING, duration INT, campaign INT, pdays INT, previous INT,
  poutcome STRING, y STRING
) STORED AS ORC;

-- Run validate_data.py first. OVERWRITE intentionally refreshes this project's
-- derived table from the selected input, rather than appending duplicate rows.
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
