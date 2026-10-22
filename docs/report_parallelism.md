# Efficient Data Handling through Data Parallelism

Status: implementation ready; full-stack execution and screenshots pending.

## 01. Load the

01_preview.csv and printed rows demonstrate validated Spark DataFrame loading.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 02. Implement a method

Round-robin repartition(n) balances row counts without concentrating common job categories on one partition. 01_partition_sizes_N.csv records actual partition sizes. Aggregation later shuffles by job; adaptive execution can coalesce shuffle partitions.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 03. Identify and calculate

02_parallel_job_balance.csv uses distributed groupBy(job).avg(balance); benchmark repeats partition grids without replicating the dataset. Compare actual medians and invariant aggregate outputs.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 04. Perform a parallel operation

Loan amount is absent: loan is yes/no. 02_top5_loan_holder_age_bands.csv ranks decade age bands by personal-loan-holder counts and shows average account balance separately. This is a labeled proxy, not a solution for unavailable loan amounts.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 05. Choose a classification

Logistic regression uses distributed optimization and offers interpretable coefficients. It handles the binary subscription target; the main modeling module supplies cross-validation.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 06. Partition the dataset

03_04_partitioned_model.json records the partitioned training model and held-out metrics. Approximately 80/20 deterministic split; training repartitions to the largest grid size. Explain shuffle overhead, skew, sparse feature size, and driver/executor memory limits.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 07. Implement resource monitoring

resource_samples.json samples driver Python CPU/RSS, local JVM child RSS, host CPU/memory each second. Spark UI/event logs supply executor CPU time, GC, memory/spill, shuffle, and task metrics. Local driver metrics do not measure remote executors; fill observations from actual runs.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 08. Manage multiple parallel tasks

06_scheduled_tasks.json comes from two bounded independent preprocessing actions launched with InheritableThread and separate FAIR scheduler pools/job groups. CV parallelism is independently limited to avoid oversubscription. Show Spark UI jobs/stages and discuss measured resource contention.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.
