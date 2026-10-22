# Execution guide: GitHub Codespaces

This is the default execution route. You do **not** need a university lab, Google Cloud project, SSH host or preinstalled Hadoop/Hive.

Everything below runs in a GitHub Codespace in your browser. The Windows workstation is used only to upload source, read documents and download small final deliverables. Large software downloads and all training occur in Codespaces.

**Verification status:** the setup/configuration code has lightweight offline tests; the complete Codespaces installation and Hadoop/Hive/Spark workflow have not been run from this workstation. Expect to check each success gate. A successful syntax check is not a verified cloud deployment.

Read [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) for what the tools do. This guide explains exactly what to run and what to inspect.

## 1. Put the updated project on GitHub

1. Use a repository owned by your **personal GitHub account**.
2. Upload/commit the current project source, including:
   - .devcontainer/devcontainer.json
   - scripts/codespaces/ and scripts/run_hadoop.sh
   - src/, mapreduce/, hive/, tests/, docs/, notebooks/, submissions/
   - requirements-remote.txt, README.md, project_rubric.txt and the five original DOCX files
3. Ensure the latest changes are actually committed and pushed; creating a codespace from an older commit will not include this setup.
4. Do not upload .venv, .idea, .codespaces-state, data or artifacts. Their relevant paths are ignored by .gitignore.

Use PyCharm's Git UI or GitHub's upload interface. This guide does not push or publish your repository automatically.

**Gate:** on GitHub, you can open scripts/codespaces/setup.sh and the devcontainer configuration from the branch you intend to run.

## 2. Create the codespace

1. Open the repository on GitHub.
2. Select **Code → Codespaces → create a codespace**. Use the advanced/new-with-options route if you need to select the machine.
3. Choose the smallest available machine satisfying **2 CPU cores, 8 GB RAM and 32 GB disk**.
4. Wait until the browser editor and terminal are available.
5. If a codespace already existed before the new devcontainer configuration, save/download existing work and use **Codespaces: Rebuild Container** to apply the supplied Python 3.11 environment.

The container's post-create command performs only host checks. It does not automatically download Hadoop/Hive or start services.

Check account quota before spending time installing. GitHub personal accounts have an included allowance; usage depends on your plan and remaining quota. A machine's disk size is different from the monthly GB-month storage allowance. Stopping ends compute usage but retains storage usage. Export necessary work before deleting the codespace. [Billing](https://docs.github.com/en/billing/concepts/product-billing/github-codespaces), [included usage](https://docs.github.com/en/codespaces/troubleshooting/troubleshooting-included-usage).

**Gate:** the terminal is at /workspaces/your-repository, has Python 3.11+, and the initial host check finishes. If your quota or permissions block Codespaces, this route is unavailable until that account issue is resolved.

**Assignment detail:** Codespaces is a remote development environment on one host. The Hadoop/Hive and streaming DOCX files explicitly request local development execution/screenshots. State that your development host is a codespace and confirm course acceptance if remote execution is restricted. Do not describe this as multiple physical machines.

## 3. Install the environment once

In the **Codespaces browser terminal**, at the repository root:

```bash
pwd
bash scripts/codespaces/setup.sh
```

The script checks that it is running in an x86_64 GitHub Codespace under /workspaces. It refuses to install on this Windows workstation.

It creates a persistent, ignored .codespaces-state directory containing:

| Component | Version/selection | Purpose |
|---|---|---|
| Hadoop | 3.3.6 | HDFS, YARN and Hadoop Streaming |
| Hive | 4.0.1 | HiveServer2, CSV staging, typed tables and queries |
| Java for Hadoop/Hive | Adoptium Java 8, current GA patch selected at installation | Compatible runtime for this pinned stack |
| Java for Spark | Adoptium Java 17, current GA patch selected at installation | Separate Spark runtime |
| Python environment | Supplied Python 3.11 dev container plus a persistent venv | Isolates project packages |
| PySpark | 3.5.7 | Spark EDA, ML and streaming |
| Pandas / Matplotlib / psutil | Pinned in requirements-remote.txt | Small summaries, plots and monitoring |

Exact Java release names, source URLs and checksums are recorded in .codespaces-state/tools/*/.bank-install.json. Apache archives are verified against published checksums. Installation archives are removed after successful extraction; installed runtimes are reused on a setup retry.

**Storage:** setup requires at least 10 GiB free before extraction. These are substantial remote downloads and can take time; do not repeat setup for every job. The chosen Hadoop/Hive versions are an isolated educational stack. Keep forwarded ports private and do not expose it as a public service.

**Gate:** the terminal prints SETUP_COMPLETE. This means installation/configuration finished, not that the services or assignments have already passed their runtime checks.

If setup fails:

- Inspect .codespaces-state/logs/setup.log and the first error.
- For a transient download/package-install error, rerun the same setup command; verified installed tools are reused.
- If free disk is insufficient, inspect df -h and the Codespaces storage display.
- If a tool directory exists without its installation marker, preserve/rename that incomplete directory before retrying. Do not delete the whole state directory or format HDFS to fix an installer.
- If extraction reports tarfile.data_filter is unavailable, rebuild using the supplied Python dev container.

No sudo, Docker-in-Docker, SSH daemon or systemd service setup is required by these scripts.

## 4. Prepare one run and validate the dataset

```bash
bash scripts/codespaces/run.sh prepare
```

This command:

1. Creates a new run directory if none is selected.
2. Downloads the original UCI bank.csv only if data/bank.csv does not exist.
3. Validates the canonical 17-column semicolon schema and both target classes.
4. Records row count, label counts and SHA256.
5. Runs lightweight tests. Spark integration remains skipped by default.

The UCI downloader supports the outer archive containing bank.zip, reads only bank.csv, and bounds download/extraction sizes. If downloading is blocked, upload the instructor's original bank.csv to data/bank.csv and repeat prepare.

**Gate:** PREPARED appears and validation/tests pass. Synthetic unit-test data is not used as the assignment dataset.

Find the selected run directory:

```bash
source scripts/codespaces/env.sh
bank_resume
printf '%s\n' "$BANK_WORK"
cat "$BANK_WORK/input.json"
```

All stage commands automatically restore this current run. You do not need to set JDBC URLs, installation paths or run IDs yourself.

**Do not run new in the middle of a successful run.** It selects a separate run directory; use it only when intentionally beginning a fresh execution.

## 5. Execute the five Hadoop jobs

```bash
bash scripts/codespaces/run.sh hadoop
```

The runner starts one NameNode, one DataNode, one ResourceManager and one NodeManager directly. This is real **single-host pseudo-distributed Hadoop**, with separate Java processes and actual HDFS/YARN execution.

The script formats HDFS only when the project NameNode is genuinely uninitialized and both storage directories are empty. It refuses to format an existing/inconsistent data directory.

After HDFS and YARN are ready, the runner:

1. Uploads the validated CSV to HDFS.
2. Runs balance_by_job, housing_by_education, contacts_by_month, duration_by_poutcome and balance_by_age.
3. Checks each Hadoop _SUCCESS marker.
4. Saves all reducer partitions as small TSV files.

Resource settings limit heaps and task concurrency. The two reducers can run sequentially; two reducers do not imply two worker machines.

**Gate:** HADOOP_READY, five successful jobs and STAGE_COMPLETE: hadoop. Inspect unexpected Banking/RejectedRows counters before accepting the result.

Open the **Ports** panel in the browser editor:

| Port | UI | Use |
|---|---|---|
| 9870 | HDFS NameNode | HDFS status/files and single DataNode evidence |
| 8088 | YARN ResourceManager | Applications, job IDs and task/resource evidence |

Keep visibility **Private**. Open the forwarded UI from the Ports panel; localhost on your Windows laptop is not the codespace's localhost.

**Capture before proceeding to Spark:** HDFS listing, Hadoop job/counter evidence and the code/output under each of the five Hadoop questions.

Inspect results without rerunning jobs:

```bash
source scripts/codespaces/env.sh
bank_resume
ls "$BANK_WORK/logs"
cat "$BANK_WORK/logs/mapreduce-balance_by_job.tsv"
```

Reducer columns are key, sum, nonmissing count, mean. Use count for count questions and mean for mean questions. The launcher can reuse completed jobs when explicitly invoked by this runner in resume mode; it checks existing HDFS input against the local file's SHA256.

## 6. Create Hive tables and run every Hive query

```bash
bash scripts/codespaces/run.sh hive
```

This reuses/starts Hadoop, initializes the private Derby catalog if it does not exist, then starts HiveServer2 on 127.0.0.1:10000. The embedded catalog belongs to that one HiveServer2 process; there is no separate PostgreSQL or metastore daemon to install.

It executes:

1. hive/01_tables.hql with the correct HDFS RAW_PATH automatically supplied.
2. DESCRIBE banking_data.client_info.
3. A raw/typed row-count comparison with the validated CSV.
4. All questions in hive/02_analysis.hql.

**What the selected 01_tables.hql does:** it creates banking_data, registers a string-typed external CSV table at the uploaded HDFS directory, then casts/normalizes records into typed ORC client_info. INSERT OVERWRITE refreshes this project's derived table. A separate manual LOAD DATA step is unnecessary for this implementation.

Hive runs through its MapReduce engine in this small educational environment; no Tez installation is added. The source dataset is intentionally small.

**Gate:** HIVE_READY, HIVE_COUNTS_OK with matching raw/typed/input counts, and STAGE_COMPLETE: hive.

**Capture now:** table schema, ingestion/count verification and each H01–H13 query with its actual result. Use [report_hadoop_hive.md](report_hadoop_hive.md).

Inspect logs:

```bash
source scripts/codespaces/env.sh
bank_resume
cat "$BANK_WORK/hive-counts.tsv"
less "$BANK_WORK/logs/hive-analysis.log"
```

If startup fails, inspect .codespaces-state/logs/hive/server.out, ready.log and schema-init.log/schema-info.log. Do not run another schema tool/metastore process against an active embedded catalog, and do not delete a completed catalog to fix an unrelated SQL error.

H10 documents that year-over-year counts cannot be computed without a year field. H11 is descriptive education-balance screening, not proof of fraud.

## 7. Execute Spark EDA

```bash
bash scripts/codespaces/run.sh eda
```

The runner first stops this project's Hadoop/Hive services to free memory. Persistent HDFS blocks and catalog data remain intact.

Spark uses Java 17, local[2], a 1 GiB driver heap and host-local files. Hadoop/Hive classpath settings are isolated from Spark, and fs.defaultFS is explicitly file:/// for this phase. The age-UDF module is distributed to Python workers.

**Gate:** STAGE_COMPLETE: eda, numbered CSV/JSON summaries and both PNG charts.

```bash
source scripts/codespaces/env.sh
bank_resume
ls "$BANK_WORK/eda"
cat "$BANK_WORK/eda/01_quality.json"
```

Open 07_job_counts.png and 10_credit_default_counts.png in the browser editor and capture them with the relevant source/table output. Match all transformations and results to [report_eda.md](report_eda.md).

Pandas conversion is bounded to samples/small aggregates. This does not copy the entire dataset to local Windows storage.

## 8. Train, tune and save the model

```bash
bash scripts/codespaces/run.sh train
```

This runs the complete preprocessing/logistic-regression pipeline with four parameter combinations, three CV folds and concurrency one, then evaluates the selected model on held-out test records.

**Gate:** STAGE_COMPLETE: train, 06_metrics.json, 08_coefficients.json and the train/pipeline directory.

```bash
source scripts/codespaces/env.sh
bank_resume
cat "$BANK_WORK/train/02_preprocessing.json"
cat "$BANK_WORK/train/06_metrics.json"
cat "$BANK_WORK/train/08_coefficients.json"
```

Use [report_modeling.md](report_modeling.md). Include missing/outlier handling, class/fold counts, CV scores, test metrics, majority baseline and coefficient interpretation.

The saved pipeline includes its fitted imputer, encoders and classifier. Streaming reuses it. **Do not retrain to inspect predictions.**

Duration is excluded by default because it is unavailable before the last call finishes. An optional all-predictor retrospective model must be clearly labeled; it is not necessary for the default pre-call modeling route. Do not promise a particular accuracy or execution time.

## 9. Run data-parallelism experiments

```bash
bash scripts/codespaces/run.sh parallel
```

This compares 1, 2 and 4 partitions, repeats each measurement three times, checks aggregate consistency, trains one partitioned model and schedules two independent preprocessing actions.

**Gate:** STAGE_COMPLETE: parallel and the following outputs:

- 01_partition_sizes_*.csv
- 05_benchmark.json
- 03_04_partitioned_model.json
- 06_scheduled_tasks.json
- resource_samples.json

```bash
source scripts/codespaces/env.sh
bank_resume
cat "$BANK_WORK/parallel/05_benchmark.json"
```

Use [report_parallelism.md](report_parallelism.md). More partitions may be slower for this small dataset. Report what was measured.

Driver/local-process observations come from psutil. Spark UI/event logs provide task/executor/shuffle/GC observations. They are complementary measurements, not interchangeable.

Loan amounts are absent; the age-band loan-holder-count table is a clearly labeled proxy, not a loan-amount calculation.

## 10. Run streaming with the saved model

```bash
bash scripts/codespaces/run.sh stream
```

The runner creates up to 300 replay events in 20-row JSON chunks, then processes the available files with one file per microbatch. Four queries produce:

| Query | Output |
|---|---|
| predictions | Scored records in Parquet, including unique event IDs and probabilities |
| job_means | Console updates for cumulative counts, balance means and contact-duration means |
| window_10s | Console updates for 10-second event-time windows |
| window_60s | Console updates for 1-minute event-time windows |

There are separate durable checkpoints. Timestamps/arrival disorder are simulated; these are marketing/contact records, not actual payment transactions.

**Gate:** STAGE_COMPLETE: stream, stream/predictions, stream/query_progress.jsonl and query checkpoint directories.

Use [report_streaming.md](report_streaming.md). Capture code, job means and both window sizes from logs. The next step prints sample prediction records and verifies counts.

Watermark behavior depends on processed event-time progress, not elapsed wall time. Some last windows can remain open without later event times. A late record can still have a prediction while being excluded from a window.

The finite run processes a file stream in successive batches; it does not claim connection to a live banking service. A two-terminal live-arrival variant is in section 15 if specifically needed.

## 11. Verify counts and summarize measured results

```bash
bash scripts/codespaces/run.sh verify
```

This checks:

- Required result files exist and are nonempty.
- Hive raw/typed and EDA counts match the validated CSV.
- The replay event count matches its bounded configuration.
- Every emitted event has exactly one unique, non-null prediction.
- Both window queries have recorded progress.
- Model, timing and driver resource observations are summarized.

**Gate:** RUN_OUTPUT_CHECK_OK and STAGE_COMPLETE: verify.

```bash
source scripts/codespaces/env.sh
bank_resume
cat "$BANK_WORK/run-check.json"
```

A successful output check is not a finished course submission. You still need code/output screenshots and measured written answers.

query_progress.jsonl samples lastProgress; very fast batches might not all be retained. Report observed watermarks and actual recorded drop metrics. If the recorded drop count is zero, do not invent another value; use the optional live run if explicit observed late drops are required.

## 12. Open saved Spark applications and complete evidence

During an active Spark job, open private forwarded port **4040** for the live UI. It may move to another port if occupied; check startup logs.

After the jobs finish:

```bash
bash scripts/codespaces/services.sh history
```

**Gate:** HISTORY_READY. Open private forwarded port **18080**, select the EDA/training/parallelism/streaming application, and inspect jobs, stages, executors and available metrics.

Event logs are retained in .codespaces-state/events. The History Server avoids rerunning completed jobs solely to take UI screenshots. Save the application IDs and actual local[2] execution details.

Fill the five annotated DOCX copies under submissions/ using their question guides. Save completed versions under different filenames. Place screenshots in the current run:

```bash
source scripts/codespaces/env.sh
bank_resume
mkdir -p "$BANK_WORK/screenshots"
printf '%s\n' "$BANK_WORK/screenshots"
```

Upload screenshots through the browser editor or download the small report/results files and complete Word documents on Windows. The assignment's code/output evidence must match actual runs.

Never regenerate templates over completed answers. An unexecuted notebook or an evidence-pending paragraph is not finished evidence.

## 13. Package and download small deliverables

After verification, and after saving completed documents/screenshots:

```bash
bash scripts/codespaces/run.sh package
```

**Gate:** PACKAGE_READY prints the absolute submission.zip path. Find it in the Codespaces file explorer, right-click and choose **Download**.

The package includes source/configuration, original/annotated documents, reference notebooks, small result tables/plots, run logs, screenshots and installed-version metadata. It excludes the Python environment, Hadoop/Hive/Java installations, HDFS blocks, catalog database, replay files, model directories and streaming checkpoints.

Files over 8 MiB are not included by the result collector. Inspect the final ZIP and include any additional course-required large artifacts separately if needed.

The packaging command refuses to overwrite an existing ZIP. If you complete more screenshots afterward, rename/download the previous ZIP first, then create a fresh snapshot.

**Final completion gate:** all 68 mapped prompts have the appropriate actual output/interpretation or a truthful missing-data explanation. Submit the completed reports/notebooks and supporting files manually to the dashboard.

## 14. Stop, reconnect and recover

### Stop without deleting data

```bash
bash scripts/codespaces/services.sh stop
```

Then open GitHub's Codespaces page and choose **Stop codespace**. Stopping services alone does not stop Codespaces compute billing/allowance usage.

Persistent project files remain under /workspaces. GitHub preserves that directory across stop/start and container rebuilds, but deleting the codespace deletes its local runtime/results. Export and/or push necessary work before deletion. [Persistence](https://docs.github.com/en/codespaces/developing-in-a-codespace/rebuilding-the-container-in-a-codespace).

### Resume the same run

Reopen the same codespace and terminal at the repository root:

```bash
source scripts/codespaces/env.sh
bank_resume
printf '%s\n' "$BANK_WORK"
bash scripts/codespaces/services.sh status
```

Then run only the unfinished stage. The runner skips marked completed stages and logs new attempts separately within the same stage log. Daemons are restarted when needed, using persistent HDFS and catalog data.

Do not reformat HDFS, reinstall all software or retrain successful models to resume.

### Start a deliberately new run

```bash
bash scripts/codespaces/run.sh new
bash scripts/codespaces/run.sh prepare
```

This changes the current-run pointer but retains old run directories. Use this when the input dataset changed or when intentionally beginning a clean execution. Later stages refer to the newly selected run, so do not mix its paths with old evidence.

### Retry a partially failed Spark stage

The runner refuses to overwrite an existing incomplete stage directory. Preserve just that failed directory before repeating the stage, for example:

```bash
source scripts/codespaces/env.sh
bank_resume
mv "$BANK_WORK/eda" "$BANK_WORK/eda-failed-$(date -u +%Y%m%dT%H%M%SZ)"
bash scripts/codespaces/run.sh eda
```

Only use that command when EDA failed and its directory exists. Apply the same pattern to a failed train/parallel/stream directory. A failed stream can reuse a successfully published replay-input and the saved model.

### Common failures

| Symptom | Inspect / action |
|---|---|
| Setup says not Codespaces or wrong architecture | Use the supplied dev container in an x86_64 codespace; never bypass the guard on Windows |
| Old/missing setup scripts | Commit/push the current branch; update the codespace checkout |
| Insufficient quota or unable to create codespace | Check personal-account Codespaces eligibility, remaining usage and machine availability |
| Setup download/checksum error | Setup log and network; retry setup, keeping verified tools |
| NameNode/DataNode not ready | .codespaces-state/logs/hadoop; verify disk, host settings and persistent VERSION; never force-format existing data |
| YARN has no RUNNING node | NodeManager logs, free RAM/disk and generated 3072 MiB resource settings |
| Hadoop task memory failure | Inspect YARN/container logs; do not launch parallel manual jobs; consider a larger codespace if needed |
| Partial Hadoop output exists | Preserve it; the runner does not delete HDFS output. Use a new run for a clean Hadoop attempt; completed job outputs are reused only when safe |
| Hive schema initialization failed | schema-init.log/schema-info.log; keep the existing catalog and diagnose the first error |
| Derby database already booted/locked | Stop this project's HiveServer2 before schema tools; do not run a second catalog owner |
| Hive SQL or row-count mismatch | hive-ingestion.log, hive-counts.stderr and the canonical CSV; the runner automatically supplies RAW_PATH |
| Python worker import or Java gateway error | Check env.sh/runtime installation and first error; keep Hadoop Java 8 and Spark Java 17 separate |
| Stage output already exists | Inspect a completed stage; preserve/rename only an incomplete stage before retrying |
| Dataset changed during a run | Use new then prepare; do not apply old model/evidence to changed input silently |
| Streaming cannot load model | Complete train first and use its whole pipeline directory |
| Zero recorded late drops | Inspect watermark progress; use optional live arrival if necessary, and report the actual observation |
| History UI has no application | Check event logs and .codespaces-state/logs/history.out; verify the Spark job completed |
| Port unavailable after switching to Spark | Hadoop/Hive are intentionally stopped to free RAM; view their saved logs or restart services explicitly |
| Codespace stopped/rebuilt | Reopen, source env.sh, bank_resume; persistent state is under /workspaces |
| Memory/disk repeatedly exhausted | Stop project services; inspect actual usage; use a larger Codespace if quota permits. Success on the smallest machine is not guaranteed |

The setup uses single-user sandbox authentication/resource settings, not production security or capacity settings. Keep forwarded ports private.

## 15. Optional variants—do not repeat work unnecessarily

### Run the normal execution stages in one command

After setup:

```bash
bash scripts/codespaces/run.sh all
```

This runs prepare, Hadoop, Hive, EDA, training, parallelism, streaming and verification in sequence. It does **not** complete screenshots, written answers or dashboard submission. Stage-by-stage execution is preferable for your first run because you can inspect each checkpoint and capture Hadoop/Hive UIs before Spark stops them.

### Live arrival with two terminals

Use only if the finite demonstration is insufficient for the requested evidence.

Terminal A:

```bash
source scripts/codespaces/env.sh
bank_resume
bash scripts/codespaces/services.sh stop
mkdir "$BANK_WORK/live-input"
bank_log stream-live bank_spark src/spark_job.py stream \
  --input "$BANK_WORK/live-input" --model "$BANK_WORK/train/pipeline" \
  --output "$BANK_WORK/stream-live" --seconds 240 --partitions 2
```

After query startup is visible, Terminal B:

```bash
source scripts/codespaces/env.sh
bank_resume
bank_log replay-live "$BANK_PY" -B scripts/replay.py \
  --input "$BANK_DATA" --output "$BANK_WORK/live-input" \
  --rows 300 --chunk-size 20 --interval 5
```

Use set -euo pipefail in manually sourced terminals if you want the same failure handling as the runner. A time limit does not guarantee all events were consumed on a slow host: check the actual live prediction count. The standard verify command checks the finite stream, so verify a live run's own paths separately.

### Colab or reference notebooks

The default route uses scripts plus screenshots to avoid duplicate training. The three supplied notebooks remain available for EDA/modeling/parallelism if required. Set their source/data/output paths in the remote runtime, prepare dependencies there, run cells in order, and save actual outputs and observations. Hadoop/Hive and streaming retain their own environment/evidence requirements.

### Multi-machine execution

This setup is one codespace host. Separate HDFS/YARN processes and Spark partitions do not constitute multiple machines. If the instructor mandates multiple worker hosts, this configuration cannot supply that evidence; a separate multi-node environment is required.

## 16. Data limitations to include in the final answers

- bank.csv has no year/contact-date field, so year-over-year contacts cannot be computed truthfully.
- loan is a yes/no field; no loan-amount/principal field exists.
- balance is an account-balance attribute, not a longitudinal annual series or loan amount.
- duration is contact duration and is excluded from pre-call prediction by default.
- Replay timestamps are simulated; this is not a real payment feed.

Use [SUBMISSION.md](SUBMISSION.md) and [requirements.json](requirements.json) to match results to the original questions.

Implementation files: [setup.sh](../scripts/codespaces/setup.sh), [setup_runtime.py](../scripts/codespaces/setup_runtime.py), [configure.py](../scripts/codespaces/configure.py), [env.sh](../scripts/codespaces/env.sh), [services.sh](../scripts/codespaces/services.sh), [run.sh](../scripts/codespaces/run.sh), [inspect_run.py](../scripts/codespaces/inspect_run.py).

Compatibility references: [Hive releases and Hadoop 3.3.6 compatibility](https://hive.apache.org/general/downloads/), [Hive Java requirements](https://github.com/apache/hive#requirements), [Hadoop single-node setup](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/SingleCluster.html), [Spark 3.5.7](https://spark.apache.org/docs/3.5.7/).
