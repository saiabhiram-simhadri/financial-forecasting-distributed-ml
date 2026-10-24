# Hadoop/Hive execution guide: GitHub Codespaces

This is the default execution route. You do **not** need a university lab, Google Cloud project, SSH host or preinstalled Hadoop/Hive.

Everything below runs in a GitHub Codespace in your browser. The Windows workstation is used only to upload source, read documents and download small final deliverables. Large runtime downloads and Hadoop/Hive jobs occur in Codespaces. The other four assignments were completed separately in notebooks.

**Verification status:** the setup/configuration code has lightweight offline tests; the complete Codespaces installation and Hadoop/Hive workflow have not been run from this workstation. Expect to check each success gate. A successful syntax check is not a verified cloud deployment.

Read [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) for what the tools do. This guide explains exactly what to run and what to inspect. For screenshots, copy the helpers in [How to take the pictures](#how-to-take-the-pictures-in-this-guide) once, then follow the question-by-question tables in each execution section.

## 1. Put the updated project on GitHub

1. Use a repository owned by your **personal GitHub account**.
2. Upload/commit the current project source, including:
   - .devcontainer/devcontainer.json
   - scripts/codespaces/ and scripts/run_hadoop.sh
   - src/, mapreduce/, hive/, tests/, docs/, submissions/
   - README.md, project_rubric.txt and the Hadoop/Hive DOCX
3. Ensure the latest changes are actually committed and pushed; creating a codespace from an older commit will not include this setup.
4. Do not upload .venv, .idea, .codespaces-state, data or artifacts. Their relevant paths are ignored by .gitignore.

Use PyCharm's Git UI or GitHub's upload interface. This guide does not push or publish your repository automatically.

**Gate:** on GitHub, you can open scripts/codespaces/setup.sh and the devcontainer configuration from the branch you intend to run.

**Pictures:** no assignment screenshot is needed for uploading the repository.

## 2. Create the codespace

1. Open the repository on GitHub.
2. Select **Code → Codespaces → create a codespace**. Use the advanced/new-with-options route if you need to select the machine.
3. Choose the smallest available machine satisfying **2 CPU cores, 8 GB RAM and 32 GB disk**.
4. Wait until the browser editor and terminal are available.
5. If a codespace already existed before the new devcontainer configuration, save/download existing work and use **Codespaces: Rebuild Container** to apply the supplied Python 3.11 environment.

The container's post-create command performs only host checks. It does not automatically download Hadoop/Hive or start services.

Check account quota before spending time installing. GitHub personal accounts have an included allowance; usage depends on your plan and remaining quota. A machine's disk size is different from the monthly GB-month storage allowance. Stopping ends compute usage but retains storage usage. Export necessary work before deleting the codespace. [Billing](https://docs.github.com/en/billing/concepts/product-billing/github-codespaces), [included usage](https://docs.github.com/en/codespaces/troubleshooting/troubleshooting-included-usage).

**Gate:** the terminal is at /workspaces/your-repository, has Python 3.11+, and the initial host check finishes. If your quota or permissions block Codespaces, this route is unavailable until that account issue is resolved.

**Assignment detail:** Codespaces is a remote development environment on one host. The Hadoop/Hive DOCX explicitly request local development execution/screenshots. State that your development host is a codespace and confirm course acceptance if remote execution is restricted. Do not describe this as multiple physical machines.

**Pictures:** no assignment screenshot is needed for creating the codespace.

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
| Python | Supplied Python 3.11 plus a persistent venv | Standard library only; no pip packages |

Exact Java release names, source URLs and checksums are recorded in .codespaces-state/tools/*/.bank-install.json. Apache archives are verified against published checksums. Installation archives are removed after successful extraction; installed runtimes are reused on a setup retry.

**Storage:** setup requires at least 10 GiB free before extraction. These are substantial remote downloads and can take time; do not repeat setup for every job. The chosen Hadoop/Hive versions are an isolated educational stack. Keep forwarded ports private and do not expose it as a public service.

**Gate:** the terminal prints SETUP_COMPLETE. This means installation/configuration finished, not that the services or assignments have already passed their runtime checks.

If setup fails:

- Inspect .codespaces-state/logs/setup.log and the first error.
- For a transient download error, rerun the same setup command; verified installed tools are reused.
- If free disk is insufficient, inspect df -h and the Codespaces storage display.
- If a tool directory exists without its installation marker, preserve/rename that incomplete directory before retrying. Do not delete the whole state directory or format HDFS to fix an installer.
- If extraction reports tarfile.data_filter is unavailable, rebuild using the supplied Python dev container.

No sudo, Docker-in-Docker, SSH daemon or systemd service setup is required by these scripts.

**Pictures:** optionally save the terminal showing SETUP_COMPLETE as SETUP-output. This is setup evidence, not an answer to an analysis question.

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


**Pictures:** save the input.json output above as INPUT-output if you need to document the dataset, row count and validation. The assignment-specific pictures start in Step 5.

### How to take the pictures in this guide

After a stage finishes, use its **Take pictures now** table before moving on.

For **every row**:

1. Copy the command in **Code picture** into the Codespaces terminal. It prints the exact source file and numbered lines. Press **Windows + Shift + S**, capture that printed code, and save/paste it as **ID-code**.
2. Copy the command in **Output picture**. Capture the actual result as **ID-output**.
3. Put those two pictures below the question named in that row, followed by 2–3 sentences using your actual values. If a row lists two code commands, take **ID-code-a** and **ID-code-b**.
4. Keep headers and all rows readable. Use additional pictures if necessary. A screenshot of STAGE_COMPLETE alone is not an analysis answer.

**Copy this helper block once into your Codespaces terminal after setup/prepare. Copy it again only if you open a new terminal.** It reads files and logs; it does not rerun analyses.

```bash
source scripts/codespaces/env.sh
bank_resume

cpic() {
  printf '\nCODE: %s — lines %s to %s\n' "$1" "$2" "$3"
  nl -ba "$1" | sed -n "${2},${3}p"
}

rpic() {
  printf '\nOUTPUT: %s/%s\n' "$BANK_WORK" "$1"
  case "$1" in
    logs/mapreduce-*.tsv)
      printf 'KEY\tSUM\tNONMISSING_COUNT\tMEAN\n'
      cat "$BANK_WORK/$1"
      ;;
    *) cat "$BANK_WORK/$1" ;;
  esac
}


lpic() {
  less -S -p "$2" "$BANK_WORK/$1"
}
```

- **cpic** displays source code; **rpic** displays a saved result table.
- **lpic** opens an existing log at a search phrase. Press **n** for the next match, arrows/Space to position the actual table, and **q** to return to the terminal. If the first match is SQL text, advance to the following result table; include its column headings. Use left/right arrows for a wide table.
- IDs below are picture names, not new jobs. Commands assume you are at the repository root and the corresponding stage succeeded.
- Source line numbers match this checkout. If you edited a source file, check the printed code before capturing it.
- Keep the same existing Codespace/current run. Updating this guide does not require a new codespace, setup or Hadoop rerun.

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

### Step 5 screenshot checklist: exactly what to put under each question

**First, take these four shared code pictures once:**

| Save picture as | Run this exact command |
|---|---|
| MAP-code | `cpic mapreduce/mapper.py 15 32` |
| REDUCE-code | `cpic mapreduce/reducer.py 8 25` |
| LOGIC-code | `cpic src/banklib.py 63 79` |
| AGE-code | `cpic src/banklib.py 15 18` |

You may reuse those actual pictures. For each MR answer below, paste **MAP-code + REDUCE-code + LOGIC-code**, then the row's output picture. For MR05 also paste **AGE-code**. You do not have to find separate Python programs for each task: LOGIC-code contains the five task branches.

**Then take the following output pictures:**

| Picture ID / original Hadoop question | Code picture to paste under this question | Output picture: run this exact command |
|---|---|---|
| HDFS — create directory and transfer bank.csv | `cpic scripts/run_hadoop.sh 6 18` | Run the HDFS-listing block immediately below this table |
| MR01 — average account balance for each job | MAP + REDUCE + LOGIC; balance_by_job is lines 65–66 | `rpic logs/mapreduce-balance_by_job.tsv` |
| MR02 — housing loan counts for each education category | MAP + REDUCE + LOGIC; housing_by_education is lines 67–68 | `rpic logs/mapreduce-housing_by_education.tsv` |
| MR03 — contacts by month and subscription status | MAP + REDUCE + LOGIC; contacts_by_month is lines 69–70 | `rpic logs/mapreduce-contacts_by_month.tsv` |
| MR04 — average duration for each previous campaign outcome | MAP + REDUCE + LOGIC; duration_by_poutcome is lines 71–72 | `rpic logs/mapreduce-duration_by_poutcome.tsv` |
| MR05 — summarized age/balance relationship | MAP + REDUCE + LOGIC + AGE; balance_by_age is lines 73–74 | `rpic logs/mapreduce-balance_by_age.tsv` |

**HDFS-output picture:** capture the terminal after this block. It must show the HDFS path and bank.csv with a nonzero size.

```bash
BANK_HDFS_RAW="/user/$(id -un)/banking/raw/$(basename "$BANK_WORK")"
printf 'HDFS input: %s\n' "$BANK_HDFS_RAW"
hdfs dfs -ls "$BANK_HDFS_RAW"
```

If Hadoop was stopped, restart services only, then repeat that listing:

```bash
bash scripts/codespaces/services.sh hadoop
```

**One additional execution-proof picture:** open private port **8088**, find the five banking-* applications for this run, and capture their names, IDs and final **SUCCEEDED** status. Save as **HADOOP-success**. If the old applications are no longer visible after a service restart, use `lpic logs/hadoop.log 'completed successfully'` and capture the actual job completion/counters instead. If that phrase is absent in your log, search for its actual success/application-ID lines; do not manufacture a success screen.

**What the output means:** all five files have KEY, SUM, NONMISSING_COUNT, MEAN. For MR02/MR03 use the count column, not the mean (which is 1 for count tasks). For MR01/MR04/MR05 use the mean and nonmissing count. MR04 is in seconds. MR05 uses <30, 30–60 and >60; group means alone are not a correlation coefficient.

**Where to paste:** the first six prompts in your copied **Data Analysis and Management using Hadoop & Hive** document: HDFS, MR01, MR02, MR03, MR04, MR05 in that order. Include both code and output under each prompt. Add the shared HADOOP-success picture after those answers. Keep all saved TSVs and logs as supporting files.

## 6. Create Hive tables and run every Hive query

```bash
bash scripts/codespaces/run.sh hive
```

This reuses/starts Hadoop, initializes the private Derby catalog if it does not exist, then starts HiveServer2 on 127.0.0.1:10000. The embedded catalog belongs to that one HiveServer2 process; there is no separate PostgreSQL or metastore daemon to install.

It executes:

1. A headerless CSV copy at banking/hive-raw/RUN_ID, followed by hive/01_tables.hql with that RAW_PATH automatically supplied. The original CSV and Hadoop input remain intact.
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


### Take pictures now — Hive questions

Use the cpic/rpic/lpic helpers above. **Each row is one original Hive question.** Save **ID-code** and **ID-output**, then paste them under the named question in the Hadoop/Hive DOCX.

For log results, capture the **table following the Hxx label**, including the listed column headings. Do not photograph only the SELECT statement. For the married/loan query, a readable sample plus the full saved log is appropriate supporting evidence; hundreds of rows do not need to fit in one picture.

| ID / question | Code picture: run | Output picture: run, then show |
|---|---|---|
| H01A — create banking_data | `cpic hive/01_tables.hql 3 4` | Run the read-only SHOW block below; show banking_data in the result |
| H01B — define typed client_info table | `cpic hive/01_tables.hql 23 28` | `lpic logs/hive-schema.log 'col_name'`; show column names and numeric/string types |
| H01C — load CSV into the table | `cpic hive/01_tables.hql 8 18` and `cpic hive/01_tables.hql 32 38` | `rpic hive-counts.tsv`; show raw and typed counts |
| H02A — total number of clients | `cpic hive/02_analysis.hql 2 3` | `lpic logs/hive-analysis.log 'H02 total clients'`; show the total_clients table |
| H02B — first 10 rows | `cpic hive/02_analysis.hql 4 4` | Same H02 log section; scroll to the 10 client rows and their column headings |
| H03 — married clients with personal loan | `cpic hive/02_analysis.hql 6 7` | `lpic logs/hive-analysis.log 'H03 married'`; show result headers and matching rows |
| H04 — 10 highest balances | `cpic hive/02_analysis.hql 9 11` | `lpic logs/hive-analysis.log 'H04 highest'`; show job, marital, balance and all 10 rows |
| H05 — average age by job | `cpic hive/02_analysis.hql 13 14` | `lpic logs/hive-analysis.log 'H05 average'`; show job, clients, avg_age |
| H06 — credit defaults by education | `cpic hive/02_analysis.hql 16 18` | `lpic logs/hive-analysis.log 'H06 credit'`; show education, defaulted_clients |
| H07 — five jobs with highest average balance | `cpic hive/02_analysis.hql 20 23` | `lpic logs/hive-analysis.log 'H07 top 5'`; show job, avg_balance, clients, subscription_pct |
| H08 — busiest month and success rate | `cpic hive/02_analysis.hql 25 28` | `lpic logs/hive-analysis.log 'H08 busiest'`; show month, contacts, success_pct |
| H09 — correlation between age and balance | `cpic hive/02_analysis.hql 30 31` | `lpic logs/hive-analysis.log 'H09 age'`; show age_balance_pearson_r |
| H10 — year-over-year trend | `cpic hive/02_analysis.hql 33 34` | `lpic logs/hive-analysis.log 'H10 year'`; show the missing-year explanation, not an invented annual table |
| H11 — education/balance anomaly candidates | `cpic hive/02_analysis.hql 36 54` | `lpic logs/hive-analysis.log 'H11 education'`; show education, avg_balance, descriptive_z_score and review_flag; use extra pictures for wide columns |
| H12 — previous outcome and current subscription | `cpic hive/02_analysis.hql 58 61` | `lpic logs/hive-analysis.log 'H12 previous'`; show poutcome, clients, subscription_pct |
| H13 — contact duration for subscribers/non-subscribers | `cpic hive/02_analysis.hql 63 65` | `lpic logs/hive-analysis.log 'H13 contact'`; show y, clients, avg_contact_seconds |

**H01A-output only:** this reads catalog metadata; it does not reload data or rerun the analysis jobs.

```bash
bank_hive --silent=true -e "SHOW DATABASES LIKE 'banking_data'; SHOW TABLES IN banking_data;"
```

If Hive is already stopped, use `bash scripts/codespaces/services.sh hive` before that SHOW command. Saved log pictures do not require a running Hive server.

## 7. Verify saved results

```bash
bash scripts/codespaces/run.sh verify
```

This reads completion markers, five MapReduce TSVs, Hive logs and raw/typed counts. It submits no jobs. Expect RUN_OUTPUT_CHECK_OK and STAGE_COMPLETE: verify. Screenshots and written answers remain manual.

## 8. Package and download

Complete the 22 prompts in [report_hadoop_hive.md](report_hadoop_hive.md). Save pictures in the current run:

```bash
source scripts/codespaces/env.sh
bank_resume
mkdir -p "$BANK_WORK/screenshots"
printf '%s\n' "$BANK_WORK/screenshots"
```

```bash
bash scripts/codespaces/run.sh package
```

Download the submission.zip path printed after PACKAGE_READY using Explorer → right-click → Download. It includes Hadoop/Hive source, assignment/guide, small results/logs and screenshots. It excludes installed tools, HDFS blocks and the catalog. Result files over 8 MiB are excluded. Inspect the ZIP before submitting. Download/rename an existing ZIP before packaging again.

Submit your four separate notebooks alongside this deliverable; they are not checked or collected here. Packaging needs no assignment screenshot; required evidence is listed in Steps 5 and 6.

## 9. Stop and resume

```bash
bash scripts/codespaces/services.sh stop
```

Then stop the Codespace in GitHub to stop compute usage. Reopen the same Codespace, enter the repository root, and run:

```bash
source scripts/codespaces/env.sh
bank_resume
printf 'Saved run: %s\n' "$BANK_WORK"
bash scripts/codespaces/services.sh status
```

Run only unfinished stages. Completed stages are skipped. Existing tools and HDFS/catalog/results remain under the same persistent paths. Do not repeat setup, reformat HDFS or delete the catalog to reconnect. Deleting the Codespace deletes its local data; download evidence first. No resume screenshot is needed.

YARN port 8088 can lose its application list on ResourceManager restart because recovery was not enabled. Capture browser evidence immediately after jobs finish. An empty list does not by itself mean saved results were deleted. Restart services without submitting jobs:

```bash
bash scripts/codespaces/services.sh hadoop
```

For genuinely new input only, use run.sh new then run.sh prepare to select a fresh result directory. Previous directories are retained.

## 10. Troubleshooting without starting over

- SLF4J multiple-bindings messages alone are warnings. Inspect the actual exception and completion/readiness line.
- Logger argument rejected: updated services.sh uses HADOOP_CLIENT_OPTS, not --hiveconf hive.root.logger.
- Protobuf Internal.checkNotNull: updated env.sh requests mapreduce.job.user.classpath.first=true for Hive. Do not delete Hadoop jars.
- 4522 instead of 4521 rows: update run.sh and 01_tables.hql together. They create a headerless HDFS copy under hive-raw/RUN_ID and set header skipping to zero for the existing staging table. Keep the expected-count check.
- Hive startup: inspect .codespaces-state/logs/hive/server.out and ready.log. Wait for HIVE_READY.
- Job failure: inspect the application's YARN Diagnostics and local container logs in .codespaces-state/yarn-logs. Find the first relevant exception before retrying.
- Partial Hadoop output: preserve it; the runner refuses to erase it. Diagnose before selecting a new run. Successful outputs are reused only with matching input.
- Low resources: inspect df -h and free -h; run stages sequentially. Do not reinstall as a troubleshooting shortcut.

The installer now needs only Java 8, Hadoop, Hive and standard-library Python. Existing Spark/Java 17 installations are left untouched in an old Codespace; they are not used by this workflow. No rebuild or reinstall is needed for your successful existing run.