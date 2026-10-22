# Submission checklist

Implementation and templates are provided; experiment outputs still require execution.

1. Use the real bank.csv and record source, citation/license, SHA256, validation counts and schema.
2. Run all Hadoop jobs and Hive queries. Capture each question's code/output, job IDs, rejection counters, HDFS listing and typed Hive schema.
3. Execute EDA, modeling and parallelism notebooks (save outputs), or run their scripts and capture screenshots. Match numbered artifacts to the report guides.
4. Include missing/outlier handling, class counts, CV scores, held-out confusion counts, ROC-AUC/PR-AUC/precision/recall/F1, majority baseline and coefficient interpretation. Label duration-enabled models retrospective.
5. Include partition sizes, repeated timings/medians, FAIR scheduling evidence, driver metrics and executor observations. State local[2] versus multi-node execution.
6. Capture streaming job means, both windows, prediction records, watermark progress and actual late-data observations. Compare emitted records with saved prediction counts.
7. In each annotated DOCX under submissions/, replace evidence-pending paragraphs with actual screenshots, values and interpretation. For Colab-eligible sections, an executed notebook is explicitly allowed instead.
8. Explain the missing-year and missing-loan-amount questions, cross-sectional balance limits and simulated stream semantics. These cannot be truthfully filled with invented numeric answers.
9. Include scripts, Hive queries, executed notebooks, small CSV/JSON summaries, PNG charts and required evidence. Keep raw data, models, checkpoints and event logs on the compute volume unless required by the dashboard.
10. Upload completed copies/notebooks and supporting files to the course dashboard manually. Nothing is submitted automatically.

Suggested screenshot labels: Hadoop-MR01..05; Hive-H01..13; EDA-01..12; ML-01..08; Streaming-01..04; Parallel-01..06. Capture matching code and real output for every original question.

Hadoop/Hive and streaming instructions explicitly request local execution. The execution guide now configures a GitHub Codespace as the development host for storage constraints; obtain instructor acceptance if required.

scripts/build_submission_templates.py regenerates annotated copies and Markdown question guides from the original five DOCX files plus docs/requirements.json. It overwrites generated DOCX copies and Markdown guides; preserve completed reports under new names first. Notebooks are maintained separately through the IDE notebook tools.
