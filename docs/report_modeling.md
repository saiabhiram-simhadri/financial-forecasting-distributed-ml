# Predictive Modeling for Banking Trends

Status: implementation ready; full-stack execution and screenshots pending.

## 01. Load the bank.csv

spark_job.py train uses the shared CSV schema and validation.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 02. Perform basic exploratory

Printed schema, 01_preview.csv, and the separate EDA outputs provide initial exploration.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 03. Handle missing values

Numeric medians fit inside every CV training fold; blank/?/NA/null numerical values are treated as missing. Categorical missing becomes unknown; unseen categories are retained.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 04. Handle Outliers

02_preprocessing.json reports training-only IQR outlier counts. Fixed signed log(balance) and log1p count transforms reduce extreme influence while retaining legitimate high/negative balances. No test-set quantiles are learned.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 05. Convert categorical

StringIndexer(handleInvalid=keep) and OneHotEncoder encode all nine categorical predictors; the target is deterministically yes=1/no=0.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 06. Create a feature vector

VectorAssembler combines imputed transformed numerical columns, contacted_before, and encoded categoricals. Duration is excluded for pre-call prediction; --include-duration is a separately labeled retrospective variant.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 07. Choose a classification

Logistic regression is a scalable distributed baseline with interpretable conditional coefficients and regularization. It estimates subscription, not credit risk or market prices.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 08. Split the data

A deterministic seeded row hash gives approximately 80/20 train/test after exact duplicate removal; duplicate rows cannot cross boundaries. It is not a temporal split because reliable dates are unavailable.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 09. Train the model

The full pipeline is fit within each CV fold, then the selected pipeline refits on the whole training split. pipeline/ stores the fitted Spark PipelineModel for streaming.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 10. Evaluate the model

06_metrics.json reports ROC-AUC, PR-AUC, accuracy, positive-class precision/recall/F1, confusion counts, prevalence, and always-no baseline. Interpret PR-AUC and recall in light of imbalance; the fixed threshold is 0.5.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 11. Perform hyperparameter

ParamGridBuilder explores regParam=[0.01,0.1] and elasticNetParam=[0,0.5]; CrossValidator defaults to three folds, parallelism=1 and collectSubModels=False. Select using mean PR-AUC; test data remains untouched during selection.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.

## 12. Analyze the feature

08_coefficients.json maps logistic coefficients to assembled feature names and includes the intercept. Magnitudes are affected by units and transformed scales; associations do not imply causal effects.

EVIDENCE PENDING: Execute on the documented environment. Insert screenshots of this question's code and actual output, then add observed values and interpretation. No execution result is claimed by this template.
