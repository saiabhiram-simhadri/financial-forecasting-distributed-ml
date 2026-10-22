"""Train-only preprocessing, bounded cross-validation, evaluation, and persistence."""
from pathlib import Path
import time
from banklib import NUMERIC, CATEGORICAL, COLUMNS
from spark_common import export, save_json


def make_pipeline(include_duration=False):
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.ml.feature import SQLTransformer, Imputer, StringIndexer, OneHotEncoder, VectorAssembler
    numeric = [c for c in NUMERIC if include_duration or c != 'duration']
    # Fixed monotonic transforms reduce extreme-value influence without learning from test data.
    expressions = []
    for c in numeric:
        if c == 'balance':
            value = 'SIGN(balance) * LOG1P(ABS(balance))'
        elif c in ('campaign', 'previous', 'duration'):
            value = f'LOG1P({c})'
        elif c == 'pdays':
            value = 'CASE WHEN pdays = -1 THEN 0.0 ELSE LOG1P(pdays) END'
        else:
            value = c
        expressions.append(f'{value} AS {c}_robust')
    expressions.append('CASE WHEN pdays IS NULL THEN NULL WHEN pdays = -1 THEN 0.0 ELSE 1.0 END AS contacted_before')
    numeric_features = [f'{c}_robust' for c in numeric] + ['contacted_before']
    engineered = SQLTransformer(statement='SELECT *, ' + ', '.join(expressions) + ' FROM __THIS__')
    imputed = [c + '_imputed' for c in numeric_features]
    stages = [engineered, Imputer(strategy='median', inputCols=numeric_features, outputCols=imputed)]
    stages += [StringIndexer(inputCol=c, outputCol=c + '_idx', handleInvalid='keep',
                             stringOrderType='alphabetAsc') for c in CATEGORICAL]
    stages += [OneHotEncoder(inputCols=[c + '_idx' for c in CATEGORICAL],
                             outputCols=[c + '_ohe' for c in CATEGORICAL], handleInvalid='keep')]
    stages += [VectorAssembler(inputCols=imputed + [c + '_ohe' for c in CATEGORICAL],
                                outputCol='features', handleInvalid='error')]
    lr = LogisticRegression(maxIter=60, regParam=0.1, elasticNetParam=0.0, standardization=True)
    return Pipeline(stages=stages + [lr]), lr


def split(df, folds=3):
    from pyspark.sql import functions as F
    # Exact duplicates cannot cross split boundaries; all data columns participate in hashing.
    unique = df.dropDuplicates(COLUMNS)
    bucket = F.pmod(F.xxhash64(*COLUMNS, F.lit(2026)), F.lit(100))
    train = unique.filter(bucket < 80)
    test = unique.filter(bucket >= 80)
    train = train.withColumn('_fold', F.pmod(F.xxhash64(*COLUMNS, F.lit(91)), F.lit(folds)).cast('int'))
    return train, test


def evaluate(predictions):
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    counts = {(r['label'], r['prediction']): r['count'] for r in predictions.groupBy('label', 'prediction').count().collect()}
    tp, fp = counts.get((1.0, 1.0), 0), counts.get((0.0, 1.0), 0)
    tn, fn = counts.get((0.0, 0.0), 0), counts.get((1.0, 0.0), 0)
    n = tp + tn + fp + fn
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {'test_rows': n, 'tp': tp, 'fp': fp, 'tn': tn, 'fn': fn,
            'accuracy': (tp + tn) / n, 'positive_precision': precision, 'positive_recall': recall,
            'positive_f1': 2 * precision * recall / (precision + recall) if precision + recall else 0.0,
            'roc_auc': BinaryClassificationEvaluator(metricName='areaUnderROC').evaluate(predictions),
            'pr_auc': BinaryClassificationEvaluator(metricName='areaUnderPR').evaluate(predictions),
            'always_no_accuracy': (tn + fp) / n, 'positive_prevalence': (tp + fn) / n,
            'threshold': 0.5}


def run(df, output, include_duration=False, folds=3, cv_parallelism=1):
    from pyspark.sql import functions as F
    from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    root = Path(output)
    root.mkdir(parents=True, exist_ok=True)
    df.printSchema()
    export(df, root, '01_preview', 10)
    train, test = split(df, folds)
    train = train.cache()
    test = test.cache()
    try:
        train_counts = {r['label']: r['count'] for r in train.groupBy('label').count().collect()}
        test_counts = {r['label']: r['count'] for r in test.groupBy('label').count().collect()}
        if len(train_counts) != 2 or len(test_counts) != 2:
            raise ValueError('Both split sets need both labels; use more data (not a tiny preview).')
        fold_counts = train.groupBy('_fold', 'label').count().collect()
        if len(fold_counts) != folds * 2:
            raise ValueError('Every CV fold needs both labels; use fewer folds or more data.')
        numeric = [c for c in NUMERIC if include_duration or c != 'duration']
        for c in numeric:
            if not train.filter(F.col(c).isNotNull()).limit(1).count():
                raise ValueError(f'Training column {c} is entirely missing; median imputation cannot fit.')
        # Report outlier counts; do not delete valid extreme balances or use test quantiles.
        quantiles = train.approxQuantile(numeric, [0.25, 0.75], 0.01)
        outliers = {}
        for c, q in zip(numeric, quantiles):
            lo, hi = q[0] - 1.5 * (q[1] - q[0]), q[1] + 1.5 * (q[1] - q[0])
            outliers[c] = {'q1': q[0], 'q3': q[1], 'iqr_outliers': train.filter((F.col(c) < lo) | (F.col(c) > hi)).count()}
        save_json(root / '02_preprocessing.json', {'train_counts': train_counts, 'test_counts': test_counts,
                  'fold_counts': [r.asDict() for r in fold_counts], 'outliers_training_only': outliers,
                  'missing_strategy': 'numeric medians fit within each CV training fold; unknown category retained',
                  'outlier_strategy': 'signed log balance and log1p count/duration transforms; no valid extremes deleted',
                  'include_duration': include_duration, 'raw_rows': df.count(),
                  'unique_rows': sum(train_counts.values()) + sum(test_counts.values())})
        pipeline, lr = make_pipeline(include_duration)
        grid = ParamGridBuilder().addGrid(lr.regParam, [0.01, 0.1]).addGrid(lr.elasticNetParam, [0.0, 0.5]).build()
        evaluator = BinaryClassificationEvaluator(metricName='areaUnderPR')
        cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator,
                            numFolds=folds, foldCol='_fold', seed=42, parallelism=cv_parallelism,
                            collectSubModels=False)
        started = time.perf_counter()
        fitted = cv.fit(train)
        model = fitted.bestModel
        prediction = model.transform(test).cache()
        try:
            metrics = evaluate(prediction)
            metrics.update({'fit_seconds': time.perf_counter() - started,
                            'best_regParam': model.stages[-1].getRegParam(),
                            'best_elasticNetParam': model.stages[-1].getElasticNetParam(),
                            'cv_pr_auc': [{'regParam': p[lr.regParam], 'elasticNetParam': p[lr.elasticNetParam],
                                           'mean_pr_auc': score} for p, score in zip(grid, fitted.avgMetrics)]})
            save_json(root / '06_metrics.json', metrics)
            export(prediction.select('y', 'label', 'prediction', 'probability'), root, '06_predictions', 30)
            metadata = prediction.schema['features'].metadata['ml_attr']['attrs']
            names = {a['idx']: a['name'] for attrs in metadata.values() for a in attrs}
            coefficients = [{'feature': names.get(i, str(i)), 'coefficient': float(value)}
                            for i, value in enumerate(model.stages[-1].coefficients)]
            coefficients.sort(key=lambda row: abs(row['coefficient']), reverse=True)
            save_json(root / '08_coefficients.json', {'intercept': float(model.stages[-1].intercept),
                      'coefficients': coefficients,
                      'caveat': 'Conditional log odds on transformed features; associations, not causal effects.'})
            model.write().save(str(root / 'pipeline'))
            print(metrics)
        finally:
            prediction.unpersist()
        return model
    finally:
        train.unpersist()
        test.unpersist()
