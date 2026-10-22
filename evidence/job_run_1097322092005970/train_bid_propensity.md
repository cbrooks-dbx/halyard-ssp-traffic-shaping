# Task `train_bid_propensity`

- task run id: 600156339404322
- notebook: `src/notebooks/03_train_bid_propensity`
- started: 2026-10-20 18:14:04 UTC, duration 166s
- result: **SUCCESS**

Code cells and the output each one produced in this run, exported from the Jobs API.

# 03 - Bid-propensity model (MLflow + Unity Catalog model registry)

**Question the model answers:** if Halyard sends this bid request to this DSP, how likely is the DSP to bid?

**Why a model and not just historical bid rates:** traffic segments are long-tailed. Most
DSP x publisher x format x device x country x path x user-ID combinations see only a handful of calls a
week, so their observed bid rate is noise, and new publishers have no history at all. A gradient-boosted
model shares strength across segments (it learns "Tidewater only buys video and CTV" once, not per
publisher). We prove the lift below by comparing both approaches on the same out-of-time holdout.

**How the threshold is chosen:** not by accuracy, by the business guardrail. We pick the largest score
cut-off that keeps at least 99% of net revenue, measured on the holdout.

### Cell 2
```python
dbutils.widgets.text("catalog", "cmegdemos_catalog")
dbutils.widgets.text("schema", "ssp_traffic_shaping")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
fq = f"{catalog}.{schema}"
MODEL_NAME = f"{fq}.bid_propensity"
```
Output:
```text
[]
```

### Cell 3
```python
import numpy as np
import pandas as pd
import mlflow
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

CAT = ["dsp_id", "publisher_id", "vertical", "inventory_type", "ads_txt_status", "ad_format",
       "device_type", "os", "geo_country", "supply_path"]
NUM = ["floor_cpm", "user_id_present", "event_hour"]
SEGMENT_KEY = ["dsp_id", "publisher_id", "ad_format", "device_type", "geo_country", "supply_path", "user_id_present"]

dates = [r.event_date for r in spark.sql(f"SELECT DISTINCT event_date FROM {fq}.silver_dsp_calls ORDER BY 1").collect()]
cutoff = dates[-4]
print(f"{len(dates)} days of calls: {dates[0]} .. {dates[-1]}. Train < {cutoff} <= holdout")

cols = ", ".join(CAT + NUM + ["bid", "won", "net_revenue_usd", "event_date"])
train_sdf = spark.sql(f"SELECT {cols} FROM {fq}.silver_dsp_calls WHERE event_date < '{cutoff}'")
test_sdf = spark.sql(f"SELECT {cols} FROM {fq}.silver_dsp_calls WHERE event_date >= '{cutoff}'")
n_train_all = train_sdf.count()
train = train_sdf.sample(fraction=min(1.0, 1_500_000 / n_train_all), seed=42).toPandas()
test = test_sdf.toPandas()
for df in (train, test):
    df["user_id_present"] = df["user_id_present"].astype(int)
    df["bid"] = df["bid"].astype(int)
print(f"train rows {len(train):,} (sampled from {n_train_all:,}), holdout rows {len(test):,}, train bid rate {train.bid.mean():.4f}")
```
Output:
```text
{"ts": "2026-10-20 18:14:27.059", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzkzMjEzNjk1MjcwMzY4Ny9ydW5zLzEwOTczMjIwOTIwMDU5NzAQASABMiQwMWExNWE4NS00NGRiLTczOGUtOTM3ZC00NGM2NjNkNjRiYjQ6JGQwOTE4ZDFkLWZhMDItMzMxNS04YTk2LTYyMDliNTc1NmQwNkoMCO3e3tYGEMDVovcCUAJYAWABaIOh8tSRxaMNiAEA.", "context": {}}
{"ts": "2026-10-20 18:14:27.059", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzkzMjEzNjk1MjcwMzY4Ny9ydW5zLzEwOTczMjIwOTIwMDU5NzAQASABMiQwMWExNWE4NS00NGRiLTczOGUtOTM3ZC00NGM2NjNkNjRiYjQ6JGQwOTE4ZDFkLWZhMDItMzMxNS04YTk2LTYyMDliNTc1NmQwNkoMCO3e3tYGEMDVovcCUAJYAWABaIOh8tSRxaMNiAEA.", "context": {}}
{"ts": "2026-10-20 18:14:27.059", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzkzMjEzNjk1MjcwMzY4Ny9ydW5zLzEwOTczMjIwOTIwMDU5NzAQASABMiQwMWExNWE4NS00NGRiLTczOGUtOTM3ZC00NGM2NjNkNjRiYjQ6JGQwOTE4ZDFkLWZhMDItMzMxNS04YTk2LTYyMDliNTc1NmQwNkoMCO3e3tYGEMDVovcCUAJYAWABaIOh8tSRxaMNiAEA.", "context": {}}
{"ts": "2026-10-20 18:14:27.059", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzkzMjEzNjk1MjcwMzY4Ny9ydW5zLzEwOTczMjIwOTIwMDU5NzAQASABMiQwMWExNWE4NS00NGRiLTczOGUtOTM3ZC00NGM2NjNkNjRiYjQ6JGQwOTE4ZDFkLWZhMDItMzMxNS04YTk2LTYyMDliNTc1NmQwNkoMCO3e3tYGEMDVovcCUAJYAWABaIOh8tSRxaMNiAEA.", "context": {}}
{"ts": "2026-10-20 18:14:27.059", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzkzMjEzNjk1MjcwMzY4Ny9ydW5zLzEwOTczMjIwOTIwMDU5NzAQASABMiQwMWExNWE4NS00NGRiLTczOGUtOTM3ZC00NGM2NjNkNjRiYjQ6JGQwOTE4ZDFkLWZhMDItMzMxNS04YTk2LTYyMDliNTc1NmQwNkoMCO3e3tYGEMDVovcCUAJYAWABaIOh8tSRxaMNiAEA.", "context": {}}
{"ts": "2026-10-20 18:14:27.059", "level": "WARNING", "logger": "pyspark.sql.connect.logging", "msg": "Effective usage policy for this session is cct.Cipqb2JzLzkzMjEzNjk1MjcwMzY4Ny9ydW5zLzEwOTczMjIwOTIwMDU5NzAQASABMiQwMWExNWE4NS00NGRiLTczOGUtOTM3ZC00NGM2NjNkNjRiYjQ6JGQwOTE4ZDFkLWZhMDItMzMxNS04YTk2LTYyMDliNTc1NmQwNkoMCO3e3tYGEMDVovcCUAJYAWABaIOh8tSRxaMNiAEA.", "context": {}}
14 days of calls: 2026-10-06 .. 2026-10-19. Train < 2026-10-16 <= holdout
train rows 1,499,915 (sampled from 3,418,508), holdout rows 1,367,017, train bid rate 0.1023
```

### Cell 4
```python
def build_model():
    pre = ColumnTransformer([
        ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1, encoded_missing_value=-1), CAT),
        ("num", "passthrough", NUM),  # trees are invariant to monotone transforms, so floor_cpm needs no log
    ])
    clf = HistGradientBoostingClassifier(
        categorical_features=list(range(len(CAT))), max_iter=300, learning_rate=0.08,
        max_leaf_nodes=63, l2_regularization=1.0, early_stopping=True, validation_fraction=0.1, random_state=42)
    return Pipeline([("pre", pre), ("clf", clf)])


THROTTLE_SEND, REDUCE_SEND = 0.05, 0.5  # must match the policy in 04_score_throttle_rules


def guardrail_curve(df, score_col, label):
    """Segment-level policy curve for the deployed three-tier policy at each threshold t:
    score < t -> THROTTLE (send 5%), t <= score < 2t -> REDUCE (send 50%), else ALLOW.
    Revenue at risk is the conservative bound: every win in traffic not sent is assumed lost."""
    seg = df.groupby(SEGMENT_KEY, observed=True).agg(
        calls=("bid", "size"), bids=("bid", "sum"), rev=("net_revenue_usd", "sum"), score=(score_col, "mean")).reset_index()
    tot_calls, tot_rev, tot_bids = seg.calls.sum(), seg.rev.sum(), seg.bids.sum()
    out = []
    for t in np.unique(np.concatenate([np.linspace(0.0, 0.05, 201), np.linspace(0.05, 0.15, 41)])):
        cut = np.where(seg.score < t, 1 - THROTTLE_SEND, np.where(seg.score < 2 * t, 1 - REDUCE_SEND, 0.0))
        out.append((label, float(t), (seg.calls * cut).sum() / tot_calls, (seg.rev * cut).sum() / tot_rev,
                    (seg.bids * cut).sum() / tot_bids, int((cut > 0).sum())))
    return pd.DataFrame(out, columns=["method", "threshold", "calls_cut_pct", "revenue_at_risk_pct", "bids_lost_pct", "segments_shaped"])


def best_at_guardrail(curve, max_risk=0.01):
    ok = curve[curve.revenue_at_risk_pct <= max_risk]
    return ok.sort_values("calls_cut_pct").iloc[-1]
```
Output:
```text
[]
```

### Cell 5
```python
mlflow.set_registry_uri("databricks-uc")
me = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{me}/halyard-bid-propensity")

with mlflow.start_run(run_name="hgb_bid_propensity") as run:
    model = build_model()
    model.fit(train[CAT + NUM], train["bid"])
    test["p_bid"] = model.predict_proba(test[CAT + NUM])[:, 1]

    metrics = {
        "holdout_auc": roc_auc_score(test.bid, test.p_bid),
        "holdout_pr_auc": average_precision_score(test.bid, test.p_bid),
        "holdout_log_loss": log_loss(test.bid, test.p_bid),
        "holdout_brier": brier_score_loss(test.bid, test.p_bid),
        "holdout_bid_rate": test.bid.mean(),
        "holdout_mean_pred": test.p_bid.mean(),
    }

    # Baseline: each segment's observed bid rate in the training window. To be fair to the baseline, segments
    # with fewer than 30 training calls fall back to the DSP x format rate instead of a noisy (often 0) estimate.
    seg_hist = train.groupby(SEGMENT_KEY).bid.agg(hist_rate="mean", hist_n="size").reset_index()
    df_hist = train.groupby(["dsp_id", "ad_format"]).bid.mean().rename("dsp_fmt_rate").reset_index()
    test = test.merge(seg_hist, on=SEGMENT_KEY, how="left").merge(df_hist, on=["dsp_id", "ad_format"], how="left")
    test["hist_rate"] = np.where(test.hist_n.fillna(0) >= 30, test.hist_rate, test.dsp_fmt_rate)
    test["hist_rate"] = test.hist_rate.fillna(train.bid.mean())

    curve = pd.concat([guardrail_curve(test, "p_bid", "ml_model"), guardrail_curve(test, "hist_rate", "historical_rate")])
    # Guardrail comes from value_assumptions (Finance owns it). Tune with 10% headroom so the rule set still holds
    # the guardrail when applied to a different week of traffic than the holdout it was tuned on.
    min_retention = spark.sql(f"SELECT value FROM {fq}.value_assumptions WHERE parameter = 'min_revenue_retention'").first()[0]
    max_risk = (1 - min_retention) * 0.9
    ml_pt = best_at_guardrail(curve[curve.method == "ml_model"], max_risk)
    base_pt = best_at_guardrail(curve[curve.method == "historical_rate"], max_risk)
    metrics.update({
        "threshold": ml_pt.threshold,
        "ml_calls_cut_at_99pct_revenue": ml_pt.calls_cut_pct,
        "ml_revenue_at_risk": ml_pt.revenue_at_risk_pct,
        "baseline_calls_cut_at_99pct_revenue": base_pt.calls_cut_pct,
        "baseline_revenue_at_risk": base_pt.revenue_at_risk_pct,
    })
    mlflow.log_metrics(metrics)
    mlflow.log_params({"train_rows": len(train), "holdout_rows": len(test), "cutoff_date": str(cutoff),
                       "features": ",".join(CAT + NUM), "segment_key": ",".join(SEGMENT_KEY)})
    sample = train[CAT + NUM].head(50)
    mlflow.sklearn.log_model(model, "model", signature=infer_signature(sample, model.predict_proba(sample)[:, 1]),
                             input_example=sample.head(3), registered_model_name=MODEL_NAME)
    run_id = run.info.run_id

for k, v in metrics.items():
    print(f"{k:<40} {v:.4f}")
```
Output:
```text
/databricks/python/lib/python3.12/site-packages/mlflow/types/utils.py:440: UserWarning: Hint: Inferred schema contains integer column(s). Integer columns in Python cannot represent missing values. If your input data contains missing values at inference time, it will be encoded as floats and will cause a schema enforcement error. The best way to avoid this problem is to infer the model schema based on a realistic data sample (training dataset) that includes missing values. Alternatively, you can declare integer columns as doubles (float64) whenever these columns may have missing values. See `Handling Integers With Missing Values <https://www.mlflow.org/docs/latest/models.html#handling-integers-with-missing-values>`_ for more details.
  warnings.warn(
2026/10/20 18:16:29 WARNING mlflow.models.model: `artifact_path` is deprecated. Please use `name` instead.
🔗 View Logged Model at: https://fevm-cmegdemos.cloud.databricks.com/ml/experiments/2698138752958397/models/m-53404968006d4f16830942114511d1d4?o=7474656585748611
Registered model 'cmegdemos_catalog.ssp_traffic_shaping.bid_propensity' already exists. Creating a new version of this model...
🔗 Created version '3' of model 'cmegdemos_catalog.ssp_traffic_shaping.bid_propensity': https://fevm-cmegdemos.cloud.databricks.com/explore/data/models/cmegdemos_catalog/ssp_traffic_shaping/bid_propensity/version/3?o=7474656585748611
holdout_auc                              0.9071
holdout_pr_auc                           0.5606
holdout_log_loss                         0.2062
holdout_brier                            0.0623
holdout_bid_rate                         0.1023
holdout_mean_pred                        0.1026
threshold                                0.0057
ml_calls_cut_at_99pct_revenue            0.3548
ml_revenue_at_risk                       0.0088
baseline_calls_cut_at_99pct_revenue      0.3077
baseline_revenue_at_risk                 0.0090
```

### Calibration (predicted vs actual bid rate by decile)
Traffic shaping decisions use the probability itself, so calibration matters more than AUC.

### Cell 7
```python
test["decile"] = pd.qcut(test.p_bid.rank(method="first"), 10, labels=False) + 1
calib = test.groupby("decile").agg(calls=("bid", "size"), predicted=("p_bid", "mean"), actual=("bid", "mean")).round(4)
print(calib.to_string())
```
Output:
```text
calls  predicted  actual
decile                           
1       136702     0.0007  0.0004
2       136702     0.0020  0.0017
3       136701     0.0039  0.0033
4       136702     0.0070  0.0064
5       136702     0.0136  0.0119
6       136701     0.0300  0.0281
7       136702     0.0646  0.0657
8       136701     0.1273  0.1275
9       136702     0.2340  0.2349
10      136702     0.5426  0.5432
```

### Policy curve at the revenue guardrail: ML vs historical bid rate

### Cell 9
```python
print(f"ML model        : cut {ml_pt.calls_cut_pct:.1%} of outbound calls, {ml_pt.revenue_at_risk_pct:.2%} revenue at risk, threshold p_bid < {ml_pt.threshold:.4f}")
print(f"Historical rate : cut {base_pt.calls_cut_pct:.1%} of outbound calls, {base_pt.revenue_at_risk_pct:.2%} revenue at risk")
print(f"Lift from ML    : {ml_pt.calls_cut_pct - base_pt.calls_cut_pct:+.1%} more calls removed at the same revenue guardrail")
show = curve[curve.threshold.isin([0.005, 0.01, 0.02, 0.03, 0.05, 0.08, 0.1])].copy()
for c in ["calls_cut_pct", "revenue_at_risk_pct", "bids_lost_pct"]:
    show[c] = (show[c] * 100).round(2)
print(show.to_string(index=False))

spark.createDataFrame(curve).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{fq}.threshold_curve")
spark.sql(f"COMMENT ON TABLE {fq}.threshold_curve IS 'Holdout policy curve: share of outbound calls cut vs share of net revenue at risk, per scoring method and threshold'")
```
Output:
```text
ML model        : cut 35.5% of outbound calls, 0.88% revenue at risk, threshold p_bid < 0.0057
Historical rate : cut 30.8% of outbound calls, 0.90% revenue at risk
Lift from ML    : +4.7% more calls removed at the same revenue guardrail
         method  threshold  calls_cut_pct  revenue_at_risk_pct  bids_lost_pct  segments_shaped
       ml_model      0.005          33.13                 0.71           0.81            39573
       ml_model      0.010          43.34                 1.61           1.70            48118
       ml_model      0.020          51.72                 3.26           3.29            56201
       ml_model      0.030          56.65                 4.83           5.05            61817
       ml_model      0.050          63.32                 8.52           8.96            70053
       ml_model      0.080          69.84                14.44          15.51            78299
historical_rate      0.005          32.58                 1.00           1.11            24028
historical_rate      0.010          39.63                 1.64           1.81            29011
historical_rate      0.020          46.18                 2.91           3.08            29986
historical_rate      0.030          51.10                 4.58           4.81            39760
historical_rate      0.050          56.91                 7.76           8.17            46841
historical_rate      0.080          65.58                14.53          15.54            66155
```

### Cell 10
```python
client = MlflowClient()
version = max(int(v.version) for v in client.search_model_versions(f"name='{MODEL_NAME}'") if v.run_id == run_id)
client.set_registered_model_alias(MODEL_NAME, "champion", version)
client.set_model_version_tag(MODEL_NAME, str(version), "throttle_threshold", f"{ml_pt.threshold:.5f}")
client.set_model_version_tag(MODEL_NAME, str(version), "calls_cut_at_guardrail", f"{ml_pt.calls_cut_pct:.4f}")
client.update_registered_model(MODEL_NAME, description="Probability a DSP bids on a given outbound bid request. Drives Halyard traffic-shaping rules.")
print(f"Registered {MODEL_NAME} v{version} as @champion (MLflow run {run_id})")
```
Output:
```text
Registered cmegdemos_catalog.ssp_traffic_shaping.bid_propensity v3 as @champion (MLflow run d157d559f3114d699c2b8e1ba670716e)
```
