import argparse
import json
import math
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.calibration import CalibratedClassifierCV
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    f1_score,
    precision_recall_fscore_support,
    mean_absolute_error,
    mean_squared_error,
    median_absolute_error,
    r2_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


DEFAULT_OUTPUT_DIR = Path("output/reports")
DEFAULT_INPUT_CSV = DEFAULT_OUTPUT_DIR / "inventory_state_v2_latest.csv"
ROUTE_ENGINEERED_FEATURE_MARKERS = (
    "dac_spd_",
    "dep_tod_bin",
    "search_tod_bin",
    "dtd_bucket",
    "fare_ladder_",
    "dtd_x_",
)


def _timestamp_token(ts_mode: str = "local") -> str:
    now = datetime.now() if ts_mode == "local" else datetime.utcnow()
    token = now.strftime("%Y%m%d_%H%M%S_%f")
    if ts_mode == "utc":
        return f"{token}_UTC"
    return token


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Train first baseline models on inventory_state_v2 (classification: price move, "
            "regression: next-fare delta) with time-based split."
        )
    )
    p.add_argument("--input-csv", default=str(DEFAULT_INPUT_CSV))
    p.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    p.add_argument("--timestamp-tz", choices=["local", "utc"], default="local")
    p.add_argument("--airline")
    p.add_argument(
        "--route-group",
        help="Comma-separated route keys (ORG-DST). Enables per-route threshold tuning summaries.",
    )
    p.add_argument("--origin")
    p.add_argument("--destination")
    p.add_argument("--cabin")
    p.add_argument("--adt", type=int)
    p.add_argument("--chd", type=int)
    p.add_argument("--inf", type=int)
    p.add_argument("--test-fraction", type=float, default=0.2)
    p.add_argument("--max-rows", type=int)
    p.add_argument("--random-state", type=int, default=42)
    p.add_argument(
        "--stage-a-calibration",
        choices=["none", "sigmoid", "isotonic"],
        default="none",
        help="Optional probability calibration method for two-stage Stage A classifier",
    )
    p.add_argument(
        "--stage-a-calibration-cv",
        type=int,
        default=3,
        help="Cross-validation folds for Stage A probability calibration",
    )
    p.add_argument(
        "--min-move-delta",
        type=float,
        default=0.0,
        help="Treat |y_next_search_lowest_fare_delta| below this threshold as no-move in the two-stage baseline",
    )
    p.add_argument(
        "--min-stage-b-moves",
        type=int,
        default=20,
        help="Minimum moved training rows required to fit Stage B (two-stage baseline)",
    )
    p.add_argument(
        "--stage-b-model",
        choices=["ridge", "rf"],
        default="ridge",
        help="Stage B regressor for moved rows in two-stage baseline",
    )
    p.add_argument(
        "--min-test-moves",
        type=int,
        default=1,
        help="Minimum move rows required in test split for two-stage evaluation (route viability gate)",
    )
    p.add_argument(
        "--route-rolling-folds",
        type=int,
        default=0,
        help="Optional TimeSeriesSplit folds for route-specific rolling two-stage evaluation",
    )
    p.add_argument(
        "--rolling-viability-rule",
        choices=["beats_zero_folds", "mean_rmse"],
        default="beats_zero_folds",
        help=(
            "Rule for route-level rolling_viable_rmse flag: beats_zero_folds=minimum fold count beats zero "
            "baseline; mean_rmse=rolling mean best RMSE < rolling mean zero-baseline RMSE"
        ),
    )
    p.add_argument(
        "--rolling-viability-min-beat-folds",
        type=int,
        default=2,
        help="Minimum rolling folds beating zero baseline (RMSE) when --rolling-viability-rule=beats_zero_folds",
    )
    p.add_argument(
        "--feature-ablation",
        choices=["none", "drop_route_engineered"],
        default="none",
        help="Optional feature ablation for controlled A/B experiments",
    )
    return p.parse_args()


def _load_dataset(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    df = pd.read_csv(path)
    if "observed_at_utc" in df.columns:
        df["observed_at_utc"] = pd.to_datetime(df["observed_at_utc"], errors="coerce")
    if "departure" in df.columns:
        df["departure"] = pd.to_datetime(df["departure"], errors="coerce")
    if "arrival" in df.columns:
        df["arrival"] = pd.to_datetime(df["arrival"], errors="coerce")
    return df


def _apply_filters(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    out = df.copy()
    if args.airline:
        wanted = {x.strip().upper() for x in str(args.airline).split(",") if x.strip()}
        if "airline" in out.columns:
            out = out[out["airline"].astype(str).str.upper().isin(wanted)]
    if getattr(args, "route_group", None):
        wanted_routes = {
            x.strip().upper().replace(" ", "")
            for x in str(args.route_group).split(",")
            if x.strip()
        }
        if "route_key" in out.columns:
            out = out[out["route_key"].astype(str).str.upper().isin(wanted_routes)]
    if args.origin and "origin" in out.columns:
        out = out[out["origin"].astype(str).str.upper() == args.origin.upper()]
    if args.destination and "destination" in out.columns:
        out = out[out["destination"].astype(str).str.upper() == args.destination.upper()]
    if args.cabin and "cabin" in out.columns:
        out = out[out["cabin"].astype(str).str.lower() == args.cabin.lower()]
    for arg_name, col in [("adt", "adt_count"), ("chd", "chd_count"), ("inf", "inf_count")]:
        val = getattr(args, arg_name)
        if val is not None and col in out.columns:
            out = out[out[col].fillna(-999999).astype(int) == int(val)]
    if args.max_rows and len(out) > args.max_rows:
        out = out.sort_values("observed_at_utc").tail(args.max_rows)
    return out.reset_index(drop=True)


def _is_bool_like(series: pd.Series) -> bool:
    vals = series.dropna().unique()
    if len(vals) == 0:
        return False
    if pd.api.types.is_bool_dtype(series):
        return True
    if len(vals) <= 4 and set(pd.Series(vals).astype(str).str.lower()) <= {"0", "1", "true", "false"}:
        return True
    return False


def _prepare_feature_matrix(df: pd.DataFrame, args: argparse.Namespace | None = None) -> tuple[pd.DataFrame, dict]:
    # Exclude leakage/future labels and high-cardinality identifiers.
    exclude_cols = {
        "scrape_id",
        "probe_group_id",
        "probe_join_id",
        "flight_key",
        "y_next_search_scrape_id",
        "y_next_search_observed_at_utc",
        "scraped_at",
        "departure_date",
        "observed_at_utc",
        "departure",
        "arrival",
        "inventory_confidence_summary",
        "party_gap_profile",  # compact string duplicate of numeric priors
    }
    label_cols = [c for c in df.columns if c.startswith("y_")]
    exclude_cols.update(label_cols)

    feature_cols = [c for c in df.columns if c not in exclude_cols]

    feature_ablation = str(getattr(args, "feature_ablation", "none") or "none").lower() if args else "none"
    if feature_ablation == "drop_route_engineered":
        feature_cols = [c for c in feature_cols if not _is_route_engineered_feature_name(c)]

    # Drop obviously unusable all-null columns
    feature_cols = [c for c in feature_cols if not df[c].isna().all()]

    X = df[feature_cols].copy()

    # Normalize bool-like columns to boolean to keep preprocessing simple.
    bool_like_cols = []
    for c in list(X.columns):
        if _is_bool_like(X[c]):
            bool_like_cols.append(c)
            X[c] = X[c].map(lambda v: np.nan if pd.isna(v) else str(v).lower() in {"1", "true"})

    # Convert datetimes (if any slipped through) to unix seconds.
    for c in list(X.columns):
        if pd.api.types.is_datetime64_any_dtype(X[c]):
            X[c] = X[c].view("int64") / 1e9

    numeric_cols = [c for c in X.columns if pd.api.types.is_numeric_dtype(X[c])]
    categorical_cols = [c for c in X.columns if c not in numeric_cols]

    meta = {
        "feature_cols": feature_cols,
        "numeric_cols": numeric_cols,
        "categorical_cols": categorical_cols,
        "bool_like_cols": bool_like_cols,
        "party_gap_feature_cols": [c for c in feature_cols if c.startswith("party_gap_profile_")],
        "feature_ablation": feature_ablation,
        "route_engineered_feature_cols": [c for c in feature_cols if _is_route_engineered_feature_name(c)],
    }
    return X, meta


def _requested_route_keys(args: argparse.Namespace, df: pd.DataFrame) -> list[str]:
    route_keys: list[str] = []
    if getattr(args, "route_group", None):
        route_keys = [
            x.strip().upper().replace(" ", "")
            for x in str(args.route_group).split(",")
            if x.strip()
        ]
    elif args.origin and args.destination:
        route_keys = [f"{str(args.origin).upper()}-{str(args.destination).upper()}"]
    # Keep only routes present in filtered dataset and preserve requested order.
    if "route_key" not in df.columns:
        return []
    present = set(df["route_key"].astype(str).str.upper().dropna().unique().tolist())
    deduped = []
    seen = set()
    for rk in route_keys:
        if rk in present and rk not in seen:
            deduped.append(rk)
            seen.add(rk)
    return deduped


def _is_route_engineered_feature_name(name: str) -> bool:
    s = str(name)
    return any(marker in s for marker in ROUTE_ENGINEERED_FEATURE_MARKERS)


def _normalize_top_feature_rows(rows: list[dict], top_n: int = 10) -> list[dict]:
    out = []
    for rank, item in enumerate((rows or [])[:top_n], start=1):
        feature = item.get("feature")
        if not feature:
            continue
        metric_name = "importance" if "importance" in item else ("coef" if "coef" in item else None)
        metric_value = item.get(metric_name) if metric_name else None
        out.append(
            {
                "rank": int(rank),
                "feature": str(feature),
                "metric_name": metric_name,
                "metric_value": (float(metric_value) if metric_value is not None else None),
                "is_route_engineered_feature": _is_route_engineered_feature_name(str(feature)),
            }
        )
    return out


def _summarize_top_feature_names(rows: list[dict], top_n: int = 5) -> list[str]:
    return [str(r.get("feature")) for r in (rows or [])[:top_n] if r.get("feature")]


def _time_split(df: pd.DataFrame, test_fraction: float) -> tuple[pd.Index, pd.Index, str]:
    if "observed_at_utc" not in df.columns:
        raise ValueError("observed_at_utc is required for time split")
    d = df[df["observed_at_utc"].notna()].sort_values("observed_at_utc")
    if d.empty:
        raise ValueError("No rows with valid observed_at_utc")
    n = len(d)
    test_n = max(1, int(math.floor(n * test_fraction)))
    split_idx = max(1, n - test_n)
    train_idx = d.index[:split_idx]
    test_idx = d.index[split_idx:]
    split_ts = d.iloc[split_idx]["observed_at_utc"] if split_idx < n else d.iloc[-1]["observed_at_utc"]
    return train_idx, test_idx, str(split_ts)


def _build_preprocessor(meta: dict) -> ColumnTransformer:
    numeric_cols = meta["numeric_cols"]
    categorical_cols = meta["categorical_cols"]

    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler(with_mean=False)),
        ]
    )
    categorical_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    transformers = []
    if numeric_cols:
        transformers.append(("num", numeric_pipe, numeric_cols))
    if categorical_cols:
        transformers.append(("cat", categorical_pipe, categorical_cols))
    return ColumnTransformer(transformers=transformers, remainder="drop")


def _classification_run(df: pd.DataFrame, X: pd.DataFrame, meta: dict, args: argparse.Namespace) -> dict:
    target = "y_next_search_price_move_class"
    d = df[df[target].notna()].copy()
    d = d[d[target].astype(str).isin(["up", "down", "same"])]
    if len(d) < 100:
        return {"skipped": True, "reason": f"Insufficient labeled rows for classification ({len(d)})"}

    train_idx, test_idx, split_ts = _time_split(d, args.test_fraction)
    Xd = X.loc[d.index]
    y = d[target].astype(str)
    X_train, X_test = Xd.loc[train_idx], Xd.loc[test_idx]
    y_train, y_test = y.loc[train_idx], y.loc[test_idx]

    pre = _build_preprocessor(meta)
    clf = Pipeline(
        steps=[
            ("pre", pre),
            ("model", LogisticRegression(max_iter=1000, n_jobs=None, random_state=args.random_state)),
        ]
    )
    clf.fit(X_train, y_train)
    pred = clf.predict(X_test)

    majority = y_train.mode().iloc[0]
    baseline_pred = np.repeat(majority, len(y_test))

    # Top coefficients (one-vs-rest averaged abs coeffs).
    feature_importance = []
    try:
        feat_names = clf.named_steps["pre"].get_feature_names_out()
        coef = clf.named_steps["model"].coef_
        if coef.ndim == 2:
            score = np.mean(np.abs(coef), axis=0)
        else:
            score = np.abs(coef)
        top_idx = np.argsort(score)[::-1][:20]
        feature_importance = [
            {"feature": str(feat_names[i]), "importance": float(score[i])} for i in top_idx
        ]
    except Exception:
        feature_importance = []

    return {
        "skipped": False,
        "target": target,
        "rows_total": int(len(d)),
        "rows_train": int(len(train_idx)),
        "rows_test": int(len(test_idx)),
        "split_timestamp_utc": split_ts,
        "class_distribution_train": {str(k): int(v) for k, v in y_train.value_counts().to_dict().items()},
        "class_distribution_test": {str(k): int(v) for k, v in y_test.value_counts().to_dict().items()},
        "metrics": {
            "accuracy": float(accuracy_score(y_test, pred)),
            "balanced_accuracy": float(balanced_accuracy_score(y_test, pred)),
            "macro_f1": float(f1_score(y_test, pred, average="macro", zero_division=0)),
        },
        "baseline_majority_class": str(majority),
        "baseline_metrics": {
            "accuracy": float(accuracy_score(y_test, baseline_pred)),
            "balanced_accuracy": float(balanced_accuracy_score(y_test, baseline_pred)),
            "macro_f1": float(f1_score(y_test, baseline_pred, average="macro", zero_division=0)),
        },
        "top_features": feature_importance,
    }


def _regression_run(df: pd.DataFrame, X: pd.DataFrame, meta: dict, args: argparse.Namespace) -> dict:
    target = "y_next_search_lowest_fare_delta"
    d = df[df[target].notna()].copy()
    if len(d) < 100:
        return {"skipped": True, "reason": f"Insufficient labeled rows for regression ({len(d)})"}

    train_idx, test_idx, split_ts = _time_split(d, args.test_fraction)
    Xd = X.loc[d.index]
    y = pd.to_numeric(d[target], errors="coerce")
    d = d[y.notna()]
    Xd = Xd.loc[d.index]
    y = y.loc[d.index]
    train_idx = train_idx.intersection(d.index)
    test_idx = test_idx.intersection(d.index)
    if len(train_idx) < 50 or len(test_idx) < 20:
        return {"skipped": True, "reason": "Insufficient rows after numeric target filtering"}

    X_train, X_test = Xd.loc[train_idx], Xd.loc[test_idx]
    y_train, y_test = y.loc[train_idx], y.loc[test_idx]

    pre = _build_preprocessor(meta)
    reg = Pipeline(
        steps=[
            ("pre", pre),
            ("model", Ridge(alpha=1.0, random_state=args.random_state)),
        ]
    )
    reg.fit(X_train, y_train)
    pred = reg.predict(X_test)

    baseline_const = float(np.median(y_train))
    baseline_pred = np.repeat(baseline_const, len(y_test))

    top_coef = []
    try:
        feat_names = reg.named_steps["pre"].get_feature_names_out()
        coef = np.asarray(reg.named_steps["model"].coef_).ravel()
        top_idx = np.argsort(np.abs(coef))[::-1][:20]
        top_coef = [{"feature": str(feat_names[i]), "coef": float(coef[i])} for i in top_idx]
    except Exception:
        top_coef = []

    rmse = math.sqrt(float(mean_squared_error(y_test, pred)))
    rmse_base = math.sqrt(float(mean_squared_error(y_test, baseline_pred)))

    return {
        "skipped": False,
        "target": target,
        "rows_total": int(len(d)),
        "rows_train": int(len(train_idx)),
        "rows_test": int(len(test_idx)),
        "split_timestamp_utc": split_ts,
        "metrics": {
            "mae": float(mean_absolute_error(y_test, pred)),
            "rmse": rmse,
            "median_ae": float(median_absolute_error(y_test, pred)),
            "r2": float(r2_score(y_test, pred)),
        },
        "baseline_constant_delta": baseline_const,
        "baseline_metrics": {
            "mae": float(mean_absolute_error(y_test, baseline_pred)),
            "rmse": rmse_base,
            "median_ae": float(median_absolute_error(y_test, baseline_pred)),
            "r2": float(r2_score(y_test, baseline_pred)),
        },
        "top_features": top_coef,
    }


def _two_stage_regression_run(
    df: pd.DataFrame,
    X: pd.DataFrame,
    meta: dict,
    args: argparse.Namespace,
    train_idx_override: pd.Index | None = None,
    test_idx_override: pd.Index | None = None,
    split_timestamp_override: str | None = None,
) -> dict:
    target_delta = "y_next_search_lowest_fare_delta"
    d = df[df[target_delta].notna()].copy()
    y_delta = pd.to_numeric(d[target_delta], errors="coerce")
    d = d[y_delta.notna()].copy()
    y_delta = y_delta.loc[d.index]
    if len(d) < 100:
        return {"skipped": True, "reason": f"Insufficient labeled rows for two-stage regression ({len(d)})"}

    if train_idx_override is not None and test_idx_override is not None:
        train_idx = pd.Index(train_idx_override)
        test_idx = pd.Index(test_idx_override)
        # Rolling-fold callers pass indices from the pre-filter route slice; drop rows
        # removed by target-label filtering before using .loc below.
        train_idx = train_idx[train_idx.isin(d.index)]
        test_idx = test_idx[test_idx.isin(d.index)]
        split_ts = split_timestamp_override or (
            str(d.loc[test_idx, "observed_at_utc"].min()) if "observed_at_utc" in d.columns and len(test_idx) else None
        )
    else:
        train_idx, test_idx, split_ts = _time_split(d, args.test_fraction)
    if len(train_idx) < 50 or len(test_idx) < 20:
        return {"skipped": True, "reason": "Insufficient rows after time split"}

    Xd = X.loc[d.index]
    X_train_all, X_test_all = Xd.loc[train_idx], Xd.loc[test_idx]
    y_train_delta, y_test_delta = y_delta.loc[train_idx], y_delta.loc[test_idx]

    min_move_delta = float(max(args.min_move_delta or 0.0, 0.0))
    # Stage A: move vs no-move (binary, same time split), with optional minimum |delta| threshold.
    if min_move_delta > 0:
        y_train_move = (np.abs(y_train_delta) >= min_move_delta).astype(int)
        y_test_move = (np.abs(y_test_delta) >= min_move_delta).astype(int)
    else:
        y_train_move = (y_train_delta != 0).astype(int)
        y_test_move = (y_test_delta != 0).astype(int)
    min_test_moves = int(max(getattr(args, "min_test_moves", 1) or 1, 0))
    test_move_count = int(np.sum(y_test_move))
    if test_move_count < min_test_moves:
        return {
            "skipped": True,
            "reason": (
                "Insufficient move rows in Stage A test split "
                f"({test_move_count} < min_test_moves={min_test_moves})"
            ),
            "min_test_moves": min_test_moves,
            "test_move_count": test_move_count,
        }
    if y_train_move.nunique() < 2:
        return {"skipped": True, "reason": "Stage A train split has only one class (move/no-move)"}

    pre_a = _build_preprocessor(meta)
    stage_a_base = Pipeline(
        steps=[
            ("pre", pre_a),
            (
                "model",
                RandomForestClassifier(
                    n_estimators=300,
                    random_state=args.random_state,
                    class_weight="balanced_subsample",
                    n_jobs=-1,
                    min_samples_leaf=2,
                    max_depth=None,
                ),
            ),
        ]
    )
    stage_a_calibration = str(getattr(args, "stage_a_calibration", "none") or "none").lower()
    stage_a_calibration_cv = int(max(getattr(args, "stage_a_calibration_cv", 3) or 3, 2))
    if stage_a_calibration != "none":
        stage_a = CalibratedClassifierCV(
            estimator=stage_a_base,
            method=stage_a_calibration,
            cv=stage_a_calibration_cv,
        )
    else:
        stage_a = stage_a_base
    # RandomForest can return strongly peaked probabilities; keep threshold sweep to tune precision/recall tradeoff.
    stage_a.fit(X_train_all, y_train_move)
    p_move = stage_a.predict_proba(X_test_all)[:, 1]
    default_threshold = 0.5
    y_pred_move = (p_move >= default_threshold).astype(int)

    # Stage B: predict delta on moved rows only (train on moved train rows only)
    moved_train_idx = y_train_move[y_train_move == 1].index
    moved_test_idx = y_test_move[y_test_move == 1].index
    min_stage_b_moves = int(max(getattr(args, "min_stage_b_moves", 20) or 20, 1))
    if len(moved_train_idx) < min_stage_b_moves:
        return {
            "skipped": True,
            "reason": (
                "Insufficient moved rows for Stage B training "
                f"({len(moved_train_idx)} < min_stage_b_moves={min_stage_b_moves})"
            ),
            "min_stage_b_moves": min_stage_b_moves,
        }

    X_train_moved = Xd.loc[moved_train_idx]
    y_train_moved_delta = y_delta.loc[moved_train_idx]
    X_test_moved = Xd.loc[moved_test_idx]
    y_test_moved_delta = y_delta.loc[moved_test_idx]

    pre_b = _build_preprocessor(meta)
    stage_b_model_choice = str(getattr(args, "stage_b_model", "ridge") or "ridge").lower()
    if stage_b_model_choice == "rf":
        stage_b_estimator = RandomForestRegressor(
            n_estimators=400,
            random_state=args.random_state,
            n_jobs=-1,
            min_samples_leaf=2,
        )
    else:
        stage_b_estimator = Ridge(alpha=1.0, random_state=args.random_state)
    stage_b = Pipeline(
        steps=[
            ("pre", pre_b),
            ("model", stage_b_estimator),
        ]
    )
    stage_b.fit(X_train_moved, y_train_moved_delta)

    # Stage B predictions across all test rows (used for threshold sweep + default threshold)
    stage_b_pred_all = stage_b.predict(X_test_all)
    # Combined prediction on full test set: if Stage A predicts no-move => 0, else Stage B delta
    pred_delta_all = np.where(y_pred_move == 1, stage_b_pred_all, 0.0)

    y_test_delta_arr = np.asarray(y_test_delta, dtype=float)

    # Baselines
    zero_baseline = np.zeros(len(y_test_delta_arr), dtype=float)
    median_all_baseline_val = float(np.median(y_train_delta))
    median_all_baseline = np.repeat(median_all_baseline_val, len(y_test_delta_arr))
    moved_train_median = float(np.median(y_train_moved_delta)) if len(y_train_moved_delta) else 0.0
    oracle_stage_a_pred = np.zeros(len(X_test_all), dtype=float)
    if len(moved_test_idx) > 0:
        moved_test_positions = np.where(y_test_move.to_numpy() == 1)[0]
        oracle_stage_a_pred[moved_test_positions] = stage_b.predict(X_test_moved)

    # Stage A metrics
    prec, rec, f1_bin, _ = precision_recall_fscore_support(
        y_test_move, y_pred_move, average="binary", zero_division=0
    )
    move_rate_train = float(np.mean(y_train_move))
    move_rate_test = float(np.mean(y_test_move))
    majority_move_baseline = int(round(np.mean(y_train_move)) >= 0.5)
    baseline_move_pred = np.repeat(majority_move_baseline, len(y_test_move))

    # Stage B moved-only metrics (diagnostic)
    stage_b_metrics = None
    stage_b_baseline_metrics = None
    if len(moved_test_idx) > 0:
        pred_moved_only = stage_b.predict(X_test_moved)
        stage_b_metrics = {
            "rows_test_moved": int(len(moved_test_idx)),
            "mae": float(mean_absolute_error(y_test_moved_delta, pred_moved_only)),
            "rmse": math.sqrt(float(mean_squared_error(y_test_moved_delta, pred_moved_only))),
            "median_ae": float(median_absolute_error(y_test_moved_delta, pred_moved_only)),
            "r2": float(r2_score(y_test_moved_delta, pred_moved_only)),
        }
        moved_baseline = np.repeat(moved_train_median, len(y_test_moved_delta))
        stage_b_baseline_metrics = {
            "median_constant_delta": moved_train_median,
            "mae": float(mean_absolute_error(y_test_moved_delta, moved_baseline)),
            "rmse": math.sqrt(float(mean_squared_error(y_test_moved_delta, moved_baseline))),
            "median_ae": float(median_absolute_error(y_test_moved_delta, moved_baseline)),
            "r2": float(r2_score(y_test_moved_delta, moved_baseline)),
        }

    def _reg_metrics(y_true: np.ndarray, y_hat: np.ndarray) -> dict:
        return {
            "mae": float(mean_absolute_error(y_true, y_hat)),
            "rmse": math.sqrt(float(mean_squared_error(y_true, y_hat))),
            "median_ae": float(median_absolute_error(y_true, y_hat)),
            "r2": float(r2_score(y_true, y_hat)),
        }

    def _bin_metrics(y_true: np.ndarray, y_hat: np.ndarray) -> dict:
        p, r, f, _ = precision_recall_fscore_support(y_true, y_hat, average="binary", zero_division=0)
        return {
            "accuracy": float(accuracy_score(y_true, y_hat)),
            "balanced_accuracy": float(balanced_accuracy_score(y_true, y_hat)),
            "precision_move": float(p),
            "recall_move": float(r),
            "f1_move": float(f),
        }

    # Threshold tuning for Stage A gating (optimize combined-delta metrics)
    threshold_values = [round(float(t), 2) for t in np.arange(0.50, 1.00, 0.01)]
    threshold_sweep = []
    for thr in threshold_values:
        y_pred_move_thr = (p_move >= thr).astype(int)
        pred_delta_thr = np.where(y_pred_move_thr == 1, stage_b_pred_all, 0.0)
        regm = _reg_metrics(y_test_delta_arr, pred_delta_thr)
        binm = _bin_metrics(y_test_move, y_pred_move_thr)
        threshold_sweep.append(
            {
                "threshold": float(thr),
                "predicted_move_count": int(np.sum(y_pred_move_thr)),
                "actual_move_count": int(np.sum(y_test_move)),
                "combined_delta_metrics": regm,
                "stage_a_metrics": binm,
            }
        )

    best_by_rmse = min(threshold_sweep, key=lambda r: r["combined_delta_metrics"]["rmse"]) if threshold_sweep else None
    best_by_mae = min(threshold_sweep, key=lambda r: r["combined_delta_metrics"]["mae"]) if threshold_sweep else None

    # Feature importance snapshots
    stage_a_top = []
    stage_b_top = []
    try:
        if hasattr(stage_a, "named_steps"):
            pre_a_fitted = stage_a.named_steps["pre"]
            model_a_fitted = stage_a.named_steps["model"]
        elif hasattr(stage_a, "calibrated_classifiers_") and stage_a.calibrated_classifiers_:
            # CalibratedClassifierCV wraps cloned estimators; inspect first calibrated estimator.
            est0 = stage_a.calibrated_classifiers_[0].estimator
            pre_a_fitted = est0.named_steps["pre"]
            model_a_fitted = est0.named_steps["model"]
        else:
            raise AttributeError("Unsupported Stage A estimator shape")
        feat_names_a = pre_a_fitted.get_feature_names_out()
        if hasattr(model_a_fitted, "feature_importances_"):
            imp_a = np.asarray(model_a_fitted.feature_importances_).ravel()
            top_idx = np.argsort(np.abs(imp_a))[::-1][:20]
            stage_a_top = [{"feature": str(feat_names_a[i]), "importance": float(imp_a[i])} for i in top_idx]
        elif hasattr(model_a_fitted, "coef_"):
            coef_a = np.asarray(model_a_fitted.coef_).ravel()
            top_idx = np.argsort(np.abs(coef_a))[::-1][:20]
            stage_a_top = [{"feature": str(feat_names_a[i]), "coef": float(coef_a[i])} for i in top_idx]
        else:
            stage_a_top = []
    except Exception:
        stage_a_top = []
    try:
        feat_names_b = stage_b.named_steps["pre"].get_feature_names_out()
        model_b = stage_b.named_steps["model"]
        if hasattr(model_b, "feature_importances_"):
            imp_b = np.asarray(model_b.feature_importances_).ravel()
            top_idx = np.argsort(np.abs(imp_b))[::-1][:20]
            stage_b_top = [{"feature": str(feat_names_b[i]), "importance": float(imp_b[i])} for i in top_idx]
        elif hasattr(model_b, "coef_"):
            coef_b = np.asarray(model_b.coef_).ravel()
            top_idx = np.argsort(np.abs(coef_b))[::-1][:20]
            stage_b_top = [{"feature": str(feat_names_b[i]), "coef": float(coef_b[i])} for i in top_idx]
        else:
            stage_b_top = []
    except Exception:
        stage_b_top = []

    result = {
        "skipped": False,
        "targets": {
            "stage_a": (
                f"move_flag(abs(y_delta)>={min_move_delta:g})"
                if min_move_delta > 0
                else "move_flag(y_delta!=0)"
            ),
            "stage_b": target_delta,
        },
        "min_move_delta": min_move_delta,
        "min_test_moves": min_test_moves,
        "min_stage_b_moves": min_stage_b_moves,
        "rows_total": int(len(d)),
        "rows_train": int(len(train_idx)),
        "rows_test": int(len(test_idx)),
        "split_timestamp_utc": split_ts,
        "stage_a": {
            "rows_train": int(len(train_idx)),
            "rows_test": int(len(test_idx)),
            "move_rate_train": move_rate_train,
            "move_rate_test": move_rate_test,
            "metrics": {
                "accuracy": float(accuracy_score(y_test_move, y_pred_move)),
                "balanced_accuracy": float(balanced_accuracy_score(y_test_move, y_pred_move)),
                "precision_move": float(prec),
                "recall_move": float(rec),
                "f1_move": float(f1_bin),
            },
            "baseline_majority_move_class": majority_move_baseline,
            "baseline_metrics": {
                "accuracy": float(accuracy_score(y_test_move, baseline_move_pred)),
                "balanced_accuracy": float(balanced_accuracy_score(y_test_move, baseline_move_pred)),
                "precision_move": float(
                    precision_recall_fscore_support(
                        y_test_move, baseline_move_pred, average="binary", zero_division=0
                    )[0]
                ),
                "recall_move": float(
                    precision_recall_fscore_support(
                        y_test_move, baseline_move_pred, average="binary", zero_division=0
                    )[1]
                ),
                "f1_move": float(
                    precision_recall_fscore_support(
                        y_test_move, baseline_move_pred, average="binary", zero_division=0
                    )[2]
                ),
            },
            "model_family": "random_forest",
            "calibration": {
                "enabled": stage_a_calibration != "none",
                "method": stage_a_calibration,
                "cv": stage_a_calibration_cv if stage_a_calibration != "none" else None,
            },
            "top_features": stage_a_top,
        },
        "stage_b": {
            "model_family": "random_forest_regressor" if stage_b_model_choice == "rf" else "ridge",
            "rows_train_moved": int(len(moved_train_idx)),
            "rows_test_moved": int(len(moved_test_idx)),
            "metrics_moved_only": stage_b_metrics,
            "baseline_metrics_moved_only": stage_b_baseline_metrics,
            "top_features": stage_b_top,
        },
        "combined_delta_prediction": {
            "description": "Predict 0 when Stage A predicts no-move; otherwise use Stage B delta prediction",
            "default_stage_a_threshold": default_threshold,
            "metrics": _reg_metrics(y_test_delta_arr, pred_delta_all),
            "baseline_zero_delta_metrics": _reg_metrics(y_test_delta_arr, zero_baseline),
            "baseline_median_all_delta": median_all_baseline_val,
            "baseline_median_all_metrics": _reg_metrics(y_test_delta_arr, median_all_baseline),
            "oracle_stage_a_metrics": _reg_metrics(y_test_delta_arr, oracle_stage_a_pred),
            "predicted_move_count": int(np.sum(y_pred_move)),
            "actual_move_count": int(np.sum(y_test_move)),
            "threshold_tuning": {
                "threshold_grid": threshold_values,
                "best_threshold_by_rmse": best_by_rmse,
                "best_threshold_by_mae": best_by_mae,
                "sweep": threshold_sweep,
            },
        },
    }

    # Explicitly report whether any threshold beats the zero-delta baseline on combined metrics.
    zero_base = result["combined_delta_prediction"]["baseline_zero_delta_metrics"]
    tuning = result["combined_delta_prediction"]["threshold_tuning"]
    rmse_candidates = [r for r in threshold_sweep if r["combined_delta_metrics"]["rmse"] < zero_base["rmse"]]
    mae_candidates = [r for r in threshold_sweep if r["combined_delta_metrics"]["mae"] < zero_base["mae"]]
    tuning["best_threshold_beating_zero_baseline_by_rmse"] = (
        min(rmse_candidates, key=lambda r: r["combined_delta_metrics"]["rmse"]) if rmse_candidates else None
    )
    tuning["best_threshold_beating_zero_baseline_by_mae"] = (
        min(mae_candidates, key=lambda r: r["combined_delta_metrics"]["mae"]) if mae_candidates else None
    )
    tuning["zero_baseline_beat_flags"] = {
        "any_threshold_beats_zero_rmse": bool(rmse_candidates),
        "any_threshold_beats_zero_mae": bool(mae_candidates),
    }
    return result


def _build_route_rolling_two_stage_reports(
    d: pd.DataFrame,
    Xr: pd.DataFrame,
    meta_r: dict,
    args: argparse.Namespace,
    route_key: str,
) -> list[dict]:
    n_splits = int(max(getattr(args, "route_rolling_folds", 0) or 0, 0))
    if n_splits < 2:
        return []
    if len(d) < (n_splits + 1) * 20:
        return []

    if "observed_at_utc" in d.columns:
        d = d.sort_values(["observed_at_utc", "flight_key"], na_position="last").reset_index(drop=True)
        Xr = Xr.loc[d.index]

    reports: list[dict] = []
    try:
        tscv = TimeSeriesSplit(n_splits=n_splits)
    except Exception:
        return reports

    for fold_no, (train_pos, test_pos) in enumerate(tscv.split(d), start=1):
        train_idx = d.index[np.asarray(train_pos)]
        test_idx = d.index[np.asarray(test_pos)]
        split_ts = None
        if "observed_at_utc" in d.columns and len(test_idx):
            split_ts = str(pd.to_datetime(d.loc[test_idx, "observed_at_utc"], errors="coerce").min())
        reg2_fold = _two_stage_regression_run(
            d,
            Xr,
            meta_r,
            args,
            train_idx_override=train_idx,
            test_idx_override=test_idx,
            split_timestamp_override=split_ts,
        )
        row = {
            "route_key": route_key,
            "fold": int(fold_no),
            "rows_total": int(len(d)),
            "rows_train": int(len(train_idx)),
            "rows_test": int(len(test_idx)),
            "min_move_delta": float(getattr(args, "min_move_delta", 0.0) or 0.0),
            "min_test_moves": int(max(getattr(args, "min_test_moves", 1) or 1, 0)),
            "min_stage_b_moves": int(max(getattr(args, "min_stage_b_moves", 20) or 20, 1)),
            "stage_a_calibration_method": str(getattr(args, "stage_a_calibration", "none") or "none"),
            "stage_b_model_family": str(getattr(args, "stage_b_model", "ridge") or "ridge"),
            "skipped": bool(reg2_fold.get("skipped", False)),
            "reason": reg2_fold.get("reason"),
            "sparse_stage_b": False,
            "sparse_test_moves": False,
            "beats_zero_rmse": False,
            "beats_zero_mae": False,
            "best_threshold_by_rmse": None,
            "best_rmse": None,
            "best_threshold_by_mae": None,
            "best_mae": None,
            "zero_baseline_rmse": None,
            "zero_baseline_mae": None,
            "actual_move_count": reg2_fold.get("test_move_count"),
        }
        if reg2_fold.get("skipped", False):
            reason = str(reg2_fold.get("reason") or "")
            row["sparse_stage_b"] = "Insufficient moved rows for Stage B training" in reason
            row["sparse_test_moves"] = "Insufficient move rows in Stage A test split" in reason
            reports.append(row)
            continue

        comb = reg2_fold.get("combined_delta_prediction", {}) or {}
        tune = comb.get("threshold_tuning", {}) or {}
        best_rmse = tune.get("best_threshold_by_rmse") or {}
        best_mae = tune.get("best_threshold_by_mae") or {}
        zero_metrics = comb.get("baseline_zero_delta_metrics") or {}
        beat_flags = tune.get("zero_baseline_beat_flags") or {}
        row.update(
            {
                "actual_move_count": comb.get("actual_move_count"),
                "predicted_move_count_default": comb.get("predicted_move_count"),
                "stage_a_balanced_accuracy": ((reg2_fold.get("stage_a") or {}).get("metrics") or {}).get("balanced_accuracy"),
                "stage_a_f1_move": ((reg2_fold.get("stage_a") or {}).get("metrics") or {}).get("f1_move"),
                "beats_zero_rmse": bool(beat_flags.get("any_threshold_beats_zero_rmse", False)),
                "beats_zero_mae": bool(beat_flags.get("any_threshold_beats_zero_mae", False)),
                "best_threshold_by_rmse": best_rmse.get("threshold"),
                "best_rmse": (best_rmse.get("combined_delta_metrics") or {}).get("rmse"),
                "best_threshold_by_mae": best_mae.get("threshold"),
                "best_mae": (best_mae.get("combined_delta_metrics") or {}).get("mae"),
                "zero_baseline_rmse": zero_metrics.get("rmse"),
                "zero_baseline_mae": zero_metrics.get("mae"),
                "split_timestamp_utc": reg2_fold.get("split_timestamp_utc"),
            }
        )
        reports.append(row)
    return reports


def _derive_route_model_priority(row: dict) -> tuple[str, str]:
    """Consolidate route readiness into a single label for downstream selection."""
    if bool(row.get("two_stage_skipped", False)):
        if bool(row.get("sparse_stage_b", False)):
            return "hold", "sparse_stage_b"
        return "hold", "two_stage_skipped"

    rolling_total = row.get("two_stage_rolling_folds_total")
    if rolling_total is not None:
        rolling_eval = int(row.get("two_stage_rolling_folds_evaluated") or 0)
        sparse_test = int(row.get("two_stage_rolling_sparse_test_moves_count") or 0)
        sparse_b = int(row.get("two_stage_rolling_sparse_stage_b_count") or 0)
        viable_rmse = row.get("rolling_viable_rmse")
        viable_mae = row.get("rolling_viable_mae")
        any_rmse = bool(row.get("two_stage_rolling_any_fold_beats_zero_rmse", False))
        any_mae = bool(row.get("two_stage_rolling_any_fold_beats_zero_mae", False))

        if rolling_eval == 0:
            return "hold", "no_rolling_eval_folds"
        if viable_rmse is True and viable_mae is True:
            return "high", "rolling_viable_rmse_mae"
        if viable_rmse is True:
            return "candidate", "rolling_viable_rmse_only"
        if any_rmse or any_mae:
            return "watch", "some_rolling_fold_signal_no_viability"
        if sparse_test > 0 or sparse_b > 0:
            return "watch", "rolling_sparse_folds"
        return "hold", "rolling_no_signal"

    beats_rmse = bool(row.get("beats_zero_rmse", False))
    beats_mae = bool(row.get("beats_zero_mae", False))
    if beats_rmse and beats_mae:
        return "candidate", "single_split_beats_zero_rmse_mae"
    if beats_rmse:
        return "candidate", "single_split_beats_zero_rmse"
    if beats_mae:
        return "watch", "single_split_beats_zero_mae"
    if bool(row.get("sparse_stage_b", False)):
        return "watch", "single_split_sparse_stage_b"
    return "hold", "single_split_no_signal"


def _build_route_threshold_summaries(df: pd.DataFrame, args: argparse.Namespace) -> tuple[list[dict], list[dict], list[dict]]:
    route_keys = _requested_route_keys(args, df)
    if not route_keys:
        return [], [], []

    rows: list[dict] = []
    threshold_report_rows: list[dict] = []
    rolling_report_rows: list[dict] = []
    for route_key in route_keys:
        d = df[df["route_key"].astype(str).str.upper() == route_key].copy()
        if d.empty:
            rows.append({"route_key": route_key, "skipped": True, "reason": "No rows after route filter"})
            continue
        if "observed_at_utc" in d.columns:
            d = d.sort_values(["observed_at_utc", "flight_key"], na_position="last").reset_index(drop=True)
        Xr, meta_r = _prepare_feature_matrix(d, args)
        cls_r = _classification_run(d, Xr, meta_r, args)
        reg_r = _regression_run(d, Xr, meta_r, args)
        reg2_r = _two_stage_regression_run(d, Xr, meta_r, args)
        rolling_rows = _build_route_rolling_two_stage_reports(d, Xr, meta_r, args, route_key)
        rolling_report_rows.extend(rolling_rows)

        row = {
            "route_key": route_key,
            "rows": int(len(d)),
            "features": int(len(meta_r["feature_cols"])),
            "classification_skipped": bool(cls_r.get("skipped", False)),
            "regression_skipped": bool(reg_r.get("skipped", False)),
            "two_stage_skipped": bool(reg2_r.get("skipped", False)),
            # Explicit route-level viability flags for downstream automation
            "sparse_stage_b": False,
            "beats_zero_rmse": False,
            "beats_zero_mae": False,
            "rolling_viable_rmse": None,
            "rolling_viable_mae": None,
        }
        if cls_r.get("skipped"):
            row["classification_reason"] = cls_r.get("reason")
        else:
            row["classification_balanced_accuracy"] = cls_r["metrics"]["balanced_accuracy"]
            row["classification_macro_f1"] = cls_r["metrics"]["macro_f1"]
        if reg_r.get("skipped"):
            row["regression_reason"] = reg_r.get("reason")
        else:
            row["regression_rmse"] = reg_r["metrics"]["rmse"]
            row["regression_mae"] = reg_r["metrics"]["mae"]
            row["regression_baseline_rmse"] = reg_r["baseline_metrics"]["rmse"]
            row["regression_baseline_mae"] = reg_r["baseline_metrics"]["mae"]

        if reg2_r.get("skipped"):
            row["two_stage_reason"] = reg2_r.get("reason")
            row["sparse_stage_b"] = bool(
                "Insufficient moved rows for Stage B training" in str(reg2_r.get("reason") or "")
            )
        else:
            comb = reg2_r["combined_delta_prediction"]
            tune = comb.get("threshold_tuning", {})
            stage_a_top = _normalize_top_feature_rows((reg2_r.get("stage_a", {}) or {}).get("top_features") or [], top_n=15)
            stage_b_top = _normalize_top_feature_rows((reg2_r.get("stage_b", {}) or {}).get("top_features") or [], top_n=15)
            stage_a_top_eng = [r for r in stage_a_top if r.get("is_route_engineered_feature")]
            stage_b_top_eng = [r for r in stage_b_top if r.get("is_route_engineered_feature")]
            beats_rmse = bool((tune.get("zero_baseline_beat_flags") or {}).get("any_threshold_beats_zero_rmse", False))
            beats_mae = bool((tune.get("zero_baseline_beat_flags") or {}).get("any_threshold_beats_zero_mae", False))
            row.update(
                {
                    "min_move_delta": reg2_r.get("min_move_delta", 0.0),
                    "min_stage_b_moves": reg2_r.get("min_stage_b_moves"),
                    "two_stage_default_rmse": comb["metrics"]["rmse"],
                    "two_stage_default_mae": comb["metrics"]["mae"],
                    "two_stage_zero_baseline_rmse": comb["baseline_zero_delta_metrics"]["rmse"],
                    "two_stage_zero_baseline_mae": comb["baseline_zero_delta_metrics"]["mae"],
                    "two_stage_default_predicted_moves": comb.get("predicted_move_count"),
                    "two_stage_default_actual_moves": comb.get("actual_move_count"),
                    "two_stage_stage_a_balanced_accuracy": reg2_r["stage_a"]["metrics"]["balanced_accuracy"],
                    "two_stage_stage_a_f1_move": reg2_r["stage_a"]["metrics"]["f1_move"],
                    "two_stage_stage_a_calibration_method": (
                        (reg2_r.get("stage_a", {}).get("calibration") or {}).get("method")
                    ),
                    "two_stage_stage_b_model_family": (reg2_r.get("stage_b", {}) or {}).get("model_family"),
                    "two_stage_stage_a_top_features": stage_a_top,
                    "two_stage_stage_b_top_features": stage_b_top,
                    "two_stage_stage_a_top_engineered_features": stage_a_top_eng,
                    "two_stage_stage_b_top_engineered_features": stage_b_top_eng,
                    "two_stage_stage_a_top_feature_names": _summarize_top_feature_names(stage_a_top, top_n=8),
                    "two_stage_stage_b_top_feature_names": _summarize_top_feature_names(stage_b_top, top_n=8),
                    "two_stage_stage_a_top_engineered_feature_names": _summarize_top_feature_names(stage_a_top_eng, top_n=8),
                    "two_stage_stage_b_top_engineered_feature_names": _summarize_top_feature_names(stage_b_top_eng, top_n=8),
                    "two_stage_any_threshold_beats_zero_rmse": beats_rmse,
                    "two_stage_any_threshold_beats_zero_mae": beats_mae,
                    "beats_zero_rmse": beats_rmse,
                    "beats_zero_mae": beats_mae,
                    "sparse_stage_b": False,
                }
            )
            best_rmse = tune.get("best_threshold_by_rmse") or {}
            best_mae = tune.get("best_threshold_by_mae") or {}
            best_rmse_zero = tune.get("best_threshold_beating_zero_baseline_by_rmse") or {}
            best_mae_zero = tune.get("best_threshold_beating_zero_baseline_by_mae") or {}
            row.update(
                {
                    "two_stage_best_threshold_by_rmse": best_rmse.get("threshold"),
                    "two_stage_best_rmse": (best_rmse.get("combined_delta_metrics") or {}).get("rmse"),
                    "two_stage_best_threshold_by_mae": best_mae.get("threshold"),
                    "two_stage_best_mae": (best_mae.get("combined_delta_metrics") or {}).get("mae"),
                    "two_stage_best_threshold_beating_zero_rmse": best_rmse_zero.get("threshold"),
                    "two_stage_best_rmse_beating_zero": (best_rmse_zero.get("combined_delta_metrics") or {}).get("rmse"),
                    "two_stage_best_threshold_beating_zero_mae": best_mae_zero.get("threshold"),
                    "two_stage_best_mae_beating_zero": (best_mae_zero.get("combined_delta_metrics") or {}).get("mae"),
                }
            )
            zero_rmse = (comb.get("baseline_zero_delta_metrics") or {}).get("rmse")
            zero_mae = (comb.get("baseline_zero_delta_metrics") or {}).get("mae")
            for trow in (tune.get("sweep") or []):
                cmet = trow.get("combined_delta_metrics") or {}
                threshold_report_rows.append(
                    {
                        "route_key": route_key,
                        "rows": int(len(d)),
                        "min_move_delta": reg2_r.get("min_move_delta"),
                        "min_stage_b_moves": reg2_r.get("min_stage_b_moves"),
                        "stage_a_calibration_method": (
                            (reg2_r.get("stage_a", {}).get("calibration") or {}).get("method")
                        ),
                        "stage_a_calibration_enabled": bool(
                            (reg2_r.get("stage_a", {}).get("calibration") or {}).get("enabled")
                        ),
                        "stage_b_model_family": (reg2_r.get("stage_b", {}) or {}).get("model_family"),
                        "threshold": trow.get("threshold"),
                        "predicted_move_count": trow.get("predicted_move_count"),
                        "actual_move_count": trow.get("actual_move_count"),
                        "stage_a_balanced_accuracy": (trow.get("stage_a_metrics") or {}).get("balanced_accuracy"),
                        "stage_a_f1_move": (trow.get("stage_a_metrics") or {}).get("f1_move"),
                        "combined_rmse": cmet.get("rmse"),
                        "combined_mae": cmet.get("mae"),
                        "combined_r2": cmet.get("r2"),
                        "zero_baseline_rmse": zero_rmse,
                        "zero_baseline_mae": zero_mae,
                        "beats_zero_rmse": (
                            (cmet.get("rmse") is not None and zero_rmse is not None and cmet.get("rmse") < zero_rmse)
                        ),
                        "beats_zero_mae": (
                            (cmet.get("mae") is not None and zero_mae is not None and cmet.get("mae") < zero_mae)
                        ),
                    }
                )

        if rolling_rows:
            eval_rows = [r for r in rolling_rows if not r.get("skipped")]
            row["two_stage_rolling_folds_requested"] = int(max(getattr(args, "route_rolling_folds", 0) or 0, 0))
            row["two_stage_rolling_folds_total"] = int(len(rolling_rows))
            row["two_stage_rolling_folds_evaluated"] = int(len(eval_rows))
            row["two_stage_rolling_folds_skipped"] = int(len(rolling_rows) - len(eval_rows))
            row["two_stage_rolling_sparse_stage_b_count"] = int(sum(1 for r in rolling_rows if r.get("sparse_stage_b")))
            row["two_stage_rolling_sparse_test_moves_count"] = int(sum(1 for r in rolling_rows if r.get("sparse_test_moves")))
            if eval_rows:
                rmse_vals = [float(r["best_rmse"]) for r in eval_rows if r.get("best_rmse") is not None]
                mae_vals = [float(r["best_mae"]) for r in eval_rows if r.get("best_mae") is not None]
                zero_rmse_vals = [float(r["zero_baseline_rmse"]) for r in eval_rows if r.get("zero_baseline_rmse") is not None]
                zero_mae_vals = [float(r["zero_baseline_mae"]) for r in eval_rows if r.get("zero_baseline_mae") is not None]
                row["two_stage_rolling_beats_zero_rmse_count"] = int(sum(1 for r in eval_rows if r.get("beats_zero_rmse")))
                row["two_stage_rolling_beats_zero_mae_count"] = int(sum(1 for r in eval_rows if r.get("beats_zero_mae")))
                row["two_stage_rolling_any_fold_beats_zero_rmse"] = bool(row["two_stage_rolling_beats_zero_rmse_count"] > 0)
                row["two_stage_rolling_any_fold_beats_zero_mae"] = bool(row["two_stage_rolling_beats_zero_mae_count"] > 0)
                row["two_stage_rolling_rmse_mean"] = float(np.mean(rmse_vals)) if rmse_vals else None
                row["two_stage_rolling_rmse_median"] = float(np.median(rmse_vals)) if rmse_vals else None
                row["two_stage_rolling_zero_rmse_mean"] = float(np.mean(zero_rmse_vals)) if zero_rmse_vals else None
                row["two_stage_rolling_mae_mean"] = float(np.mean(mae_vals)) if mae_vals else None
                row["two_stage_rolling_mae_median"] = float(np.median(mae_vals)) if mae_vals else None
                row["two_stage_rolling_zero_mae_mean"] = float(np.mean(zero_mae_vals)) if zero_mae_vals else None
                viability_rule = str(getattr(args, "rolling_viability_rule", "beats_zero_folds") or "beats_zero_folds")
                min_beat_folds = int(max(getattr(args, "rolling_viability_min_beat_folds", 2) or 1, 1))
                row["rolling_viability_rule_rmse"] = viability_rule
                row["rolling_viability_min_beat_folds"] = min_beat_folds
                if viability_rule == "mean_rmse":
                    row["rolling_viable_rmse"] = bool(
                        row.get("two_stage_rolling_rmse_mean") is not None
                        and row.get("two_stage_rolling_zero_rmse_mean") is not None
                        and float(row["two_stage_rolling_rmse_mean"]) < float(row["two_stage_rolling_zero_rmse_mean"])
                    )
                else:
                    row["rolling_viable_rmse"] = bool(
                        int(row.get("two_stage_rolling_beats_zero_rmse_count") or 0) >= min_beat_folds
                    )
                row["rolling_viability_rule_mae"] = viability_rule
                if viability_rule == "mean_rmse":
                    row["rolling_viable_mae"] = bool(
                        row.get("two_stage_rolling_mae_mean") is not None
                        and row.get("two_stage_rolling_zero_mae_mean") is not None
                        and float(row["two_stage_rolling_mae_mean"]) < float(row["two_stage_rolling_zero_mae_mean"])
                    )
                else:
                    row["rolling_viable_mae"] = bool(
                        int(row.get("two_stage_rolling_beats_zero_mae_count") or 0) >= min_beat_folds
                    )

        priority, priority_reason = _derive_route_model_priority(row)
        row["route_model_priority"] = priority
        row["route_model_priority_reason"] = priority_reason
        rows.append(row)
    return rows, threshold_report_rows, rolling_report_rows


def _write_outputs(summary: dict, out_dir: Path, tz_mode: str) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = _timestamp_token(tz_mode)
    base = f"inventory_state_baseline_{ts}"
    json_path = out_dir / f"{base}.json"
    md_path = out_dir / f"{base}.md"
    latest_json = out_dir / "inventory_state_baseline_latest.json"
    latest_md = out_dir / "inventory_state_baseline_latest.md"
    route_threshold_csv = None
    route_threshold_latest_csv = None
    route_threshold_route_latest_csv = None
    route_rolling_eval_csv = None
    route_rolling_eval_latest_csv = None
    route_rolling_eval_route_latest_csv = None
    route_feature_impact_csv = None
    route_feature_impact_latest_csv = None
    route_feature_impact_route_latest_csv = None

    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    cls = summary.get("classification", {})
    reg = summary.get("regression", {})
    reg2 = summary.get("two_stage_regression", {})
    route_summaries = summary.get("route_threshold_summaries") or []
    route_threshold_reports = summary.get("route_threshold_tuning_reports") or []
    route_rolling_reports = summary.get("route_rolling_evaluation_reports") or []
    md_lines = [
        "# Inventory State Baseline Model Summary",
        "",
        f"- dataset_path: `{summary['dataset']['input_csv']}`",
        f"- rows_loaded: `{summary['dataset']['rows_loaded']}`",
        f"- rows_after_filters: `{summary['dataset']['rows_after_filters']}`",
        f"- feature_count: `{summary['features']['feature_count']}`",
        f"- numeric_features: `{summary['features']['numeric_feature_count']}`",
        f"- categorical_features: `{summary['features']['categorical_feature_count']}`",
        f"- party_gap_profile_feature_count: `{summary['features']['party_gap_profile_feature_count']}`",
        "",
        "## Classification (y_next_search_price_move_class)",
        "",
    ]
    if cls.get("skipped"):
        md_lines.append(f"- skipped: {cls.get('reason')}")
    else:
        md_lines.extend(
            [
                f"- rows train/test: `{cls['rows_train']}` / `{cls['rows_test']}`",
                f"- split timestamp (UTC): `{cls['split_timestamp_utc']}`",
                f"- accuracy: `{cls['metrics']['accuracy']:.4f}` (baseline `{cls['baseline_metrics']['accuracy']:.4f}`)",
                f"- balanced_accuracy: `{cls['metrics']['balanced_accuracy']:.4f}` (baseline `{cls['baseline_metrics']['balanced_accuracy']:.4f}`)",
                f"- macro_f1: `{cls['metrics']['macro_f1']:.4f}` (baseline `{cls['baseline_metrics']['macro_f1']:.4f}`)",
            ]
        )
    md_lines.extend(["", "## Regression (y_next_search_lowest_fare_delta)", ""])
    if reg.get("skipped"):
        md_lines.append(f"- skipped: {reg.get('reason')}")
    else:
        md_lines.extend(
            [
                f"- rows train/test: `{reg['rows_train']}` / `{reg['rows_test']}`",
                f"- split timestamp (UTC): `{reg['split_timestamp_utc']}`",
                f"- MAE: `{reg['metrics']['mae']:.2f}` (baseline `{reg['baseline_metrics']['mae']:.2f}`)",
                f"- RMSE: `{reg['metrics']['rmse']:.2f}` (baseline `{reg['baseline_metrics']['rmse']:.2f}`)",
                f"- MedianAE: `{reg['metrics']['median_ae']:.2f}` (baseline `{reg['baseline_metrics']['median_ae']:.2f}`)",
                f"- R2: `{reg['metrics']['r2']:.4f}` (baseline `{reg['baseline_metrics']['r2']:.4f}`)",
            ]
        )
    md_lines.extend(["", "## Two-Stage Regression (move/no-move + moved-row delta)", ""])
    if reg2.get("skipped"):
        md_lines.append(f"- skipped: {reg2.get('reason')}")
    else:
        s1 = reg2["stage_a"]
        s2 = reg2["stage_b"]
        comb = reg2["combined_delta_prediction"]
        md_lines.extend(
            [
                f"- rows train/test: `{reg2['rows_train']}` / `{reg2['rows_test']}`",
                f"- split timestamp (UTC): `{reg2['split_timestamp_utc']}`",
                f"- Stage A move threshold (|delta|): `{reg2.get('min_move_delta', 0)}`",
                f"- Stage A move-rate train/test: `{s1['move_rate_train']:.4f}` / `{s1['move_rate_test']:.4f}`",
                f"- Stage A balanced_accuracy: `{s1['metrics']['balanced_accuracy']:.4f}` (baseline `{s1['baseline_metrics']['balanced_accuracy']:.4f}`)",
                f"- Stage A F1(move): `{s1['metrics']['f1_move']:.4f}` (baseline `{s1['baseline_metrics']['f1_move']:.4f}`)",
                f"- Stage B moved rows train/test: `{s2['rows_train_moved']}` / `{s2['rows_test_moved']}`",
            ]
        )
        if s2.get("metrics_moved_only"):
            md_lines.extend(
                [
                    f"- Stage B moved-only MAE: `{s2['metrics_moved_only']['mae']:.2f}` (baseline `{s2['baseline_metrics_moved_only']['mae']:.2f}`)",
                    f"- Stage B moved-only RMSE: `{s2['metrics_moved_only']['rmse']:.2f}` (baseline `{s2['baseline_metrics_moved_only']['rmse']:.2f}`)",
                ]
            )
        md_lines.extend(
            [
                f"- Combined delta MAE: `{comb['metrics']['mae']:.2f}` (zero baseline `{comb['baseline_zero_delta_metrics']['mae']:.2f}`; median baseline `{comb['baseline_median_all_metrics']['mae']:.2f}`)",
                f"- Combined delta RMSE: `{comb['metrics']['rmse']:.2f}` (zero baseline `{comb['baseline_zero_delta_metrics']['rmse']:.2f}`; median baseline `{comb['baseline_median_all_metrics']['rmse']:.2f}`)",
                f"- Combined delta R2: `{comb['metrics']['r2']:.4f}` (zero baseline `{comb['baseline_zero_delta_metrics']['r2']:.4f}`; median baseline `{comb['baseline_median_all_metrics']['r2']:.4f}`)",
                f"- Oracle Stage-A gated RMSE (upper bound): `{comb['oracle_stage_a_metrics']['rmse']:.2f}`",
            ]
        )
        tuning = comb.get("threshold_tuning") or {}
        best_rmse = tuning.get("best_threshold_by_rmse")
        best_mae = tuning.get("best_threshold_by_mae")
        if best_rmse:
            md_lines.append(
                f"- Best threshold by combined RMSE: `{best_rmse['threshold']:.2f}` "
                f"(RMSE `{best_rmse['combined_delta_metrics']['rmse']:.2f}`, "
                f"MAE `{best_rmse['combined_delta_metrics']['mae']:.2f}`, "
                f"pred_moves `{best_rmse['predicted_move_count']}`)"
            )
        if best_mae:
            md_lines.append(
                f"- Best threshold by combined MAE: `{best_mae['threshold']:.2f}` "
                f"(MAE `{best_mae['combined_delta_metrics']['mae']:.2f}`, "
                f"RMSE `{best_mae['combined_delta_metrics']['rmse']:.2f}`, "
                f"pred_moves `{best_mae['predicted_move_count']}`)"
            )
        best_rmse_beat_zero = tuning.get("best_threshold_beating_zero_baseline_by_rmse")
        best_mae_beat_zero = tuning.get("best_threshold_beating_zero_baseline_by_mae")
        md_lines.append(
            "- Best threshold beating zero baseline by RMSE: "
            + (
                f"`{best_rmse_beat_zero['threshold']:.2f}` (RMSE `{best_rmse_beat_zero['combined_delta_metrics']['rmse']:.2f}`)"
                if best_rmse_beat_zero
                else "`none`"
            )
        )
        md_lines.append(
            "- Best threshold beating zero baseline by MAE: "
            + (
                f"`{best_mae_beat_zero['threshold']:.2f}` (MAE `{best_mae_beat_zero['combined_delta_metrics']['mae']:.2f}`)"
                if best_mae_beat_zero
                else "`none`"
            )
        )
    if route_summaries:
        md_lines.extend(["", "## Route-Specific Threshold Summaries", ""])
        filt = summary.get("dataset", {}).get("filters", {})
        md_lines.extend(
            [
                f"- route_group: `{filt.get('route_group') or '(none)'}`",
                f"- min_move_delta: `{filt.get('min_move_delta')}`",
                f"- min_test_moves: `{filt.get('min_test_moves')}`",
                f"- min_stage_b_moves: `{filt.get('min_stage_b_moves')}`",
                f"- rolling_viability_rule: `{filt.get('rolling_viability_rule')}`",
                f"- rolling_viability_min_beat_folds: `{filt.get('rolling_viability_min_beat_folds')}`",
                f"- stage_a_calibration: `{filt.get('stage_a_calibration')}`",
                f"- stage_a_calibration_cv: `{filt.get('stage_a_calibration_cv')}`",
                f"- stage_b_model: `{filt.get('stage_b_model')}`",
                f"- feature_ablation: `{filt.get('feature_ablation')}`",
                "",
            ]
        )
        if route_threshold_reports:
            md_lines.append(
                "- Route threshold tuning report CSV will be written as an artifact "
                "(combined for route batch, plus route-specific latest when one route is requested)."
            )
            md_lines.append("")
        if route_rolling_reports:
            md_lines.append(
                "- Route rolling evaluation CSV will be written as an artifact "
                "(combined for route batch, plus route-specific latest when one route is requested)."
            )
            md_lines.append("")
        md_lines.append(
            "| Route | Rows | Priority | 2-stage | SparseB | Beats0(RMSE) | Beats0(MAE) | BestThr(RMSE) | BestRMSE | BestThr>0(RMSE) | BestThr(MAE) | BestMAE |"
        )
        md_lines.append("| --- | ---: | --- | --- | :---: | :---: | :---: | ---: | ---: | ---: | ---: | ---: |")
        for r in route_summaries:
            if r.get("two_stage_skipped", False):
                md_lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(r.get("route_key", "?")),
                            str(r.get("rows", "")),
                            str(r.get("route_model_priority", "")),
                            "skipped",
                            "Y" if r.get("sparse_stage_b") else "N",
                            "N",
                            "N",
                            "",
                            "",
                            "",
                            "",
                            "",
                        ]
                    )
                    + " |"
                )
                continue
            md_lines.append(
                "| "
                + " | ".join(
                    [
                        str(r.get("route_key", "")),
                        str(r.get("rows", "")),
                        str(r.get("route_model_priority", "")),
                        "ok",
                        "Y" if r.get("sparse_stage_b") else "N",
                        "Y" if r.get("beats_zero_rmse") else "N",
                        "Y" if r.get("beats_zero_mae") else "N",
                        (
                            f"{r.get('two_stage_best_threshold_by_rmse'):.2f}"
                            if r.get("two_stage_best_threshold_by_rmse") is not None
                            else ""
                        ),
                        (
                            f"{r.get('two_stage_best_rmse'):.2f}"
                            if r.get("two_stage_best_rmse") is not None
                            else ""
                        ),
                        (
                            f"{r.get('two_stage_best_threshold_beating_zero_rmse'):.2f}"
                            if r.get("two_stage_best_threshold_beating_zero_rmse") is not None
                            else ""
                        ),
                        (
                            f"{r.get('two_stage_best_threshold_by_mae'):.2f}"
                            if r.get("two_stage_best_threshold_by_mae") is not None
                            else ""
                        ),
                        (
                            f"{r.get('two_stage_best_mae'):.2f}"
                            if r.get("two_stage_best_mae") is not None
                            else ""
                        ),
                    ]
                )
                + " |"
            )
        if route_rolling_reports:
            md_lines.extend(["", "### Route Rolling Time-Fold Evaluation (Two-Stage)", ""])
            md_lines.append(
                "| Route | Folds Eval/Total | SparseTest | SparseB | AnyBeat0(RMSE) | AnyBeat0(MAE) | RollingViable(RMSE) | RollingViable(MAE) | Mean BestRMSE | Mean ZeroRMSE |"
            )
            md_lines.append("| --- | ---: | ---: | ---: | :---: | :---: | :---: | :---: | ---: | ---: |")
            for r in route_summaries:
                if r.get("two_stage_rolling_folds_total") is None:
                    continue
                md_lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(r.get("route_key", "")),
                            f"{r.get('two_stage_rolling_folds_evaluated', 0)}/{r.get('two_stage_rolling_folds_total', 0)}",
                            str(r.get("two_stage_rolling_sparse_test_moves_count", 0)),
                            str(r.get("two_stage_rolling_sparse_stage_b_count", 0)),
                            "Y" if r.get("two_stage_rolling_any_fold_beats_zero_rmse") else "N",
                            "Y" if r.get("two_stage_rolling_any_fold_beats_zero_mae") else "N",
                            (
                                "Y" if r.get("rolling_viable_rmse") is True else
                                ("N" if r.get("rolling_viable_rmse") is False else "")
                            ),
                            (
                                "Y" if r.get("rolling_viable_mae") is True else
                                ("N" if r.get("rolling_viable_mae") is False else "")
                            ),
                            (
                                f"{r.get('two_stage_rolling_rmse_mean'):.2f}"
                                if r.get("two_stage_rolling_rmse_mean") is not None
                                else ""
                            ),
                            (
                                f"{r.get('two_stage_rolling_zero_rmse_mean'):.2f}"
                                if r.get("two_stage_rolling_zero_rmse_mean") is not None
                                else ""
                            ),
                        ]
                    )
                    + " |"
                )
        md_lines.extend(["", "### Route Feature Impact (Two-Stage Top Features)", ""])
        md_lines.append(
            "Top features are route-specific model features for the two-stage run. "
            "Engineered = matches route/time/fare-ladder engineered feature families."
        )
        md_lines.append("")
        for r in route_summaries:
            if r.get("two_stage_skipped", False):
                continue
            md_lines.append(f"#### {r.get('route_key')}")
            a_eng = r.get("two_stage_stage_a_top_engineered_feature_names") or []
            b_eng = r.get("two_stage_stage_b_top_engineered_feature_names") or []
            a_top = r.get("two_stage_stage_a_top_feature_names") or []
            b_top = r.get("two_stage_stage_b_top_feature_names") or []
            md_lines.append(
                "- Stage A top engineered features: "
                + (", ".join(f"`{x}`" for x in a_eng[:5]) if a_eng else "`none in top set`")
            )
            md_lines.append(
                "- Stage B top engineered features: "
                + (", ".join(f"`{x}`" for x in b_eng[:5]) if b_eng else "`none in top set`")
            )
            md_lines.append(
                "- Stage A top features (all): "
                + (", ".join(f"`{x}`" for x in a_top[:8]) if a_top else "`none`")
            )
            md_lines.append(
                "- Stage B top features (all): "
                + (", ".join(f"`{x}`" for x in b_top[:8]) if b_top else "`none`")
            )
            md_lines.append("")
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    latest_md.write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")

    if route_threshold_reports:
        route_threshold_csv = out_dir / f"route_threshold_tuning_{ts}.csv"
        route_threshold_latest_csv = out_dir / "route_threshold_tuning_latest.csv"
        route_df = pd.DataFrame(route_threshold_reports)
        # Stable ordering for manual review.
        sort_cols = [c for c in ["route_key", "threshold"] if c in route_df.columns]
        if sort_cols:
            route_df = route_df.sort_values(sort_cols).reset_index(drop=True)
        route_df.to_csv(route_threshold_csv, index=False)
        route_df.to_csv(route_threshold_latest_csv, index=False)

        route_keys = sorted(set(route_df["route_key"].astype(str).tolist())) if "route_key" in route_df.columns else []
        if len(route_keys) == 1:
            safe_route = route_keys[0].replace("/", "_").replace("\\", "_")
            route_threshold_route_latest_csv = out_dir / f"route_threshold_tuning_{safe_route}_latest.csv"
            route_df.to_csv(route_threshold_route_latest_csv, index=False)

    if route_rolling_reports:
        route_rolling_eval_csv = out_dir / f"route_rolling_eval_{ts}.csv"
        route_rolling_eval_latest_csv = out_dir / "route_rolling_eval_latest.csv"
        rolling_df = pd.DataFrame(route_rolling_reports)
        sort_cols = [c for c in ["route_key", "fold"] if c in rolling_df.columns]
        if sort_cols:
            rolling_df = rolling_df.sort_values(sort_cols).reset_index(drop=True)
        rolling_df.to_csv(route_rolling_eval_csv, index=False)
        rolling_df.to_csv(route_rolling_eval_latest_csv, index=False)
        route_keys = sorted(set(rolling_df["route_key"].astype(str).tolist())) if "route_key" in rolling_df.columns else []
        if len(route_keys) == 1:
            safe_route = route_keys[0].replace("/", "_").replace("\\", "_")
            route_rolling_eval_route_latest_csv = out_dir / f"route_rolling_eval_{safe_route}_latest.csv"
            rolling_df.to_csv(route_rolling_eval_route_latest_csv, index=False)

    if route_summaries:
        impact_rows: list[dict] = []
        for r in route_summaries:
            route_key = r.get("route_key")
            for stage_name, key in [
                ("stage_a", "two_stage_stage_a_top_features"),
                ("stage_b", "two_stage_stage_b_top_features"),
            ]:
                for item in (r.get(key) or []):
                    impact_rows.append(
                        {
                            "route_key": route_key,
                            "rows": r.get("rows"),
                            "beats_zero_rmse": r.get("beats_zero_rmse"),
                            "beats_zero_mae": r.get("beats_zero_mae"),
                            "sparse_stage_b": r.get("sparse_stage_b"),
                            "min_move_delta": r.get("min_move_delta"),
                            "min_stage_b_moves": r.get("min_stage_b_moves"),
                            "stage": stage_name,
                            "rank": item.get("rank"),
                            "feature": item.get("feature"),
                            "metric_name": item.get("metric_name"),
                            "metric_value": item.get("metric_value"),
                            "is_route_engineered_feature": item.get("is_route_engineered_feature"),
                        }
                    )
        if impact_rows:
            route_feature_impact_csv = out_dir / f"route_feature_impact_{ts}.csv"
            route_feature_impact_latest_csv = out_dir / "route_feature_impact_latest.csv"
            impact_df = pd.DataFrame(impact_rows)
            impact_df = impact_df.sort_values(["route_key", "stage", "rank"], na_position="last").reset_index(drop=True)
            impact_df.to_csv(route_feature_impact_csv, index=False)
            impact_df.to_csv(route_feature_impact_latest_csv, index=False)
            route_keys = sorted(set(impact_df["route_key"].astype(str).tolist())) if "route_key" in impact_df.columns else []
            if len(route_keys) == 1:
                safe_route = route_keys[0].replace("/", "_").replace("\\", "_")
                route_feature_impact_route_latest_csv = out_dir / f"route_feature_impact_{safe_route}_latest.csv"
                impact_df.to_csv(route_feature_impact_route_latest_csv, index=False)

    return {
        "json": str(json_path),
        "md": str(md_path),
        "latest_json": str(latest_json),
        "latest_md": str(latest_md),
        "route_threshold_tuning_csv": str(route_threshold_csv) if route_threshold_csv else None,
        "route_threshold_tuning_latest_csv": (
            str(route_threshold_latest_csv) if route_threshold_latest_csv else None
        ),
        "route_threshold_tuning_route_latest_csv": (
            str(route_threshold_route_latest_csv) if route_threshold_route_latest_csv else None
        ),
        "route_rolling_eval_csv": str(route_rolling_eval_csv) if route_rolling_eval_csv else None,
        "route_rolling_eval_latest_csv": (
            str(route_rolling_eval_latest_csv) if route_rolling_eval_latest_csv else None
        ),
        "route_rolling_eval_route_latest_csv": (
            str(route_rolling_eval_route_latest_csv) if route_rolling_eval_route_latest_csv else None
        ),
        "route_feature_impact_csv": str(route_feature_impact_csv) if route_feature_impact_csv else None,
        "route_feature_impact_latest_csv": (
            str(route_feature_impact_latest_csv) if route_feature_impact_latest_csv else None
        ),
        "route_feature_impact_route_latest_csv": (
            str(route_feature_impact_route_latest_csv) if route_feature_impact_route_latest_csv else None
        ),
    }


def main() -> None:
    args = _parse_args()
    input_csv = Path(args.input_csv)
    out_dir = Path(args.output_dir)

    df = _load_dataset(input_csv)
    rows_loaded = len(df)
    df = _apply_filters(df, args)
    rows_after_filters = len(df)
    if df.empty:
        raise SystemExit("No rows after filters")

    # Keep sorted for time split reproducibility and debugging.
    if "observed_at_utc" in df.columns:
        df = df.sort_values(["observed_at_utc", "flight_key"], na_position="last").reset_index(drop=True)

    X, meta = _prepare_feature_matrix(df, args)

    # Coverage stats for v2 route-level priors.
    pg_cols = meta["party_gap_feature_cols"]
    pg_non_null = {c: int(df[c].notna().sum()) for c in pg_cols if c in df.columns}

    classification = _classification_run(df, X, meta, args)
    regression = _regression_run(df, X, meta, args)
    two_stage_regression = _two_stage_regression_run(df, X, meta, args)
    route_threshold_summaries, route_threshold_tuning_reports, route_rolling_evaluation_reports = _build_route_threshold_summaries(df, args)

    summary = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "dataset": {
            "input_csv": str(input_csv),
            "rows_loaded": int(rows_loaded),
            "rows_after_filters": int(rows_after_filters),
            "filters": {
                "airline": args.airline,
                "route_group": args.route_group,
                "origin": args.origin,
                "destination": args.destination,
                "cabin": args.cabin,
                "adt": args.adt,
                "chd": args.chd,
                "inf": args.inf,
                "test_fraction": args.test_fraction,
                "max_rows": args.max_rows,
                "min_move_delta": args.min_move_delta,
                "min_test_moves": args.min_test_moves,
                "min_stage_b_moves": args.min_stage_b_moves,
                "rolling_viability_rule": args.rolling_viability_rule,
                "rolling_viability_min_beat_folds": args.rolling_viability_min_beat_folds,
                "stage_a_calibration": args.stage_a_calibration,
                "stage_a_calibration_cv": args.stage_a_calibration_cv,
                "stage_b_model": args.stage_b_model,
                "feature_ablation": args.feature_ablation,
            },
            "observed_at_min_utc": str(df["observed_at_utc"].min()) if "observed_at_utc" in df.columns else None,
            "observed_at_max_utc": str(df["observed_at_utc"].max()) if "observed_at_utc" in df.columns else None,
        },
        "features": {
            "feature_count": int(len(meta["feature_cols"])),
            "numeric_feature_count": int(len(meta["numeric_cols"])),
            "categorical_feature_count": int(len(meta["categorical_cols"])),
            "party_gap_profile_feature_count": int(len(pg_cols)),
            "party_gap_profile_feature_cols": pg_cols,
            "party_gap_profile_non_null_counts": pg_non_null,
            "route_engineered_feature_count": int(len(meta.get("route_engineered_feature_cols", []))),
            "route_engineered_feature_cols": meta.get("route_engineered_feature_cols", []),
            "feature_ablation": meta.get("feature_ablation", "none"),
        },
        "classification": classification,
        "regression": regression,
        "two_stage_regression": two_stage_regression,
        "route_threshold_summaries": route_threshold_summaries,
        "route_threshold_tuning_reports": route_threshold_tuning_reports,
        "route_rolling_evaluation_reports": route_rolling_evaluation_reports,
    }
    paths = _write_outputs(summary, out_dir, args.timestamp_tz)
    summary["artifacts"] = paths

    # Rewrite JSON after artifact paths added.
    Path(paths["latest_json"]).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    Path(paths["json"]).write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(
        "inventory_state_baseline:"
        f" rows={rows_after_filters}"
        f" features={len(meta['feature_cols'])}"
        f" clf={'skipped' if classification.get('skipped') else 'ok'}"
        f" reg={'skipped' if regression.get('skipped') else 'ok'}"
        f" reg2={'skipped' if two_stage_regression.get('skipped') else 'ok'}"
        f" -> {paths['json']}, {paths['md']}"
    )


if __name__ == "__main__":
    main()
