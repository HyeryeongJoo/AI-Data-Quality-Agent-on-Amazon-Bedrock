"""Anomaly Detector — statistical and contextual anomaly detection node.

This node detects anomalies that rule-based validation might miss:
1. Statistical anomalies: Z-Score, IQR, Isolation Forest
2. Contextual anomalies: Conditional, Correlation, Rare Combination

Pipeline position: rule_validator → anomaly_detector → llm_analyzer
"""

import logging
from collections import Counter, defaultdict

import numpy as np

from ai_dq_agent.agents._node_utils import node_wrapper
from ai_dq_agent.tools import pipeline_state_write, s3_read_objects, s3_write_objects

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions for column type detection
# ---------------------------------------------------------------------------


def _get_numeric_columns(records: list[dict]) -> list[str]:
    """Identify numeric columns from sample records."""
    if not records:
        return []

    numeric_cols = []
    sample = records[:100]  # Check first 100 records

    # Get all column names
    all_cols = set()
    for r in sample:
        all_cols.update(r.keys())

    for col in all_cols:
        values = [r.get(col) for r in sample if r.get(col) is not None]
        if not values:
            continue
        # Check if values are numeric
        numeric_count = sum(1 for v in values if isinstance(v, (int, float)) and not isinstance(v, bool))
        if numeric_count / len(values) > 0.8:  # 80% threshold
            numeric_cols.append(col)

    return sorted(numeric_cols)


def _get_categorical_columns(records: list[dict], exclude_cols: list[str] | None = None) -> list[str]:
    """Identify categorical columns (string with limited unique values)."""
    if not records:
        return []

    exclude = set(exclude_cols or [])
    exclude.add("record_id")  # Always exclude ID columns

    categorical_cols = []
    sample = records[:100]

    all_cols = set()
    for r in sample:
        all_cols.update(r.keys())

    for col in all_cols:
        if col in exclude:
            continue

        values = [r.get(col) for r in sample if r.get(col) is not None]
        if not values:
            continue

        # Check if values are strings with limited unique values
        str_count = sum(1 for v in values if isinstance(v, str))
        if str_count / len(values) > 0.8:
            unique_ratio = len(set(values)) / len(values)
            if unique_ratio < 0.5:  # Less than 50% unique = likely categorical
                categorical_cols.append(col)

    return sorted(categorical_cols)


# ---------------------------------------------------------------------------
# Statistical anomaly detection functions
# ---------------------------------------------------------------------------


def detect_zscore(
    records: list[dict],
    numeric_cols: list[str],
    threshold: float = 3.0,
) -> list[dict]:
    """Z-Score based univariate anomaly detection.

    Flags values where |z-score| > threshold (default: 3.0).
    """
    anomalies = []

    for col in numeric_cols:
        values = [r.get(col) for r in records
                  if r.get(col) is not None and isinstance(r.get(col), (int, float)) and not isinstance(r.get(col), bool)]
        if len(values) < 10:  # Need enough samples
            continue

        arr = np.array(values, dtype=float)
        mean = np.mean(arr)
        std = np.std(arr)
        if std == 0:
            continue

        for r in records:
            val = r.get(col)
            if val is None or not isinstance(val, (int, float)) or isinstance(val, bool):
                continue

            z = abs((val - mean) / std)
            if z > threshold:
                anomalies.append({
                    "record_id": str(r.get("record_id", "")),
                    "rule_id": f"anomaly_zscore_{col}",
                    "error_type": "statistical_anomaly",
                    "method": "zscore",
                    "target_columns": [col],
                    "current_values": {col: val},
                    "reason": f"Z-Score {z:.2f} > {threshold} (평균 {mean:.2f}, 표준편차 {std:.2f})",
                    "severity": "info",
                })

    return anomalies


def detect_iqr(
    records: list[dict],
    numeric_cols: list[str],
    k: float = 1.5,
) -> list[dict]:
    """IQR (Interquartile Range) based anomaly detection.

    Flags values outside [Q1 - k*IQR, Q3 + k*IQR].
    """
    anomalies = []

    for col in numeric_cols:
        values = [r.get(col) for r in records
                  if r.get(col) is not None and isinstance(r.get(col), (int, float)) and not isinstance(r.get(col), bool)]
        if len(values) < 10:
            continue

        arr = np.array(values, dtype=float)
        q1, q3 = np.percentile(arr, [25, 75])
        iqr = q3 - q1
        if iqr == 0:
            continue

        lower = q1 - k * iqr
        upper = q3 + k * iqr

        for r in records:
            val = r.get(col)
            if val is None or not isinstance(val, (int, float)) or isinstance(val, bool):
                continue

            if val < lower or val > upper:
                anomalies.append({
                    "record_id": str(r.get("record_id", "")),
                    "rule_id": f"anomaly_iqr_{col}",
                    "error_type": "statistical_anomaly",
                    "method": "iqr",
                    "target_columns": [col],
                    "current_values": {col: val},
                    "reason": f"IQR 범위 [{lower:.2f}, {upper:.2f}] 밖 (Q1={q1:.2f}, Q3={q3:.2f})",
                    "severity": "info",
                })

    return anomalies


def detect_isolation_forest(
    records: list[dict],
    numeric_cols: list[str],
    contamination: float = 0.02,
) -> list[dict]:
    """Isolation Forest based multivariate anomaly detection.

    Uses scikit-learn's IsolationForest to detect anomalies in multi-dimensional space.
    """
    if len(records) < 20 or len(numeric_cols) < 2:
        return []

    try:
        import pandas as pd
        from sklearn.ensemble import IsolationForest
    except ImportError:
        logger.warning("scikit-learn not installed, skipping Isolation Forest")
        return []

    anomalies = []

    # Build feature matrix
    df = pd.DataFrame(records)
    available_cols = [c for c in numeric_cols if c in df.columns]
    if len(available_cols) < 2:
        return []

    features = df[available_cols].apply(pd.to_numeric, errors="coerce").fillna(0)

    # Fit model
    model = IsolationForest(contamination=contamination, random_state=42, n_jobs=-1)
    predictions = model.fit_predict(features)

    # Collect anomalies
    for i, pred in enumerate(predictions):
        if pred == -1:  # Anomaly
            r = records[i]
            anomalies.append({
                "record_id": str(r.get("record_id", "")),
                "rule_id": "anomaly_isolation_forest",
                "error_type": "statistical_anomaly",
                "method": "isolation_forest",
                "target_columns": available_cols,
                "current_values": {c: r.get(c) for c in available_cols},
                "reason": f"다변량 Isolation Forest 이상치 (분석 컬럼: {', '.join(available_cols)})",
                "severity": "info",
            })

    return anomalies


# ---------------------------------------------------------------------------
# Contextual anomaly detection functions
# ---------------------------------------------------------------------------


def detect_conditional_anomaly(
    records: list[dict],
    category_col: str,
    value_col: str,
    threshold: float = 2.5,
) -> list[dict]:
    """Conditional outlier detection within categories.

    Detects values that are outliers within their category group.
    Example: A laptop weighing 10kg is normal globally (0-50kg range),
             but abnormal for the "laptop" category (avg 1.5kg).
    """
    anomalies = []

    # Group by category
    groups: dict[str, list[tuple[dict, float]]] = defaultdict(list)
    for r in records:
        cat = r.get(category_col)
        val = r.get(value_col)
        if cat is not None and val is not None and isinstance(val, (int, float)):
            groups[str(cat)].append((r, val))

    # Detect anomalies within each group
    for cat, items in groups.items():
        if len(items) < 5:  # Need enough samples per category
            continue

        values = [v for _, v in items]
        mean = np.mean(values)
        std = np.std(values)
        if std == 0:
            continue

        for r, val in items:
            z = abs((val - mean) / std)
            if z > threshold:
                anomalies.append({
                    "record_id": str(r.get("record_id", "")),
                    "rule_id": f"anomaly_conditional_{category_col}_{value_col}",
                    "error_type": "contextual_anomaly",
                    "method": "conditional",
                    "target_columns": [category_col, value_col],
                    "current_values": {category_col: cat, value_col: val},
                    "reason": f"'{cat}' 그룹 내 이상치 (그룹 평균 {mean:.2f}, Z-Score {z:.2f})",
                    "severity": "info",
                })

    return anomalies


def detect_correlation_anomaly(
    records: list[dict],
    col_pairs: list[tuple[str, str]],
    threshold: float = 3.0,
) -> list[dict]:
    """Cross-column correlation based anomaly detection.

    Detects records where the relationship between two columns deviates
    significantly from the expected correlation.
    Example: Distance is short but delivery time is very long.
    """
    try:
        from sklearn.linear_model import LinearRegression
    except ImportError:
        logger.warning("scikit-learn not installed, skipping correlation anomaly detection")
        return []

    anomalies = []

    for col_x, col_y in col_pairs:
        # Collect valid data points
        valid_data = []
        for r in records:
            x_val = r.get(col_x)
            y_val = r.get(col_y)
            if x_val is not None and y_val is not None:
                if isinstance(x_val, (int, float)) and isinstance(y_val, (int, float)):
                    valid_data.append((r, x_val, y_val))

        if len(valid_data) < 20:
            continue

        X = np.array([[d[1]] for d in valid_data])
        y = np.array([d[2] for d in valid_data])

        # Fit linear regression
        model = LinearRegression().fit(X, y)
        predictions = model.predict(X)
        residuals = y - predictions

        # Find outliers in residuals
        residual_std = np.std(residuals)
        if residual_std == 0:
            continue

        for i, (r, x_val, y_val) in enumerate(valid_data):
            z = abs(residuals[i]) / residual_std
            if z > threshold:
                anomalies.append({
                    "record_id": str(r.get("record_id", "")),
                    "rule_id": f"anomaly_correlation_{col_x}_{col_y}",
                    "error_type": "contextual_anomaly",
                    "method": "correlation",
                    "target_columns": [col_x, col_y],
                    "current_values": {col_x: x_val, col_y: y_val},
                    "reason": f"{col_x}-{col_y} 상관관계 이탈 (예상 {predictions[i]:.2f}, 실제 {y_val}, 잔차 Z-Score {z:.2f})",
                    "severity": "info",
                })

    return anomalies


def detect_rare_combination(
    records: list[dict],
    combination_cols: list[str],
    min_support: float = 0.01,
) -> list[dict]:
    """Rare value combination detection.

    Detects records with unusual combinations of categorical values.
    Example: ('Delivered', 'Payment Pending') is a rare combination.
    """
    anomalies = []

    # Build combinations
    combinations = []
    for r in records:
        combo = tuple(str(r.get(col, "")) for col in combination_cols)
        if "" not in combo:
            combinations.append((r, combo))

    if not combinations:
        return []

    # Count combination frequencies
    combo_counts = Counter(c for _, c in combinations)
    total = len(combinations)

    # Detect rare combinations
    for r, combo in combinations:
        support = combo_counts[combo] / total
        if support < min_support:
            anomalies.append({
                "record_id": str(r.get("record_id", "")),
                "rule_id": f"anomaly_rare_combination_{'_'.join(combination_cols)}",
                "error_type": "contextual_anomaly",
                "method": "rare_combination",
                "target_columns": combination_cols,
                "current_values": dict(zip(combination_cols, combo)),
                "reason": f"희귀 조합 (출현율 {support:.2%}, 기준 {min_support:.2%} 미만)",
                "severity": "info",
            })

    return anomalies


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


@node_wrapper("anomaly_detector")
def invoke_anomaly_detector(state: dict) -> dict:
    """Run statistical and contextual anomaly detection.

    This node runs after rule_validator and adds additional suspects
    that were not caught by rule-based validation.

    Expects state keys:
        - s3_staging_prefix: S3 path prefix for pipeline data
        - suspects_s3_path: Path to existing suspects from rule_validator
        - suspect_count: Current suspect count
        - anomaly_methods: List of methods to use (optional)

    Updates state with:
        - suspect_count: Updated count including anomalies
        - anomaly_stats: Statistics about detected anomalies
    """
    result = {**state}
    pipeline_id = state.get("pipeline_id", "unknown")

    # Get configuration
    methods = state.get("anomaly_methods", [
        "zscore", "iqr", "isolation_forest",
        "conditional", "correlation", "rare_combination",
    ])

    logger.info("[%s] anomaly_detector: methods=%s", pipeline_id, methods)

    # Load all records for anomaly detection
    read_resp = s3_read_objects(
        s3_path=f"{state['s3_staging_prefix']}data.jsonl",
        file_format="jsonl",
    )
    records = read_resp.get("records", [])

    if not records:
        logger.warning("[%s] No records to analyze", pipeline_id)
        result["anomaly_stats"] = {"total_added": 0}
        return result

    # Identify column types
    numeric_cols = _get_numeric_columns(records)
    categorical_cols = _get_categorical_columns(records, exclude_cols=numeric_cols)

    logger.info(
        "[%s] Detected columns: numeric=%s, categorical=%s",
        pipeline_id, numeric_cols, categorical_cols
    )

    all_anomalies: list[dict] = []

    # --- Statistical anomaly detection ---
    if "zscore" in methods and numeric_cols:
        zscore_anomalies = detect_zscore(records, numeric_cols)
        all_anomalies.extend(zscore_anomalies)
        logger.info("[%s] Z-Score anomalies: %d", pipeline_id, len(zscore_anomalies))

    if "iqr" in methods and numeric_cols:
        iqr_anomalies = detect_iqr(records, numeric_cols)
        all_anomalies.extend(iqr_anomalies)
        logger.info("[%s] IQR anomalies: %d", pipeline_id, len(iqr_anomalies))

    if "isolation_forest" in methods and len(numeric_cols) >= 2:
        if_anomalies = detect_isolation_forest(records, numeric_cols)
        all_anomalies.extend(if_anomalies)
        logger.info("[%s] Isolation Forest anomalies: %d", pipeline_id, len(if_anomalies))

    # --- Contextual anomaly detection ---
    if "conditional" in methods and categorical_cols and numeric_cols:
        # Test category-value combinations (limit to avoid explosion)
        conditional_anomalies = []
        for cat_col in categorical_cols[:3]:  # Top 3 categorical
            for num_col in numeric_cols[:3]:  # Top 3 numeric
                conditional_anomalies.extend(
                    detect_conditional_anomaly(records, cat_col, num_col)
                )
        all_anomalies.extend(conditional_anomalies)
        logger.info("[%s] Conditional anomalies: %d", pipeline_id, len(conditional_anomalies))

    if "correlation" in methods and len(numeric_cols) >= 2:
        # Test numeric column pairs (limit to top 5 pairs)
        col_pairs = [
            (numeric_cols[i], numeric_cols[j])
            for i in range(len(numeric_cols))
            for j in range(i + 1, len(numeric_cols))
        ][:5]
        correlation_anomalies = detect_correlation_anomaly(records, col_pairs)
        all_anomalies.extend(correlation_anomalies)
        logger.info("[%s] Correlation anomalies: %d", pipeline_id, len(correlation_anomalies))

    if "rare_combination" in methods and len(categorical_cols) >= 2:
        rare_anomalies = detect_rare_combination(records, categorical_cols[:2])
        all_anomalies.extend(rare_anomalies)
        logger.info("[%s] Rare combination anomalies: %d", pipeline_id, len(rare_anomalies))

    # --- Deduplicate by record_id ---
    seen: set[str] = set()
    unique_anomalies: list[dict] = []
    for a in all_anomalies:
        rid = a["record_id"]
        if rid not in seen:
            seen.add(rid)
            unique_anomalies.append(a)

    # --- Load existing suspects and merge ---
    existing_suspects: list[dict] = []
    if state.get("suspects_s3_path"):
        try:
            existing_resp = s3_read_objects(
                s3_path=state["suspects_s3_path"],
                file_format="jsonl",
            )
            existing_suspects = existing_resp.get("records", [])
        except Exception as e:
            logger.warning("[%s] Could not load existing suspects: %s", pipeline_id, e)

    # Exclude anomalies already in suspects
    existing_ids = {str(s.get("record_id", "")) for s in existing_suspects}
    new_anomalies = [a for a in unique_anomalies if a["record_id"] not in existing_ids]

    # Merge
    merged_suspects = existing_suspects + new_anomalies

    # --- Write updated suspects to S3 ---
    suspects_s3_path = state.get("suspects_s3_path", f"{state['s3_staging_prefix']}suspects.jsonl")
    if merged_suspects:
        s3_write_objects(
            s3_path=suspects_s3_path,
            data=merged_suspects,
            file_format="jsonl",
        )

    # --- Compute statistics ---
    statistical_count = len([a for a in new_anomalies if a["error_type"] == "statistical_anomaly"])
    contextual_count = len([a for a in new_anomalies if a["error_type"] == "contextual_anomaly"])

    result["suspects_s3_path"] = suspects_s3_path
    result["suspect_count"] = len(merged_suspects)
    result["anomaly_stats"] = {
        "pipeline_id": pipeline_id,
        "statistical_count": statistical_count,
        "contextual_count": contextual_count,
        "total_added": len(new_anomalies),
        "methods_used": methods,
        "numeric_cols_analyzed": numeric_cols,
        "categorical_cols_analyzed": categorical_cols,
    }
    result["_records_processed"] = len(records)

    # Update pipeline state
    pipeline_state_write(key="suspect_count", value=len(merged_suspects))
    pipeline_state_write(key="anomaly_stats", value=result["anomaly_stats"])

    logger.info(
        "[%s] anomaly_detector: added %d anomalies (statistical=%d, contextual=%d), total suspects=%d",
        pipeline_id, len(new_anomalies), statistical_count, contextual_count, len(merged_suspects)
    )

    return result
