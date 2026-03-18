# 이상치 탐지 노드 추가 및 버전별 파이프라인 구현 계획

## Context

현재 DQ Agent 파이프라인은 `rule_validator → llm_analyzer` 구조로, 규칙 기반 검증에서 발견된 의심 항목만 LLM이 분석합니다. 이 구조의 한계는 **규칙으로 잡히지 않지만, 통계적으로 그리고 문맥상으로 이상한 데이터**를 놓칠 수 있다는 점입니다.

예를 들어:
- **통계적 이상**: 대부분의 택배 무게가 1~3kg인데 25kg인 데이터 (규칙 범위 내지만 분포상 이상)
- **문맥적 이상**: "노트북"인데 무게가 10kg (품목-무게 조합이 비정상)
- **패턴 이상**: 한 주소로 하루 100건 배송 (개별 규칙은 통과하지만 집계 패턴이 이상)

이러한 데이터는 규칙 기반 검증을 통과하므로 LLM 분석 대상에서 제외되어, 결국 검출되지 않습니다.

이 계획은 **이상치 탐지(Anomaly Detection) 노드**를 추가한 v2 파이프라인을 구현하여, 규칙을 통과했지만 통계적/문맥적으로 의심스러운 데이터를 추가로 발견하고, 이를 LLM 분석 대상에 포함시킵니다. 사용자는 기존 버전(v1)과 새 버전(v2)을 각각 실행하여 검출률을 비교할 수 있습니다.

---

## 아키텍처 변경

### 파이프라인 비교

```
[v1 - 기존]
coordinator → rule_validator → llm_analyzer → report_notify → correction

[v2 - 이상치탐지 추가]
coordinator → rule_validator → anomaly_detector → llm_analyzer → report_notify → correction
                    │                  │
                    │                  └─ 통계적 이상치를 suspects에 추가
                    └─ 규칙 기반 suspects
```

### 이상치 탐지 기법 (AWS 아키텍처)

| 기법 | 구현 방식 | AWS 서비스 | 특징 |
|------|----------|-----------|------|
| **Z-Score** | Python (numpy) | EC2/AgentCore | 단변량, 정규분포 가정, 빠름 |
| **IQR** | Python (numpy) | EC2/AgentCore | 단변량, 이상치에 강건 |
| **Isolation Forest** | scikit-learn | EC2/AgentCore | 다변량, ML 기반, 복잡한 패턴 |

**모든 기법을 구현**하고, 사용자가 UI에서 원하는 기법을 선택할 수 있도록 합니다.

---

## 문맥적 이상 탐지 (Contextual Anomaly Detection)

### 문맥적 이상이란?

개별 값은 정상 범위 내지만, **다른 컬럼과의 관계**나 **도메인 지식** 관점에서 비정상인 데이터입니다.

```
예시: 택배 데이터
┌─────────────┬──────────┬────────────┐
│ 품목        │ 무게(kg) │ 개별 검증  │
├─────────────┼──────────┼────────────┤
│ 노트북      │ 10.0     │ ✅ 0~50kg  │  ← 개별로는 정상
│ 스마트폰    │ 5.0      │ ✅ 0~50kg  │  ← 개별로는 정상
└─────────────┴──────────┴────────────┘
       ↓ 문맥적 분석
┌─────────────┬──────────┬────────────────────┐
│ 품목        │ 무게(kg) │ 문맥적 검증        │
├─────────────┼──────────┼────────────────────┤
│ 노트북      │ 10.0     │ ❌ 노트북 평균 1.5kg│  ← 문맥상 이상
│ 스마트폰    │ 5.0      │ ❌ 스마트폰 평균 0.2kg│  ← 문맥상 이상
└─────────────┴──────────┴────────────────────┘
```

### 문맥적 이상 탐지 기법

#### 1. 조건부 이상치 탐지 (Conditional Outlier Detection)

특정 조건(카테고리, 상태 등)별로 분포를 분석하여 이상치 탐지:

```python
def detect_conditional_anomaly(records: list[dict], category_col: str, value_col: str) -> list[dict]:
    """카테고리별 조건부 이상치 탐지

    예: 품목(category_col)별 무게(value_col) 분포에서 이상치 탐지
    - 노트북 그룹에서 무게 10kg → 이상치 (노트북 평균 1.5kg)
    - 가전제품 그룹에서 무게 10kg → 정상 (가전제품 평균 8kg)
    """
    from collections import defaultdict
    import numpy as np

    # 카테고리별 그룹화
    groups = defaultdict(list)
    for r in records:
        cat = r.get(category_col)
        val = r.get(value_col)
        if cat and val is not None:
            groups[cat].append((r, val))

    anomalies = []
    for cat, items in groups.items():
        values = [v for _, v in items]
        if len(values) < 5:  # 샘플 부족 시 스킵
            continue

        mean, std = np.mean(values), np.std(values)
        if std == 0:
            continue

        for r, val in items:
            z = abs((val - mean) / std)
            if z > 2.5:  # 카테고리 내에서 이상치
                anomalies.append({
                    "record_id": r.get("record_id"),
                    "error_type": "contextual_anomaly",
                    "method": "conditional",
                    "target_columns": [category_col, value_col],
                    "current_values": {category_col: cat, value_col: val},
                    "reason": f"{cat} 그룹 내 이상치 (평균 {mean:.2f}, Z-Score {z:.2f})",
                    "severity": "info",
                })
    return anomalies
```

#### 2. 크로스컬럼 상관관계 기반 탐지 (Cross-Column Correlation)

두 컬럼 간의 예상 관계에서 벗어난 데이터 탐지:

```python
def detect_correlation_anomaly(records: list[dict], col_pairs: list[tuple[str, str]]) -> list[dict]:
    """컬럼 간 상관관계 이탈 탐지

    예: (거리, 배송시간) - 거리가 짧은데 배송시간이 긴 경우
        (무게, 배송비) - 무게가 무거운데 배송비가 낮은 경우
    """
    import numpy as np
    from sklearn.linear_model import LinearRegression

    anomalies = []
    for col_x, col_y in col_pairs:
        # 유효한 데이터만 추출
        valid_data = [(r, r.get(col_x), r.get(col_y))
                      for r in records
                      if r.get(col_x) is not None and r.get(col_y) is not None]

        if len(valid_data) < 20:
            continue

        X = np.array([[d[1]] for d in valid_data])
        y = np.array([d[2] for d in valid_data])

        # 선형 회귀로 예상값 계산
        model = LinearRegression().fit(X, y)
        predictions = model.predict(X)
        residuals = y - predictions

        # 잔차의 이상치 = 상관관계 이탈
        residual_std = np.std(residuals)
        for i, (r, x_val, y_val) in enumerate(valid_data):
            if abs(residuals[i]) > 3 * residual_std:
                anomalies.append({
                    "record_id": r.get("record_id"),
                    "error_type": "contextual_anomaly",
                    "method": "correlation",
                    "target_columns": [col_x, col_y],
                    "current_values": {col_x: x_val, col_y: y_val},
                    "reason": f"{col_x}-{col_y} 상관관계 이탈 (예상 {predictions[i]:.2f}, 실제 {y_val})",
                    "severity": "info",
                })
    return anomalies
```

#### 3. 조합 패턴 이상 탐지 (Combination Pattern)

자주 나타나지 않는 값 조합 탐지:

```python
def detect_rare_combination(records: list[dict], combination_cols: list[str], min_support: float = 0.01) -> list[dict]:
    """희귀 조합 패턴 탐지

    예: (배송상태, 결제상태) 조합
        - ('배송완료', '결제완료') → 99% → 정상
        - ('배송완료', '결제대기') → 0.1% → 희귀 → 의심
    """
    from collections import Counter

    # 조합 빈도 계산
    combinations = []
    for r in records:
        combo = tuple(r.get(col) for col in combination_cols)
        if None not in combo:
            combinations.append((r, combo))

    combo_counts = Counter(c for _, c in combinations)
    total = len(combinations)

    anomalies = []
    for r, combo in combinations:
        support = combo_counts[combo] / total
        if support < min_support:
            anomalies.append({
                "record_id": r.get("record_id"),
                "error_type": "contextual_anomaly",
                "method": "rare_combination",
                "target_columns": combination_cols,
                "current_values": dict(zip(combination_cols, combo)),
                "reason": f"희귀 조합 (출현율 {support:.2%}, 기준 {min_support:.2%})",
                "severity": "info",
            })
    return anomalies
```

### anomaly_detector 노드에서 문맥적 탐지 통합

```python
@node_wrapper("anomaly_detector")
def invoke_anomaly_detector(state: dict) -> dict:
    """통계적 + 문맥적 이상치 탐지"""
    methods = state.get("anomaly_methods", ["zscore", "iqr", "isolation_forest",
                                            "conditional", "correlation", "rare_combination"])

    all_anomalies = []

    # 통계적 이상치 탐지
    if "zscore" in methods:
        all_anomalies.extend(detect_zscore(records, numeric_cols))
    if "iqr" in methods:
        all_anomalies.extend(detect_iqr(records, numeric_cols))
    if "isolation_forest" in methods:
        all_anomalies.extend(detect_isolation_forest(records, numeric_cols))

    # 문맥적 이상치 탐지
    if "conditional" in methods:
        # 품목별 무게, 거리별 배송시간 등
        all_anomalies.extend(detect_conditional_anomaly(records, "product_category", "weight_kg"))
        all_anomalies.extend(detect_conditional_anomaly(records, "region", "delivery_days"))

    if "correlation" in methods:
        # 거리-배송시간, 무게-배송비 상관관계
        all_anomalies.extend(detect_correlation_anomaly(records, [
            ("distance_km", "delivery_days"),
            ("weight_kg", "shipping_cost"),
        ]))

    if "rare_combination" in methods:
        # 배송상태-결제상태, 배송유형-지역 조합
        all_anomalies.extend(detect_rare_combination(records, ["delivery_status", "payment_status"]))
        all_anomalies.extend(detect_rare_combination(records, ["shipping_type", "region"]))

    # ... 중복 제거 및 병합
```

### UI에서 문맥적 탐지 기법 선택

```typescript
const ANOMALY_METHOD_OPTIONS = [
  // 통계적 기법
  { value: 'zscore', label: 'Z-Score', description: '단변량 통계 (평균 ± 3σ)' },
  { value: 'iqr', label: 'IQR', description: '단변량 통계 (사분위수 기반)' },
  { value: 'isolation_forest', label: 'Isolation Forest', description: '다변량 ML 기반 탐지' },
  // 문맥적 기법
  { value: 'conditional', label: '조건부 이상치', description: '카테고리별 분포 분석 (예: 품목별 무게)' },
  { value: 'correlation', label: '상관관계 이탈', description: '컬럼 간 관계 분석 (예: 거리-배송시간)' },
  { value: 'rare_combination', label: '희귀 조합', description: '드문 값 조합 탐지 (예: 상태 조합)' },
];
```

---

## 구현 상세

### 1. Backend 변경

#### 1.1 models.py - 요청 모델 확장
**파일**: `/home/ec2-user/bedrock-dq-agent/web/backend/models.py`

```python
from typing import Literal

class RunValidationRequest(BaseModel):
    s3_data_path: str = "s3://dq-agent-staging-dev-joohyery/sample/data.jsonl"
    dry_run: bool = False
    pipeline_version: Literal["v1", "v2"] = "v1"  # "v1" (기존) or "v2" (이상치탐지)
    anomaly_methods: list[str] = [
        # 통계적 기법
        "zscore", "iqr", "isolation_forest",
        # 문맥적 기법
        "conditional", "correlation", "rare_combination"
    ]
```

#### 1.2 validation.py - AgentCore 페이로드 전달
**파일**: `/home/ec2-user/bedrock-dq-agent/web/backend/routers/validation.py`

`_invoke_agentcore()` 함수에서 `pipeline_version` 추가:
```python
payload = json.dumps({
    "trigger_type": "schedule",
    "dry_run": dry_run,
    "s3_data_path": s3_data_path,
    "pipeline_id": pipeline_id,
    "pipeline_version": pipeline_version,  # NEW
})
```

---

### 2. Agent Pipeline 변경

#### 2.1 anomaly_detector.py - 새 노드 생성
**파일**: `/home/ec2-user/bedrock-dq-agent/agent/src/ai_dq_agent/agents/anomaly_detector.py`

```python
import numpy as np
from sklearn.ensemble import IsolationForest
from ai_dq_agent.agents._node_utils import node_wrapper, validate_state_keys

def detect_zscore(records: list[dict], numeric_cols: list[str], threshold: float = 3.0) -> list[dict]:
    """Z-Score 기반 단변량 이상치 탐지"""
    anomalies = []
    for col in numeric_cols:
        values = [r.get(col) for r in records if r.get(col) is not None]
        if not values:
            continue
        mean, std = np.mean(values), np.std(values)
        if std == 0:
            continue
        for i, r in enumerate(records):
            val = r.get(col)
            if val is not None and abs((val - mean) / std) > threshold:
                anomalies.append({
                    "record_id": r.get("record_id"),
                    "error_type": "statistical_anomaly",
                    "method": "zscore",
                    "target_columns": [col],
                    "current_values": {col: val},
                    "reason": f"Z-Score {abs((val - mean) / std):.2f} > {threshold}",
                    "severity": "info",
                })
    return anomalies

def detect_iqr(records: list[dict], numeric_cols: list[str], k: float = 1.5) -> list[dict]:
    """IQR 기반 단변량 이상치 탐지"""
    anomalies = []
    for col in numeric_cols:
        values = [r.get(col) for r in records if r.get(col) is not None]
        if len(values) < 4:
            continue
        q1, q3 = np.percentile(values, [25, 75])
        iqr = q3 - q1
        lower, upper = q1 - k * iqr, q3 + k * iqr
        for r in records:
            val = r.get(col)
            if val is not None and (val < lower or val > upper):
                anomalies.append({
                    "record_id": r.get("record_id"),
                    "error_type": "statistical_anomaly",
                    "method": "iqr",
                    "target_columns": [col],
                    "current_values": {col: val},
                    "reason": f"IQR 범위 [{lower:.2f}, {upper:.2f}] 밖",
                    "severity": "info",
                })
    return anomalies

def detect_isolation_forest(records: list[dict], numeric_cols: list[str], contamination: float = 0.02) -> list[dict]:
    """Isolation Forest 기반 다변량 이상치 탐지"""
    import pandas as pd
    df = pd.DataFrame(records)
    features = df[numeric_cols].fillna(0)
    if len(features) < 10:
        return []
    model = IsolationForest(contamination=contamination, random_state=42)
    predictions = model.fit_predict(features)
    anomalies = []
    for i, pred in enumerate(predictions):
        if pred == -1:
            r = records[i]
            anomalies.append({
                "record_id": r.get("record_id"),
                "error_type": "statistical_anomaly",
                "method": "isolation_forest",
                "target_columns": numeric_cols,
                "current_values": {c: r.get(c) for c in numeric_cols},
                "reason": "다변량 Isolation Forest 이상치",
                "severity": "info",
            })
    return anomalies

@node_wrapper("anomaly_detector")
def invoke_anomaly_detector(state: dict) -> dict:
    """통계적 + 문맥적 이상치 탐지로 추가 suspects 발견"""
    result = {**state}
    methods = state.get("anomaly_methods", [
        "zscore", "iqr", "isolation_forest",
        "conditional", "correlation", "rare_combination"
    ])

    # 1. S3에서 전체 레코드 로드
    records = load_records_from_s3(state["s3_staging_prefix"])

    # 2. 컬럼 타입 분석
    numeric_cols = get_numeric_columns(records)
    categorical_cols = get_categorical_columns(records)

    all_anomalies = []

    # 3. 통계적 이상치 탐지
    if "zscore" in methods:
        all_anomalies.extend(detect_zscore(records, numeric_cols))
    if "iqr" in methods:
        all_anomalies.extend(detect_iqr(records, numeric_cols))
    if "isolation_forest" in methods:
        all_anomalies.extend(detect_isolation_forest(records, numeric_cols))

    # 4. 문맥적 이상치 탐지
    if "conditional" in methods:
        # 카테고리별 수치 분포 이상 (예: 품목별 무게)
        for cat_col in categorical_cols:
            for num_col in numeric_cols:
                all_anomalies.extend(detect_conditional_anomaly(records, cat_col, num_col))

    if "correlation" in methods:
        # 수치 컬럼 간 상관관계 이탈
        col_pairs = [(numeric_cols[i], numeric_cols[j])
                     for i in range(len(numeric_cols))
                     for j in range(i+1, len(numeric_cols))]
        all_anomalies.extend(detect_correlation_anomaly(records, col_pairs[:5]))  # 상위 5쌍

    if "rare_combination" in methods:
        # 희귀 조합 패턴
        if len(categorical_cols) >= 2:
            all_anomalies.extend(detect_rare_combination(records, categorical_cols[:2]))

    # 5. 중복 제거 (record_id 기준)
    seen = set()
    unique_anomalies = []
    for a in all_anomalies:
        key = a["record_id"]
        if key not in seen:
            seen.add(key)
            unique_anomalies.append(a)

    # 6. 기존 suspects와 병합
    existing_suspects = load_suspects_from_s3(state["suspects_s3_path"])
    merged_suspects = existing_suspects + unique_anomalies

    # 7. S3에 업데이트된 suspects 저장
    save_suspects_to_s3(merged_suspects, state["suspects_s3_path"])

    result["suspect_count"] = len(merged_suspects)
    result["anomaly_stats"] = {
        "statistical_count": len([a for a in unique_anomalies if a["error_type"] == "statistical_anomaly"]),
        "contextual_count": len([a for a in unique_anomalies if a["error_type"] == "contextual_anomaly"]),
        "total_added": len(unique_anomalies),
    }
    result["_records_processed"] = len(records)

    return result
```

#### 2.2 graph.py - 버전별 파이프라인 빌드
**파일**: `/home/ec2-user/bedrock-dq-agent/agent/src/ai_dq_agent/agents/graph.py`

```python
class DQPipeline:
    def __init__(self, pipeline_version: str = "v1") -> None:
        self._state: dict = {}
        self._version = pipeline_version
        self._graph = self._build_graph()

    def _build_graph(self):
        builder = GraphBuilder()

        # 공통 노드 등록
        builder.add_node(self._node(invoke_coordinator, "coordinator"), "coordinator")
        builder.add_node(self._node(invoke_rule_validator, "rule_validator"), "rule_validator")
        builder.add_node(self._node(invoke_llm_analyzer, "llm_analyzer"), "llm_analyzer")
        builder.add_node(self._node(invoke_report_notify, "report_notify"), "report_notify")
        builder.add_node(self._node(invoke_correction, "correction"), "correction")

        builder.set_entry_point("coordinator")
        builder.add_edge("coordinator", "rule_validator", condition=self._has_data)

        if self._version == "v2":
            # v2: anomaly_detector 노드 추가
            builder.add_node(self._node(invoke_anomaly_detector, "anomaly_detector"), "anomaly_detector")
            builder.add_edge("rule_validator", "anomaly_detector", condition=self._has_data_after_rules)
            builder.add_edge("anomaly_detector", "llm_analyzer", condition=self._has_suspects)
            builder.add_edge("anomaly_detector", "report_notify", condition=lambda s: not self._has_suspects(s))
        else:
            # v1: 기존 흐름
            builder.add_edge("rule_validator", "llm_analyzer", condition=self._has_suspects)
            builder.add_edge("rule_validator", "report_notify", condition=lambda s: not self._has_suspects(s))

        builder.add_edge("llm_analyzer", "report_notify")
        builder.add_edge("report_notify", "correction")

        return builder.build()

def build_pipeline(pipeline_version: str = "v1"):
    """버전별 파이프라인 빌드"""
    try:
        return DQPipeline(pipeline_version=pipeline_version)
    except ImportError:
        return _SimplePipeline(pipeline_version=pipeline_version)
```

#### 2.3 main.py - 버전 파라미터 처리
**파일**: `/home/ec2-user/bedrock-dq-agent/agent/src/ai_dq_agent/main.py`

```python
def run_pipeline(
    trigger_type: str = "schedule",
    event_records: list[dict] | None = None,
    dry_run: bool = False,
    s3_data_path: str | None = None,
    pipeline_id: str | None = None,
    pipeline_version: str = "v1",  # NEW
) -> dict:
    graph = build_pipeline(pipeline_version=pipeline_version)
    # ...
```

---

### 3. Frontend 변경

#### 3.1 App.tsx - 네비게이션 분리
**파일**: `/home/ec2-user/bedrock-dq-agent/web/frontend/src/App.tsx`

```typescript
const NAV_ITEMS = [
  { type: 'link', text: '에이전트 아키텍처', href: '/architecture' },
  { type: 'divider' },
  { type: 'link', text: '데이터 검증 (기존)', href: '/validation-v1' },
  { type: 'link', text: '데이터 검증 (이상치탐지)', href: '/validation-v2' },
];

const PAGE_META = {
  // 기존...
  '/validation-v1': {
    title: '데이터 검증 (기존)',
    description: '규칙 기반 검증 → LLM 분석 파이프라인',
  },
  '/validation-v2': {
    title: '데이터 검증 (이상치탐지)',
    description: '규칙 기반 검증 → 이상치 탐지 → LLM 분석 파이프라인',
  },
};

// 렌더링
{(activeHref === '/validation-v1' || activeHref === '/validation-v2') && (
  <SpaceBetween size="l">
    <SampleDataTable ... />
    <ValidationRunner
      hasData={records.length > 0}
      s3DataPath={dataSource}
      pipelineVersion={activeHref === '/validation-v2' ? 'v2' : 'v1'}
      onValidationComplete={handleValidationComplete}
      onError={notifyError}
    />
    {validationResult && <ValidationResults result={validationResult} />}
  </SpaceBetween>
)}
```

#### 3.2 ValidationRunner.tsx - 이상치 탐지 기법 선택 UI (v2 전용)
**파일**: `/home/ec2-user/bedrock-dq-agent/web/frontend/src/components/ValidationRunner.tsx`

```typescript
import Multiselect from '@cloudscape-design/components/multiselect';

const ANOMALY_METHOD_OPTIONS = [
  { value: 'zscore', label: 'Z-Score', description: '단변량 통계 (평균 ± 3σ)' },
  { value: 'iqr', label: 'IQR', description: '단변량 통계 (Q1-1.5*IQR ~ Q3+1.5*IQR)' },
  { value: 'isolation_forest', label: 'Isolation Forest', description: '다변량 ML 기반 탐지' },
];

// v2일 때만 표시
{pipelineVersion === 'v2' && (
  <FormField label="이상치 탐지 기법">
    <Multiselect
      selectedOptions={selectedMethods}
      onChange={({ detail }) => setSelectedMethods(detail.selectedOptions)}
      options={ANOMALY_METHOD_OPTIONS}
      placeholder="탐지 기법 선택"
    />
  </FormField>
)}
```

#### 3.3 ValidationRunner.tsx - 동적 스테이지 및 Props 확장
**파일**: `/home/ec2-user/bedrock-dq-agent/web/frontend/src/components/ValidationRunner.tsx`

```typescript
interface Props {
  hasData: boolean;
  s3DataPath?: string;
  pipelineVersion: 'v1' | 'v2';  // NEW
  onValidationComplete: (result: ValidationResult) => void;
  onError: (msg: string) => void;
}

const STAGE_ORDER_V1 = ['coordinator', 'rule_validator', 'llm_analyzer', 'report_notify', 'correction'];
const STAGE_ORDER_V2 = ['coordinator', 'rule_validator', 'anomaly_detector', 'llm_analyzer', 'report_notify', 'correction'];

const STAGE_LABELS: Record<string, string> = {
  coordinator: 'Coordinator (데이터 수집)',
  rule_validator: 'Rule Validator (규칙 검증)',
  anomaly_detector: 'Anomaly Detector (이상치 탐지)',  // NEW
  llm_analyzer: 'LLM Analyzer (AI 분석)',
  report_notify: 'Report & Notify (리포트 생성)',
  correction: 'Correction (데이터 보정)',
};

// 컴포넌트 내부
const [selectedMethods, setSelectedMethods] = useState([
  { value: 'zscore', label: 'Z-Score' },
  { value: 'iqr', label: 'IQR' },
  { value: 'isolation_forest', label: 'Isolation Forest' },
]);

// 버전에 따라 스테이지 순서 선택
const stageOrder = pipelineVersion === 'v2' ? STAGE_ORDER_V2 : STAGE_ORDER_V1;

// API 호출 시 선택된 기법 전달
const handleStart = async () => {
  const methods = pipelineVersion === 'v2'
    ? selectedMethods.map(m => m.value)
    : [];
  const resp = await startValidation(s3DataPath, pipelineVersion, methods);
  // ...
};
```

#### 3.4 client.ts - API 호출 수정
**파일**: `/home/ec2-user/bedrock-dq-agent/web/frontend/src/api/client.ts`

```typescript
export async function startValidation(
  s3DataPath?: string,
  pipelineVersion: string = 'v1',
  anomalyMethods: string[] = ['zscore', 'iqr', 'isolation_forest']
): Promise<{ job_id: string; status: string }> {
  const res = await fetch(`${getBaseUrl()}/api/run-validation`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      s3_data_path: s3DataPath || 's3://dq-agent-staging-dev-joohyery/sample/data.jsonl',
      dry_run: true,
      pipeline_version: pipelineVersion,
      anomaly_methods: anomalyMethods,  // v2에서 사용할 기법들
    }),
  });
  // ...
}
```

---

### 4. 의존성 추가

**파일**: `/home/ec2-user/bedrock-dq-agent/agent/pyproject.toml`

```toml
dependencies = [
    # 기존 의존성...
    "scikit-learn>=1.3.0",  # Isolation Forest
    "numpy>=1.24.0",        # Z-Score 계산
]
```

---

## 수정할 파일 목록

| 파일 | 변경 내용 |
|------|----------|
| `web/backend/models.py` | `pipeline_version`, `anomaly_methods` 필드 추가 |
| `web/backend/routers/validation.py` | AgentCore 페이로드에 버전 및 기법 전달 |
| `agent/src/ai_dq_agent/agents/anomaly_detector.py` | **새 파일** - Z-Score, IQR, Isolation Forest 이상치 탐지 |
| `agent/src/ai_dq_agent/agents/graph.py` | 버전별 파이프라인 빌드 로직 |
| `agent/src/ai_dq_agent/main.py` | `pipeline_version`, `anomaly_methods` 파라미터 추가 |
| `agent/pyproject.toml` | scikit-learn, numpy, pandas 의존성 |
| `web/frontend/src/App.tsx` | 네비게이션 분리 (v1/v2), PAGE_META 추가 |
| `web/frontend/src/components/ValidationRunner.tsx` | 동적 스테이지, 버전 prop, 기법 선택 Multiselect UI |
| `web/frontend/src/api/client.ts` | API 호출에 버전 및 기법 전달 |
| `web/frontend/src/types.ts` | anomaly 관련 타입 추가 |

---

## 검증 방법

### 1. 로컬 테스트
```bash
cd bedrock-dq-agent/agent
pip install -e ".[dev]"
python -c "from ai_dq_agent.agents.anomaly_detector import invoke_anomaly_detector; print('OK')"
```

### 2. 파이프라인 버전 테스트
```bash
# v1 테스트
python -m ai_dq_agent.main --trigger schedule --dry-run

# v2 테스트 (anomaly_detector 포함)
python -m ai_dq_agent.main --trigger schedule --dry-run --pipeline-version v2
```

### 3. 웹 UI 테스트
1. 프론트엔드 빌드: `cd web/frontend && npm run build`
2. 백엔드 실행: `cd web/backend && python main.py`
3. 브라우저에서 두 버전 각각 실행하여 비교:
   - 데이터 검증 (기존) → suspects 수 확인
   - 데이터 검증 (이상치탐지) → suspects 수 확인 (더 많아야 함)

### 4. 검출률 비교
| 지표 | v1 (기존) | v2 (이상치탐지) |
|------|----------|----------------|
| 규칙 기반 suspects | N건 | N건 |
| 이상치 suspects | - | M건 |
| 총 suspects | N건 | N+M건 |
| LLM 판정 오류 | X건 | Y건 |

---

## 구현 순서

1. Backend: models.py, validation.py (5분)
2. Agent: anomaly_detector.py 생성 (1시간)
3. Agent: graph.py 버전 분기 (30분)
4. Agent: main.py 파라미터 추가 (10분)
5. Agent: pyproject.toml 의존성 (5분)
6. Frontend: types.ts, client.ts (15분)
7. Frontend: App.tsx 네비게이션 (20분)
8. Frontend: ValidationRunner.tsx (30분)
9. 통합 테스트 (30분)
