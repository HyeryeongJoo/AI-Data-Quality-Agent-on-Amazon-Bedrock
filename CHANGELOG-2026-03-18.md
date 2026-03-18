# AI DQ Agent 변경 이력 — 2026-03-18

## 1. 판정 캐시 전체 제거

DynamoDB 기반 판정 캐시를 전체 코드베이스에서 제거했습니다.

### 변경 파일

| 파일 | 변경 내용 |
|------|-----------|
| `agent/src/ai_dq_agent/tools/cache_tools.py` | 파일 삭제 |
| `agent/src/ai_dq_agent/tools/__init__.py` | `judgment_cache_read`, `judgment_cache_write` import/export 제거 |
| `agent/src/ai_dq_agent/agents/dq_semantic_agent.py` | cache 도구, `_build_cache_key`, cache read/write 로직, `cache_hit_count` 제거 |
| `agent/src/ai_dq_agent/agents/llm_analyzer.py` | cache 로직 제거, 직접 LLM 분석으로 변경 |
| `agent/src/ai_dq_agent/agents/_node_utils.py` | `cache_hit_count` 로그 출력 제거 |
| `agent/src/ai_dq_agent/models/analysis.py` | `cache_hit_count: int = 0` 필드 제거 |
| `agent/src/ai_dq_agent/settings.py` | `dynamodb_cache_table` 설정 제거 |
| `web/frontend/src/types.ts` | `AnalysisStats`에서 `cache_hit_count` 타입 제거 |

### 제거 이유

- 캐시 히트율이 낮아 실효성 부족
- DynamoDB 의존성 및 코드 복잡도 증가
- 캐시 키 충돌 가능성으로 인한 오판정 리스크

---

## 2. 동적 규칙 생성 프로파일링 개선

고객 피드백: "LLM이 생성한 허용 코드 목록에 실제 유효한 코드가 누락되는 경우 발견"

### 근본 원인

`_run_full_profiling`에서 컬럼별 **Top-5 빈도값만** LLM에 전달 (`counter.most_common(5)`) → 출현 빈도가 낮은 유효 코드가 LLM에 보이지 않아 `allowed_values`에서 누락

### 변경 내용

| 파일 | 변경 내용 |
|------|-----------|
| `agent/src/ai_dq_agent/agents/dq_validator_agent.py` | 저카디널리티 컬럼(고유값 ≤50): 전체 고유값 전달 (`most_common()`), 고카디널리티 컬럼: Top-5 → Top-20 확대 (`most_common(20)`), `all_values_included` 플래그 추가 |
| `agent/src/ai_dq_agent/tools/rule_generate_tools.py` | 시스템 프롬프트에 보수적 규칙 생성 지시 추가 — `all_values_included: true`이면 전체값을 그대로 `allowed_values`로 사용, `false`이면 `allowed_values` 대신 format/pattern 기반 규칙 우선 고려 |

### 개선 효과

- 코드 컬럼 등 고유값이 50개 이하인 컬럼은 모든 유효 코드가 LLM에 전달되어 희소 코드 누락 방지
- 고카디널리티 컬럼은 LLM이 불완전한 값 목록임을 인지하고 보수적으로 규칙 생성

---

## 3. 프론트엔드 UI 개선

### 3-1. 탭 이름 변경 (`App.tsx`)

| 변경 전 | 변경 후 |
|---------|---------|
| 데이터 검증 (기존) | 기본 검증 (규칙 + LLM) |
| 데이터 검증 (이상치탐지) | 확장 검증 (규칙 + 이상치 + LLM) |

### 3-2. 단계 이름 변경 (`AgentIntro.tsx`, `ValidationResults.tsx`)

- "DQ Validator" → "Rule Validator" 전체 변경 (10개소)

### 3-3. 검증 결과 요약 메트릭 개선 (`ValidationResults.tsx`)

**v1 (기본 검증) — 4컬럼 1행:**

```
규칙 기반 의심 항목 → LLM 분석 대상 → 오탐 제거 (정상 판정) → LLM 오류 판정
```

**v2 (확장 검증) — 3컬럼 2행:**

```
[Row 1: 의심 항목 수집]
규칙 기반 의심 항목 → 이상치 탐지 추가 의심 → LLM 분석 대상 (합산)

[Row 2: LLM 분석 결과]
오탐 제거 (정상 판정) → LLM 오류 판정 (HIGH·MEDIUM·LOW) → 오탐율
```

- 파이프라인 흐름 순서(좌→우)로 메트릭 재배치
- 이상치 탐지 메트릭 추가: 통계적/맥락적 건수, 탐지 기법 표시

### 3-4. Popover 툴팁 추가 (`ValidationResults.tsx`)

**오탐 제거 설명:**
- 오탐 제거란 무엇인지 실제 예시와 함께 설명
- 예: 중량 0.005kg → 규칙: 범위 초과 → LLM: "서류 배송이므로 정상" → 오탐 제거

**통계적 vs 맥락적 이상치 탐지:**
- 통계적: 값 하나만 보고 전체 분포에서 극단적인지 판단 (Z-Score, IQR, Isolation Forest)
- 맥락적: 값 자체는 정상이지만 다른 조건과 함께 보면 이상 (조건부 이상치, 상관관계 이탈, 희귀 조합)
- 한 줄 요약: "혼자 보면 멀쩡한데, 같이 보면 이상한 것"

### 3-5. 상태 컬럼 3-state (`ValidationResults.tsx`)

| 상태 | 조건 | 표시 |
|------|------|------|
| 오류 확정 | `judgment.is_error === true` | StatusIndicator type="error" |
| 정상 판정 (오탐) | `judgment.is_error === false` | StatusIndicator type="success" |
| 미판정 | judgment 없음 | StatusIndicator type="pending" |

### 3-6. 전체 컬럼 정렬 가능 (`ValidationResults.tsx`)

- 레코드 ID: `sortingField`
- 출처, 상태, 오류 유형, 심각도, 위반 건수, 신뢰도, 판정 근거, 보정 추천: `sortingComparator`

### 3-7. 보정 추천 안내 문구 (`ValidationResults.tsx`)

테이블 description에 추가:
> 보정 추천은 형식 오류(우편번호, 전화번호), 범위 초과(음수 중량) 등 올바른 값을 추론할 수 있는 경우에만 제공됩니다. 통계적 이상치는 이상 여부만 판단하고 올바른 값을 알 수 없어 보정 추천이 제공되지 않습니다.

### 3-8. 의심 항목 고유 레코드 기준 통일 (`ValidationResults.tsx`)

- 규칙 기반 의심 항목 수를 행 기준(`suspect_count`) → 고유 `record_id` 기준으로 변경
- "고유 레코드 기준" 라벨 추가

### 3-9. Judgment dedup 불일치 수정 (`ValidationResults.tsx`)

- **문제**: 같은 record_id에 judgment가 복수 존재 시, 백엔드는 first-wins / 프론트엔드는 last-wins → 요약과 상세 테이블 간 오탐 건수 불일치
- **수정**: 프론트엔드도 first-wins로 통일

---

## 4. 아키텍처 설명 업데이트 (`AgentIntro.tsx`)

- 기술 스택 테이블에서 "판정 캐시 (Amazon DynamoDB)" 행 제거
- LLM Analyzer NodeCard 설명에서 캐시 언급 제거
- v2.1 업데이트 로그에 프로파일링 개선 3항목 추가:
  - 저카디널리티 컬럼 전체 고유값 전달
  - Top-5 → Top-20 확대, `all_values_included` 플래그
  - 규칙 생성 프롬프트에 `allowed_values` 보수적 생성 지시

---

## 5. 정렬 버그 수정

레코드별 상세 검증 결과 테이블의 컬럼 정렬이 올바르게 동작하지 않는 문제를 수정했습니다.

### 변경 파일

| 파일 | 변경 내용 |
|------|-----------|
| `web/frontend/src/components/ValidationResults.tsx` | 4개 컬럼 정렬 로직 수정 |

### 수정 내용

| 컬럼 | 수정 전 | 수정 후 |
|------|---------|---------|
| 레코드 ID | 문자열 비교 (`'9' > '10'`으로 잘못 정렬) | 숫자 비교, 숫자가 아닌 경우 문자열 fallback |
| 검증 유형 | 알파벳 순 (`both < llm < rule`, 의미 없음) | 의미 기반 순서: `both(규칙+LLM)` → `rule(규칙)` → `llm(LLM)` |
| 심각도 | suspects 비어있으면 `Math.min()` → `Infinity` → `NaN`으로 정렬 깨짐 | 빈 배열일 때 fallback 값 적용 |
| defaultState | `sortingField: 'record_id'`가 `sortingComparator`와 충돌 | 기본 정렬 없이 초기화 |

---

## 6. 배포

| 대상 | 상태 |
|------|------|
| 프론트엔드 (S3 → EC2 → CloudFront) | 배포 완료 |
| Bedrock AgentCore Runtime | 재배포 완료 |

---

## 7. 커밋 이력

| 커밋 | 메시지 |
|------|--------|
| `007bdb9` | Remove judgment cache, improve rule generation profiling, enhance validation UI |
| `8caf799` | Rename validation tab labels for clarity |
| `7878955` | Fix suspect count to unique record basis and fix judgment dedup mismatch |
| `2aac2b5` | Fix sorting bugs in detail table columns |
