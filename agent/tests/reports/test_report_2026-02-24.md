# ai-dq-agent-v2 E2E 테스트 보고서

- **일시**: 2026-02-24
- **Python**: 3.12.12
- **pytest**: 9.0.2
- **결과**: 22건 통과 / 0건 실패 (3.10초)

---

## 테스트 요약

| # | 테스트명 | 분류 | 결과 |
|---|---------|------|------|
| 1 | test_sample_data_generation | 데이터 | 통과 |
| 2 | test_profile_compute | 도구 | 통과 |
| 3 | test_range_check_road_addr_yn | 도구 | 통과 |
| 4 | test_range_check_status_code | 도구 | 통과 |
| 5 | test_range_check_weight_kg | 도구 | 통과 |
| 6 | test_regex_validate_tracking_id | 도구 | 통과 |
| 7 | test_regex_validate_phone_format | 도구 | 통과 |
| 8 | test_timestamp_compare_temporal | 도구 | 통과 |
| 9 | test_impact_score_compute | 도구 | 통과 |
| 10 | test_pipeline_state_read_write | 도구 | 통과 |
| 11 | test_delegation_to_semantic_analyzer | 도구 | 통과 |
| 12 | test_health_score_computation | 에이전트 | 통과 |
| 13 | test_full_pipeline_dry_run | 파이프라인 | 통과 |
| 14 | test_coordinator_with_event_records | 에이전트 | 통과 |
| 15 | test_coordinator_no_data_early_exit | 에이전트 | 통과 |
| 16 | test_graph_routing_no_data | 그래프 | 통과 |
| 17 | test_graph_routing_rule_validator | 그래프 | 통과 |
| 18 | test_graph_routing_report | 그래프 | 통과 |
| 19 | test_rule_validator_detects_all_error_types | 에이전트 | 통과 |
| 20 | test_semantic_errors_pass_all_static_rules | 시맨틱 | 통과 |
| 21 | test_error_catalog_coverage | 데이터 | 통과 |
| 22 | test_compound_records_have_multiple_errors | 에이전트 | 통과 |

---

## 테스트 분류별 설명

### 데이터 검증 (2건)
- **test_sample_data_generation**: 100건 데이터셋의 구조(20컬럼), 6개 에러 그룹(A~F)의 의도된 오류가 정확히 생성되는지 검증
- **test_error_catalog_coverage**: 에러 카탈로그에 등록된 모든 레코드 ID가 실제 데이터셋에 존재하고, 클린 레코드(1-50)가 카탈로그에 포함되지 않았는지 검증

### 도구 단위 테스트 (9건)
- **range_check 3종**: road_addr_yn 허용값(0,1), status_code 허용값(5개), weight_kg 범위(0.01~30.0) 위반 검출
- **regex_validate 2종**: tracking_id 숫자 10~15자리 패턴, 전화번호 0XX-XXXX-XXXX 패턴 위반 검출
- **timestamp_compare**: dispatch_time > arrival_time 시간순서 역전 검출
- **impact_score_compute**: 심각도별 영향도 점수 계산 및 정렬 확인
- **pipeline_state_read_write**: 에이전트 간 상태 공유(읽기/쓰기/병합) 확인
- **delegation_to_semantic_analyzer**: 규칙 검증기 → 시맨틱 분석기 동적 위임 및 pipeline_state 저장 확인

### 에이전트/그래프 테스트 (8건)
- **health_score_computation**: 건강(1.0), 경고(0.5~1.0), 위험(<0.5) 세 가지 시나리오의 헬스 스코어 계산
- **full_pipeline_dry_run**: 6개 스테이지 전체 파이프라인 dry-run 실행 (모든 AWS 서비스 모킹)
- **coordinator 2종**: 이벤트 레코드 처리(100건), 데이터 없을 때 조기 종료
- **graph_routing 3종**: coordinator 후 분기(데이터 유무), rule_validator 후 분기(의심건/위임건), report 후 분기(dry-run/승인상태)
- **rule_validator_detects_all_error_types**: 4가지 오류 유형(범위/포맷/시간/크로스컬럼) 동시 검출 확인

### 시맨틱 오류 검증 (2건)
- **test_semantic_errors_pass_all_static_rules**: Group C 레코드(66-85)가 **모든 정적 규칙을 통과**하는지 확인 → LLM 분석이 필수적임을 증명
- **test_compound_records_have_multiple_errors**: Group F 레코드(98-100)가 3가지 이상 오류 유형에 동시 검출되는지 확인

---

## 데이터셋 구성

- **총 레코드 수**: 100건 (레코드당 20개 컬럼)
- **파일**: `tests/fixtures/sample_delivery_data.jsonl` (56KB)

### 에러 그룹

| 그룹 | 레코드 | 건수 | 설명 | 검출 방법 |
|------|--------|------|------|----------|
| A (1-50) | 클린 데이터 | 50건 | 오류 없는 정상 데이터 | — |
| B (51-65) | 규칙 기반 오류 | 15건 | 범위, 포맷, 시간순서 위반 | range_check, regex_validate, timestamp_compare |
| C (66-85) | 시맨틱 오류 | 20건 | **LLM만 검출 가능한 의미적 모순** | llm_batch_analyze |
| D (86-92) | 모호한 경계 케이스 | 7건 | 반복 추론(3단계)이 필요한 애매한 건 | PRIMARY → REFLECTION → DEEP_ANALYSIS |
| E (93-97) | 크로스 컬럼 + 위임 | 5건 | 주소↔허브 불일치, 주소↔플래그 불일치 | address_normalize + delegate_to_agent |
| F (98-100) | 복합 오류 | 3건 | 1개 레코드에 3가지 이상 오류 동시 존재 | 모든 도구 |

### 컬럼 목록 (20개)

| 컬럼명 | 설명 | 데이터 타입 |
|--------|------|------------|
| record_id | 레코드 고유 번호 | string |
| tracking_id | 운송장 번호 | string |
| sender_name | 발송인 이름 | string |
| sender_phone | 발송인 전화번호 | string |
| receiver_name | 수신인 이름 | string |
| receiver_phone | 수신인 전화번호 | string |
| address | 배송 주소 | string |
| road_addr_yn | 도로명주소 여부 (0/1) | integer |
| weight_kg | 무게 (kg) | float |
| item_category | 물품 분류 | string |
| item_description | 물품 상세 설명 | string |
| dispatch_time | 발송 시각 (HH:MM:SS) | string |
| arrival_time | 도착 시각 (HH:MM:SS) | string |
| status_code | 배송 상태 코드 | string |
| delivery_attempt_count | 배송 시도 횟수 | integer |
| fee_amount | 배송비 (원) | integer |
| payment_method | 결제 방법 | string |
| cod_amount | 착불 금액 (원) | float |
| hub_code | 허브 코드 | string |
| special_handling | 특수 취급 구분 | string |

---

## 검증 규칙 (정적 8개 + 동적 1개)

| 규칙 ID | 오류 유형 | 대상 컬럼 | 심각도 | 설명 |
|---------|----------|----------|--------|------|
| R001-road_addr_yn_enum | 범위 초과 | road_addr_yn | critical | 0 또는 1만 허용 |
| R001-status_code_enum | 범위 초과 | status_code | critical | 5개 허용 상태 코드만 허용 |
| R001-weight_kg_range | 범위 초과 | weight_kg | critical | 0.01~30.0kg 범위 |
| R002-time_format_hhmmss | 포맷 불일치 | dispatch_time, arrival_time | warning | HH:MM:SS 형식 필수 |
| R002-tracking_id_format | 포맷 불일치 | tracking_id | critical | 숫자 10~15자리 |
| R002-phone_format | 포맷 불일치 | sender_phone, receiver_phone | warning | 0XX-XXXX-XXXX 형식 |
| R003-dispatch_before_arrival | 시간순서 위반 | dispatch_time → arrival_time | critical | 발송 < 도착 |
| R004-address_road_yn_match | 크로스 컬럼 불일치 | address ↔ road_addr_yn | warning | 주소 유형과 플래그 일치 |
| AUTO-001 (동적 생성) | 크로스 컬럼 불일치 | sender_name ↔ receiver_name | warning | 발송인=수신인 자기 발송 |

---

## 파이프라인 실행 결과 (Full Dry Run)

### 파이프라인 구조

```
coordinator → profiler → schema_analyzer → rule_validator
                                                ↓
              correction ← report_notify ← semantic_analyzer
```

6개 스테이지 모두 결정론적 폴백 모드(Strands Agent 미사용)로 정상 완료.

### 규칙 기반 검출 결과

| 오류 유형 | 검출 레코드 | 위반 건수 |
|----------|-----------|----------|
| 범위 초과 (out_of_range) | 51, 52, 53, 54, 55, 98, 99 | 7건 이상 |
| 포맷 불일치 (format_inconsistency) | 56, 57, 58, 59, 98 | 5건 이상 |
| 시간순서 위반 (temporal_violation) | 60, 61, 62, 63, 64, 65, 98, 100 | 8건 이상 |
| 크로스 컬럼 불일치 (cross_column) | 93, 94, 95, 96, 97 | 5건 |

---

## 시맨틱 오류 쇼케이스 (Group C — LLM 필수)

이 20건의 레코드는 **모든 정적 규칙을 통과**하지만, 의미적으로 분석하면 명백한 오류입니다.
이것이 LLM 기반 데이터 품질 검증이 필요한 핵심 이유입니다.

| 유형 | 레코드 | 구체적 예시 |
|------|--------|-----------|
| 무게 ↔ 카테고리 모순 | 66, 67, 68 | 28kg "계약서 서류", 50g "소파", 100g "65인치 TV" |
| 착불 ↔ 결제방법 불일치 | 69, 70, 71 | "선불"인데 착불 35,000원, "착불"인데 착불금액 0원 |
| 상태 ↔ 배송시도 모순 | 72, 73, 74 | PICKUP인데 5회 시도, DELIVERED인데 0회 시도 |
| 특수취급 ↔ 내용물 불일치 | 75, 76, 77 | 냉동삼겹살인데 상온, 프로그래밍 교재인데 냉장 |
| 요금 이상치 | 78, 79, 80 | 25kg인데 무료배송, 300g 서류인데 85,000원 |
| 배송 로직 이상 | 81, 82, 83 | 8회 시도인데 아직 배송중, 5분 만에 배달 완료 |
| 자기 자신 발송 | 84, 85 | 발송인과 수신인이 동일인물 + 동일 연락처 |

> **`test_semantic_errors_pass_all_static_rules` 테스트가 증명**: Group C 레코드는 어떤 정적 규칙으로도 검출되지 않으며, LLM 시맨틱 분석이 필수적입니다.

---

## 모호한 경계 케이스 (Group D — 반복 추론 대상)

| 레코드 | 시나리오 | 왜 모호한가 |
|--------|---------|------------|
| 86 | weight_kg = 29.9 | 30kg 제한 바로 아래 — 대형 정수기 필터이므로 정상일 수 있음 |
| 87 | weight_kg = 0.01 (10g) | 보석함이라면 가능, 단순 입력 오류일 수도 |
| 88 | weight_kg = 30.0 | 정확히 경계값 — 원목 사이드 테이블이면 가능 |
| 89 | 김민수 → 김민수(대리) | 동명이인인지, 대리 수령인지 판단 불가 |
| 90 | 이서연 → 이서연 (부산지점) | 본인 발송인지, 지점 간 배송인지 모호 |
| 91 | 상온 라면 + 냉장 표시 | 라면은 상온이지만 식품으로 냉장 마킹할 수도 |
| 92 | 배송출발 + 3회 시도 | 배달 당일 3차 시도는 불가능하지 않음 |

이 레코드들은 **PRIMARY → REFLECTION → DEEP_ANALYSIS** 3단계 반복 추론 루프를 작동시킵니다.

---

## 모킹된 AWS 서비스

| 서비스 | 모킹 라이브러리 | 용도 |
|--------|---------------|------|
| S3 | moto | 데이터 스테이징, 보고서, 규칙 파일 저장 |
| DynamoDB | moto | 상태, 캐시, 리니지, 프로파일, 격리 (7개 테이블) |
| Bedrock | unittest.mock | LLM converse API (시맨틱 분석) |
| Slack | unittest.mock | 알림 메시지 발송 |

---

## pytest 원본 출력

```
============================= test session starts ==============================
platform linux -- Python 3.12.12, pytest-9.0.2, pluggy-1.6.0
22 passed, 1 warning in 3.10s
======================== 22 passed, 1 warning in 3.10s =========================
```
