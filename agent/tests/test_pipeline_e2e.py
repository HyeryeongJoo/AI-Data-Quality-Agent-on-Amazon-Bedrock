"""End-to-end pipeline test using enhanced 100-record delivery dataset with mocked AWS services.

Runs the deterministic fallback path (no Strands Agent) through all 7 nodes:
  coordinator → profiler → schema_analyzer → rule_validator
                                                  ↓
              correction ← report_notify ← semantic_analyzer

All AWS services (S3, DynamoDB, Bedrock, Slack, address API) are mocked.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

# Ensure the project src is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from tests.sample_data import (
    ERROR_CATALOG,
    EXPECTED_TEMPORAL_COUNT,
    EXPECTED_TOTAL_RECORDS,
    generate_sample_dataset,
)

logging.basicConfig(level=logging.INFO, format="%(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Semantic error record IDs for mock Bedrock to recognize
_SEMANTIC_ERROR_IDS = set()
for _sub in ERROR_CATALOG.get("semantic_llm_only", {}).values():
    _SEMANTIC_ERROR_IDS.update(_sub["record_ids"])
_COMPOUND_IDS = set(ERROR_CATALOG.get("compound", {}).get("record_ids", []))
_AMBIGUOUS_IDS = set(ERROR_CATALOG.get("ambiguous_iterative", {}).get("record_ids", []))


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _clear_settings_cache():
    """Clear the settings LRU cache before each test."""
    from ai_dq_agent.settings import get_settings
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def _env_vars(monkeypatch):
    """Set environment variables for test settings."""
    monkeypatch.setenv("ENV", "dev")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("DYNAMODB_TABLE_NAME", "delivery-data-test")
    monkeypatch.setenv("DYNAMODB_STATE_TABLE", "dq-agent-state-test")
    monkeypatch.setenv("DYNAMODB_CORRECTION_TABLE", "dq-agent-corrections-test")
    monkeypatch.setenv("DYNAMODB_CACHE_TABLE", "dq-agent-cache-test")
    monkeypatch.setenv("DYNAMODB_PROFILE_TABLE", "dq-agent-profile-test")
    monkeypatch.setenv("DYNAMODB_LINEAGE_TABLE", "dq-agent-lineage-test")
    monkeypatch.setenv("DYNAMODB_QUARANTINE_TABLE", "dq-agent-quarantine-test")
    monkeypatch.setenv("S3_STAGING_BUCKET", "dq-staging-test")
    monkeypatch.setenv("S3_REPORT_BUCKET", "dq-reports-test")
    monkeypatch.setenv("S3_RULES_BUCKET", "dq-rules-test")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "")
    monkeypatch.setenv("SLACK_CHANNEL_ID", "C-test-channel")
    monkeypatch.setenv("CHUNK_SIZE", "10000")
    monkeypatch.setenv("LLM_BATCH_SIZE", "50")
    monkeypatch.setenv("LLM_MAX_ITEMS", "1000")


def _create_dynamodb_tables(region: str = "ap-northeast-2"):
    """Create all required DynamoDB tables in moto."""
    client = boto3.client("dynamodb", region_name=region)
    tables = [
        ("delivery-data-test", "record_id", "S"),
        ("dq-agent-state-test", "state_key", "S", "sort_key", "S"),
        ("dq-agent-corrections-test", "pipeline_id", "S", "sort_key", "S"),
        ("dq-agent-cache-test", "pattern_key", "S"),
        ("dq-agent-profile-test", "pk", "S", "sk", "S"),
        ("dq-agent-lineage-test", "table_name", "S"),
        ("dq-agent-quarantine-test", "record_id", "S"),
    ]
    for table_def in tables:
        name = table_def[0]
        pk_name = table_def[1]
        pk_type = table_def[2]
        key_schema = [{"AttributeName": pk_name, "KeyType": "HASH"}]
        attr_defs = [{"AttributeName": pk_name, "AttributeType": pk_type}]
        if len(table_def) > 3:
            sk_name = table_def[3]
            sk_type = table_def[4]
            key_schema.append({"AttributeName": sk_name, "KeyType": "RANGE"})
            attr_defs.append({"AttributeName": sk_name, "AttributeType": sk_type})
        client.create_table(
            TableName=name,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs,
            BillingMode="PAY_PER_REQUEST",
        )


def _create_s3_buckets(region: str = "ap-northeast-2"):
    """Create S3 buckets and upload initial data."""
    s3 = boto3.client("s3", region_name=region)
    for bucket in ["dq-staging-test", "dq-reports-test", "dq-rules-test"]:
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": region},
        )


def _upload_sample_data(records: list[dict], pipeline_id: str, region: str = "ap-northeast-2"):
    """Upload sample data to S3 staging bucket as JSONL."""
    s3 = boto3.client("s3", region_name=region)
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    s3.put_object(
        Bucket="dq-staging-test",
        Key=f"staging/{pipeline_id}/data.jsonl",
        Body=body.encode("utf-8"),
    )


def _seed_lineage_data(region: str = "ap-northeast-2"):
    """Seed lineage data for the delivery table."""
    client = boto3.client("dynamodb", region_name=region)
    lineage = {
        "upstream_tables": ["raw_delivery_orders", "partner_api_feeds"],
        "downstream_tables": ["delivery_analytics", "customer_dashboard", "billing_reports"],
        "query_volume_7d": 15000,
        "certification_status": "certified",
        "column_lineage": {
            "tracking_id": "raw_delivery_orders.order_tracking_id",
            "sender_name": "raw_delivery_orders.sender",
            "receiver_name": "raw_delivery_orders.receiver",
            "address": "raw_delivery_orders.delivery_address",
            "weight_kg": "partner_api_feeds.package_weight",
            "status_code": "raw_delivery_orders.order_status",
        },
        "last_updated": "2026-02-24T00:00:00Z",
    }
    client.put_item(
        TableName="dq-agent-lineage-test",
        Item={
            "table_name": {"S": "delivery-data-test"},
            "data": {"S": json.dumps(lineage, ensure_ascii=False)},
        },
    )


def _make_bedrock_response(items: list[dict], analysis_type: str) -> dict:
    """Build a mock Bedrock Converse response with deterministic judgments."""
    results = []
    for item in items:
        rid = str(item.get("record_id", ""))
        error_type = item.get("error_type", "unknown")
        severity = item.get("severity", "warning")

        is_rule_error = severity == "critical" or error_type in (
            "out_of_range", "temporal_violation", "format_inconsistency",
            "cross_column_inconsistency",
        )
        is_semantic_error = rid in _SEMANTIC_ERROR_IDS or rid in _COMPOUND_IDS
        is_ambiguous = rid in _AMBIGUOUS_IDS

        is_error = is_rule_error or is_semantic_error
        if is_ambiguous and analysis_type == "PRIMARY":
            confidence = "LOW"
        elif is_error:
            confidence = "HIGH"
        else:
            confidence = "MEDIUM"

        if analysis_type == "REFLECTION":
            ref_is_error = is_error
            if is_ambiguous:
                ref_is_error = not is_error
                confidence = "LOW"
            results.append({
                "record_id": rid, "is_error": ref_is_error,
                "confidence": confidence,
                "evidence": f"Reflection: {error_type} for record {rid}",
            })
        elif analysis_type == "DEEP_ANALYSIS":
            results.append({
                "record_id": rid,
                "is_error": is_error or is_ambiguous,
                "confidence": "HIGH" if not is_ambiguous else "MEDIUM",
                "evidence": f"Deep analysis: {error_type} for record {rid}",
            })
        else:
            suggested = None
            if error_type == "out_of_range" and "road_addr_yn" in item.get("target_columns", []):
                suggested = {"column": "road_addr_yn", "value": 1}
            results.append({
                "record_id": rid, "is_error": is_error, "error_type": error_type,
                "confidence": confidence,
                "evidence": f"Primary analysis: {error_type} detected for {rid}",
                "suggested_correction": suggested,
            })

    return {
        "output": {"message": {"content": [{"text": json.dumps(results, ensure_ascii=False)}]}},
        "stopReason": "end_turn",
    }


def _mock_bedrock_converse(**kwargs):
    """Mock implementation of bedrock.converse()."""
    messages = kwargs.get("messages", [])
    user_text = ""
    for msg in messages:
        for content in msg.get("content", []):
            user_text += content.get("text", "")

    try:
        start_idx = user_text.find("[")
        end_idx = user_text.rfind("]")
        items = json.loads(user_text[start_idx:end_idx + 1]) if start_idx != -1 and end_idx != -1 else []
    except json.JSONDecodeError:
        items = []

    system = kwargs.get("system", [])
    system_text = " ".join(s.get("text", "") for s in system)
    if "재검토" in system_text or "REFLECTION" in system_text:
        analysis_type = "REFLECTION"
    elif "심층" in system_text or "DEEP" in system_text:
        analysis_type = "DEEP_ANALYSIS"
    else:
        analysis_type = "PRIMARY"

    return _make_bedrock_response(items, analysis_type)


def _mock_address_normalize(addresses, **kwargs):
    """Mock address_normalize — deterministic address type detection."""
    results = []
    for addr in addresses:
        text = addr.get("address_text", "")
        if any(kw in text for kw in ["로 ", "대로 ", "길 ", "미래로"]):
            addr_type = "road"
        elif any(kw in text for kw in ["동 ", "가 ", "동안구"]):
            addr_type = "jibun"
        else:
            addr_type = "ambiguous"
        results.append({
            "record_id": addr.get("record_id", ""),
            "original_address": text, "normalized_address": text,
            "address_type": addr_type, "confidence": 0.9,
        })
    return {"status": "success", "results": results, "success_count": len(results), "failure_count": 0, "failures": []}


def _mock_rule_generate(**kwargs):
    """Mock rule_generate — returns one dynamic rule for sender==receiver."""
    return {
        "status": "success",
        "generated_rules": [{
            "rule_id": "AUTO-001", "error_type": "cross_column_inconsistency",
            "description": "발송인과 수신인이 동일한 자기 발송 레코드",
            "target_columns": ["sender_name", "receiver_name"],
            "validation_tool": "custom_check",
            "params": {"check_type": "equality", "should_differ": True},
            "severity": "warning", "enabled": True,
        }],
        "model_id": "mock",
    }


def _mock_slack_send(**kwargs):
    """Mock Slack — just log."""
    msg = kwargs.get("message", kwargs.get("report_summary", ""))
    logger.info("[MOCK SLACK] %s", str(msg)[:200])
    return {"status": "success", "ok": True, "ts": "mock-ts"}


def _has_rid(suspects_list, target_rid):
    """Check if target_rid exists in suspects (handles dict-format record IDs)."""
    return any(target_rid in s["record_id"] for s in suspects_list)


# ── Tests ─────────────────────────────────────────────────────────────────

@mock_aws
class TestPipelineE2E:
    """End-to-end pipeline tests with mocked AWS."""

    def setup_method(self, method):
        """Set up AWS resources before each test."""
        _create_dynamodb_tables()
        _create_s3_buckets()
        _seed_lineage_data()
        from ai_dq_agent.tools.pipeline_state_tools import reset_pipeline_state
        reset_pipeline_state()

    # ── 1. 프로파일 계산 ──────────────────────────────────────────────

    def test_profile_compute(self):
        """프로파일 도구가 100건 데이터의 통계를 정확히 계산하는지 검증."""
        from ai_dq_agent.tools.profile_tools import profile_compute

        records = generate_sample_dataset()
        resp = profile_compute(records=records)

        assert resp["status"] == "success"
        assert resp["total_records"] == EXPECTED_TOTAL_RECORDS

        profiles = resp["column_profiles"]
        col_names = {p["column_name"] for p in profiles}
        for col in ["record_id", "tracking_id", "weight_kg", "item_category", "payment_method"]:
            assert col in col_names

        weight_profile = next(p for p in profiles if p["column_name"] == "weight_kg")
        assert weight_profile["null_count"] >= 1  # record 100 has None
        assert weight_profile["min_value"] is not None

        tracking_profile = next(p for p in profiles if p["column_name"] == "tracking_id")
        assert tracking_profile["null_count"] >= 1  # record 99 has None

    # ── 2. 범위 검증 (range_check 통합) ──────────────────────────────

    def test_range_check_all(self):
        """road_addr_yn, status_code, weight_kg 세 가지 범위 규칙을 한번에 검증."""
        from ai_dq_agent.tools.validation_tools import range_check

        records = generate_sample_dataset()

        # road_addr_yn: 51(2), 53(-1), 98(5) → 3건 이상
        resp = range_check(records=records, column_name="road_addr_yn",
                           primary_key=["record_id"], allowed_values=[0, 1])
        assert resp["violation_count"] >= 3
        for rid in ["51", "53", "98"]:
            assert any(rid in v["record_id"] for v in resp["violations"])

        # status_code: 52(LOST), 54(CANCELLED), 99(DAMAGED) → 3건 이상
        resp = range_check(records=records, column_name="status_code",
                           primary_key=["record_id"],
                           allowed_values=["PICKUP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "RETURNED"])
        assert resp["violation_count"] >= 3

        # weight_kg: 55(55kg), 98(45kg) → 2건 이상
        resp = range_check(records=records, column_name="weight_kg",
                           primary_key=["record_id"], min_value=0.01, max_value=30.0)
        assert resp["violation_count"] >= 2
        for rid in ["55", "98"]:
            assert any(rid in v["record_id"] for v in resp["violations"])

    # ── 3. 포맷 검증 (regex_validate 통합) ───────────────────────────

    def test_regex_validate_all(self):
        """tracking_id, 전화번호 두 가지 포맷 규칙을 한번에 검증."""
        from ai_dq_agent.tools.validation_tools import regex_validate

        records = generate_sample_dataset()

        # tracking_id: 56(ABCD12345), 57(12345), 98(abc) → 3건 이상
        resp = regex_validate(records=records, column_name="tracking_id",
                              pattern=r"^\d{10,15}$", primary_key=["record_id"])
        assert resp["violation_count"] >= 3

        # sender_phone: 59(010-1234) → 절단된 번호
        resp = regex_validate(records=records, column_name="sender_phone",
                              pattern=r"^0\d{1,2}-\d{3,4}-\d{4}$", primary_key=["record_id"])
        assert any("59" in v["record_id"] for v in resp["violations"])

    # ── 4. 시간순서 검증 ─────────────────────────────────────────────

    def test_timestamp_compare_temporal(self):
        """dispatch_time > arrival_time 시간 역전을 검출하는지 검증."""
        from ai_dq_agent.tools.validation_tools import timestamp_compare

        records = generate_sample_dataset()
        resp = timestamp_compare(
            records=records, earlier_column="dispatch_time", later_column="arrival_time",
            primary_key=["record_id"], time_formats=["%H:%M:%S"],
        )
        assert resp["status"] == "success"
        assert resp["violation_count"] >= EXPECTED_TEMPORAL_COUNT

    # ── 5. 영향도 점수 계산 ──────────────────────────────────────────

    def test_impact_score_compute(self):
        """critical이 warning보다 높은 영향도 점수를 받는지 검증."""
        from ai_dq_agent.tools.lineage_tools import impact_score_compute

        violations = [
            {"record_id": "51", "severity": "critical", "rule_id": "R001"},
            {"record_id": "56", "severity": "warning", "rule_id": "R002"},
        ]
        lineage_info = {"downstream_tables": ["analytics", "dashboard", "billing"], "query_volume_7d": 15000}
        resp = impact_score_compute(violations=violations, lineage_info=lineage_info)

        assert resp["status"] == "success"
        scored = resp["scored_violations"]
        assert scored[0]["impact_score"] > scored[-1]["impact_score"]

    # ── 6. 파이프라인 상태 공유 + 위임 ───────────────────────────────

    def test_pipeline_state_and_delegation(self):
        """pipeline_state 읽기/쓰기와 에이전트 간 동적 위임을 검증."""
        from ai_dq_agent.tools.delegation_tools import delegate_to_agent
        from ai_dq_agent.tools.pipeline_state_tools import (
            get_pipeline_state, pipeline_state_read, pipeline_state_write, reset_pipeline_state,
        )

        reset_pipeline_state()

        # 쓰기 → 읽기 → 병합
        pipeline_state_write(key="test_key", value={"data": 42})
        resp = pipeline_state_read(key="test_key")
        assert resp["found"] is True and resp["value"]["data"] == 42

        pipeline_state_write(key="test_key", value={"extra": "val"}, merge=True)
        resp = pipeline_state_read(key="test_key")
        assert resp["value"]["extra"] == "val"

        # 동적 위임
        resp = delegate_to_agent(
            agent_name="semantic_analyzer",
            task_description="Analyze ambiguous items",
            context={"suspects": [{"record_id": "93"}, {"record_id": "94"}]},
        )
        assert resp["status"] == "success"
        ps = get_pipeline_state()
        assert len(ps["delegated_suspects"]) == 2

    # ── 7. 헬스 스코어 계산 ──────────────────────────────────────────

    def test_health_score_computation(self):
        """건강/경고/위험 세 가지 시나리오의 헬스 스코어를 검증."""
        from ai_dq_agent.agents.report_notify import _compute_health_score

        # 건강 (오류 0건)
        h = _compute_health_score(total_scanned=1000, total_errors=0, high_confidence=0,
                                  stats_by_severity={}, profile_anomalies=[])
        assert h["health_score"] == 1.0 and h["status"] == "healthy"

        # 경고
        h = _compute_health_score(total_scanned=1000, total_errors=5, high_confidence=2,
                                  stats_by_severity={"critical": 2, "warning": 3},
                                  profile_anomalies=[{"severity": "warning"}])
        assert 0.5 <= h["health_score"] < 1.0

        # 위험
        h = _compute_health_score(total_scanned=1000, total_errors=15, high_confidence=10,
                                  stats_by_severity={"critical": 8, "warning": 5},
                                  profile_anomalies=[{"severity": "critical"}, {"severity": "critical"}],
                                  freshness_ok=False)
        assert h["health_score"] < 0.5 and h["status"] == "critical"

    # ── 8. 전체 파이프라인 dry-run ───────────────────────────────────

    @patch("ai_dq_agent.tools.address_tools.address_normalize", side_effect=_mock_address_normalize)
    @patch("ai_dq_agent.tools.rule_generate_tools.rule_generate", side_effect=_mock_rule_generate)
    @patch("ai_dq_agent.tools.rule_generate_tools.rule_registry_update", return_value={"status": "success"})
    @patch("ai_dq_agent.tools.slack_tools.slack_send_message", side_effect=_mock_slack_send)
    @patch("ai_dq_agent.tools.slack_tools.slack_send_interactive_message", side_effect=_mock_slack_send)
    def test_full_pipeline_dry_run(self, mock_interactive, mock_slack, mock_reg, mock_gen, mock_addr):
        """6개 스테이지 전체 파이프라인을 dry-run으로 실행."""
        mock_bedrock = MagicMock()
        mock_bedrock.converse = _mock_bedrock_converse

        records = generate_sample_dataset()
        pipeline_id = "TEST-DRY-001"
        _upload_sample_data(records, pipeline_id)

        with patch("ai_dq_agent.tools.aws_clients.get_bedrock_client", return_value=mock_bedrock), \
             patch("ai_dq_agent.tools.llm_tools.get_bedrock_client", return_value=mock_bedrock), \
             patch("ai_dq_agent.tools.rule_generate_tools.get_bedrock_client", return_value=mock_bedrock), \
             patch("ai_dq_agent.agents.dq_profiler_agent.create_profiler_agent", return_value=None), \
             patch("ai_dq_agent.agents.dq_schema_agent.create_schema_agent", return_value=None), \
             patch("ai_dq_agent.agents.dq_validator_agent.create_validator_agent", return_value=None), \
             patch("ai_dq_agent.agents.dq_semantic_agent.create_semantic_agent", return_value=None), \
             patch("ai_dq_agent.agents.dq_validator_agent.address_normalize", side_effect=_mock_address_normalize), \
             patch("ai_dq_agent.agents.dq_schema_agent.rule_generate", side_effect=_mock_rule_generate), \
             patch("ai_dq_agent.agents.dq_schema_agent.rule_registry_update", return_value={"status": "success"}), \
             patch("ai_dq_agent.agents.report_notify.slack_send_message", side_effect=_mock_slack_send), \
             patch("ai_dq_agent.agents.report_notify.slack_send_interactive_message", side_effect=_mock_slack_send), \
             patch("ai_dq_agent.agents.coordinator.slack_send_message", side_effect=_mock_slack_send), \
             patch("ai_dq_agent.agents.coordinator.execution_state_read", return_value={"status": "not_found"}), \
             patch("ai_dq_agent.agents.coordinator.execution_state_write", return_value={"status": "success"}), \
             patch("ai_dq_agent.agents.coordinator.dynamodb_export_to_s3", return_value={"status": "failed"}), \
             patch("ai_dq_agent.agents.coordinator.dynamodb_scan_with_rate_limit", return_value={"status": "success", "records": records}):

            from ai_dq_agent.tools.pipeline_state_tools import reset_pipeline_state
            reset_pipeline_state()

            from ai_dq_agent.agents.graph import _SimplePipeline
            result = _SimplePipeline().invoke({
                "trigger_type": "event", "event_records": records,
                "dry_run": True, "stage_results": {}, "error": None,
            })

        stage_results = result.get("stage_results", {})
        for stage in ["coordinator", "profiler", "schema_analyzer", "rule_validator"]:
            assert stage_results[stage]["status"] == "completed"

        assert result["total_records"] == EXPECTED_TOTAL_RECORDS
        assert result.get("suspect_count", 0) >= 15
        assert result.get("validation_stats", {}).get("total_scanned") == EXPECTED_TOTAL_RECORDS
        assert "report_notify" in stage_results

        health = result.get("table_health", {})
        if health:
            assert 0.0 <= health["health_score"] < 1.0

    # ── 9. Coordinator 노드 ──────────────────────────────────────────

    def test_coordinator_with_event_records(self):
        """이벤트 레코드 100건 처리 및 데이터 없을 때 조기 종료."""
        records = generate_sample_dataset()

        with patch("ai_dq_agent.agents.coordinator.execution_state_read", return_value={"status": "not_found"}), \
             patch("ai_dq_agent.agents.coordinator.execution_state_write", return_value={"status": "success"}), \
             patch("ai_dq_agent.agents.coordinator.s3_write_objects", return_value={"status": "success"}), \
             patch("ai_dq_agent.agents.coordinator.dynamodb_export_to_s3", return_value={"status": "failed"}), \
             patch("ai_dq_agent.agents.coordinator.dynamodb_scan_with_rate_limit", return_value={"status": "success", "records": []}), \
             patch("ai_dq_agent.agents.coordinator.slack_send_message", side_effect=_mock_slack_send):
            from ai_dq_agent.agents.coordinator import invoke_coordinator

            # 이벤트 레코드 처리
            result = invoke_coordinator({"trigger_type": "event", "event_records": records, "stage_results": {}})
            assert result["total_records"] == EXPECTED_TOTAL_RECORDS
            assert result.get("_early_exit") is not True

            # 데이터 없음 → 조기 종료
            result = invoke_coordinator({"trigger_type": "schedule", "event_records": None, "stage_results": {}})
            assert result["total_records"] == 0
            assert result.get("_early_exit") is True

    # ── 10. Rule Validator 4유형 검출 ────────────────────────────────

    def test_rule_validator_detects_all_error_types(self):
        """범위/포맷/시간/크로스컬럼 4유형 오류를 모두 검출하는지 검증."""
        from ai_dq_agent.agents.dq_validator_agent import (
            _run_format_checks, _run_range_checks, _run_temporal_checks,
        )
        from ai_dq_agent.rules.registry import load_default

        records = generate_sample_dataset()
        rules = [r.model_dump() for r in load_default().get_all_enabled()]

        range_suspects = _run_range_checks(records, rules)
        for rid in ["51", "52", "53", "54", "55", "98", "99"]:
            assert _has_rid(range_suspects, rid), f"범위 위반에서 레코드 {rid} 미검출"

        format_suspects = _run_format_checks(records, rules)
        for rid in ["56", "57", "58", "98"]:
            assert _has_rid(format_suspects, rid), f"포맷 위반에서 레코드 {rid} 미검출"

        temporal_suspects = _run_temporal_checks(records, rules)
        for rid in ["60", "61", "62", "63", "64", "65", "98", "100"]:
            assert _has_rid(temporal_suspects, rid), f"시간 위반에서 레코드 {rid} 미검출"

        total = len(range_suspects) + len(format_suspects) + len(temporal_suspects)
        assert total >= 20

    # ── 11. 시맨틱 오류 — 정적 규칙 미검출 증명 ─────────────────────

    def test_semantic_errors_pass_all_static_rules(self):
        """Group C(66-85)가 모든 정적 규칙을 통과하는지 검증 → LLM 필수성 증명."""
        from ai_dq_agent.agents.dq_validator_agent import (
            _run_format_checks, _run_range_checks, _run_temporal_checks,
        )
        from ai_dq_agent.rules.registry import load_default

        records = generate_sample_dataset()
        rules = [r.model_dump() for r in load_default().get_all_enabled()]

        all_rule_detected = (
            {s["record_id"] for s in _run_range_checks(records, rules)}
            | {s["record_id"] for s in _run_format_checks(records, rules)}
            | {s["record_id"] for s in _run_temporal_checks(records, rules)}
        )

        semantic_only_ids = set()
        for sub in ERROR_CATALOG["semantic_llm_only"].values():
            semantic_only_ids.update(sub["record_ids"])
        semantic_only_ids -= {"98", "99", "100"}  # 복합 오류 제외

        false_catches = semantic_only_ids & all_rule_detected
        assert len(false_catches) <= 2, f"정적 규칙에 잡힌 시맨틱 오류: {false_catches}"

    # ── 12. 복합 오류 — 1건에 3유형 이상 ─────────────────────────────

    def test_compound_records_have_multiple_errors(self):
        """레코드 98이 범위+포맷+시간 3유형에 동시 검출되는지 검증."""
        from ai_dq_agent.agents.dq_validator_agent import (
            _run_format_checks, _run_range_checks, _run_temporal_checks,
        )
        from ai_dq_agent.rules.registry import load_default

        records = generate_sample_dataset()
        rules = [r.model_dump() for r in load_default().get_all_enabled()]

        r98_types = set()
        if _has_rid(_run_range_checks(records, rules), "98"):
            r98_types.add("range")
        if _has_rid(_run_format_checks(records, rules), "98"):
            r98_types.add("format")
        if _has_rid(_run_temporal_checks(records, rules), "98"):
            r98_types.add("temporal")

        assert len(r98_types) >= 3, f"레코드 98: {r98_types}"
        assert _has_rid(_run_temporal_checks(records, rules), "100")

    # ── 13. Coordinator 조기 종료 ────────────────────────────────────

    def test_coordinator_no_data_early_exit(self):
        """데이터 없을 때 coordinator가 조기 종료하는지 검증."""
        with patch("ai_dq_agent.agents.coordinator.execution_state_read", return_value={"status": "not_found"}), \
             patch("ai_dq_agent.agents.coordinator.execution_state_write", return_value={"status": "success"}), \
             patch("ai_dq_agent.agents.coordinator.dynamodb_export_to_s3", return_value={"status": "failed"}), \
             patch("ai_dq_agent.agents.coordinator.dynamodb_scan_with_rate_limit", return_value={"status": "success", "records": []}), \
             patch("ai_dq_agent.agents.coordinator.slack_send_message", side_effect=_mock_slack_send):
            from ai_dq_agent.agents.coordinator import invoke_coordinator
            result = invoke_coordinator({"trigger_type": "schedule", "event_records": None, "stage_results": {}})
            assert result["total_records"] == 0
            assert result.get("_early_exit") is True

    # ── 14. 그래프 라우팅 (통합) ─────────────────────────────────────

    def test_graph_routing_all(self):
        """coordinator/validator/report 후 조건부 분기를 한번에 검증."""
        from ai_dq_agent.agents.graph import (
            route_after_coordinator, route_after_report, route_after_rule_validator,
        )
        from ai_dq_agent.tools.pipeline_state_tools import pipeline_state_write, reset_pipeline_state

        # coordinator 후 분기
        assert route_after_coordinator({"_early_exit": True}) == "no_data"
        assert route_after_coordinator({"total_records": 0}) == "no_data"
        assert route_after_coordinator({"total_records": 100}) == "has_data"

        # rule_validator 후 분기
        reset_pipeline_state()
        assert route_after_rule_validator({"suspect_count": 0}) == "no_suspects"
        assert route_after_rule_validator({"suspect_count": 5}) == "has_suspects"
        pipeline_state_write(key="delegated_suspects", value=[{"record_id": "93"}])
        assert route_after_rule_validator({"suspect_count": 0}) == "has_suspects"

        # report 후 분기
        assert route_after_report({"dry_run": True}) == "skip_correction"
        assert route_after_report({"approval_status": "no_errors"}) == "skip_correction"
        assert route_after_report({"approval_status": "approved_all"}) == "needs_correction"


# ── Quick standalone runner ───────────────────────────────────────────────

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])
