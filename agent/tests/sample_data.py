"""Enhanced Korean delivery company dataset with multi-layer data quality issues.

Generates 100 delivery records for "한진택배" (Hanjin Express) across 15 columns,
with intentional errors spanning 6 categories:

  Group A (1-50):   Clean records
  Group B (51-65):  Rule-detectable errors (range, format, temporal)
  Group C (66-85):  Semantic errors — LLM-only detectable
  Group D (86-92):  Ambiguous / edge cases — iterative reasoning needed
  Group E (93-97):  Cross-column + delegation candidates
  Group F (98-100): Compound / multi-error records

Design philosophy:
- Groups A + B verify that rule-based validation catches obvious problems.
- Group C showcases *why LLM is necessary*: these records pass every static rule
  yet are clearly wrong when *semantic reasoning* is applied.
- Group D forces PRIMARY → REFLECTION → DEEP_ANALYSIS iterative reasoning.
- Group E triggers dynamic delegation from rule_validator → semantic_analyzer.
- Group F combines multiple issues to test compound error handling.
"""

from __future__ import annotations

import random

random.seed(42)

# ── Constants ─────────────────────────────────────────────────────────────

KOREAN_NAMES = [
    "김민수", "이서연", "박지훈", "최수진", "정현우",
    "강다은", "조영호", "윤예진", "장동혁", "한소연",
    "오민지", "서재원", "임하늘", "권태호", "송유진",
    "류현정", "배준서", "홍지민", "신민규", "문채원",
]

ROAD_ADDRESSES = [
    ("서울특별시 강남구 테헤란로 152", "SEL"),
    ("서울특별시 송파구 올림픽로 300", "SEL"),
    ("경기도 성남시 분당구 판교역로 166", "GGI"),
    ("부산광역시 해운대구 해운대해변로 264", "PUS"),
    ("대구광역시 수성구 동대구로 405", "TAE"),
    ("인천광역시 연수구 송도미래로 30", "ICN"),
    ("경기도 수원시 영통구 광교중앙로 145", "GGI"),
    ("대전광역시 유성구 대학로 291", "DJN"),
    ("경기도 고양시 일산동구 중앙로 1261", "GGI"),
    ("울산광역시 남구 삼산로 274", "ULS"),
]

JIBUN_ADDRESSES = [
    ("서울특별시 강남구 역삼동 823-34", "SEL"),
    ("서울특별시 마포구 서교동 395-166", "SEL"),
    ("경기도 성남시 분당구 삼평동 629", "GGI"),
    ("부산광역시 사하구 감천동 10-2", "PUS"),
    ("대구광역시 중구 동성로2가 55", "TAE"),
    ("인천광역시 남동구 구월동 1455-3", "ICN"),
    ("경기도 안양시 동안구 비산동 1107-3", "GGI"),
    ("광주광역시 북구 운암동 577-12", "GJU"),
    ("경기도 용인시 수지구 죽전동 892", "GGI"),
    ("충청북도 청주시 상당구 석교동 67-3", "CCN"),
]

STATUS_CODES = ["PICKUP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "RETURNED"]
ITEM_CATEGORIES = ["서류", "의류", "전자제품", "식품", "가구", "화장품", "도서", "생활용품"]
PAYMENT_METHODS = ["선불", "착불", "신용카드", "계좌이체"]
SPECIAL_HANDLING = ["none", "fragile", "refrigerated", "keep_dry", "heavy_item"]
HUB_CODES = ["SEL", "GGI", "PUS", "TAE", "ICN", "DJN", "GJU", "ULS", "CCN"]


# ── Helpers ───────────────────────────────────────────────────────────────

def _tid() -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(random.choice([10, 11, 12, 13])))


def _phone() -> str:
    prefix = random.choice(["010", "02", "031", "032", "051", "053"])
    if prefix == "02":
        return f"02-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
    return f"{prefix}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"


def _dispatch_arrival() -> tuple[str, str]:
    dh = random.randint(6, 14)
    ah = dh + random.randint(2, 8)
    if ah > 23:
        ah = 23
    return (
        f"{dh:02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
        f"{ah:02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}",
    )


def _addr_with_hub(road: bool = True):
    src = ROAD_ADDRESSES if road else JIBUN_ADDRESSES
    addr, hub = random.choice(src)
    return addr, (1 if road else 0), hub


def _fee(weight: float) -> int:
    """Realistic fee calculation: base 3000 + 500 per kg."""
    return int(3000 + weight * 500)


def _clean_record(rid: int) -> dict:
    sender = random.choice(KOREAN_NAMES)
    receiver = random.choice([n for n in KOREAN_NAMES if n != sender])
    road = random.random() < 0.5
    addr, road_yn, hub = _addr_with_hub(road)
    dt, at = _dispatch_arrival()
    w = round(random.uniform(0.3, 25.0), 2)
    cat = random.choice(ITEM_CATEGORIES)
    status = random.choice(STATUS_CODES)
    attempts = random.randint(1, 3) if status == "DELIVERED" else random.randint(0, 1)
    payment = random.choice(PAYMENT_METHODS)
    cod = round(random.uniform(5000, 50000), -2) if payment == "착불" else 0
    sh = "refrigerated" if cat == "식품" and random.random() < 0.4 else "none"
    if w > 20:
        sh = "heavy_item"

    return {
        "record_id": str(rid),
        "tracking_id": _tid(),
        "sender_name": sender,
        "sender_phone": _phone(),
        "receiver_name": receiver,
        "receiver_phone": _phone(),
        "address": addr,
        "road_addr_yn": road_yn,
        "weight_kg": w,
        "item_category": cat,
        "item_description": f"{cat} 배송",
        "dispatch_time": dt,
        "arrival_time": at,
        "status_code": status,
        "delivery_attempt_count": attempts,
        "fee_amount": _fee(w),
        "payment_method": payment,
        "cod_amount": cod,
        "hub_code": hub,
        "special_handling": sh,
    }


# ── Dataset generation ────────────────────────────────────────────────────

def generate_sample_dataset() -> list[dict]:
    """Generate 100 delivery records with multi-layer DQ issues."""
    records: list[dict] = []

    # ================================================================
    # GROUP A: Clean records (1-50)
    # ================================================================
    for i in range(1, 51):
        records.append(_clean_record(i))

    # ================================================================
    # GROUP B: Rule-detectable errors (51-65)
    # ================================================================

    # -- B1: Out-of-range (51-55) --
    r = _clean_record(51)
    r["road_addr_yn"] = 2  # ERROR: must be 0 or 1
    records.append(r)

    r = _clean_record(52)
    r["status_code"] = "LOST"  # ERROR: not in allowed list
    records.append(r)

    r = _clean_record(53)
    r["road_addr_yn"] = -1  # ERROR
    records.append(r)

    r = _clean_record(54)
    r["status_code"] = "CANCELLED"  # ERROR
    records.append(r)

    r = _clean_record(55)
    r["weight_kg"] = 55.0  # ERROR: exceeds 30kg limit
    r["fee_amount"] = _fee(55.0)
    records.append(r)

    # -- B2: Format inconsistency (56-59) --
    r = _clean_record(56)
    r["tracking_id"] = "ABCD12345"  # ERROR: letters + too short
    records.append(r)

    r = _clean_record(57)
    r["tracking_id"] = "12345"  # ERROR: only 5 digits
    records.append(r)

    r = _clean_record(58)
    r["dispatch_time"] = "14:30"  # ERROR: missing seconds
    r["arrival_time"] = "20:00"   # ERROR: missing seconds
    records.append(r)

    r = _clean_record(59)
    r["sender_phone"] = "010-1234"  # ERROR: truncated phone
    records.append(r)

    # -- B3: Temporal violations (60-65) --
    for i in range(60, 66):
        r = _clean_record(i)
        r["dispatch_time"] = "20:00:00"  # dispatch AFTER arrival
        r["arrival_time"] = "08:00:00"
        r["status_code"] = "DELIVERED"
        records.append(r)

    # ================================================================
    # GROUP C: Semantic errors — LLM-only detectable (66-85)
    #
    # These records pass EVERY static rule (valid range, format,
    # temporal order) yet are clearly wrong when *meaning* is
    # considered. This is the core showcase for LLM-based DQ.
    # ================================================================

    # -- C1: Weight ↔ Category mismatch (66-68) --
    #    "서류" weighing 28kg is not documents.
    #    "가구" (furniture) weighing 0.05kg is not furniture.
    r = _clean_record(66)
    r["item_category"] = "서류"            # documents
    r["item_description"] = "계약서 서류 발송"
    r["weight_kg"] = 28.0                  # ERROR: 28kg documents?
    r["fee_amount"] = _fee(28.0)
    r["special_handling"] = "heavy_item"
    records.append(r)

    r = _clean_record(67)
    r["item_category"] = "가구"            # furniture
    r["item_description"] = "소파 배송"
    r["weight_kg"] = 0.05                  # ERROR: 50g furniture?
    r["fee_amount"] = _fee(0.05)
    records.append(r)

    r = _clean_record(68)
    r["item_category"] = "전자제품"
    r["item_description"] = "65인치 TV 배송"
    r["weight_kg"] = 0.1                   # ERROR: 100g TV?
    r["fee_amount"] = _fee(0.1)
    records.append(r)

    # -- C2: COD ↔ Payment method inconsistency (69-71) --
    #    "선불" means prepaid — cod_amount should be 0.
    #    "착불" means COD — cod_amount should be > 0.
    r = _clean_record(69)
    r["payment_method"] = "선불"
    r["cod_amount"] = 35000                # ERROR: prepaid with COD amount
    records.append(r)

    r = _clean_record(70)
    r["payment_method"] = "착불"
    r["cod_amount"] = 0                    # ERROR: COD with 0 amount
    records.append(r)

    r = _clean_record(71)
    r["payment_method"] = "신용카드"
    r["cod_amount"] = 25000                # ERROR: card payment with COD amount
    records.append(r)

    # -- C3: Status ↔ Attempt count contradiction (72-74) --
    #    PICKUP status with 5 attempts is illogical.
    #    DELIVERED with 0 attempts is contradictory.
    r = _clean_record(72)
    r["status_code"] = "PICKUP"
    r["delivery_attempt_count"] = 5        # ERROR: still at pickup after 5 attempts?
    records.append(r)

    r = _clean_record(73)
    r["status_code"] = "DELIVERED"
    r["delivery_attempt_count"] = 0        # ERROR: delivered with 0 attempts?
    records.append(r)

    r = _clean_record(74)
    r["status_code"] = "RETURNED"
    r["delivery_attempt_count"] = 0        # ERROR: returned without any attempt?
    records.append(r)

    # -- C4: Special handling ↔ Category mismatch (75-77) --
    #    Refrigerated food without cold chain.
    #    Non-perishable with refrigerated flag.
    r = _clean_record(75)
    r["item_category"] = "식품"
    r["item_description"] = "냉동 삼겹살 5kg"
    r["special_handling"] = "none"         # ERROR: frozen meat with no cold chain
    records.append(r)

    r = _clean_record(76)
    r["item_category"] = "식품"
    r["item_description"] = "프리미엄 한우 선물세트"
    r["special_handling"] = "none"         # ERROR: premium beef with no cold chain
    r["weight_kg"] = 8.5
    records.append(r)

    r = _clean_record(77)
    r["item_category"] = "도서"
    r["item_description"] = "프로그래밍 교재 3권"
    r["special_handling"] = "refrigerated" # ERROR: books don't need refrigeration
    records.append(r)

    # -- C5: Fee amount anomaly (78-80) --
    #    Fee doesn't match weight/distance patterns.
    r = _clean_record(78)
    r["weight_kg"] = 25.0
    r["fee_amount"] = 0                    # ERROR: free shipping for 25kg?
    records.append(r)

    r = _clean_record(79)
    r["weight_kg"] = 0.3
    r["item_category"] = "서류"
    r["fee_amount"] = 85000                # ERROR: 85,000원 for a 300g document?
    records.append(r)

    r = _clean_record(80)
    r["weight_kg"] = 5.0
    r["fee_amount"] = 500                  # ERROR: 500원 is below minimum cost
    records.append(r)

    # -- C6: Delivery logic anomalies (81-83) --
    #    Too many delivery attempts suggests a problem.
    #    Signature obtained but still in transit.
    r = _clean_record(81)
    r["status_code"] = "IN_TRANSIT"
    r["delivery_attempt_count"] = 8        # ERROR: 8 attempts but still in transit?
    records.append(r)

    r = _clean_record(82)
    r["item_category"] = "전자제품"
    r["item_description"] = "맥북 프로 16인치"
    r["fee_amount"] = 3000                 # ERROR: minimum fee for high-value electronics
    r["special_handling"] = "none"         # ERROR: no fragile handling for laptop
    records.append(r)

    r = _clean_record(83)
    r["status_code"] = "DELIVERED"
    r["delivery_attempt_count"] = 1
    r["dispatch_time"] = "09:00:00"
    r["arrival_time"] = "09:05:00"         # SUSPICIOUS: delivered in 5 minutes?
    records.append(r)

    # -- C7: Suspicious same-person shipments (84-85) --
    r = _clean_record(84)
    r["sender_name"] = "김민수"
    r["receiver_name"] = "김민수"          # ERROR: self-shipment
    r["sender_phone"] = "010-1234-5678"
    r["receiver_phone"] = "010-1234-5678"  # Same phone too
    records.append(r)

    r = _clean_record(85)
    r["sender_name"] = "이서연"
    r["receiver_name"] = "이서연"          # ERROR: self-shipment
    records.append(r)

    # ================================================================
    # GROUP D: Ambiguous / Edge cases — iterative reasoning (86-92)
    #
    # PRIMARY analysis may disagree with REFLECTION, triggering
    # DEEP_ANALYSIS. These test the 3-pass iterative reasoning.
    # ================================================================

    # -- D1: Borderline weight values (86-88) --
    r = _clean_record(86)
    r["weight_kg"] = 29.9                  # AMBIGUOUS: just under 30kg limit
    r["item_category"] = "생활용품"
    r["item_description"] = "정수기 필터 대형 세트"
    r["special_handling"] = "heavy_item"
    records.append(r)

    r = _clean_record(87)
    r["weight_kg"] = 0.01                  # AMBIGUOUS: 10g — could be jewelry or error
    r["item_category"] = "생활용품"
    r["item_description"] = "보석함 배송"
    r["fee_amount"] = _fee(0.01)
    records.append(r)

    r = _clean_record(88)
    r["weight_kg"] = 30.0                  # EDGE: exactly at limit
    r["item_category"] = "가구"
    r["item_description"] = "원목 사이드 테이블"
    r["special_handling"] = "heavy_item"
    records.append(r)

    # -- D2: Similar but different names (89-90) --
    r = _clean_record(89)
    r["sender_name"] = "김민수"
    r["receiver_name"] = "김민수(대리)"    # AMBIGUOUS: same person or delegate?
    records.append(r)

    r = _clean_record(90)
    r["sender_name"] = "이서연"
    r["receiver_name"] = "이서연 (부산지점)"  # AMBIGUOUS: branch office
    records.append(r)

    # -- D3: Plausible-looking but subtly wrong (91-92) --
    r = _clean_record(91)
    r["item_category"] = "식품"
    r["item_description"] = "상온 보관 라면 박스"
    r["special_handling"] = "refrigerated" # AMBIGUOUS: ramen is room-temp but marked cold
    r["weight_kg"] = 12.0
    records.append(r)

    r = _clean_record(92)
    r["status_code"] = "OUT_FOR_DELIVERY"
    r["delivery_attempt_count"] = 3        # AMBIGUOUS: 3rd attempt for out-for-delivery?
    r["dispatch_time"] = "06:00:00"
    r["arrival_time"] = "21:00:00"         # Very long day — might be valid
    records.append(r)

    # ================================================================
    # GROUP E: Cross-column + delegation candidates (93-97)
    # ================================================================

    # -- E1: Address ↔ hub_code mismatch (93-95) --
    r = _clean_record(93)
    r["address"] = "서울특별시 강남구 테헤란로 152"
    r["road_addr_yn"] = 1
    r["hub_code"] = "PUS"                  # ERROR: Seoul address → Busan hub
    records.append(r)

    r = _clean_record(94)
    r["address"] = "부산광역시 해운대구 해운대해변로 264"
    r["road_addr_yn"] = 1
    r["hub_code"] = "SEL"                  # ERROR: Busan address → Seoul hub
    records.append(r)

    r = _clean_record(95)
    r["address"] = "대전광역시 유성구 대학로 291"
    r["road_addr_yn"] = 1
    r["hub_code"] = "GJU"                  # ERROR: Daejeon address → Gwangju hub
    records.append(r)

    # -- E2: Address type mismatch (96-97) --
    r = _clean_record(96)
    r["address"] = "서울특별시 강남구 테헤란로 152"  # road address
    r["road_addr_yn"] = 0                  # ERROR: should be 1
    records.append(r)

    r = _clean_record(97)
    r["address"] = "서울특별시 마포구 서교동 395-166"  # jibun address
    r["road_addr_yn"] = 1                  # ERROR: should be 0
    records.append(r)

    # ================================================================
    # GROUP F: Compound / multi-error records (98-100)
    # ================================================================

    r = _clean_record(98)
    r["tracking_id"] = "abc"               # ERROR: format
    r["road_addr_yn"] = 5                  # ERROR: out_of_range
    r["dispatch_time"] = "23:59:59"        # ERROR: temporal
    r["arrival_time"] = "01:00:00"
    r["item_category"] = "서류"
    r["weight_kg"] = 45.0                  # ERROR: range + semantic (docs ≠ 45kg)
    r["fee_amount"] = 0                    # ERROR: semantic (free for 45kg)
    r["status_code"] = "DELIVERED"
    records.append(r)

    r = _clean_record(99)
    r["tracking_id"] = None                # null
    r["status_code"] = "DAMAGED"           # ERROR: out_of_range
    r["sender_name"] = "한소연"
    r["receiver_name"] = "한소연"          # ERROR: self-shipment
    r["payment_method"] = "선불"
    r["cod_amount"] = 50000                # ERROR: semantic (prepaid + COD)
    records.append(r)

    r = _clean_record(100)
    r["weight_kg"] = None                  # null
    r["dispatch_time"] = "18:00:00"        # ERROR: temporal
    r["arrival_time"] = "09:00:00"
    r["item_category"] = "식품"
    r["item_description"] = "킹크랩 생물 배송"
    r["special_handling"] = "none"         # ERROR: semantic (live seafood with no cold chain)
    r["delivery_attempt_count"] = 7        # ERROR: semantic (7 attempts for seafood?)
    records.append(r)

    return records


# ── Error catalog ─────────────────────────────────────────────────────────

ERROR_CATALOG = {
    "rule_detectable": {
        "out_of_range": {
            "record_ids": ["51", "52", "53", "54", "55", "98", "99"],
            "description": "road_addr_yn ∉ {0,1}, invalid status_code, weight > 30kg",
        },
        "format_inconsistency": {
            "record_ids": ["56", "57", "58", "59", "98"],
            "description": "bad tracking_id, missing time seconds, truncated phone",
        },
        "temporal_violation": {
            "record_ids": ["60", "61", "62", "63", "64", "65", "98", "100"],
            "description": "dispatch_time > arrival_time",
        },
        "cross_column_inconsistency": {
            "record_ids": ["93", "94", "95", "96", "97"],
            "description": "address↔road_addr_yn mismatch, address↔hub_code mismatch",
        },
    },
    "semantic_llm_only": {
        "weight_category_mismatch": {
            "record_ids": ["66", "67", "68"],
            "description": "Weight contradicts item category (28kg docs, 50g furniture, 100g TV)",
        },
        "cod_payment_inconsistency": {
            "record_ids": ["69", "70", "71"],
            "description": "Payment method contradicts COD amount (prepaid w/ COD, COD w/ 0원)",
        },
        "status_attempt_contradiction": {
            "record_ids": ["72", "73", "74"],
            "description": "Delivery attempts contradict status (5 attempts at PICKUP, 0 at DELIVERED)",
        },
        "special_handling_mismatch": {
            "record_ids": ["75", "76", "77"],
            "description": "Handling flags contradict contents (frozen meat w/o cold, books w/ cold)",
        },
        "fee_anomaly": {
            "record_ids": ["78", "79", "80"],
            "description": "Fee doesn't match weight/item (0원 for 25kg, 85k for 300g docs)",
        },
        "delivery_logic_anomaly": {
            "record_ids": ["81", "82", "83"],
            "description": "Suspicious delivery patterns (8 attempts in-transit, 5-min delivery)",
        },
        "self_shipment": {
            "record_ids": ["84", "85", "99"],
            "description": "Sender and receiver are same person",
        },
    },
    "ambiguous_iterative": {
        "record_ids": ["86", "87", "88", "89", "90", "91", "92"],
        "description": "Edge cases requiring PRIMARY→REFLECTION→DEEP_ANALYSIS reasoning",
    },
    "compound": {
        "record_ids": ["98", "99", "100"],
        "description": "Multiple error types in a single record",
    },
}

# ── Expected counts for test assertions ───────────────────────────────────

EXPECTED_OUT_OF_RANGE_COUNT = 7    # 51,52,53,54,55 + 98,99
EXPECTED_FORMAT_COUNT = 5          # 56,57,58(×2 cols),59 + 98 (minus null-skipped)
EXPECTED_TEMPORAL_COUNT = 8        # 60-65 + 98 + 100
EXPECTED_SEMANTIC_ERROR_COUNT = 20 # Groups C (66-85)
EXPECTED_AMBIGUOUS_COUNT = 7       # Group D (86-92)
EXPECTED_TOTAL_RECORDS = 100


# ── Pretty-print for CLI ─────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    data = generate_sample_dataset()
    print(f"Generated {len(data)} records with {len(data[0])} columns\n")
    print("Columns:", list(data[0].keys()))

    print("\n── Sample clean record (#5) ──")
    print(json.dumps(data[4], ensure_ascii=False, indent=2))

    print("\n── Rule-detectable error (#55 - weight out of range) ──")
    print(json.dumps(next(r for r in data if r["record_id"] == "55"), ensure_ascii=False, indent=2))

    print("\n── Semantic error (#66 - 28kg documents) ──")
    print(json.dumps(next(r for r in data if r["record_id"] == "66"), ensure_ascii=False, indent=2))

    print("\n── Semantic error (#69 - prepaid with COD amount) ──")
    print(json.dumps(next(r for r in data if r["record_id"] == "69"), ensure_ascii=False, indent=2))

    print("\n── Semantic error (#75 - frozen meat, no cold chain) ──")
    print(json.dumps(next(r for r in data if r["record_id"] == "75"), ensure_ascii=False, indent=2))

    print("\n── Ambiguous edge case (#89 - 김민수 → 김민수(대리)) ──")
    print(json.dumps(next(r for r in data if r["record_id"] == "89"), ensure_ascii=False, indent=2))

    print("\n── Compound error (#98 - 5 errors in 1 record) ──")
    print(json.dumps(next(r for r in data if r["record_id"] == "98"), ensure_ascii=False, indent=2))

    print(f"\n── Error summary ──")
    for cat, info in ERROR_CATALOG.items():
        if isinstance(info, dict) and "record_ids" in info:
            print(f"  {cat}: {len(info['record_ids'])} records — {info['description']}")
        else:
            for sub, sub_info in info.items():
                print(f"  {cat}.{sub}: {len(sub_info['record_ids'])} records — {sub_info['description']}")
