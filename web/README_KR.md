# AI 데이터 퀄리티 검증 에이전트 — 웹 데모

> **규칙 기반 결정론적 검증**과 **LLM 시맨틱 분석**(Amazon Bedrock)을 결합한 하이브리드 데이터 품질 검증 파이프라인. **Amazon Bedrock AgentCore Runtime**에서 서버리스로 실행됩니다.

[English README](./README.md)

## 개요

이 웹 애플리케이션은 물류(택배/배송) 데이터를 대상으로 한 AI 기반 데이터 품질(DQ) 검증 에이전트 데모입니다. 정적 규칙으로 **이미 알고 있는 오류**를 탐지하고, LLM이 동적으로 생성한 규칙과 시맨틱 분석으로 **아직 모르는 오류**까지 자율적으로 발견합니다. DAG 기반 파이프라인으로 전체 과정이 자동화됩니다.

### 주요 기능

- **하이브리드 검증**: YAML 정적 규칙의 결정론적 검사 + LLM 동적 규칙의 적응적 탐지
- **2단계 LLM 검증**: PRIMARY 분석 + REFLECTION 자기 검증으로 환각(Hallucination) 기반 오탐 최소화
- **의심 항목 필터링**: 규칙 기반 사전 필터링으로 LLM 분석 대상을 약 78% 축소하여 비용·지연 최적화
- **Human-in-the-Loop 보정**: 모든 데이터 보정은 사람의 승인 후에만 실행 (스냅샷 + 격리 + 감사 로그)
- **실시간 파이프라인 모니터링**: SSE 기반 단계별 실시간 진행 상황 추적
- **CSV 업로드 지원**: S3 샘플 데이터 외에 사용자 CSV 파일을 직접 업로드하여 검증 가능

## 아키텍처

```
┌─────────────┐    ┌───────────────┐    ┌──────────────┐    ┌────────────────┐    ┌────────────┐
│ Coordinator │ →  │ DQ Validator   │ →  │ LLM Analyzer │ →  │ Report & Notify│ →  │ Correction │
│ 데이터 추출  │    │ 규칙 기반 검증  │    │ AI 시맨틱 분석 │    │ 리포트 & 알림   │    │ HITL 보정   │
└─────────────┘    └───────────────┘    └──────────────┘    └────────────────┘    └────────────┘
```

**파이프라인 흐름** (DAG 기반 순차 실행, 조건부 라우팅 지원):

1. **Coordinator** — DynamoDB/S3에서 택배 데이터를 추출하고, 스테이징하며, 파이프라인 상태를 초기화
2. **DQ Validator** — 스키마 추론 → 동적 규칙 생성(LLM) → 정적 + 동적 규칙으로 결정론적 전수 스캔
3. **LLM Analyzer** — 의심 항목에 대해 PRIMARY + REFLECTION 2단계 검증 수행 (의심 항목 0건 시 건너뜀)
4. **Report & Notify** — 건강도 점수 산출, Markdown 리포트를 S3에 생성, Slack 알림 발송
5. **Correction** — HIGH 신뢰도 오류 격리, 승인된 보정을 스냅샷과 함께 적용 (dry_run 시 건너뜀)

### 왜 이 아키텍처인가?

| 과제 | 기존 방식의 한계 | 이 에이전트의 해결 방식 |
|------|----------------|---------------------|
| 사전 정의 규칙의 한계 | 규칙에 없는 오류는 탐지 불가 | LLM이 데이터 프로파일을 분석하여 동적 규칙을 자동 생성 |
| LLM 판정의 불확실성 | LLM만으로 검증 시 환각으로 오탐 위험 | 2단계 자기 검증(PRIMARY + REFLECTION)으로 오탐 최소화 |
| 대규모 데이터 처리 | 모든 레코드를 LLM에 보내면 비용·지연 과다 | 규칙 기반 1차 필터링 → 의심 항목만 LLM 분석 (약 78% 비용 절감) |
| 자동 보정의 위험 | AI 자동 수정은 비즈니스 리스크 발생 | Human-in-the-Loop: 승인 후에만 보정 실행, 롤백 가능 |

## 기술 스택

| 영역 | 기술 |
|------|------|
| **프론트엔드** | React 18 + TypeScript + [Cloudscape Design System](https://cloudscape.design/) |
| **백엔드** | Python FastAPI + Uvicorn |
| **에이전트 런타임** | Amazon Bedrock AgentCore Runtime |
| **LLM 모델** | Claude Sonnet 4 (Bedrock Converse API 경유) |
| **에이전트 프레임워크** | [Strands Agents SDK](https://github.com/strands-agents/sdk-python) |
| **인프라** | AWS CloudFormation (VPC + EC2 + CloudFront) |
| **스토리지** | Amazon S3 (스테이징, 리포트, 스냅샷), Amazon DynamoDB (원본 데이터, 판정 캐시) |

## 프로젝트 구조

```
dq-agent-web/
├── backend/
│   ├── main.py                 # FastAPI 앱 진입점 (포트 8001)
│   ├── models.py               # Pydantic 요청/응답 모델
│   ├── requirements.txt        # Python 의존성
│   └── routers/
│       ├── data.py             # GET /api/sample-data, POST /api/upload-csv
│       └── validation.py       # POST /api/run-validation, SSE 스트리밍, AgentCore 호출
├── frontend/
│   ├── package.json            # React + Cloudscape 의존성
│   ├── vite.config.ts          # Vite 번들러 설정
│   └── src/
│       ├── App.tsx             # 메인 레이아웃 (아키텍처 / 검증 실행 탭)
│       ├── types.ts            # TypeScript 인터페이스 (API 계약)
│       ├── api/client.ts       # API 클라이언트 (fetch 래퍼)
│       ├── hooks/
│       │   └── useNotifications.ts
│       └── components/
│           ├── AgentIntro.tsx       # 에이전트 아키텍처 설명 페이지
│           ├── SampleDataTable.tsx  # 데이터 뷰어 (S3 로드 & CSV 업로드)
│           ├── ValidationRunner.tsx # 파이프라인 실행 + SSE 실시간 진행 추적
│           └── ValidationResults.tsx # 결과 대시보드 (요약, 단계별 결과, 상세 테이블, 보정)
├── dq-agent-web-stack.yaml     # CloudFormation 템플릿 (VPC + EC2 + CloudFront)
├── deploy.sh                   # 원클릭 배포 스크립트
└── start.sh                    # 로컬 개발 시작 스크립트
```

## 시작하기

### 사전 요구 사항

- Python 3.12+
- Node.js 22+
- AWS CLI (적절한 자격 증명 설정 완료)
- Amazon Bedrock 접근 권한 (Claude Sonnet 모델)

### 로컬 개발

```bash
# 1. 백엔드 의존성 설치
cd backend
pip install -r requirements.txt

# 2. 프론트엔드 의존성 설치 및 빌드
cd ../frontend
npm install
npm run build

# 3. 애플리케이션 시작
cd ..
./start.sh
# → 백엔드: http://localhost:8001
# → 프론트엔드: 백엔드에서 정적 파일로 서빙
```

### AWS 배포 (CloudFormation)

포함된 CloudFormation 템플릿이 전체 스택을 프로비저닝합니다:
- 퍼블릭 서브넷과 인터넷 게이트웨이를 포함한 VPC
- EC2 인스턴스 (ARM64 Graviton, m7g.medium) + SSM 관리
- HTTPS 접근을 위한 CloudFront 배포
- S3, Bedrock, AgentCore 권한이 포함된 IAM 역할
- SSM Run Command를 통한 자동화된 배포

```bash
# 원클릭 배포
./deploy.sh
# → 앱 패키징 → S3 업로드 → CloudFormation 스택 배포
# → 완료 시 CloudFront URL 출력
```

## API 엔드포인트

| 메서드 | 엔드포인트 | 설명 |
|--------|----------|------|
| `GET` | `/api/health` | 헬스 체크 |
| `GET` | `/api/sample-data` | S3에서 택배 샘플 데이터 로드 (JSONL) |
| `POST` | `/api/upload-csv` | CSV 파일 업로드, JSONL 변환 후 S3 저장 |
| `POST` | `/api/run-validation` | 비동기 검증 파이프라인 시작 (job_id 반환) |
| `GET` | `/api/validation-status/{job_id}` | 작업 상태 조회 |
| `GET` | `/api/validation-results/{job_id}` | 완료된 검증 결과 조회 |
| `GET` | `/api/validation-stream/{job_id}` | 파이프라인 실시간 진행 상황 SSE 스트림 |

## 검증 규칙 유형

### 정적 규칙 (YAML 정의)

| 유형 | 검증 도구 | 예시 |
|------|----------|------|
| `out_of_range` (범위 초과) | `range_check` | 중량이 0.01~30.0kg 범위 초과 |
| `format_inconsistency` (포맷 불일치) | `regex_validate` | 전화번호가 `0XX-XXXX-XXXX` 패턴 불일치 |
| `temporal_violation` (시간순서 위반) | `timestamp_compare` | 배송완료 시간이 발송 시간보다 이전 |
| `cross_column_inconsistency` (크로스컬럼 불일치) | `address_classify`, `value_condition` | 도로명 주소인데 `road_addr_yn=0` |

### 동적 규칙 (LLM이 런타임에 자동 생성)

- LLM이 데이터 프로파일 분석 결과를 기반으로 자동 생성 (접두사 `AUTO-NNN`)
- 정적 규칙과 동일한 결정론적 검증 도구 사용
- 스키마 fingerprint(SHA-256) 기반 S3 캐시 (TTL 1시간)

## LLM 2단계 검증 프로세스

1. **PRIMARY**: LLM이 의심 항목 분석 → `is_error` + `confidence` (HIGH/MEDIUM/LOW) + 판정 근거
2. **REFLECTION**: 독립적인 LLM 호출로 PRIMARY 결과 재검토
   - PRIMARY와 REFLECTION이 일치 → 신뢰도 유지
   - 불일치 → 신뢰도 LOW로 강제 하향
3. HIGH 신뢰도 판정만 캐시에 저장하여 동일 패턴 재활용

## 건강도 점수 산정

```
가중 오류율 = (HIGH 오류 × 1.0 + MEDIUM·LOW 오류 × 0.5) / 전체 스캔 레코드
건강도 = 1.0 − 가중 오류율
```

| 상태 | 기준 |
|------|------|
| 정상 (healthy) | 80% 이상 |
| 주의 (warning) | 50% ~ 79% |
| 위험 (critical) | 50% 미만 |

규칙 기반 의심 항목(suspect)만으로는 감점되지 않으며, LLM이 실제 오류로 확정한 건만 건강도에 반영됩니다.

## 라이선스

이 프로젝트는 데모 목적으로 제공됩니다. 서드파티 컴포넌트의 라이선스는 각 의존성의 라이선스를 참조하세요.
