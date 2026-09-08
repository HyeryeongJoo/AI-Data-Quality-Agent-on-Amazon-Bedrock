# AI Data Quality Agent on Amazon Bedrock

[Strands Agents SDK](https://github.com/strands-agents/sdk-python)와 [Amazon Bedrock](https://aws.amazon.com/bedrock/)을 활용한 AI 데이터 품질 검증 에이전트입니다. [Amazon Bedrock AgentCore](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/)에 배포하며, 웹 대시보드를 통해 대화형 검증을 수행합니다.

## 아키텍처

두 가지 파이프라인 버전을 제공합니다:

**v1 — 기본 검증 (규칙 + LLM)** — 5노드 파이프라인:
```
Coordinator → Rule Validator → LLM Analyzer → Report & Notify → Correction
```

**v2 — 확장 검증 (규칙 + 이상치 + LLM)** — 6노드 파이프라인:
```
Coordinator → Rule Validator → Anomaly Detector → LLM Analyzer → Report & Notify → Correction
```

### 파이프라인 노드

| 노드 | 유형 | 역할 |
|------|------|------|
| **Coordinator** | 결정론적 | DynamoDB / S3에서 데이터 추출, 파이프라인 상태 초기화 |
| **Rule Validator** | 하이브리드 (결정론적 + LLM) | 정적 규칙 (YAML) + LLM 생성 동적 규칙, 전체 데이터 프로파일링, 결정론적 전수 검사 |
| **Anomaly Detector** (v2 전용) | 통계적 | Z-Score, IQR, Isolation Forest, 조건부 이상치, 상관관계, 희귀 조합 탐지 |
| **LLM Analyzer** | LLM (Claude) | 의심 레코드 시맨틱 분석, 오류/오탐 분류 및 신뢰도 (HIGH/MEDIUM/LOW), 보정 제안 |
| **Report & Notify** | 결정론적 | DQ 보고서 생성 (S3), 건강도 점수 산출, Slack 알림 |
| **Correction** | 결정론적 | Human-in-the-Loop 승인 기반 수정 + 격리 |

### 4계층 검증

| 계층 | 방식 | 예시 |
|------|------|------|
| 1. 정적 규칙 | 사전 정의 YAML 규칙 | 전화번호 패턴 불일치 |
| 2. 동적 규칙 | 데이터 프로파일링 기반 LLM 생성 규칙 | "COD 결제인데 배송비 = 0" |
| 3. 이상치 탐지 | 통계 알고리즘 (v2) | 배송비 Z-Score 4.2 이상치 |
| 4. LLM 분석 | 맥락적 시맨틱 분석 | "0.005kg은 서류 배송이므로 정상" → 오탐 제거 |

**45개 도구** — 검증, 프로파일링, 규칙 생성, 계보 분석, S3/DynamoDB, Slack 등

### Rule Validator: 정적 규칙 + 동적 규칙

Rule Validator는 두 가지 보완적인 규칙 유형을 결합하여 탐지 커버리지를 극대화합니다.

#### 정적 규칙 (`agent/src/ai_dq_agent/rules/default_rules.yaml`)

도메인 전문가가 직접 작성한 비즈니스 검증 규칙입니다. 결정론적(Deterministic)으로 동작하여 동일한 입력에 항상 동일한 결과를 반환합니다.

| 오류 유형 | 검증 도구 | 예시 |
|-----------|-----------|------|
| `out_of_range` | `range_check` | `weight_kg`이 0.01~30.0 kg 범위 초과, `status_code`가 허용 enum 외 값 |
| `format_inconsistency` | `regex_validate` | 전화번호 패턴 불일치, 운송장번호가 10~15자리 숫자 아님 |
| `temporal_violation` | `timestamp_compare` | `delivery_time`이 `dispatch_time`보다 이전 |
| `cross_column_inconsistency` | `address_classify`, `value_condition` | 도로명 주소인데 `road_addr_yn=0`, 착불 결제인데 `cod_amount=0` |

정적 규칙은 이미 알고 있는 오류를 빠르고 저렴하게 탐지합니다. 한계는 사전에 정의하지 않은 패턴은 탐지할 수 없다는 점입니다.

#### 동적 규칙 (런타임에 LLM이 자동 생성, ID: `AUTO-NNN`)

Claude가 전체 데이터 프로파일링 결과를 분석하여, 정적 규칙이 커버하지 못하는 새로운 검증 규칙을 자동 생성합니다. 샘플 몇 건이 아닌 전체 데이터 분포에 기반한 규칙을 만들기 위해 LLM을 2회 호출하는 2단계 프로세스를 사용합니다:

```
1단계  스키마 추론 + S3 캐시 확인 (SHA-256 fingerprint, TTL 1시간)
       └─ 캐시 HIT  → 2~4단계 건너뜀, 기존 규칙 재활용 (실행 시간 약 40초 절감)
       └─ 캐시 MISS → 계속 진행

2단계  LLM 1차 호출 (스키마 + 샘플 5건)
       → "어떤 크로스컬럼 조건을 조사해야 하는가?"
       → 예: "착불 결제인데 배송비=0인 조합", "거리 대비 배송 시간 비율"

3단계  전체 데이터 프로파일링 (LLM 호출 없음, 결정론적)
       → 컬럼별 통계: null율, 고유값 수, Top-N 분포
       → 크로스컬럼 조건의 전체 레코드 대비 매칭률 산출

4단계  LLM 2차 호출 (프로파일링 결과 + 샘플 20건)
       → "기존 규칙과 중복되지 않는 새 규칙을 생성하라"
       → 전체 데이터 분포를 근거로 정확한 규칙 생성
       → 결과를 S3에 캐시 저장 (스키마 SHA-256 fingerprint 키)

5단계  결정론적 전수 스캔
       → 정적 규칙 + 동적 규칙 전체를 모든 레코드에 적용
```

**파이프라인 실행 당 LLM 호출 요약:**

| 단계 | 호출 수 | 시점 | 목적 |
|------|---------|------|------|
| Rule Validator — 1차 | 1회 | 캐시 MISS 시에만 | 프로파일링할 크로스컬럼 조건 발견 |
| Rule Validator — 2차 | 1회 | 캐시 MISS 시에만 | 프로파일링 결과로 동적 규칙 생성 |
| LLM Analyzer | ⌈의심항목 ÷ 50⌉회 | 항상 (의심항목 > 0) | PRIMARY 분석: 오류 판정, 신뢰도 부여, 보정 제안 |
| **합계 (캐시 MISS, 의심 ≤ 50건)** | **3회** | — | — |
| **합계 (캐시 HIT, 의심 ≤ 50건)** | **1회** | — | — |

> **핵심 설계 원칙 — "LLM이 발견하고, 규칙이 검증한다"**: LLM은 어떤 패턴을 검사해야 하는지를 결정하고, 실제 레코드 검증은 결정론적 도구 함수(`range_check`, `regex_validate`, `timestamp_compare`)가 수행합니다. 이를 통해 LLM의 창의적 패턴 발견 능력과 결정론적 검증의 재현 가능성을 모두 확보합니다.

LLM을 1회가 아닌 2회 호출하는 이유: 샘플 5건만 보고 규칙을 바로 생성하면 전체 데이터의 실제 분포를 반영하지 못합니다. 2단계 방식은 전체 데이터 프로파일링을 먼저 수행하여 LLM이 실제 컬럼 분포에 근거한 정확한 규칙을 생성할 수 있도록 합니다.

### Anomaly Detector (v2 전용): 통계적 + 문맥적 이상치 탐지

Anomaly Detector는 전체 데이터셋을 분석하여 규칙 기반 검증으로는 고정된 min/max 경계나 정규식으로 표현할 수 없는 통계적·문맥적 이상 레코드를 탐지합니다.

| 기법 | 분류 | 탐지 대상 |
|------|------|-----------|
| Z-Score | 통계적 | 평균에서 3σ 이상 벗어난 단변량 극단값 |
| IQR | 통계적 | Q1−1.5×IQR ~ Q3+1.5×IQR 범위 밖의 값 |
| Isolation Forest | 통계적 (다변량) | 머신러닝 기반 다변량 이상치 |
| 조건부 이상치 | 문맥적 | 전체적으로는 정상이지만 특정 그룹 내에서 이상인 값 (예: 품목 유형별 중량) |
| 상관관계 이탈 | 문맥적 | 컬럼 간 기대 관계를 위반하는 값 (예: 거리 대비 배송 시간) |
| 희귀 조합 | 문맥적 | 컬럼 간 드문 값 조합 |

Anomaly Detector가 발견한 의심 항목은 Rule Validator 의심 항목과 병합된 후 LLM Analyzer로 전달됩니다. 중복 레코드는 자동으로 제거됩니다.

### LLM Analyzer: 시맨틱 분석 + 신뢰도 판정

LLM Analyzer(Claude Sonnet)는 전체 데이터셋이 아닌 Rule Validator와 Anomaly Detector가 수집한 의심 항목만을 분석합니다. API 호출 횟수를 최소화하기 위해 배치 단위(기본 50건/호출)로 PRIMARY 분석을 수행합니다.

각 의심 항목은 시스템 프롬프트에 명시된 신뢰도 기준으로 판정됩니다:

| 신뢰도 | 판정 기준 | 예시 |
|--------|-----------|------|
| HIGH | 데이터만으로 오류 여부를 확실히 판단할 수 있는 경우 | 형식 오류 (전화번호 자릿수), 명백한 범위 초과 (음수 중량), 논리적 모순 (배송 전 발송) |
| MEDIUM | 오류 가능성이 높지만 비즈니스 예외가 존재할 수 있는 경우 | 경계값, 비표준이지만 유효할 수 있는 형식 |
| LOW | 오류인지 확신할 수 없는 경우 | 통계적으로 드물지만 정상 범위일 수 있는 값, 문맥 의존적 판단 |

모든 신뢰도(HIGH/MEDIUM/LOW)의 판정 결과가 사용자에게 제공됩니다 — 자동으로 제외되는 항목은 없습니다.

**건강도 점수(Health Score)** 는 LLM이 확정한 오류를 신뢰도 가중치로 산출합니다:

```
가중 오류율 = (HIGH 오류 × 1.0 + MEDIUM/LOW 오류 × 0.5) / 전체 레코드 수
건강도 점수 = 1.0 − 가중 오류율
```

기준: 80% 이상 → 정상(Healthy) | 50~79% → 주의(Warning) | 50% 미만 → 위험(Critical)

## 스크린샷

### 샘플 데이터 테이블
![샘플 데이터](img/sample_data.png)
S3에서 택배 샘플 데이터를 로드하거나 CSV 파일을 직접 업로드합니다. 확장 파이프라인 (v2)에서는 이상치 탐지 기법 (Z-Score, IQR, Isolation Forest, 조건부, 상관관계, 희귀 조합) 선택을 지원합니다.

### 검증 결과 — 요약
![검증 결과 요약](img/result_summary.png)
건강도 점수 (81%), 파이프라인 흐름 메트릭 (규칙 기반 의심 → 이상치 탐지 추가 → LLM 분석 대상 → 오탐 제거 → LLM 오류 판정), 오탐율, LLM 토큰 사용량 및 비용 추정.

### 검증 결과
![검증 결과 상세](img/result.png)
레코드별 3-상태 (오류 확정 / 정상 판정 / 미판정), 전체 컬럼 정렬 지원, 위반 상세 및 규칙 ID, LLM 신뢰도, 보정 제안.

### 검증 결과 — 레코드별 상세
![검증 결과 상세 v2](img/result_details_v2.png)
Anomaly Detector가 탐지한 이상치 레코드(통계적 이상치, 문맥적 이상치)가 추가된 결과. v2 전용 오류 유형 분포를 확인할 수 있습니다.

### 동적 규칙 (자동 생성)
![동적 규칙](img/auto_rules.png)
데이터 프로파일링 기반 LLM 자동 생성 규칙 (AUTO-001 ~ AUTO-014). 허용값 검사, 형식 검증, 컬럼 간 정합성, 시간 제약 등을 포함합니다.

## 빠른 시작

### 사전 요구사항

- Python 3.12+
- Node.js 22+
- AWS CLI 설정 완료 (credentials 포함)
- Bedrock 모델 접근이 가능한 AWS 계정 (Claude Sonnet)

### 방법 A: 원커맨드 설정

```bash
git clone https://github.com/HyeryeongJoo/bedrock-dq-agent.git
cd bedrock-dq-agent

# 1. .env 생성 후 설정 입력
cp .env.example .env
# .env 편집 — 최소한 S3_STAGING_BUCKET 설정 필요

# 2. 셋업 실행 (의존성 설치, 프론트엔드 빌드, AWS 리소스 생성, 샘플 데이터 업로드)
./setup.sh

# 3. 실행
./web/start.sh
# http://localhost:8001 에서 접속
```

`setup.sh`이 수행하는 작업:
- 사전 요구사항 확인 (Python, Node.js, AWS CLI, 자격증명)
- 에이전트 Python 의존성 설치 + 가상환경 생성
- React 프론트엔드 빌드
- S3 버킷 및 DynamoDB 테이블 생성
- 샘플 배송 데이터 (100건) S3 업로드

AWS 리소스 생성을 건너뛰려면: `./setup.sh --skip-aws`

### 방법 B: 단계별 설정

```bash
# 1. 클론 및 설정
git clone https://github.com/HyeryeongJoo/bedrock-dq-agent.git
cd bedrock-dq-agent
cp .env.example .env        # AWS 설정 편집

# 2. 에이전트 설정
cd agent
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 3. 테스트 실행
pytest tests/ -v

# 4. AWS 리소스 생성 (S3 버킷 + DynamoDB 테이블)
cd ..
./scripts/setup-aws.sh

# 5. 샘플 데이터 업로드
./scripts/upload-sample-data.sh

# 6. 프론트엔드 빌드 및 실행
cd web/frontend && npm install && npm run build && cd ../..
./web/start.sh              # http://localhost:8001 에서 접속
```

### AgentCore 배포

```bash
cd agent
agentcore deploy    # Bedrock AgentCore Runtime에 코드 직접 배포
agentcore status    # 배포 상태 확인
```

### AWS 배포 (CloudFormation)

하나의 명령어로 전체 웹 애플리케이션 스택을 AWS에 배포합니다. 포함된 CloudFormation 템플릿이 모든 인프라를 자동으로 프로비저닝합니다.

#### 생성되는 리소스

```
┌─────────────────────────────────────────────────────────┐
│                    CloudFront (HTTPS)                    │
│               https://xxxxx.cloudfront.net               │
└────────────────────────┬────────────────────────────────┘
                         │ Port 8001
┌────────────────────────▼────────────────────────────────┐
│  EC2 인스턴스 (Amazon Linux 2023, Graviton m7g.medium)  │
│  ┌─────────────────────────────────────────────────┐    │
│  │  FastAPI 백엔드 (uvicorn, systemd 관리)          │    │
│  │  React 프론트엔드 (정적 파일, FastAPI에서 서빙)   │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────┐     ┌──────────────────────────────┐
│  S3 스테이징     │     │  Bedrock AgentCore Runtime   │
│  (데이터 + 보고서)│     │  (AI DQ 검증 파이프라인)      │
└─────────────────┘     └──────────────────────────────┘
```

| 리소스 | 상세 |
|--------|------|
| **VPC** | 전용 VPC (10.4.0.0/16), 퍼블릭 서브넷, IGW, 라우트 테이블 |
| **EC2** | `m7g.medium` (ARM64 Graviton), 30GB gp3 EBS, 암호화 |
| **보안 그룹** | CloudFront에서만 인바운드 허용 (AWS prefix list), 포트 8001 |
| **IAM 역할** | SSM, CloudWatch, Bedrock, S3 접근 권한의 EC2 역할 |
| **CloudFront** | HTTPS 배포, HTTP/2+3 지원, API 경로 캐싱 비활성화 |
| **SSM 문서** | 자동 배포: Python 3.12, Node.js 22 설치, 앱 빌드 및 시작 |
| **Lambda (x2)** | SSM 문서 실행 및 완료 확인 오케스트레이션 |

#### 사전 요구사항

- CloudFormation 스택, VPC, EC2, IAM 역할, CloudFront, Lambda, SSM 문서를 생성할 수 있는 권한이 설정된 AWS CLI
- 데이터 스테이징용 S3 버킷 (파이프라인이 데이터와 보고서를 읽고 쓰는 버킷)
- (선택) AgentCore를 통해 에이전트를 실행하는 경우 Bedrock AgentCore 런타임 ARN

#### 배포 단계

```bash
# 1. 필수 환경변수 설정
export AWS_REGION=us-east-1
export S3_STAGING_BUCKET=my-staging-bucket          # 필수: 데이터 및 보고서용 S3 버킷

# 2. 선택 환경변수 설정
export AGENT_RUNTIME_ARN=arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/my-agent-xxxxx
export DEPLOY_S3_BUCKET=my-deploy-bucket            # 미설정 시 dq-agent-web-deploy-<계정ID>로 자동 생성
export STACK_NAME=dq-agent-web                      # 미설정 시 dq-agent-web

# 3. 배포 실행
./web/deploy.sh
```

스크립트가 수행하는 작업:
1. **패키징** — 애플리케이션 (백엔드 + 프론트엔드 소스)을 tarball로 압축
2. **S3 버킷 생성** — 배포 패키지 저장용 (미지정 시 AWS 계정 ID에서 자동 생성)
3. **업로드** — 패키지를 S3에 업로드
4. **CloudFormation 스택 배포** — 전체 인프라 프로비저닝 후 SSM 문서를 실행하여 EC2에서 설치, 빌드, 애플리케이션 시작

#### 배포 확인

```bash
# 스크립트가 CloudFront URL과 EC2 인스턴스 ID를 출력합니다
# 헬스 체크:
curl https://<cloudfront-domain>.cloudfront.net/api/health
# 예상 응답: {"status":"ok"}

# 웹 대시보드 열기:
open https://<cloudfront-domain>.cloudfront.net
```

#### 리소스 정리

```bash
# 전체 스택 삭제 (VPC, EC2, CloudFront, IAM 역할 등)
aws cloudformation delete-stack --stack-name dq-agent-web --region us-east-1

# (선택) 배포용 S3 버킷 삭제
aws s3 rb s3://<deploy-bucket> --force
```

## 프로젝트 구조

```
bedrock-dq-agent/
├── agent/                    # 핵심 AI DQ 에이전트
│   ├── src/ai_dq_agent/     # 에이전트 소스 코드
│   │   ├── agents/           # 파이프라인 노드 (graph.py, coordinator, rule_validator, anomaly_detector, llm_analyzer 등)
│   │   ├── models/           # Pydantic 데이터 모델
│   │   ├── rules/            # 정적 검증 규칙 (YAML)
│   │   └── tools/            # 45개 @tool 함수
│   ├── config/rules/         # 도메인별 규칙
│   ├── tests/                # 단위 + 통합 테스트
│   ├── agentcore_agent.py    # AgentCore Runtime 진입점
│   └── pyproject.toml
├── web/                      # 웹 대시보드
│   ├── backend/              # FastAPI API 서버
│   ├── frontend/             # React + Cloudscape UI
│   ├── start.sh              # 로컬 개발 스크립트
│   └── deploy.sh             # AWS 배포 스크립트
├── scripts/
│   ├── check-prereqs.sh      # 사전 요구사항 확인
│   ├── setup-aws.sh          # S3 버킷 + DynamoDB 테이블 생성
│   └── upload-sample-data.sh # 테스트 데이터 S3 업로드
├── infra/
│   └── cloudformation.yaml   # 전체 스택 CloudFormation 템플릿
├── setup.sh                  # 원커맨드 셋업
├── .env.example              # 환경변수 템플릿
└── LICENSE                   # MIT
```

## 설정

모든 설정은 환경변수로 관리됩니다. 전체 목록은 [`.env.example`](.env.example)을 참고하세요.

주요 변수:

| 변수 | 설명 |
|------|------|
| `AWS_REGION` | AWS 리전 (기본값: `us-east-1`) |
| `S3_STAGING_BUCKET` | 데이터 스테이징 및 파이프라인 아티팩트용 S3 버킷 |
| `AGENT_RUNTIME_ARN` | AgentCore 런타임 ARN (비어있으면 직접 호출) |
| `SLACK_BOT_TOKEN` | Slack 알림용 봇 토큰 (선택사항) |
| `BEDROCK_MODEL_ID` | Bedrock 모델 ID (기본값: `global.anthropic.claude-sonnet-5`) |

## 실행 모드

1. **AgentCore Runtime** (프로덕션 권장): 에이전트를 Bedrock AgentCore에 배포하고 런타임 API로 호출합니다. `AGENT_RUNTIME_ARN`을 설정하세요.

2. **직접 호출** (개발): 웹 백엔드가 파이프라인을 직접 import하여 실행합니다. `AGENT_RUNTIME_ARN`을 비워두세요.

## 라이선스

[MIT](LICENSE)

---

[English documentation](README.md)
