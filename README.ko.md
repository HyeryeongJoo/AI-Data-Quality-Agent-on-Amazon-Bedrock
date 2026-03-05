# AI Data Quality Agent on Amazon Bedrock

[Strands Agents SDK](https://github.com/strands-agents/sdk-python)와 [Amazon Bedrock](https://aws.amazon.com/bedrock/)을 활용한 AI 데이터 품질 검증 에이전트입니다. [Amazon Bedrock AgentCore](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/)에 배포하며, 웹 대시보드를 통해 대화형 검증을 수행합니다.

## 아키텍처

```
coordinator → profiler → schema_analyzer → rule_validator
                                                ↓
              correction ← report_notify ← semantic_analyzer
```

**7노드 에이전트 파이프라인** (조건부 라우팅):

| 노드 | 유형 | 역할 |
|------|------|------|
| coordinator | 결정론적 | DynamoDB Stream/S3에서 데이터 추출, 파이프라인 초기화 |
| profiler | 자율적 Agent | 컬럼별 통계 프로파일링 + 트렌드 감지 |
| schema_analyzer | 자율적 Agent | 스키마 추론 + 규칙 매핑 + **동적 규칙 생성** |
| rule_validator | 자율적 Agent | 8개 정적 규칙 + 동적 규칙, **전략 전환**, **동적 위임** |
| semantic_analyzer | 자율적 Agent | LLM 시맨틱 분석 + 반복 추론 + 영향도 점수 |
| report_notify | 결정론적 | 보고서 생성 (S3) + 건강도 점수 + Slack 알림 |
| correction | 결정론적 | 승인된 항목 수정 + 불량 데이터 격리 |

**33개 도구** — 검증, 프로파일링, 계보 분석, S3/DynamoDB, Slack 등

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

### AWS 배포 (CloudFormation)

```bash
export AWS_REGION=us-east-1
export DEPLOY_S3_BUCKET=my-deploy-bucket
export S3_STAGING_BUCKET=my-staging-bucket
export AGENT_RUNTIME_ARN=arn:aws:bedrock-agentcore:...  # 선택사항

./web/deploy.sh
```

## 프로젝트 구조

```
bedrock-dq-agent/
├── agent/                    # 핵심 AI DQ 에이전트
│   ├── src/ai_dq_agent/     # 에이전트 소스 코드
│   │   ├── agents/           # 7노드 파이프라인 (graph.py)
│   │   ├── models/           # Pydantic 데이터 모델
│   │   ├── rules/            # 정적 검증 규칙 (YAML)
│   │   └── tools/            # 33개 @tool 함수
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
| `BEDROCK_MODEL_ID` | Bedrock 모델 ID (기본값: `global.anthropic.claude-sonnet-4-6`) |

## 실행 모드

1. **AgentCore Runtime** (프로덕션 권장): 에이전트를 Bedrock AgentCore에 배포하고 런타임 API로 호출합니다. `AGENT_RUNTIME_ARN`을 설정하세요.

2. **직접 호출** (개발): 웹 백엔드가 파이프라인을 직접 import하여 실행합니다. `AGENT_RUNTIME_ARN`을 비워두세요.

## 라이선스

[MIT](LICENSE)

---

[English documentation](README.md)
