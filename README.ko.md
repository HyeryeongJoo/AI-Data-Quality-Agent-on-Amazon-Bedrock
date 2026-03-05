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

## 스크린샷

### 데이터 검증 실행 — 샘플 데이터 테이블
![샘플 데이터](img/sample_data.png)
S3에서 택배 샘플 데이터를 로드하거나, CSV 파일을 직접 업로드하여 검증 데이터를 준비합니다.

### 검증 결과 — 요약 및 동적 생성 규칙
![검증 결과 요약](img/result_1.png)
건강도 점수, LLM 토큰 사용량, 비용 추정, LLM이 자동 생성한 동적 규칙 (AUTO-001 ~ AUTO-007).

### 검증 결과 — 파이프라인 단계별 실행 및 레코드별 상세
![검증 결과 상세](img/result_2.png)
단계별 실행 타임라인, 오류 유형 분포, 레코드별 규칙 위반 사항 및 LLM 판정 근거·보정 추천.

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

하나의 명령어로 전체 애플리케이션 스택을 AWS에 배포합니다. 포함된 CloudFormation 템플릿이 모든 인프라를 자동으로 프로비저닝합니다.

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

#### 코드 업데이트 배포

기존 스택에 코드 변경 사항을 배포하려면 `./web/deploy.sh`를 다시 실행합니다. 애플리케이션 코드만 변경된 경우 (CloudFormation 템플릿 변경 없음), SSM을 통해 EC2에 직접 재배포할 수 있습니다:

```bash
# 새 패키지를 S3에 업로드
aws s3 cp /tmp/dq-agent-web.tar.gz s3://<deploy-bucket>/dq-agent-web.tar.gz

# SSM으로 EC2에 재배포
aws ssm send-command \
  --instance-ids <instance-id> \
  --document-name "AWS-RunShellScript" \
  --parameters commands='[
    "aws s3 cp s3://<deploy-bucket>/dq-agent-web.tar.gz /tmp/dq-agent-web.tar.gz",
    "rm -rf /opt/dq-agent-web/backend /opt/dq-agent-web/frontend",
    "tar -xzf /tmp/dq-agent-web.tar.gz -C /opt/dq-agent-web",
    "cd /opt/dq-agent-web/frontend && npm ci && npm run build",
    "chown -R ec2-user:ec2-user /opt/dq-agent-web",
    "systemctl restart dq-agent-web"
  ]'
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
