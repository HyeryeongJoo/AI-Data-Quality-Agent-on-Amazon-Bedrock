# Bedrock DQ Agent

AI-powered Data Quality validation agent built with [Strands Agents SDK](https://github.com/strands-agents/sdk-python) and [Amazon Bedrock](https://aws.amazon.com/bedrock/). Deploys to [Amazon Bedrock AgentCore](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/) with a web dashboard for interactive validation.

## Architecture

```
coordinator → profiler → schema_analyzer → rule_validator
                                                ↓
              correction ← report_notify ← semantic_analyzer
```

**7-node agent pipeline** with conditional routing:

| Node | Type | Role |
|------|------|------|
| coordinator | Deterministic | Extract data from DynamoDB Stream / S3, initialize pipeline |
| profiler | Autonomous Agent | Per-column statistical profiling + trend detection |
| schema_analyzer | Autonomous Agent | Schema inference + rule mapping + **dynamic rule generation** |
| rule_validator | Autonomous Agent | 8 static rules + dynamic rules, **strategy switching**, **delegation** |
| semantic_analyzer | Autonomous Agent | LLM semantic analysis + iterative reasoning + impact scoring |
| report_notify | Deterministic | Generate report (S3) + health score + Slack notification |
| correction | Deterministic | Apply approved corrections + quarantine bad data |

**33 tools** across validation, profiling, lineage, S3/DynamoDB, Slack, and more.

## Quick Start

### Prerequisites

- Python 3.12+
- Node.js 22+
- AWS account with Bedrock access (Claude Sonnet model)
- S3 bucket for staging data

### 1. Clone and configure

```bash
git clone https://github.com/your-org/bedrock-dq-agent.git
cd bedrock-dq-agent
cp .env.example .env
# Edit .env with your AWS settings
```

### 2. Set up the agent

```bash
cd agent
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 3. Run tests

```bash
cd agent
pytest tests/ -v
```

### 4. Start the web dashboard (local development)

```bash
cd web/frontend
npm install
npm run build

cd ../..
./web/start.sh
# Open http://localhost:8001
```

### 5. Deploy to AWS (CloudFormation)

```bash
export AWS_REGION=us-east-1
export DEPLOY_S3_BUCKET=my-deploy-bucket
export AGENT_RUNTIME_ARN=arn:aws:bedrock-agentcore:...  # optional
export S3_STAGING_BUCKET=my-staging-bucket

./web/deploy.sh
```

## Project Structure

```
bedrock-dq-agent/
├── agent/                    # Core AI DQ agent
│   ├── src/ai_dq_agent/     # Agent source code
│   │   ├── agents/           # 7-node pipeline (graph.py)
│   │   ├── models/           # Pydantic data models
│   │   ├── rules/            # Static validation rules (YAML)
│   │   └── tools/            # 33 @tool functions
│   ├── config/rules/         # Domain-specific rules
│   ├── tests/                # Unit + integration tests
│   ├── agentcore_agent.py    # AgentCore Runtime entry point
│   └── pyproject.toml
├── web/                      # Web dashboard
│   ├── backend/              # FastAPI API server
│   ├── frontend/             # React + Cloudscape UI
│   ├── start.sh              # Local development script
│   └── deploy.sh             # AWS deployment script
├── infra/
│   └── cloudformation.yaml   # Full-stack CloudFormation template
├── .env.example              # Environment variable template
└── LICENSE                   # MIT
```

## Configuration

All configuration is via environment variables. See [`.env.example`](.env.example) for the full list.

Key variables:

| Variable | Description |
|----------|-------------|
| `AWS_REGION` | AWS region (default: `us-east-1`) |
| `S3_STAGING_BUCKET` | S3 bucket for data staging and pipeline artifacts |
| `AGENT_RUNTIME_ARN` | AgentCore runtime ARN (empty = direct invocation) |
| `SLACK_BOT_TOKEN` | Slack bot token for notifications (optional) |
| `BEDROCK_MODEL_ID` | Bedrock model ID (default: `global.anthropic.claude-sonnet-4-6`) |

## Execution Modes

1. **AgentCore Runtime** (recommended for production): Deploy the agent to Bedrock AgentCore and invoke via the runtime API. Set `AGENT_RUNTIME_ARN` in your environment.

2. **Direct Invocation** (development): The web backend imports and runs the pipeline directly. Leave `AGENT_RUNTIME_ARN` empty.

## License

[MIT](LICENSE)

---

[Korean documentation (한국어 문서)](README.ko.md)
