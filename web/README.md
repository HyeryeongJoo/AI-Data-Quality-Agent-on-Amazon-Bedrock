# AI Data Quality Validation Agent — Web Demo

> A hybrid data quality validation pipeline that combines **rule-based deterministic checks** with **LLM semantic analysis** (Amazon Bedrock), deployed on **Amazon Bedrock AgentCore Runtime**.

[한국어 README](./README_KR.md)

## Overview

This web application demonstrates an end-to-end AI-powered data quality (DQ) validation agent for logistics (delivery/parcel) data. It detects both **known errors** via static rules and **unknown errors** via LLM-driven dynamic rule generation and semantic analysis — all orchestrated through a DAG-based pipeline.

### Key Features

- **Hybrid Validation**: Static YAML rules for deterministic checks + LLM-generated dynamic rules for adaptive detection
- **2-Stage LLM Verification**: PRIMARY analysis + REFLECTION self-verification to minimize hallucination-induced false positives
- **Suspect Filtering**: Rule-based pre-filtering reduces LLM analysis scope by ~78%, optimizing cost and latency
- **Human-in-the-Loop Correction**: All data corrections require human approval with snapshot + quarantine + audit log safeguards
- **Real-time Pipeline Monitoring**: SSE-based live progress tracking for each pipeline stage
- **CSV Upload Support**: Upload custom datasets for validation alongside the default S3 sample data

## Architecture

```
┌─────────────┐    ┌───────────────┐    ┌──────────────┐    ┌────────────────┐    ┌────────────┐
│ Coordinator │ →  │ DQ Validator   │ →  │ LLM Analyzer │ →  │ Report & Notify│ →  │ Correction │
│ Data Extract│    │ Rule-based     │    │ AI Semantic   │    │ Report & Alert │    │ HITL Patch │
└─────────────┘    │ Validation     │    │ Analysis      │    └────────────────┘    └────────────┘
                   └───────────────┘    └──────────────┘
```

**Pipeline Flow** (DAG-based sequential execution with conditional routing):

1. **Coordinator** — Extracts data from DynamoDB/S3, stages it, initializes pipeline state
2. **DQ Validator** — Schema inference → dynamic rule generation (LLM) → deterministic full scan using static + dynamic rules
3. **LLM Analyzer** — PRIMARY + REFLECTION two-stage verification on suspects only (skipped if 0 suspects)
4. **Report & Notify** — Computes health score, generates Markdown report to S3, sends Slack notification
5. **Correction** — Quarantines high-confidence errors, applies approved corrections with snapshots (skipped in dry_run)

### Why This Architecture?

| Challenge | Traditional Approach | This Agent's Solution |
|-----------|---------------------|----------------------|
| Static rule limitations | Cannot detect errors outside predefined rules | LLM analyzes data profiles to auto-generate dynamic rules |
| LLM uncertainty | LLM-only validation risks hallucination-based false positives | 2-stage self-verification (PRIMARY + REFLECTION) reduces false positives |
| Large-scale processing | Sending all records to LLM is cost-prohibitive | Rule-based pre-filtering → only suspects go to LLM (~78% cost reduction) |
| Automated correction risk | AI auto-modifying data creates business risk | Human-in-the-Loop: corrections only after approval with rollback capability |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Frontend** | React 18 + TypeScript + [Cloudscape Design System](https://cloudscape.design/) |
| **Backend** | Python FastAPI + Uvicorn |
| **Agent Runtime** | Amazon Bedrock AgentCore Runtime |
| **LLM** | Claude Sonnet 4 (via Bedrock Converse API) |
| **Agent Framework** | [Strands Agents SDK](https://github.com/strands-agents/sdk-python) |
| **Infrastructure** | AWS CloudFormation (VPC + EC2 + CloudFront) |
| **Storage** | Amazon S3 (staging, reports, snapshots), Amazon DynamoDB (source data, judgment cache) |

## Project Structure

```
dq-agent-web/
├── backend/
│   ├── main.py                 # FastAPI app entry point (port 8001)
│   ├── models.py               # Pydantic request/response models
│   ├── requirements.txt        # Python dependencies
│   └── routers/
│       ├── data.py             # GET /api/sample-data, POST /api/upload-csv
│       └── validation.py       # POST /api/run-validation, SSE streaming, AgentCore invocation
├── frontend/
│   ├── package.json            # React + Cloudscape dependencies
│   ├── vite.config.ts          # Vite bundler configuration
│   └── src/
│       ├── App.tsx             # Main layout with side navigation (Architecture / Validation tabs)
│       ├── types.ts            # TypeScript interfaces for API contracts
│       ├── api/client.ts       # API client (fetch wrapper)
│       ├── hooks/
│       │   └── useNotifications.ts
│       └── components/
│           ├── AgentIntro.tsx       # Architecture explanation page
│           ├── SampleDataTable.tsx  # Data viewer with S3 load & CSV upload
│           ├── ValidationRunner.tsx # Pipeline execution with SSE progress tracking
│           └── ValidationResults.tsx # Results dashboard (summary, stages, detail table, corrections)
├── dq-agent-web-stack.yaml     # CloudFormation template (VPC + EC2 + CloudFront)
├── deploy.sh                   # One-command deployment script
└── start.sh                    # Local development start script
```

## Getting Started

### Prerequisites

- Python 3.12+
- Node.js 22+
- AWS CLI configured with appropriate credentials
- Access to Amazon Bedrock (Claude Sonnet model)

### Local Development

```bash
# 1. Install backend dependencies
cd backend
pip install -r requirements.txt

# 2. Install frontend dependencies and build
cd ../frontend
npm install
npm run build

# 3. Start the application
cd ..
./start.sh
# → Backend serves on http://localhost:8001
# → Frontend is served as static files from the backend
```

### AWS Deployment (CloudFormation)

The included CloudFormation template provisions a complete stack:
- VPC with public subnet and Internet Gateway
- EC2 instance (ARM64 Graviton, m7g.medium) with SSM management
- CloudFront distribution for HTTPS access
- IAM roles with S3, Bedrock, and AgentCore permissions
- Automated deployment via SSM Run Command

```bash
# One-command deployment
./deploy.sh
# → Packages app → Uploads to S3 → Deploys CloudFormation stack
# → Outputs CloudFront URL when complete
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/sample-data` | Load sample delivery data from S3 (JSONL) |
| `POST` | `/api/upload-csv` | Upload a CSV file, convert to JSONL, store in S3 |
| `POST` | `/api/run-validation` | Start async validation pipeline (returns job_id) |
| `GET` | `/api/validation-status/{job_id}` | Poll job status |
| `GET` | `/api/validation-results/{job_id}` | Get completed validation results |
| `GET` | `/api/validation-stream/{job_id}` | SSE stream for real-time pipeline progress |

## Validation Rule Types

### Static Rules (YAML-defined)

| Type | Tool | Example |
|------|------|---------|
| `out_of_range` | `range_check` | Weight outside 0.01–30.0 kg |
| `format_inconsistency` | `regex_validate` | Phone number doesn't match `0XX-XXXX-XXXX` |
| `temporal_violation` | `timestamp_compare` | Arrival time before dispatch time |
| `cross_column_inconsistency` | `address_classify`, `value_condition` | Road address but `road_addr_yn=0` |

### Dynamic Rules (LLM-generated at runtime)

- Auto-generated by LLM analyzing data profiles (prefixed `AUTO-NNN`)
- Uses the same deterministic validation tools as static rules
- Cached in S3 by schema fingerprint (SHA-256) with 1-hour TTL

## LLM Two-Stage Verification

1. **PRIMARY**: LLM analyzes suspect records → `is_error` + `confidence` (HIGH/MEDIUM/LOW) + evidence
2. **REFLECTION**: Independent LLM re-review of PRIMARY results
   - If PRIMARY and REFLECTION agree → confidence maintained
   - If they disagree → confidence forced to LOW
3. Only HIGH-confidence judgments are cached for reuse

## License

This project is provided as a demonstration. See individual dependency licenses for third-party components.
