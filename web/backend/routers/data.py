"""Data loading router — reads sample data from S3, CSV upload."""

import csv
import io
import json
import logging
import os
import uuid

import boto3
from fastapi import APIRouter, File, HTTPException, UploadFile

router = APIRouter()
logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("S3_STAGING_BUCKET", "")
S3_KEY = os.environ.get("S3_SAMPLE_KEY", "sample/data.jsonl")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")


@router.get("/sample-data")
async def get_sample_data():
    """Read sample delivery data from S3 and return as JSON."""
    try:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        body = obj["Body"].read().decode("utf-8")
        records = [json.loads(line) for line in body.strip().split("\n") if line.strip()]

        if not records:
            raise HTTPException(status_code=404, detail="No records found in S3")

        # Determine column order: put record_id first, then alphabetical
        all_cols = set()
        for rec in records:
            all_cols.update(rec.keys())
        priority = ["record_id", "tracking_id", "sender_name", "receiver_name",
                     "sender_phone", "receiver_phone", "address", "road_addr_yn",
                     "weight_kg", "item_category", "item_description",
                     "dispatch_time", "arrival_time", "status_code",
                     "delivery_attempt_count", "fee_amount", "payment_method",
                     "cod_amount", "hub_code", "special_handling"]
        columns = [c for c in priority if c in all_cols]
        columns += sorted(all_cols - set(columns))

        return {
            "records": records,
            "columns": columns,
            "total": len(records),
            "source": f"s3://{S3_BUCKET}/{S3_KEY}",
        }

    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"S3 key not found: {S3_KEY}")
    except Exception as e:
        logger.error("Failed to load sample data: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


def _parse_value(v):
    """빈 문자열→None, 숫자→int/float, 나머지→str."""
    if v is None or v == "":
        return None
    if not isinstance(v, str):
        return v
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        return v


def _detect_id_column(records: list[dict], columns: list[str]) -> str | None:
    """Detect which column is most likely the record ID.

    Priority:
      1. 'record_id' already exists
      2. 'id' column
      3. Column whose name contains 'id' (e.g. order_id, tracking_id) with unique values
      4. First column with all-unique, non-null values
    Returns the column name, or None if no good candidate found.
    """
    col_lower = {c: c.lower() for c in columns}

    # 1. Already has record_id
    if "record_id" in col_lower.values():
        return None  # no mapping needed

    # 2. Exact 'id'
    for c, low in col_lower.items():
        if low == "id":
            vals = [r.get(c) for r in records]
            if len(set(vals)) == len(records) and None not in vals:
                return c

    # 3. Name contains 'id' + unique values
    id_candidates = [c for c, low in col_lower.items() if "id" in low or "번호" in low or "코드" in low]
    for c in id_candidates:
        vals = [r.get(c) for r in records]
        if len(set(vals)) == len(records) and None not in vals:
            return c

    # 4. First column with all-unique non-null values
    for c in columns:
        vals = [r.get(c) for r in records]
        if len(set(vals)) == len(records) and None not in vals:
            return c

    return None


@router.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    """Upload a CSV file, convert to JSONL, store in S3, return records."""
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="CSV 파일만 업로드할 수 있습니다.")

    try:
        raw = await file.read()
        if len(raw) > 10 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="파일 크기는 10MB를 초과할 수 없습니다.")

        text = raw.decode("utf-8-sig")  # handle BOM
        reader = csv.DictReader(io.StringIO(text))
        records = [
            {k: _parse_value(v) for k, v in row.items() if k is not None}
            for row in reader
        ]

        if not records:
            raise HTTPException(status_code=400, detail="CSV 파일에 레코드가 없습니다.")

        # Column ordering (from CSV header)
        columns = list(reader.fieldnames or sorted({k for r in records for k in r}))

        # Auto-detect ID column → add record_id only in S3 JSONL (not in frontend response)
        id_col = _detect_id_column(records, columns)
        if id_col:
            logger.info("Auto-detected ID column: '%s' → record_id", id_col)
            s3_records = [
                {**rec, "record_id": rec[id_col]} for rec in records
            ]
        else:
            s3_records = records

        # Build JSONL and upload to S3
        lines = [json.dumps(rec, ensure_ascii=False) for rec in s3_records]
        jsonl_body = "\n".join(lines)
        s3_key = f"uploads/{uuid.uuid4().hex}.jsonl"
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=jsonl_body.encode("utf-8"))

        source = f"s3://{S3_BUCKET}/{s3_key}"
        logger.info("CSV uploaded: %s (%d records)", source, len(records))

        # Return original data to frontend (no record_id injection)
        return {
            "records": records,
            "columns": columns,
            "total": len(records),
            "source": source,
        }

    except HTTPException:
        raise
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="파일 인코딩을 읽을 수 없습니다. UTF-8 CSV를 사용해 주세요.")
    except Exception as e:
        logger.error("CSV upload failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
