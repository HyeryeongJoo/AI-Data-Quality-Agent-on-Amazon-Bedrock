import type { SampleDataResponse, RunValidationResponse, ValidationStatus, ValidationResult } from '../types';

function getBaseUrl(): string {
  const loc = window.location;
  const pathParts = loc.pathname.split('/');
  // Handle /proxy/PORT routing
  if (pathParts[1] === 'proxy') {
    return `/${pathParts[1]}/${pathParts[2]}`;
  }
  return '';
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const base = getBaseUrl();
  const res = await fetch(`${base}${path}`, options);
  if (!res.ok) {
    const detail = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(detail.detail || `Request failed: ${res.status}`);
  }
  return res.json();
}

export async function loadSampleData(): Promise<SampleDataResponse> {
  return request<SampleDataResponse>('/api/sample-data');
}

export async function uploadCsvFile(file: File): Promise<SampleDataResponse> {
  const base = getBaseUrl();
  const formData = new FormData();
  formData.append('file', file);
  const res = await fetch(`${base}/api/upload-csv`, { method: 'POST', body: formData });
  if (!res.ok) {
    const detail = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(detail.detail || `Upload failed: ${res.status}`);
  }
  return res.json();
}

export async function startValidation(s3DataPath?: string): Promise<RunValidationResponse> {
  return request<RunValidationResponse>('/api/run-validation', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      s3_data_path: s3DataPath || 's3://dq-agent-staging-dev-joohyery/sample/data.jsonl',
      dry_run: true,
    }),
  });
}

export async function getValidationStatus(jobId: string): Promise<ValidationStatus> {
  return request<ValidationStatus>(`/api/validation-status/${jobId}`);
}

export async function getValidationResults(jobId: string): Promise<ValidationResult> {
  return request<ValidationResult>(`/api/validation-results/${jobId}`);
}

export function getValidationStreamUrl(jobId: string): string {
  const base = getBaseUrl();
  return `${base}/api/validation-stream/${jobId}`;
}
