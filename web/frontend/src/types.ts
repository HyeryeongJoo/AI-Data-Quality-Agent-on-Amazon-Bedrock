export interface DataRecord {
  [key: string]: string | number | boolean | null;
}

export interface SampleDataResponse {
  records: DataRecord[];
  columns: string[];
  total: number;
  source: string;
}

export interface RunValidationResponse {
  job_id: string;
  status: string;
}

export interface ValidationStatus {
  job_id: string;
  status: 'running' | 'completed' | 'error';
  elapsed_seconds: number;
}

export interface Suspect {
  record_id: string;
  rule_id: string;
  error_type: string;
  target_columns: string[];
  current_values: Record<string, string | number | null>;
  reason: string;
  severity: string;
}

export interface Judgment {
  record_id: string;
  is_error: boolean;
  error_type?: string;
  confidence: 'HIGH' | 'MEDIUM' | 'LOW';
  evidence?: string;
  reasoning?: string;
  suggested_correction?: Record<string, string | number | null> | null;
  reflection_match?: boolean;
  reflection_note?: string;
}

export interface DynamicRule {
  rule_id: string;
  error_type: string;
  description: string;
  target_columns: string[];
  validation_tool: string;
  params: Record<string, unknown>;
  severity: string;
  enabled: boolean;
}

export interface ValidationStats {
  pipeline_id: string;
  total_scanned: number;
  suspect_count: number;
  suspects_s3_path: string;
  stats_by_error_type: Record<string, number>;
  stats_by_severity: Record<string, number>;
  chunk_count: number;
  dynamic_rule_count: number;
}

export interface AnalysisStats {
  pipeline_id: string;
  total_analyzed: number;
  error_count: number;
  high_confidence_count: number;
  medium_confidence_count: number;
  low_confidence_count: number;
  reflection_mismatch_count: number;
  cache_hit_count: number;
  suspect_input_count?: number;
  primary_failed_count?: number;
  reflection_failed_count?: number;
  failure_reasons?: string[];
  input_tokens?: number;
  output_tokens?: number;
}

export interface StageResult {
  status: 'completed' | 'failed';
  started_at: string;
  completed_at: string;
  duration_seconds: number;
  records_processed?: number;
  error_message?: string;
}

export interface ValidationResult {
  status: 'completed' | 'error' | 'running';
  pipeline_id?: string;
  health_score?: number;
  health_status?: string;
  stage_results?: Record<string, StageResult>;
  violation_count?: number;
  total_records?: number;
  validation_stats?: ValidationStats;
  analysis_stats?: AnalysisStats;
  suspects?: Suspect[];
  judgments?: Judgment[];
  dynamic_rules?: DynamicRule[];
  error?: string;
}

// SSE progress types
export interface StageProgress {
  status: 'pending' | 'running' | 'completed' | 'failed';
  started_at?: string;
  completed_at?: string;
}

export interface PipelineProgress {
  pipeline_id: string;
  stages: Record<string, StageProgress>;
  current_stage: string;
  updated_at: string;
}

// Combined view: suspect + judgment for a single record
export interface RecordValidationDetail {
  record_id: string;
  suspects: Suspect[];
  judgment: Judgment | null;
  has_error: boolean;
  confidence: string;
  suggested_correction: Record<string, string | number | null> | null;
  source: 'rule' | 'llm' | 'both';
}
