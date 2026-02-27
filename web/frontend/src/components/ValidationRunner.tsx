import { useState, useEffect, useRef, useCallback } from 'react';
import Container from '@cloudscape-design/components/container';
import Header from '@cloudscape-design/components/header';
import SpaceBetween from '@cloudscape-design/components/space-between';
import Button from '@cloudscape-design/components/button';
import Box from '@cloudscape-design/components/box';
import StatusIndicator from '@cloudscape-design/components/status-indicator';
import ColumnLayout from '@cloudscape-design/components/column-layout';
import Alert from '@cloudscape-design/components/alert';
import { startValidation, getValidationResults, getValidationStreamUrl } from '../api/client';
import type { ValidationResult, PipelineProgress, StageProgress } from '../types';

interface Props {
  hasData: boolean;
  s3DataPath?: string;
  onValidationComplete: (result: ValidationResult) => void;
  onError: (msg: string) => void;
}

interface ErrorInfo {
  message: string;
}

const STAGE_ORDER = ['coordinator', 'rule_validator', 'llm_analyzer', 'report_notify', 'correction'] as const;

const STAGE_LABELS: Record<string, string> = {
  coordinator: 'Coordinator (데이터 수집)',
  rule_validator: 'Rule Validator (규칙 검증)',
  llm_analyzer: 'LLM Analyzer (AI 분석)',
  report_notify: 'Report & Notify (리포트 생성)',
  correction: 'Correction (데이터 보정)',
};

function stageIndicatorType(status: StageProgress['status']): 'pending' | 'in-progress' | 'success' | 'error' {
  switch (status) {
    case 'running': return 'in-progress';
    case 'completed': return 'success';
    case 'failed': return 'error';
    default: return 'pending';
  }
}

function stageDuration(stage: StageProgress | undefined): string | null {
  if (!stage?.started_at) return null;
  if (stage.status === 'running') return '실행 중...';
  if (stage.completed_at) {
    const start = new Date(stage.started_at).getTime();
    const end = new Date(stage.completed_at).getTime();
    const secs = (end - start) / 1000;
    return `${secs.toFixed(1)}s`;
  }
  return null;
}

export default function ValidationRunner({ hasData, s3DataPath, onValidationComplete, onError }: Props) {
  const [running, setRunning] = useState(false);
  const [elapsed, setElapsed] = useState(0);
  const [lastStatus, setLastStatus] = useState<'idle' | 'completed' | 'error'>('idle');
  const [errorInfo, setErrorInfo] = useState<ErrorInfo | null>(null);
  const [lastElapsed, setLastElapsed] = useState(0);
  const [stages, setStages] = useState<Record<string, StageProgress>>({});
  const timerRef = useRef<number | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);
  const elapsedRef = useRef(0);

  // Stable refs for callbacks
  const onValidationCompleteRef = useRef(onValidationComplete);
  onValidationCompleteRef.current = onValidationComplete;
  const onErrorRef = useRef(onError);
  onErrorRef.current = onError;

  // Client-side timer
  useEffect(() => {
    if (running) {
      const start = Date.now();
      timerRef.current = window.setInterval(() => {
        const secs = Math.floor((Date.now() - start) / 1000);
        setElapsed(secs);
        elapsedRef.current = secs;
      }, 1000);
    } else {
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }
    }
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [running]);

  // Cleanup EventSource on unmount
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
      }
    };
  }, []);

  const connectSSE = useCallback((jobId: string) => {
    const url = getValidationStreamUrl(jobId);
    const es = new EventSource(url);
    eventSourceRef.current = es;

    es.addEventListener('stage_update', (e: MessageEvent) => {
      try {
        const progress: PipelineProgress = JSON.parse(e.data);
        setStages(progress.stages);
      } catch {
        // ignore parse errors
      }
    });

    es.addEventListener('completed', async () => {
      es.close();
      eventSourceRef.current = null;
      try {
        const results = await getValidationResults(jobId);
        setRunning(false);
        setLastStatus('completed');
        setLastElapsed(elapsedRef.current);
        setErrorInfo(null);
        onValidationCompleteRef.current(results);
      } catch (err: any) {
        setRunning(false);
        setLastStatus('error');
        setLastElapsed(elapsedRef.current);
        const msg = `결과 조회 실패: ${err.message || '알 수 없는 오류'}`;
        setErrorInfo({ message: msg });
        onErrorRef.current(msg);
      }
    });

    es.addEventListener('error', (e: Event) => {
      // Check if this is an SSE error event with data
      const me = e as MessageEvent;
      if (me.data) {
        es.close();
        eventSourceRef.current = null;
        try {
          const payload = JSON.parse(me.data);
          const msg = payload.message || '검증 파이프라인에서 오류가 발생했습니다.';
          setRunning(false);
          setLastStatus('error');
          setLastElapsed(elapsedRef.current);
          setErrorInfo({ message: msg });
          onErrorRef.current(msg);
        } catch {
          setRunning(false);
          setLastStatus('error');
          setLastElapsed(elapsedRef.current);
          const msg = '검증 실패 (상세 정보를 가져올 수 없습니다)';
          setErrorInfo({ message: msg });
          onErrorRef.current(msg);
        }
        return;
      }

      // Connection error — EventSource will auto-reconnect unless we close
      if (es.readyState === EventSource.CLOSED) {
        eventSourceRef.current = null;
        // Connection was closed by server — check if pipeline is still running
        // If we haven't received a completed/error event, treat it as a connection loss
        setRunning(false);
        setLastStatus('error');
        setLastElapsed(elapsedRef.current);
        const msg = '서버 연결이 끊어졌습니다. 페이지를 새로고침한 후 다시 시도해 주세요.';
        setErrorInfo({ message: msg });
        onErrorRef.current(msg);
      }
    });
  }, []);

  const handleStart = async () => {
    setRunning(true);
    setElapsed(0);
    elapsedRef.current = 0;
    setLastStatus('idle');
    setErrorInfo(null);
    setStages({});

    // Close any existing EventSource
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    try {
      const resp = await startValidation(s3DataPath);
      connectSSE(resp.job_id);
    } catch (e: any) {
      setRunning(false);
      setLastStatus('error');
      setErrorInfo({ message: e.message || '검증 시작에 실패했습니다.' });
      onError(e.message || '검증 시작에 실패했습니다.');
    }
  };

  const formatTime = (seconds: number) => {
    const m = Math.floor(seconds / 60);
    const s = seconds % 60;
    return `${m}:${String(s).padStart(2, '0')}`;
  };

  return (
    <Container
      header={
        <Header
          variant="h2"
          description="Bedrock AgentCore Runtime에서 AI DQ 에이전트 파이프라인을 실행합니다"
          actions={
            <Button
              variant="primary"
              onClick={handleStart}
              loading={running}
              disabled={!hasData || running}
              iconName="caret-right-filled"
            >
              데이터 퀄리티 검증 시작
            </Button>
          }
        >
          검증 실행
        </Header>
      }
    >
      <SpaceBetween size="l">
        {running && (
          <>
            <ColumnLayout columns={2}>
              <div>
                <Box variant="awsui-key-label">경과 시간</Box>
                <Box variant="h1" fontSize="display-l" fontWeight="bold">
                  {formatTime(elapsed)}
                </Box>
              </div>
              <div>
                <Box variant="awsui-key-label">상태</Box>
                <StatusIndicator type="in-progress">
                  AgentCore Runtime에서 파이프라인 실행 중
                </StatusIndicator>
              </div>
            </ColumnLayout>

            <SpaceBetween size="xs">
              {STAGE_ORDER.map((key) => {
                const stage = stages[key];
                const status = stage?.status ?? 'pending';
                const duration = stageDuration(stage);
                return (
                  <div key={key} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <StatusIndicator type={stageIndicatorType(status)}>
                      {STAGE_LABELS[key] || key}
                    </StatusIndicator>
                    {duration && (
                      <Box variant="small" color="text-body-secondary">
                        ({duration})
                      </Box>
                    )}
                  </div>
                );
              })}
            </SpaceBetween>
          </>
        )}

        {!running && lastStatus === 'completed' && (
          <Alert type="success" header="검증 완료">
            파이프라인이 정상적으로 완료되었습니다. (소요 시간: {formatTime(lastElapsed)})
          </Alert>
        )}

        {!running && lastStatus === 'error' && errorInfo && (
          <Alert type="error" header="검증 실패">
            <SpaceBetween size="xs">
              <Box variant="p"><strong>오류 메시지:</strong> {errorInfo.message}</Box>
              <Box variant="p" color="text-body-secondary">
                소요 시간: {formatTime(lastElapsed)} | 다시 시도하려면 "데이터 퀄리티 검증 시작" 버튼을 클릭하세요.
              </Box>
            </SpaceBetween>
          </Alert>
        )}

        {!running && lastStatus === 'idle' && (
          <Box variant="p" color="text-body-secondary">
            {hasData
              ? '"데이터 퀄리티 검증 시작" 버튼을 클릭하면 검증 파이프라인이 실행됩니다.'
              : '검증을 실행하려면 먼저 샘플 데이터를 로드하거나 CSV 파일을 업로드하세요.'}
          </Box>
        )}
      </SpaceBetween>
    </Container>
  );
}
