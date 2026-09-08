import { useMemo, useState } from 'react';
import Table from '@cloudscape-design/components/table';
import Header from '@cloudscape-design/components/header';
import Container from '@cloudscape-design/components/container';
import SpaceBetween from '@cloudscape-design/components/space-between';
import ColumnLayout from '@cloudscape-design/components/column-layout';
import Box from '@cloudscape-design/components/box';
import Badge from '@cloudscape-design/components/badge';
import Button from '@cloudscape-design/components/button';
import StatusIndicator from '@cloudscape-design/components/status-indicator';
import Pagination from '@cloudscape-design/components/pagination';
import TextFilter from '@cloudscape-design/components/text-filter';
import Alert from '@cloudscape-design/components/alert';
import ExpandableSection from '@cloudscape-design/components/expandable-section';
import Popover from '@cloudscape-design/components/popover';
import { useCollection } from '@cloudscape-design/collection-hooks';
import type { ValidationResult, RecordValidationDetail, Suspect, Judgment, StageResult, DynamicRule, AnomalyStats } from '../types';

interface Props {
  result: ValidationResult;
}

const ERROR_TYPE_KR: Record<string, string> = {
  out_of_range: '범위 초과',
  format_inconsistency: '포맷 불일치',
  temporal_violation: '시간순서 위반',
  cross_column_inconsistency: '크로스컬럼 불일치',
  statistical_anomaly: '통계적 이상치',
  contextual_anomaly: '문맥적 이상치',
};

const SEVERITY_KR: Record<string, string> = {
  critical: '심각',
  warning: '주의',
  info: '참고',
};

const HEALTH_KR: Record<string, string> = {
  healthy: '정상',
  warning: '주의',
  critical: '위험',
};

const STAGE_NAME_KR: Record<string, string> = {
  coordinator: 'Coordinator',
  rule_validator: 'Rule Validator',
  anomaly_detector: 'Anomaly Detector',
  llm_analyzer: 'LLM Analyzer',
  report_notify: 'Report & Notify',
  correction: 'Correction',
};

// Stages that belong to validation (excluding correction)
const VALIDATION_STAGES = ['coordinator', 'rule_validator', 'anomaly_detector', 'llm_analyzer', 'report_notify'];

function formatDuration(seconds: number): string {
  if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  const m = Math.floor(seconds / 60);
  const s = (seconds % 60).toFixed(0);
  return `${m}m ${s}s`;
}

const TOOL_NAME_KR: Record<string, string> = {
  range_check: '범위 검사',
  regex_validate: '정규식 검증',
  timestamp_compare: '시간 비교',
  address_classify: '주소 분류',
};

function DynamicRulesSection({ result }: Props) {
  const rules = result.dynamic_rules ?? [];
  if (rules.length === 0) return null;

  return (
    <Container header={<Header variant="h2" counter={`(${rules.length}개)`}>동적 생성 규칙</Header>}>
      <SpaceBetween size="xs">
        <Box variant="small" color="text-body-secondary">
          Rule Validator가 데이터 프로파일링 결과를 기반으로 LLM을 통해 자동 생성한 검증 규칙입니다.
        </Box>
        <Table
          variant="embedded"
          items={rules}
          columnDefinitions={[
            {
              id: 'rule_id',
              header: '규칙 ID',
              cell: item => <Box fontWeight="bold">{item.rule_id}</Box>,
              width: 110,
            },
            {
              id: 'description',
              header: '설명',
              cell: item => item.description,
              width: 300,
            },
            {
              id: 'target_columns',
              header: '대상 컬럼',
              cell: item => item.target_columns.join(', '),
              width: 180,
            },
            {
              id: 'validation_tool',
              header: '검증 방법',
              cell: item => (
                <Badge color="blue">{TOOL_NAME_KR[item.validation_tool] ?? item.validation_tool}</Badge>
              ),
              width: 120,
            },
            {
              id: 'params',
              header: '파라미터',
              cell: item => (
                <Box variant="small">
                  {Object.entries(item.params ?? {}).map(([k, v]) => `${k}: ${JSON.stringify(v)}`).join(', ')}
                </Box>
              ),
              width: 250,
            },
            {
              id: 'severity',
              header: '심각도',
              cell: item => {
                const color = item.severity === 'critical' ? 'red' : item.severity === 'warning' ? 'blue' : 'grey';
                return <Badge color={color}>{SEVERITY_KR[item.severity] ?? item.severity}</Badge>;
              },
              width: 80,
            },
          ]}
          empty={<Box>동적 규칙이 없습니다</Box>}
        />
      </SpaceBetween>
    </Container>
  );
}

function StageTimeline({ result }: Props) {
  const stageResults = result.stage_results ?? {};
  const entries = VALIDATION_STAGES
    .filter(s => s in stageResults)
    .map(s => ({ name: s, ...stageResults[s] }));

  if (entries.length === 0) return null;

  const totalDuration = entries.reduce((sum, s) => sum + (s.duration_seconds ?? 0), 0);
  const hasAnomaly = 'anomaly_detector' in stageResults;
  const versionLabel = hasAnomaly ? 'v2 (이상치탐지 포함)' : 'v1 (기존)';

  return (
    <Container header={<Header variant="h2" description={`파이프라인 버전: ${versionLabel}`}>파이프라인 단계별 실행 결과</Header>}>
      <Table
        variant="embedded"
        items={entries}
        columnDefinitions={[
          {
            id: 'stage',
            header: '단계',
            cell: item => (
              <Box fontWeight="bold">{STAGE_NAME_KR[item.name] ?? item.name}</Box>
            ),
            width: 150,
          },
          {
            id: 'status',
            header: '상태',
            cell: item => (
              <StatusIndicator type={item.status === 'completed' ? 'success' : 'error'}>
                {item.status === 'completed' ? '완료' : '실패'}
              </StatusIndicator>
            ),
            width: 100,
          },
          {
            id: 'duration',
            header: '소요 시간',
            cell: item => (
              <Box>{formatDuration(item.duration_seconds ?? 0)}</Box>
            ),
            width: 120,
          },
          {
            id: 'proportion',
            header: '비율',
            cell: item => {
              const pct = totalDuration > 0
                ? ((item.duration_seconds ?? 0) / totalDuration * 100)
                : 0;
              return (
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <div style={{
                    height: 8,
                    width: `${Math.max(pct, 2)}%`,
                    maxWidth: '100%',
                    backgroundColor: item.status === 'completed' ? '#037f0c' : '#d13212',
                    borderRadius: 4,
                  }} />
                  <Box variant="small">{pct.toFixed(1)}%</Box>
                </div>
              );
            },
            width: 200,
          },
          {
            id: 'records',
            header: '처리 레코드',
            cell: item => (
              <Box>{item.records_processed != null ? `${item.records_processed}건` : '-'}</Box>
            ),
            width: 120,
          },
          {
            id: 'time_range',
            header: '시작 → 종료',
            cell: item => {
              const start = item.started_at ? new Date(item.started_at).toLocaleTimeString('ko-KR') : '-';
              const end = item.completed_at ? new Date(item.completed_at).toLocaleTimeString('ko-KR') : '-';
              return <Box variant="small">{start} → {end}</Box>;
            },
            width: 200,
          },
          {
            id: 'error',
            header: '오류',
            cell: item => item.error_message
              ? <Box variant="small" color="text-status-error">{item.error_message}</Box>
              : <Box color="text-status-inactive">-</Box>,
            width: 200,
          },
        ]}
        empty={<Box>단계 정보가 없습니다</Box>}
      />
      <Box margin={{ top: 's' }} variant="small" color="text-body-secondary">
        총 파이프라인 소요 시간: {formatDuration(totalDuration)}
      </Box>
    </Container>
  );
}

function SummaryCards({ result }: Props) {
  const healthColor = result.health_status === 'healthy'
    ? 'text-status-success'
    : result.health_status === 'warning'
      ? 'text-status-warning'
      : 'text-status-error';

  const ANOMALY_TYPES = new Set(['statistical_anomaly', 'contextual_anomaly']);
  const allSuspects = result.suspects ?? [];
  // 규칙 기반 고유 의심 레코드 (anomaly 타입 제외)
  const ruleBasedSuspectCount = new Set(
    allSuspects.filter(s => !ANOMALY_TYPES.has(s.error_type)).map(s => String(s.record_id))
  ).size;
  // 전체 고유 의심 레코드 (규칙 + 이상치 합산)
  const uniqueSuspectCount = new Set(allSuspects.map(s => String(s.record_id))).size;
  const totalRecords = result.total_records ?? 0;
  const normalPassCount = totalRecords - uniqueSuspectCount;

  return (
    <Container header={<Header variant="h2">검증 결과 요약</Header>}>
      <ColumnLayout columns={4} variant="text-grid">
        <div>
          <Box variant="awsui-key-label">파이프라인 ID</Box>
          <Box variant="p">{result.pipeline_id}</Box>
        </div>
        <div>
          <Box variant="awsui-key-label">건강도 점수</Box>
          <Box variant="h1" color={healthColor}>
            {result.health_score != null ? `${(result.health_score * 100).toFixed(0)}%` : 'N/A'}
          </Box>
          <StatusIndicator
            type={result.health_status === 'healthy' ? 'success' : result.health_status === 'warning' ? 'warning' : 'error'}
          >
            {HEALTH_KR[result.health_status ?? ''] ?? result.health_status}
          </StatusIndicator>
        </div>
        <div>
          <Box variant="awsui-key-label">전체 스캔 레코드</Box>
          <Box variant="h1">{totalRecords.toLocaleString()}</Box>
          <SpaceBetween size="xxs">
            <Box variant="small" color="text-status-success">
              정상 통과 (의심 없음): {normalPassCount.toLocaleString()}건
            </Box>
            <Box variant="small" color="text-status-warning">
              의심 항목: {uniqueSuspectCount.toLocaleString()}건
            </Box>
          </SpaceBetween>
        </div>
        <div>
          <Box variant="awsui-key-label">LLM 오류 판정 레코드</Box>
          <Box variant="h1" color="text-status-error">
            {result.violation_count?.toLocaleString() ?? 0}
          </Box>
          <Box variant="small" color="text-body-secondary">모든 신뢰도 포함, 고유 레코드 기준</Box>
        </div>
      </ColumnLayout>

      {result.validation_stats && (() => {
        const anomaly = result.anomaly_stats;
        const hasAnomaly = anomaly && anomaly.total_added > 0;
        const analysisStats = result.analysis_stats;
        const highErrorCount = analysisStats?.high_error_count ?? 0;
        const mediumErrorCount = analysisStats?.medium_error_count ?? 0;
        const lowErrorCount = analysisStats?.low_error_count ?? 0;
        const errorCount = analysisStats?.error_count ?? 0;
        const falsePositives = (analysisStats?.total_analyzed ?? 0) - errorCount;

        const totalAnalyzed = analysisStats?.total_analyzed ?? 0;
        const popoverContent = (
          <Popover
            header="오탐 제거란?"
            content={
              <SpaceBetween size="xs">
                <Box variant="p">
                  규칙 기반 검증에서 의심 항목으로 분류되었지만, LLM이 분석한 결과 <strong>실제 오류가 아닌 정상 데이터</strong>로 판정된 건수입니다.
                </Box>
                <Box variant="p" fontWeight="bold">예시:</Box>
                <Box variant="small">
                  • 중량 0.005kg → 규칙: 범위 초과(out_of_range)로 의심 → LLM: "서류 배송이므로 정상" → <strong>오탐 제거</strong>
                </Box>
                <Box variant="small">
                  • 전화번호 010-123-456 → 규칙: 형식 불일치로 의심 → LLM: "자릿수 부족, 명백한 오류" → <strong>오류 확정</strong>
                </Box>
              </SpaceBetween>
            }
            triggerType="text"
            size="large"
          >
            <Box variant="awsui-key-label" color="text-status-info" fontSize="body-s">
              오탐 제거 (정상 판정) <Box variant="small" display="inline" color="text-status-info">ⓘ</Box>
            </Box>
          </Popover>
        );

        return hasAnomaly ? (
          <SpaceBetween size="l">
            {/* Row 1: 의심 항목 수집 단계 */}
            <Box margin={{ top: 'l' }}>
              <Box variant="small" fontWeight="bold" margin={{ bottom: 'xs' }} color="text-body-secondary">의심 항목 수집</Box>
              <ColumnLayout columns={3} variant="text-grid">
                <div>
                  <Box variant="awsui-key-label">규칙 기반 의심 항목</Box>
                  <Box variant="p">{ruleBasedSuspectCount}건</Box>
                  <Box variant="small" color="text-body-secondary">고유 레코드 기준</Box>
                </div>
                <div>
                  <Box variant="awsui-key-label">
                    <Popover
                      header="통계적 vs 맥락적 이상치 탐지"
                      size="large"
                      triggerType="text"
                      content={
                        <SpaceBetween size="s">
                          <div>
                            <Box variant="h4">통계적 (Statistical)</Box>
                            <Box variant="small">값 하나만 보고 전체 분포에서 극단적인지 판단합니다.</Box>
                            <Box variant="small" color="text-body-secondary" margin={{ top: 'xxs' }}>
                              예: 운송비가 50,000원 → 전체 운송비 분포에서 Z-Score 4.2 → 이상치
                            </Box>
                            <Box variant="small" color="text-body-secondary">
                              기법: Z-Score, IQR, Isolation Forest
                            </Box>
                          </div>
                          <div>
                            <Box variant="h4">맥락적 (Contextual)</Box>
                            <Box variant="small">값 자체는 정상이지만, 다른 조건과 함께 보면 이상한 데이터를 찾습니다.</Box>
                            <Box variant="small" color="text-body-secondary" margin={{ top: 'xxs' }}>
                              예: 운송비 5,000원은 전체에서 정상이지만, "소형 박스" 그룹 내 평균 1,900원 대비 이상치
                            </Box>
                            <Box variant="small" color="text-body-secondary">
                              예: 서울→부산인데 운송비 2,100원 → 거리 대비 운송비 상관관계 이탈
                            </Box>
                            <Box variant="small" color="text-body-secondary">
                              기법: 조건부 이상치, 상관관계 이탈, 희귀 조합
                            </Box>
                          </div>
                          <Box variant="small" fontWeight="bold">
                            한 줄 요약: "혼자 보면 멀쩡한데, 같이 보면 이상한 것"이 맥락적 이상치입니다.
                          </Box>
                        </SpaceBetween>
                      }
                    >
                      이상치 탐지 추가 의심
                    </Popover>
                  </Box>
                  <Box variant="p" color="text-status-warning">{anomaly.total_added}건</Box>
                  <Box variant="small" color="text-body-secondary">
                    통계적 {anomaly.statistical_count} + 맥락적 {anomaly.contextual_count}
                  </Box>
                  {anomaly.methods_used && anomaly.methods_used.length > 0 && (
                    <Box variant="small" color="text-body-secondary">
                      탐지 기법: {anomaly.methods_used.join(', ')}
                    </Box>
                  )}
                </div>
                <div>
                  <Box variant="awsui-key-label">LLM 분석 대상 (합산)</Box>
                  <Box variant="p">{totalAnalyzed}건</Box>
                  <Box variant="small" color="text-body-secondary">
                    규칙 {ruleBasedSuspectCount} + 이상치 {anomaly.total_added}
                  </Box>
                </div>
              </ColumnLayout>
            </Box>
            {/* Row 2: LLM 분석 결과 */}
            <Box>
              <Box variant="small" fontWeight="bold" margin={{ bottom: 'xs' }} color="text-body-secondary">LLM 분석 결과</Box>
              <ColumnLayout columns={3} variant="text-grid">
                <div>
                  {popoverContent}
                  <Box variant="p">{falsePositives}건</Box>
                </div>
                <div>
                  <Box variant="awsui-key-label">LLM 오류 판정 (전체)</Box>
                  <Box variant="p" color="text-status-error">{errorCount}건</Box>
                  <Box variant="small" color="text-body-secondary">
                    HIGH {highErrorCount} · MEDIUM {mediumErrorCount} · LOW {lowErrorCount}
                  </Box>
                </div>
                <div>
                  <Box variant="awsui-key-label">오탐율</Box>
                  <Box variant="p">{totalAnalyzed > 0 ? ((falsePositives / totalAnalyzed) * 100).toFixed(0) : 0}%</Box>
                  <Box variant="small" color="text-body-secondary">
                    {falsePositives} / {totalAnalyzed}건
                  </Box>
                </div>
              </ColumnLayout>
            </Box>
          </SpaceBetween>
        ) : (
          <Box margin={{ top: 'l' }}>
            <ColumnLayout columns={4} variant="text-grid">
              <div>
                <Box variant="awsui-key-label">규칙 기반 의심 항목</Box>
                <Box variant="p">{uniqueSuspectCount}건</Box>
                <Box variant="small" color="text-body-secondary">고유 레코드 기준</Box>
              </div>
              <div>
                <Box variant="awsui-key-label">LLM 분석 대상</Box>
                <Box variant="p">{totalAnalyzed}건</Box>
              </div>
              <div>
                {popoverContent}
                <Box variant="p">{falsePositives}건</Box>
              </div>
              <div>
                <Box variant="awsui-key-label">LLM 오류 판정 (전체)</Box>
                <Box variant="p" color="text-status-error">{errorCount}건</Box>
                <Box variant="small" color="text-body-secondary">
                  HIGH {highErrorCount} · MEDIUM {mediumErrorCount} · LOW {lowErrorCount}
                </Box>
              </div>
            </ColumnLayout>
          </Box>
        );
      })()}

      {(() => {
        const inputTokens = result.analysis_stats?.input_tokens ?? 0;
        const outputTokens = result.analysis_stats?.output_tokens ?? 0;
        const totalTokens = inputTokens + outputTokens;
        // Claude Sonnet 4 on Bedrock: $3/1M input, $15/1M output
        const inputCost = (inputTokens / 1_000_000) * 3;
        const outputCost = (outputTokens / 1_000_000) * 15;
        const totalCost = inputCost + outputCost;
        return (
          <Box margin={{ top: 'l' }}>
            <ColumnLayout columns={4} variant="text-grid">
              <div>
                <Box variant="awsui-key-label">LLM 입력 토큰</Box>
                <Box variant="p">{inputTokens.toLocaleString()}</Box>
              </div>
              <div>
                <Box variant="awsui-key-label">LLM 출력 토큰</Box>
                <Box variant="p">{outputTokens.toLocaleString()}</Box>
              </div>
              <div>
                <Box variant="awsui-key-label">총 토큰 사용량</Box>
                <Box variant="p" fontWeight="bold">{totalTokens.toLocaleString()}</Box>
              </div>
              <div>
                <Box variant="awsui-key-label">예상 LLM 비용</Box>
                <Box variant="p" fontWeight="bold">${totalCost.toFixed(4)}</Box>
                <Box variant="small" color="text-body-secondary">
                  입력 ${inputCost.toFixed(4)} + 출력 ${outputCost.toFixed(4)}
                </Box>
              </div>
            </ColumnLayout>
          </Box>
        );
      })()}

      <Box margin={{ top: 'l' }}>
        <ExpandableSection headerText="건강도 점수 산정 기준" variant="footer">
          <SpaceBetween size="xs">
            <Box variant="p">
              건강도 점수는 전체 스캔 레코드 수 대비 LLM 확정 오류의 <strong>가중 오류율</strong>로 산출합니다.
              신뢰도가 높을수록 가중치가 크게 반영됩니다.
            </Box>
            <Box variant="code">
              가중 오류율 = (HIGH 오류 × 1.0 + MEDIUM·LOW 오류 × 0.5) / 전체 스캔 레코드{'\n'}
              건강도 = 1.0 − 가중 오류율
            </Box>
            {(() => {
              const total = result.total_records ?? 0;
              // Count errors by confidence (is_error=true only)
              // Prefer backend-computed stats; fallback to counting from judgments
              const stats = result.analysis_stats;
              const judgments = result.judgments ?? [];
              const highErrors = stats?.high_error_count ?? judgments.filter(j => j.is_error && j.confidence === 'HIGH').length;
              const medLowErrors = (stats?.medium_error_count ?? judgments.filter(j => j.is_error && j.confidence === 'MEDIUM').length)
                + (stats?.low_error_count ?? judgments.filter(j => j.is_error && j.confidence === 'LOW').length);
              const weighted = highErrors * 1.0 + medLowErrors * 0.5;
              const rate = total > 0 ? weighted / total : 0;
              return (
                <Table
                  variant="embedded"
                  items={[
                    { factor: 'HIGH 신뢰도 확정 오류', weight: '× 1.0', count: highErrors, contribution: highErrors * 1.0 },
                    { factor: 'MEDIUM + LOW 신뢰도 오류', weight: '× 0.5', count: medLowErrors, contribution: medLowErrors * 0.5 },
                    { factor: '가중 오류 합계', weight: '', count: highErrors + medLowErrors, contribution: weighted },
                  ]}
                  columnDefinitions={[
                    { id: 'factor', header: '항목', cell: item => <Box fontWeight="bold">{item.factor}</Box>, width: 220 },
                    { id: 'count', header: '건수', cell: item => `${item.count}건`, width: 80 },
                    { id: 'weight', header: '가중치', cell: item => item.weight || '-', width: 80 },
                    { id: 'contribution', header: '가중 오류', cell: item => item.contribution.toFixed(1), width: 100 },
                    { id: 'calc', header: '산출', cell: item => (
                      item.weight === ''
                        ? <Box variant="small">{`${weighted.toFixed(1)} / ${total}건 = 오류율 ${(rate * 100).toFixed(1)}% → 건강도 ${((1 - rate) * 100).toFixed(0)}%`}</Box>
                        : <Box color="text-status-inactive">-</Box>
                    ), width: 320 },
                  ]}
                />
              );
            })()}
            <Box variant="small" color="text-body-secondary">
              상태 기준: 80% 이상 = 정상(healthy), 50%~79% = 주의(warning), 50% 미만 = 위험(critical).
              규칙 기반 의심 항목(suspect)만으로는 감점되지 않으며, LLM이 실제 오류로 확정한 건만 반영됩니다.
            </Box>
          </SpaceBetween>
        </ExpandableSection>
      </Box>

      <Box margin={{ top: 's' }}>
        <ExpandableSection headerText="신뢰도(HIGH / MEDIUM / LOW) 판정 기준" variant="footer">
          <SpaceBetween size="s">
            <Box variant="p">
              신뢰도는 LLM Analyzer의 PRIMARY 분석에서 <strong>명시적 판정 기준</strong>에 따라 결정됩니다.
              시스템 프롬프트에 아래 기준이 정의되어 있어, LLM이 일관된 기준으로 판정합니다.
            </Box>

            <Table
              variant="embedded"
              items={[
                { level: 'HIGH', criteria: '데이터만으로 오류 여부를 확실히 판단할 수 있는 경우', example: '형식 오류(우편번호 자릿수, 전화번호 패턴), 명백한 범위 초과(음수 중량, 미래 날짜), 논리적 모순(배송완료 시간 < 접수 시간)' },
                { level: 'MEDIUM', criteria: '오류 가능성이 높지만 비즈니스 예외가 존재할 수 있는 경우', example: '범위 경계값(최소/최대에 근접), 비표준이지만 유효할 수 있는 형식, 도메인 지식이 필요한 크로스컬럼 불일치' },
                { level: 'LOW', criteria: '오류인지 확신할 수 없는 경우', example: '통계적으로 드문 값이지만 정상 범위일 수 있는 경우, 비즈니스 컨텍스트에 따라 정상/오류가 달라지는 경우' },
              ]}
              columnDefinitions={[
                { id: 'level', header: '신뢰도', cell: item => {
                  const color = item.level === 'HIGH' ? 'red' : item.level === 'MEDIUM' ? 'blue' : 'grey';
                  return <Badge color={color}>{item.level}</Badge>;
                }, width: 100 },
                { id: 'criteria', header: '판정 기준', cell: item => <Box fontWeight="bold">{item.criteria}</Box>, width: 300 },
                { id: 'example', header: '해당 사례', cell: item => <Box variant="small">{item.example}</Box>, width: 400 },
              ]}
            />

            <Box variant="small" color="text-body-secondary">
              모든 신뢰도(HIGH/MEDIUM/LOW)의 판정 결과가 위 상세 테이블에 표시됩니다.
              사용자가 각 레코드의 신뢰도와 판정 근거를 직접 확인하고 적절성을 검토할 수 있습니다.
              건강도 점수 산정 시 HIGH 오류는 가중치 1.0, MEDIUM·LOW 오류는 가중치 0.5로 반영됩니다.
            </Box>
          </SpaceBetween>
        </ExpandableSection>
      </Box>
    </Container>
  );
}

function ErrorTypeDistribution({ result }: Props) {
  const dist = result.validation_stats?.stats_by_error_type ?? {};
  const total = Object.values(dist).reduce((a, b) => a + b, 0);
  if (total === 0) return null;

  const colors: Record<string, string> = {
    out_of_range: '#d13212',
    format_inconsistency: '#ff9900',
    temporal_violation: '#0073bb',
    cross_column_inconsistency: '#687078',
    statistical_anomaly: '#9469d6',
    contextual_anomaly: '#6b40c4',
  };

  return (
    <Container header={<Header variant="h3">오류 유형 분포</Header>}>
      <SpaceBetween size="s">
        {Object.entries(dist).map(([type, count]) => (
          <div key={type} style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <div style={{
              width: 12, height: 12, borderRadius: 2,
              backgroundColor: colors[type] || '#879596',
            }} />
            <Box variant="small" display="inline">{ERROR_TYPE_KR[type] ?? type}</Box>
            <Box variant="small" display="inline" fontWeight="bold">{count}건</Box>
            <Box variant="small" display="inline" color="text-body-secondary">
              ({((count / total) * 100).toFixed(0)}%)
            </Box>
          </div>
        ))}
      </SpaceBetween>
    </Container>
  );
}

/** Clean up record_id that may be a stringified Python dict like "{'record_id': '123'}" or "{'record_id': 123}" */
function cleanRecordId(raw: string): string {
  const cleaned = String(raw).trim();
  // Match Python dict format with string value: {'record_id': '...'} or {"record_id": "..."}
  const strMatch = cleaned.match(/['"]record_id['"]\s*:\s*['"]([^'"]+)['"]/);
  if (strMatch) return strMatch[1];
  // Match Python dict format with numeric value: {'record_id': 123}
  const numMatch = cleaned.match(/['"]record_id['"]\s*:\s*(\d+)/);
  if (numMatch) return numMatch[1];
  return cleaned;
}

function DetailTable({ result }: Props) {
  const [filterText, setFilterText] = useState('');
  const hasAnyJudgments = (result.judgments ?? []).length > 0;

  const details = useMemo<RecordValidationDetail[]>(() => {
    const suspects = result.suspects ?? [];
    const judgments = result.judgments ?? [];

    const byRecord = new Map<string, { suspects: Suspect[]; judgment: Judgment | null }>();

    for (const s of suspects) {
      const rid = cleanRecordId(String(s.record_id));
      if (!byRecord.has(rid)) byRecord.set(rid, { suspects: [], judgment: null });
      byRecord.get(rid)!.suspects.push(s);
    }
    for (const j of judgments) {
      const rid = cleanRecordId(String(j.record_id));
      if (!byRecord.has(rid)) byRecord.set(rid, { suspects: [], judgment: null });
      // First-wins: keep the first judgment per record_id (matches backend dedup logic)
      if (!byRecord.get(rid)!.judgment) {
        byRecord.get(rid)!.judgment = j;
      }
    }

    return Array.from(byRecord.entries()).map(([rid, data]) => {
      const hasSuspects = data.suspects.length > 0;
      const hasJudgment = data.judgment !== null;
      const source: 'rule' | 'llm' | 'both' =
        hasSuspects && hasJudgment ? 'both' : hasJudgment ? 'llm' : 'rule';

      return {
        record_id: rid,
        suspects: data.suspects,
        judgment: data.judgment,
        has_error: data.judgment?.is_error ?? data.suspects.length > 0,
        confidence: data.judgment?.confidence ?? 'N/A',
        suggested_correction: data.judgment?.suggested_correction ?? null,
        source,
      };
    });
  }, [result]);

  const { items, collectionProps, paginationProps } = useCollection(details, {
    filtering: {
      filteringFunction: (item) => {
        if (!filterText) return true;
        const t = filterText.toLowerCase();
        if (item.record_id.toLowerCase().includes(t)) return true;
        if (item.suspects.some(s => s.error_type.toLowerCase().includes(t))) return true;
        if (item.suspects.some(s => s.rule_id.toLowerCase().includes(t))) return true;
        if (item.confidence.toLowerCase().includes(t)) return true;
        const sourceLabel = item.source === 'both' ? '규칙 llm' : item.source === 'llm' ? 'llm' : '규칙';
        if (sourceLabel.includes(t)) return true;
        return false;
      },
    },
    pagination: { pageSize: 15 },
    sorting: {},
  });

  const severityBadge = (sev: string) => {
    const color = sev === 'critical' ? 'red' : sev === 'warning' ? 'blue' : 'grey';
    return <Badge color={color}>{SEVERITY_KR[sev] ?? sev}</Badge>;
  };

  const confidenceBadge = (conf: string) => {
    const color = conf === 'HIGH' ? 'red' : conf === 'MEDIUM' ? 'blue' : 'grey';
    return <Badge color={color}>{conf}</Badge>;
  };

  const sourceBadge = (source: 'rule' | 'llm' | 'both') => {
    if (source === 'both') return <><Badge color="blue">규칙</Badge>{' '}<Badge color="red">LLM</Badge></>;
    if (source === 'llm') return <Badge color="red">LLM 판정</Badge>;
    return <Badge color="blue">규칙 기반</Badge>;
  };

  return (
    <SpaceBetween size="s">
      {!hasAnyJudgments && (result.analysis_stats?.total_analyzed ?? 0) === 0 && (() => {
        const stats = result.analysis_stats;
        const suspectInput = stats?.suspect_input_count ?? 0;
        const primaryFailed = stats?.primary_failed_count ?? 0;
        const reasons = stats?.failure_reasons ?? [];
        const hasFailed = primaryFailed > 0;

        // Determine cause description
        let causeText: string;
        if (hasFailed && reasons.length > 0) {
          const reasonsSummary = reasons.map(r => {
            if (r.includes('Unterminated string') || r.includes('JSON parse error'))
              return 'LLM 응답이 max_tokens 제한으로 잘려 JSON 파싱에 실패했습니다. (배치 크기 축소 또는 max_tokens 증가 필요)';
            if (r.includes('timeout') || r.includes('Timeout'))
              return 'Bedrock API 호출 시간이 초과되었습니다.';
            if (r.includes('AccessDeniedException') || r.includes('UnauthorizedAccess'))
              return 'Bedrock 모델 접근 권한이 없습니다.';
            return r;
          });
          causeText = [...new Set(reasonsSummary)].join('\n');
        } else if (suspectInput === 0) {
          causeText = 'LLM Analyzer에 전달된 의심 항목(suspect)이 0건입니다. S3에서 suspects 파일을 읽지 못했을 수 있습니다.';
        } else {
          causeText = '원인을 특정할 수 없습니다. AgentCore CloudWatch 로그를 확인해 주세요.';
        }

        return (
          <Alert type="warning" header="LLM 판정 실패">
            <SpaceBetween size="xs">
              <Box variant="p">
                LLM Analyzer가 {suspectInput}건의 의심 항목에 대해 판정을 시도했으나, 결과를 생성하지 못했습니다.
                현재 결과는 모두 규칙 기반(Rule Validator) 검증 결과입니다.
              </Box>
              {hasFailed && (
                <Box variant="p">
                  <strong>실패 내역:</strong> PRIMARY 분석 실패 {primaryFailed}건
                </Box>
              )}
              <Box variant="p"><strong>원인:</strong> {causeText}</Box>
            </SpaceBetween>
          </Alert>
        );
      })()}
      <Table
        {...collectionProps}
        variant="container"
        header={
          <Header
            variant="h2"
            description="레코드별 규칙 위반 사항 및 LLM 판정 결과 · 보정 추천은 형식 오류(우편번호, 전화번호), 범위 초과(음수 중량) 등 올바른 값을 추론할 수 있는 경우에만 제공됩니다. 통계적 이상치는 이상 여부만 판단하고 올바른 값을 알 수 없어 보정 추천이 제공되지 않습니다."
          >
            레코드별 상세 검증 결과
          </Header>
        }
        items={items}
        columnDefinitions={[
          {
            id: 'record_id',
            header: '레코드 ID',
            cell: item => <Box fontWeight="bold">{item.record_id}</Box>,
            sortingComparator: (a, b) => {
              const aNum = Number(a.record_id);
              const bNum = Number(b.record_id);
              if (!isNaN(aNum) && !isNaN(bNum)) return aNum - bNum;
              return a.record_id.localeCompare(b.record_id);
            },
            width: 130,
          },
          {
            id: 'source',
            header: '검증 유형',
            cell: item => sourceBadge(item.source),
            sortingComparator: (a, b) => {
              const order: Record<string, number> = { both: 0, rule: 1, llm: 2 };
              return (order[a.source] ?? 3) - (order[b.source] ?? 3);
            },
            width: 130,
          },
          {
            id: 'status',
            header: '상태',
            cell: item => {
              if (!item.judgment) {
                return <StatusIndicator type="pending">미판정</StatusIndicator>;
              }
              if (item.judgment.is_error) {
                return <StatusIndicator type="error">오류 확정</StatusIndicator>;
              }
              return <StatusIndicator type="success">정상 판정 (오탐)</StatusIndicator>;
            },
            sortingComparator: (a, b) => {
              const order = (item: typeof a) => !item.judgment ? 2 : item.judgment.is_error ? 0 : 1;
              return order(a) - order(b);
            },
            width: 130,
          },
          {
            id: 'error_types',
            header: '오류 유형',
            cell: item => (
              <SpaceBetween size="xxs" direction="horizontal">
                {[...new Set(item.suspects.map(s => s.error_type))].map(et => (
                  <Badge key={et} color="blue">{ERROR_TYPE_KR[et] ?? et}</Badge>
                ))}
              </SpaceBetween>
            ),
            sortingComparator: (a, b) => {
              const aType = [...new Set(a.suspects.map(s => s.error_type))].join(',');
              const bType = [...new Set(b.suspects.map(s => s.error_type))].join(',');
              return aType.localeCompare(bType);
            },
            width: 200,
          },
          {
            id: 'severity',
            header: '심각도',
            cell: item => {
              const severities = [...new Set(item.suspects.map(s => s.severity))];
              return (
                <SpaceBetween size="xxs" direction="horizontal">
                  {severities.map(s => <span key={s}>{severityBadge(s)}</span>)}
                </SpaceBetween>
              );
            },
            sortingComparator: (a, b) => {
              const order: Record<string, number> = { critical: 0, warning: 1, info: 2 };
              const aVals = a.suspects.map(s => order[s.severity] ?? 3);
              const bVals = b.suspects.map(s => order[s.severity] ?? 3);
              const aMax = aVals.length > 0 ? Math.min(...aVals) : 4;
              const bMax = bVals.length > 0 ? Math.min(...bVals) : 4;
              return aMax - bMax;
            },
            width: 120,
          },
          {
            id: 'violations',
            header: '위반 상세',
            cell: item => (
              <ExpandableSection headerText={`${item.suspects.length}건의 위반`} variant="footer">
                <SpaceBetween size="xs">
                  {item.suspects.map((s, i) => (
                    <Box key={i} variant="small">
                      <strong>{s.rule_id}</strong>: {s.reason}
                      <br />
                      대상 컬럼: {s.target_columns.join(', ')} |
                      현재 값: {JSON.stringify(s.current_values)}
                    </Box>
                  ))}
                </SpaceBetween>
              </ExpandableSection>
            ),
            sortingComparator: (a, b) => a.suspects.length - b.suspects.length,
            width: 300,
          },
          {
            id: 'confidence',
            header: 'LLM 신뢰도',
            cell: item => item.judgment ? confidenceBadge(item.confidence) : <Box color="text-status-inactive">-</Box>,
            sortingComparator: (a, b) => {
              const order: Record<string, number> = { HIGH: 0, MEDIUM: 1, LOW: 2, 'N/A': 3 };
              return (order[a.confidence] ?? 3) - (order[b.confidence] ?? 3);
            },
            width: 120,
          },
          {
            id: 'evidence',
            header: 'LLM 판정 근거',
            cell: item => (
              <Box variant="small">
                {item.judgment?.evidence || item.judgment?.reasoning || '-'}
              </Box>
            ),
            sortingComparator: (a, b) => {
              const aText = a.judgment?.evidence || a.judgment?.reasoning || '';
              const bText = b.judgment?.evidence || b.judgment?.reasoning || '';
              return aText.localeCompare(bText);
            },
            width: 250,
          },
          {
            id: 'correction',
            header: '보정 추천',
            cell: item => {
              const corr = item.suggested_correction;
              if (!corr || Object.keys(corr).length === 0) {
                return <Box color="text-status-inactive">-</Box>;
              }
              const currentVals = item.suspects.length > 0 ? item.suspects[0].current_values : {};
              return (
                <ExpandableSection headerText={`${Object.keys(corr).length}건 제안`} variant="footer">
                  <SpaceBetween size="xxs">
                    {Object.entries(corr).map(([col, val]) => {
                      const orig = currentVals[col];
                      return (
                        <Box key={col} variant="small">
                          <strong>{col}</strong>:{' '}
                          <span style={{ color: '#d13212', textDecoration: 'line-through' }}>
                            {orig != null ? String(orig) : '?'}
                          </span>
                          {' → '}
                          <span style={{ color: '#037f0c', fontWeight: 600 }}>
                            {val != null ? String(val) : 'null'}
                          </span>
                        </Box>
                      );
                    })}
                  </SpaceBetween>
                </ExpandableSection>
              );
            },
            sortingComparator: (a, b) => {
              const aCount = a.suggested_correction ? Object.keys(a.suggested_correction).length : 0;
              const bCount = b.suggested_correction ? Object.keys(b.suggested_correction).length : 0;
              return aCount - bCount;
            },
            width: 220,
          },
        ]}
      resizableColumns
      stickyHeader
      stripedRows
      wrapLines
      empty={
        <Box textAlign="center" padding="l">
          <b>위반 항목이 없습니다</b>
        </Box>
      }
      filter={
        <TextFilter
          filteringText={filterText}
          onChange={({ detail }) => setFilterText(detail.filteringText)}
          filteringPlaceholder="레코드 ID, 오류 유형, 규칙으로 검색..."
        />
      }
      pagination={<Pagination {...paginationProps} />}
    />
    </SpaceBetween>
  );
}

function CorrectionSection({ result }: Props) {
  const judgments = result.judgments ?? [];
  const suspects = result.suspects ?? [];
  if (judgments.length === 0 && suspects.length === 0) return null;

  // Build correction proposals from judgments that have suggested_correction
  const proposals = judgments
    .filter(j => j.is_error && j.suggested_correction && Object.keys(j.suggested_correction).length > 0)
    .map(j => {
      const rid = cleanRecordId(String(j.record_id));
      const matchingSuspect = suspects.find(s => cleanRecordId(String(s.record_id)) === rid);
      const currentVals = matchingSuspect?.current_values ?? {};
      return { ...j, record_id: rid, currentVals };
    });

  const highProposals = proposals.filter(p => p.confidence === 'HIGH');
  const medLowProposals = proposals.filter(p => p.confidence !== 'HIGH');
  const errorCount = judgments.filter(j => j.is_error).length;
  const noCorrection = errorCount - proposals.length;

  return (
    <Container
      header={
        <Header
          variant="h2"
          description="LLM이 오류로 확정한 항목에 대해 보정값을 추천하고, 담당자 승인 후 보정을 실행합니다"
          actions={
            <Button variant="primary" disabled>
              보정 승인 및 실행
            </Button>
          }
        >
          데이터 보정 (Human-in-the-Loop)
        </Header>
      }
    >
      <SpaceBetween size="m">
        <ColumnLayout columns={4} variant="text-grid">
          <div>
            <Box variant="awsui-key-label">확정 오류</Box>
            <Box variant="h1" color="text-status-error">{errorCount}건</Box>
          </div>
          <div>
            <Box variant="awsui-key-label">보정 추천 가능</Box>
            <Box variant="h1" color="text-status-success">{proposals.length}건</Box>
          </div>
          <div>
            <Box variant="awsui-key-label">HIGH 신뢰도 추천</Box>
            <Box variant="h1">{highProposals.length}건</Box>
          </div>
          <div>
            <Box variant="awsui-key-label">추천 불가 (수동 검토 필요)</Box>
            <Box variant="h1" color="text-body-secondary">{noCorrection}건</Box>
          </div>
        </ColumnLayout>

        {proposals.length > 0 && (
          <ExpandableSection
            defaultExpanded
            headerText={`보정 추천 상세 (${proposals.length}건)`}
            variant="footer"
          >
            <Table
              variant="embedded"
              items={proposals}
              columnDefinitions={[
                {
                  id: 'record_id',
                  header: '레코드 ID',
                  cell: item => <Box fontWeight="bold">{item.record_id}</Box>,
                  width: 130,
                },
                {
                  id: 'confidence',
                  header: '신뢰도',
                  cell: item => {
                    const color = item.confidence === 'HIGH' ? 'red' : item.confidence === 'MEDIUM' ? 'blue' : 'grey';
                    return <Badge color={color}>{item.confidence}</Badge>;
                  },
                  width: 90,
                },
                {
                  id: 'error_type',
                  header: '오류 유형',
                  cell: item => <Badge color="blue">{ERROR_TYPE_KR[item.error_type ?? ''] ?? item.error_type}</Badge>,
                  width: 140,
                },
                {
                  id: 'changes',
                  header: '현재값 → 추천 보정값',
                  cell: item => {
                    const corr = item.suggested_correction ?? {};
                    return (
                      <SpaceBetween size="xxs">
                        {Object.entries(corr).map(([col, val]) => {
                          const orig = item.currentVals[col];
                          return (
                            <Box key={col} variant="small">
                              <strong>{col}</strong>:{' '}
                              <span style={{ color: '#d13212', textDecoration: 'line-through' }}>
                                {orig != null ? String(orig) : '?'}
                              </span>
                              {' → '}
                              <span style={{ color: '#037f0c', fontWeight: 600 }}>
                                {val != null ? String(val) : 'null'}
                              </span>
                            </Box>
                          );
                        })}
                      </SpaceBetween>
                    );
                  },
                  width: 350,
                },
                {
                  id: 'evidence',
                  header: '판정 근거',
                  cell: item => (
                    <Box variant="small">{item.evidence || (item as any).reasoning || '-'}</Box>
                  ),
                  width: 250,
                },
              ]}
              stripedRows
            />
          </ExpandableSection>
        )}

        <Alert type="info" header="승인 대기 중">
          위 보정 추천 결과는 LLM이 오류 데이터에서 올바른 값을 추론한 결과입니다.
          데이터 보정은 담당자의 검토 및 승인 후에만 실행됩니다.
          승인 시 사전 스냅샷이 저장되고, HIGH 신뢰도 오류는 격리(Quarantine) 후 보정됩니다.
        </Alert>

        <ColumnLayout columns={3} variant="text-grid">
          <div>
            <Box variant="awsui-key-label">보정 상태</Box>
            <StatusIndicator type="pending">승인 대기</StatusIndicator>
          </div>
          <div>
            <Box variant="awsui-key-label">승인 방식</Box>
            <Box variant="p">Human-in-the-Loop (인터랙티브 승인)</Box>
          </div>
          <div>
            <Box variant="awsui-key-label">안전장치</Box>
            <Box variant="p">사전 스냅샷 + Quarantine 격리 + 감사 로그</Box>
          </div>
        </ColumnLayout>
      </SpaceBetween>
    </Container>
  );
}

export default function ValidationResults({ result }: Props) {
  if (result.status === 'error') {
    return (
      <Alert type="error" header="검증 실패">
        {result.error || '검증 중 예기치 않은 오류가 발생했습니다.'}
      </Alert>
    );
  }

  return (
    <SpaceBetween size="l">
      <SummaryCards result={result} />

      <DynamicRulesSection result={result} />

      <StageTimeline result={result} />

      <ColumnLayout columns={1}>
        <ErrorTypeDistribution result={result} />
      </ColumnLayout>

      <DetailTable result={result} />

      <CorrectionSection result={result} />
    </SpaceBetween>
  );
}
