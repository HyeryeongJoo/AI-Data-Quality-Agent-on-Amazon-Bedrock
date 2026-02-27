import Container from '@cloudscape-design/components/container';
import Header from '@cloudscape-design/components/header';
import SpaceBetween from '@cloudscape-design/components/space-between';
import ColumnLayout from '@cloudscape-design/components/column-layout';
import Box from '@cloudscape-design/components/box';
import Badge from '@cloudscape-design/components/badge';
import StatusIndicator from '@cloudscape-design/components/status-indicator';
import ExpandableSection from '@cloudscape-design/components/expandable-section';
import Table from '@cloudscape-design/components/table';
import Alert from '@cloudscape-design/components/alert';
import Link from '@cloudscape-design/components/link';

function NodeCard({ title, type, description }: { title: string; type: string; description: string }) {
  return (
    <Container>
      <SpaceBetween size="xxs">
        <Box variant="h4">{title}</Box>
        <StatusIndicator type="info">{type}</StatusIndicator>
        <Box variant="small" color="text-body-secondary">{description}</Box>
      </SpaceBetween>
    </Container>
  );
}

function PipelineFlowDiagram() {
  return (
    <div style={{
      background: '#f2f3f3',
      borderRadius: 8,
      padding: '20px 16px',
      overflowX: 'auto',
    }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: 4,
        minWidth: 900,
        fontFamily: 'monospace',
        fontSize: 13,
        lineHeight: 1.6,
      }}>
        {[
          { label: 'Coordinator', sub: '데이터 추출', color: '#0972d3' },
          { label: 'DQ Validator', sub: '규칙 기반 검증', color: '#037f0c' },
          { label: 'LLM Analyzer', sub: 'AI 시맨틱 분석', color: '#d13212' },
          { label: 'Report & Notify', sub: '리포트 & 알림', color: '#ff9900' },
          { label: 'Correction', sub: 'HITL 보정', color: '#687078' },
        ].map((node, i, arr) => (
          <div key={node.label} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <div style={{
              border: `2px solid ${node.color}`,
              borderRadius: 8,
              padding: '8px 14px',
              background: '#fff',
              textAlign: 'center',
              minWidth: 120,
            }}>
              <div style={{ fontWeight: 700, color: node.color }}>{node.label}</div>
              <div style={{ fontSize: 11, color: '#687078' }}>{node.sub}</div>
            </div>
            {i < arr.length - 1 && (
              <div style={{ color: '#687078', fontSize: 18, fontWeight: 700, padding: '0 2px' }}>&rarr;</div>
            )}
          </div>
        ))}
      </div>
      <div style={{
        textAlign: 'center',
        marginTop: 10,
        fontSize: 12,
        color: '#687078',
      }}>
        DAG(Directed Acyclic Graph) 기반 순차 실행 | 조건부 라우팅: 의심 항목 0건 시 LLM Analyzer 건너뜀 | dry_run 시 Correction 건너뜀
      </div>
    </div>
  );
}

function ArchitectureRationale() {
  return (
    <ExpandableSection
      defaultExpanded
      headerText="왜 이 아키텍처인가?"
      variant="container"
      headerDescription="규칙 기반 + LLM 하이브리드 검증 아키텍처의 설계 근거"
    >
      <SpaceBetween size="m">
        <Alert type="info" header="핵심 설계 철학">
          기존 데이터 품질 검증은 정적 규칙에 의존하여 "이미 알고 있는 오류"만 탐지합니다.
          이 에이전트는 LLM의 문맥 이해 능력을 결합하여 "아직 모르는 오류"까지 자율적으로 발견하고,
          사람의 승인 하에 자동 보정까지 수행하는 엔드투엔드 파이프라인입니다.
        </Alert>

        <Table
          variant="embedded"
          items={[
            {
              challenge: '사전 정의 규칙의 한계',
              traditional: '규칙에 없는 오류는 탐지 불가. 새 오류 패턴 발견 시 수동으로 규칙 추가 필요.',
              solution: 'LLM이 데이터 프로파일링 결과를 분석하여 동적 규칙을 자동 생성. 데이터 변화에 적응.',
            },
            {
              challenge: 'LLM 판정의 불확실성',
              traditional: 'LLM만으로 검증하면 환각(Hallucination)으로 오탐 위험.',
              solution: '2단계 자기 검증(PRIMARY + REFLECTION)으로 오탐을 줄이고, 불일치 시 신뢰도를 하향.',
            },
            {
              challenge: '대규모 데이터 처리',
              traditional: 'LLM에 모든 레코드를 보내면 비용과 지연이 과다.',
              solution: '규칙 기반 1차 필터링(전수 스캔) → 의심 항목만 LLM 분석. 비용 최적화.',
            },
            {
              challenge: '자동 보정의 위험',
              traditional: 'AI가 자동으로 데이터를 수정하면 비즈니스 리스크 발생.',
              solution: 'Human-in-the-Loop(HITL): 담당자의 승인 후에만 보정 실행. 격리(Quarantine) + 스냅샷으로 롤백 가능.',
            },
            {
              challenge: '운영 효율성',
              traditional: '반복 검증마다 동일한 패턴을 LLM에 재전송하여 비용 낭비.',
              solution: '2단계 캐싱: (1) 동적 규칙 캐시 — 스키마 fingerprint 기반으로 S3에 캐시하여 동적 규칙 생성 LLM 호출 2회(~40초)를 절감. (2) 판정 캐시 — 동일 오류 패턴의 LLM 판정을 DynamoDB에서 재활용.',
            },
          ]}
          columnDefinitions={[
            { id: 'challenge', header: '과제', cell: item => <Box fontWeight="bold">{item.challenge}</Box>, width: 180 },
            { id: 'traditional', header: '기존 방식의 한계', cell: item => <Box variant="small">{item.traditional}</Box>, width: 320 },
            { id: 'solution', header: '이 아키텍처의 해결 방식', cell: item => <Box variant="small">{item.solution}</Box>, width: 350 },
          ]}
        />

        <Box variant="h4">아키텍처 설계 원칙</Box>
        <ColumnLayout columns={4} variant="text-grid">
          <div>
            <Box variant="h4" color="text-status-info">관심사 분리</Box>
            <Box variant="small">
              각 노드가 독립적 책임을 가져 개별 교체, 확장, 디버깅이 용이합니다.
            </Box>
          </div>
          <div>
            <Box variant="h4" color="text-status-warning">조건부 라우팅</Box>
            <Box variant="small">
              의심 항목 0건 시 LLM 건너뜀, dry_run 시 Correction 건너뜀으로 불필요한 처리를 방지합니다.
            </Box>
          </div>
          <div>
            <Box variant="h4" color="text-status-success">S3 감사 추적</Box>
            <Box variant="small">
              각 단계 결과물이 S3에 저장되어 감사 추적이 가능하고, 실패 시 중간 지점부터 재시작할 수 있습니다.
            </Box>
          </div>
          <div>
            <Box variant="h4" color="text-status-info">서버리스 실행</Box>
            <Box variant="small">
              AgentCore Runtime에서 인프라 관리 없이 실행하며, 배치/이벤트 트리거를 모두 지원합니다.
            </Box>
          </div>
        </ColumnLayout>
      </SpaceBetween>
    </ExpandableSection>
  );
}

function StaticVsDynamicRules() {
  return (
    <ExpandableSection
      defaultExpanded
      headerText="정적 규칙 vs 동적 규칙"
      variant="container"
      headerDescription="결정론적 규칙의 안정성과 LLM 동적 규칙의 적응성을 결합"
    >
      <SpaceBetween size="m">
        <ColumnLayout columns={2}>
          <Container header={<Header variant="h3"><Badge color="blue">정적 규칙 (Static Rules)</Badge></Header>}>
            <SpaceBetween size="s">
              <Box variant="p">
                YAML 파일로 사전 정의된 비즈니스 검증 규칙입니다. 도메인 전문가가 설계하며,
                결정론적(Deterministic)으로 동작하여 동일한 입력에 항상 동일한 결과를 반환합니다.
              </Box>
              <Box variant="h4">정적 규칙의 4가지 검증 유형</Box>
              <Table
                variant="embedded"
                items={[
                  { type: 'out_of_range', kr: '범위 초과', tool: 'range_check', example: '중량(weight_kg)이 0.01~30.0kg 범위 초과, 배송비가 0~100,000원 범위 초과' },
                  { type: 'format_inconsistency', kr: '포맷 불일치', tool: 'regex_validate', example: '전화번호가 0XX-XXXX-XXXX 패턴 불일치, 우편번호가 5자리 숫자 아님' },
                  { type: 'temporal_violation', kr: '시간순서 위반', tool: 'timestamp_compare', example: '배송완료 시간이 발송 시간보다 이전, 접수일이 발송일보다 이후' },
                  { type: 'cross_column_inconsistency', kr: '크로스컬럼 불일치', tool: 'address_classify, value_condition', example: '도로명 주소인데 road_addr_yn=0, 착불 결제인데 cod_amount=0, 선불인데 cod_amount>0' },
                ]}
                columnDefinitions={[
                  { id: 'kr', header: '유형', cell: item => <Badge color="blue">{item.kr}</Badge>, width: 130 },
                  { id: 'tool', header: '검증 도구', cell: item => <Box variant="code">{item.tool}</Box>, width: 140 },
                  { id: 'example', header: '예시', cell: item => <Box variant="small">{item.example}</Box>, width: 340 },
                ]}
              />
              <Box variant="small" color="text-body-secondary">
                정적 규칙은 확실한 비즈니스 로직을 반영하며, 빠르고 저렴하게 전수 스캔이 가능합니다.
                그러나 사전에 정의되지 않은 패턴은 탐지할 수 없다는 한계가 있습니다.
              </Box>
            </SpaceBetween>
          </Container>

          <Container header={<Header variant="h3"><Badge color="red">동적 규칙 (Dynamic Rules)</Badge></Header>}>
            <SpaceBetween size="s">
              <Box variant="p">
                LLM(Claude)이 데이터 프로파일링 결과를 분석하여 런타임에 자동 생성하는 규칙입니다.
                데이터의 실제 분포와 패턴에서 이상을 감지하여 정적 규칙이 놓치는 오류를 탐지합니다.
              </Box>
              <Box variant="h4">동적 규칙 생성 과정 (DQ Validator 내부)</Box>
              <Table
                variant="embedded"
                items={[
                  { step: '1', phase: '스키마 추론 + 캐시 확인', desc: '샘플링으로 컬럼 타입을 추론하고, 스키마 fingerprint(SHA-256)로 S3 캐시를 확인합니다. 캐시 HIT 시 2~4단계를 건너뛰어 약 40초를 절감합니다.' },
                  { step: '2', phase: 'LLM 1차: 프로파일 대상 발견', desc: 'LLM이 스키마와 샘플 데이터를 보고, 데이터 특성에 따라 필요한 크로스컬럼 조건을 자유롭게 제안합니다. (캐시 MISS 시에만 실행)' },
                  { step: '3', phase: '전체 데이터 프로파일링', desc: '모든 레코드에 대해 컬럼별 통계(null율, 고유값, Top-5 분포)와 크로스컬럼 조건 매칭률을 산출합니다. (캐시 MISS 시에만 실행)' },
                  { step: '4', phase: 'LLM 2차: 동적 규칙 생성 + 캐시 저장', desc: 'LLM이 프로파일 결과 + 기존 규칙을 보고, 중복되지 않는 새 규칙(AUTO-001~)을 생성합니다. 생성된 규칙은 S3에 캐시됩니다(TTL 1시간). (캐시 MISS 시에만 실행)' },
                  { step: '5', phase: '결정론적 전수 스캔', desc: '정적 + 동적 규칙 전체를 사용하여 전체 데이터를 결정론적으로 검사합니다.' },
                ]}
                columnDefinitions={[
                  { id: 'step', header: '#', cell: item => <Box fontWeight="bold">{item.step}</Box>, width: 40 },
                  { id: 'phase', header: '단계', cell: item => item.phase, width: 160 },
                  { id: 'desc', header: '설명', cell: item => <Box variant="small">{item.desc}</Box>, width: 400 },
                ]}
              />
              <Box variant="small" color="text-body-secondary">
                동적 규칙은 AUTO-NNN 형태의 ID가 부여되며, 정적 규칙과 동일한 검증 도구(range_check, regex_validate 등)를 사용합니다.
                LLM이 발견하지만, 실제 검증은 결정론적 도구로 수행하여 재현 가능성을 보장합니다.
                생성된 동적 규칙은 스키마 fingerprint 기반으로 S3에 캐시되어, 동일 스키마의 반복 검증 시 LLM 중복 호출을 건너뜁니다.
              </Box>
            </SpaceBetween>
          </Container>
        </ColumnLayout>

        <Alert type="info">
          <strong>핵심 설계 원칙 — "LLM이 발견하고, 규칙이 검증한다"</strong>: LLM은 어떤 패턴을 검사해야 하는지를 결정하는 데 사용되고,
          실제 데이터 검증은 결정론적 도구 함수(range_check, regex_validate, timestamp_compare)가 수행합니다.
          이를 통해 LLM의 창의적 패턴 발견 능력과 결정론적 검증의 재현 가능성을 모두 확보합니다.
        </Alert>
      </SpaceBetween>
    </ExpandableSection>
  );
}

function SuspectFilteringArchitecture() {
  return (
    <ExpandableSection
      defaultExpanded
      headerText="왜 전체 데이터를 LLM에 보내지 않는가?"
      variant="container"
      headerDescription="DQ Validator → LLM Analyzer 간 의심 항목(Suspect) 필터링 아키텍처"
    >
      <SpaceBetween size="m">
        <Alert type="info" header="예시: 샘플 데이터 151건 기준으로 이해하기">
          샘플 데이터 151건(정상 100건 + 이상 51건)을 예로 들겠습니다.
          DQ Validator가 151건 전체를 규칙 기반으로 전수 스캔하면, 약 34건의 의심 항목(Suspect)이 탐지됩니다.
          LLM Analyzer는 이 34건만 정밀 분석하여 최종 29건을 실제 오류로 확정하고, 5건은 오탐으로 제거합니다.
          나머지 117건은 규칙 검증을 이미 통과했으므로 LLM 호출 없이 정상 처리됩니다.
          즉, LLM이 분석하는 데이터는 전체의 약 22%에 불과합니다.
        </Alert>

        <div style={{
          background: '#f2f3f3',
          borderRadius: 8,
          padding: '20px 16px',
          overflowX: 'auto',
        }}>
          <div style={{
            display: 'flex',
            alignItems: 'stretch',
            justifyContent: 'center',
            gap: 12,
            minWidth: 800,
            fontFamily: 'monospace',
            fontSize: 13,
          }}>
            {/* Stage 1: Full Data */}
            <div style={{
              border: '2px solid #0972d3',
              borderRadius: 8,
              padding: '12px 16px',
              background: '#fff',
              textAlign: 'center',
              minWidth: 140,
            }}>
              <div style={{ fontWeight: 700, color: '#0972d3' }}>전체 데이터</div>
              <div style={{ fontSize: 28, fontWeight: 700, color: '#0972d3', margin: '4px 0' }}>151건</div>
              <div style={{ fontSize: 11, color: '#687078' }}>S3 스테이징</div>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', color: '#687078', fontSize: 18, fontWeight: 700 }}>&rarr;</div>

            {/* Stage 2: DQ Validator */}
            <div style={{
              border: '2px solid #037f0c',
              borderRadius: 8,
              padding: '12px 16px',
              background: '#fff',
              textAlign: 'center',
              minWidth: 200,
            }}>
              <div style={{ fontWeight: 700, color: '#037f0c' }}>DQ Validator</div>
              <div style={{ fontSize: 11, color: '#687078', margin: '4px 0' }}>결정론적 전수 스캔</div>
              <div style={{ display: 'flex', justifyContent: 'center', gap: 16, marginTop: 8 }}>
                <div>
                  <div style={{ fontSize: 20, fontWeight: 700, color: '#037f0c' }}>117건</div>
                  <div style={{ fontSize: 11, color: '#037f0c' }}>정상 통과</div>
                </div>
                <div style={{ borderLeft: '1px solid #e9ebed', paddingLeft: 16 }}>
                  <div style={{ fontSize: 20, fontWeight: 700, color: '#d13212' }}>34건</div>
                  <div style={{ fontSize: 11, color: '#d13212' }}>의심 항목</div>
                </div>
              </div>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', flexDirection: 'column', justifyContent: 'center', gap: 2 }}>
              <div style={{ color: '#d13212', fontSize: 11, fontWeight: 700 }}>suspect만</div>
              <div style={{ color: '#d13212', fontSize: 18, fontWeight: 700 }}>&rarr;</div>
            </div>

            {/* Stage 3: LLM Analyzer */}
            <div style={{
              border: '2px solid #d13212',
              borderRadius: 8,
              padding: '12px 16px',
              background: '#fff',
              textAlign: 'center',
              minWidth: 200,
            }}>
              <div style={{ fontWeight: 700, color: '#d13212' }}>LLM Analyzer</div>
              <div style={{ fontSize: 11, color: '#687078', margin: '4px 0' }}>PRIMARY + REFLECTION</div>
              <div style={{ display: 'flex', justifyContent: 'center', gap: 16, marginTop: 8 }}>
                <div>
                  <div style={{ fontSize: 20, fontWeight: 700, color: '#037f0c' }}>5건</div>
                  <div style={{ fontSize: 11, color: '#037f0c' }}>오탐 제거</div>
                </div>
                <div style={{ borderLeft: '1px solid #e9ebed', paddingLeft: 16 }}>
                  <div style={{ fontSize: 20, fontWeight: 700, color: '#d13212' }}>29건</div>
                  <div style={{ fontSize: 11, color: '#d13212' }}>오류 확정</div>
                </div>
              </div>
            </div>
          </div>
          <div style={{
            textAlign: 'center',
            marginTop: 12,
            fontSize: 12,
            color: '#687078',
          }}>
            [151건 기준 예시] 규칙 기반 필터링으로 LLM 분석 대상을 151건 &rarr; 34건(약 78% 절감)으로 축소 | LLM은 의심 항목의 문맥만 정밀 분석
          </div>
        </div>

        <Box variant="h4">이 설계를 선택한 3가지 이유</Box>
        <ColumnLayout columns={3} variant="text-grid">
          <div>
            <Box variant="h4" color="text-status-error">1. 비용 효율성</Box>
            <Box variant="p">
              LLM API 호출은 토큰 단위로 과금됩니다.
              151건 예시에서, 전체를 LLM에 보내면 비용이 <strong>약 4.4배</strong> 증가하지만,
              규칙 기반 1차 필터링으로 117건의 정상 데이터를 먼저 걸러내면
              LLM은 의심스러운 34건에만 집중하여 비용을 약 78% 절감할 수 있습니다.
            </Box>
          </div>
          <div>
            <Box variant="h4" color="text-status-warning">2. 처리 속도</Box>
            <Box variant="p">
              151건 예시에서, 규칙 기반 검증은 151건 전체를 <strong>수 초</strong> 내에 처리합니다.
              반면 LLM 분석은 필터링된 34건에도 <strong>약 90초</strong>가 소요됩니다.
              만약 151건 전체를 LLM에 보내면 소요 시간이 <strong>10분 이상</strong>으로 늘어납니다.
              규칙 기반 필터링으로 34건만 LLM에 전달하는 것이 속도와 정밀도의 최적 균형점입니다.
            </Box>
          </div>
          <div>
            <Box variant="h4" color="text-status-success">3. 오탐(False Positive) 최소화</Box>
            <Box variant="p">
              LLM에 정상 데이터를 보내면 "환각(Hallucination)"으로 인해
              정상 레코드를 오류로 잘못 판정하는 오탐이 발생할 수 있습니다.
              규칙으로 이미 통과한 정상 데이터를 LLM에서 다시 분석할 이유가 없으며,
              LLM은 규칙이 "의심스럽다"고 판단한 항목의 <strong>문맥적 판단</strong>에만 집중합니다.
            </Box>
          </div>
        </ColumnLayout>

        <Box variant="h4">어떤 레코드가 "의심 항목(Suspect)"이 되는가?</Box>
        <Table
          variant="embedded"
          items={[
            { type: 'out_of_range', condition: '값이 허용 범위를 벗어남', example: 'weight_kg = 0.001 (최소 0.01 미만)', action: 'LLM이 비즈니스 예외(서류 배송 등)인지 판별' },
            { type: 'format_inconsistency', condition: '정규식 패턴과 불일치', example: 'phone = 02-123-4567 (패턴: 0XX-XXXX-XXXX)', action: 'LLM이 유효한 대체 포맷인지 판별' },
            { type: 'temporal_violation', condition: '시간 순서가 역전됨', example: 'arrival_time < departure_time', action: 'LLM이 타임존/날짜 경계 이슈인지 판별' },
            { type: 'cross_column_inconsistency', condition: '컬럼 간 값이 모순됨', example: '착불 결제인데 cod_amount = 0', action: 'LLM이 비즈니스 예외 여부를 문맥적으로 판정' },
          ]}
          columnDefinitions={[
            { id: 'type', header: '검증 유형', cell: item => <Badge color="blue">{item.type}</Badge>, width: 180 },
            { id: 'condition', header: 'Suspect 조건', cell: item => item.condition, width: 200 },
            { id: 'example', header: '예시', cell: item => <Box variant="code">{item.example}</Box>, width: 260 },
            { id: 'action', header: 'LLM Analyzer의 역할', cell: item => <Box variant="small">{item.action}</Box>, width: 250 },
          ]}
        />

        <Alert type="warning">
          <strong>중요: Suspect는 "오류 확정"이 아닙니다</strong> — 규칙 기반 검증에서 탐지된 의심 항목은 실제 오류가 아닐 수 있습니다.
          예를 들어 중량이 0.005kg인 레코드는 범위 초과(out_of_range)로 탐지되지만,
          서류 배송이라면 비즈니스적으로 정상입니다.
          151건 예시에서, LLM Analyzer는 이런 문맥을 이해하여 <strong>의심 항목 34건 중 5건을 오탐으로 제거</strong>하고
          29건만 실제 오류로 확정합니다. 이것이 규칙 기반 필터링 후 LLM 정밀 분석이 필요한 이유입니다.
        </Alert>
      </SpaceBetween>
    </ExpandableSection>
  );
}

function LlmVerificationDetail() {
  return (
    <ExpandableSection
      defaultExpanded
      headerText="2단계 LLM 검증 (PRIMARY + REFLECTION)"
      variant="container"
      headerDescription="LLM 환각을 줄이기 위한 자기 검증(Self-Verification) 아키텍처"
    >
      <SpaceBetween size="m">
        <Box variant="p">
          규칙 기반 검증의 의심 항목(suspect)은 <strong>실제 오류가 아닐 수 있습니다</strong>.
          예: 중량이 0.01kg 미만이지만 비즈니스 예외(서류 배송)일 수 있음.
          LLM Analyzer는 이러한 의심 항목을 문맥적으로 분석하여 실제 오류 여부를 판정합니다.
        </Box>

        <ColumnLayout columns={2}>
          <Container header={<Header variant="h3">1단계: PRIMARY 분석</Header>}>
            <SpaceBetween size="xs">
              <Box variant="p">
                LLM(Claude Sonnet)이 의심 항목의 원본 데이터와 위반 사유를 분석하여
                <code>is_error</code>(실제 오류 여부)와 <code>confidence</code>(신뢰도)를 판정합니다.
              </Box>
              <Box variant="p">
                배치 단위(기본 50건)로 처리하여 LLM 호출 횟수를 최소화합니다.
                각 항목에 대해 오류 유형, 판정 근거, 수정 제안을 함께 반환합니다.
              </Box>
            </SpaceBetween>
          </Container>
          <Container header={<Header variant="h3">2단계: REFLECTION 자기 검증</Header>}>
            <SpaceBetween size="xs">
              <Box variant="p">
                별도의 LLM 호출로 PRIMARY 판정 결과를 재검토합니다.
                동일한 의심 항목에 대해 독립적으로 <code>is_error</code>와 <code>confidence</code>를 판정합니다.
              </Box>
              <Box variant="p">
                PRIMARY와 REFLECTION의 <code>is_error</code> 결론이 <strong>불일치</strong>하면,
                해당 항목의 신뢰도가 <Badge color="grey">LOW</Badge>로 강제 하향됩니다.
                이는 LLM 환각으로 인한 오탐을 줄이는 안전장치입니다.
              </Box>
            </SpaceBetween>
          </Container>
        </ColumnLayout>

        <div style={{
          background: '#f2f3f3',
          borderRadius: 8,
          padding: 16,
          fontFamily: 'monospace',
          fontSize: 13,
          lineHeight: 1.8,
          textAlign: 'center',
        }}>
          <div>의심 항목 (Suspects)</div>
          <div>&darr;</div>
          <div style={{ color: '#0972d3', fontWeight: 700 }}>PRIMARY LLM 분석 &rarr; is_error + confidence + evidence</div>
          <div>&darr;</div>
          <div style={{ color: '#d13212', fontWeight: 700 }}>REFLECTION LLM 검증 &rarr; is_error + confidence (독립 판정)</div>
          <div>&darr;</div>
          <div>PRIMARY.is_error === REFLECTION.is_error ?</div>
          <div style={{ display: 'flex', justifyContent: 'center', gap: 40, marginTop: 4 }}>
            <span style={{ color: '#037f0c' }}>YES &rarr; PRIMARY 신뢰도 유지</span>
            <span style={{ color: '#d13212' }}>NO &rarr; 신뢰도 LOW 강제 하향</span>
          </div>
          <div>&darr;</div>
          <div>HIGH 신뢰도 판정만 캐시에 저장 (동일 패턴 재활용)</div>
        </div>

        <Box variant="small" color="text-body-secondary">
          판정 캐시(Judgment Cache)의 키는 <code>error_type:rule_id:target_columns</code> 조합으로 구성됩니다.
          동일한 오류 패턴은 캐시에서 재활용되어 LLM 호출 비용을 절감하고 판정 일관성을 유지합니다.
          캐시는 HIGH 신뢰도 판정만 저장하여 품질을 보장합니다.
        </Box>
      </SpaceBetween>
    </ExpandableSection>
  );
}

function HitlCorrectionDetail() {
  return (
    <ExpandableSection
      headerText="Human-in-the-Loop 보정 프로세스"
      variant="container"
      headerDescription="사람의 승인 없이는 데이터를 수정하지 않는 안전한 보정 아키텍처"
    >
      <SpaceBetween size="m">
        <Box variant="p">
          AI가 발견한 오류를 자동으로 수정하면 비즈니스 리스크가 발생합니다.
          이 에이전트는 <strong>모든 보정을 사람의 승인 하에만 실행</strong>하는 Human-in-the-Loop(HITL) 패턴을 채택합니다.
        </Box>

        <Table
          variant="embedded"
          items={[
            { step: '1', phase: 'Slack 인터랙티브 알림', desc: '검증 결과 요약과 함께 Slack으로 승인 요청 메시지를 발송합니다. 건강도 점수, 오류 수, 리포트 링크가 포함됩니다.' },
            { step: '2', phase: '담당자 검토 & 승인', desc: '담당자가 리포트를 확인하고, Slack에서 전체 승인/부분 승인/거부를 선택합니다.' },
            { step: '3', phase: '사전 스냅샷 저장', desc: '보정 대상 레코드의 원본 데이터를 스냅샷으로 저장합니다. 문제 발생 시 롤백에 사용됩니다.' },
            { step: '4', phase: 'HIGH 신뢰도 오류 격리', desc: 'is_error=true이고 confidence=HIGH인 항목을 별도의 격리(Quarantine) 테이블로 분리합니다.' },
            { step: '5', phase: '보정 적용 & 감사 로그', desc: '승인된 항목에 대해 DynamoDB에 수정값을 기록합니다. 모든 변경은 감사 로그(Audit Log)로 추적됩니다.' },
          ]}
          columnDefinitions={[
            { id: 'step', header: '#', cell: item => <Box fontWeight="bold">{item.step}</Box>, width: 40 },
            { id: 'phase', header: '단계', cell: item => item.phase, width: 180 },
            { id: 'desc', header: '설명', cell: item => <Box variant="small">{item.desc}</Box>, width: 520 },
          ]}
        />

        <ColumnLayout columns={3} variant="text-grid">
          <div>
            <Box variant="h4">안전장치 1: Quarantine</Box>
            <Box variant="small">
              HIGH 신뢰도 오류는 격리 테이블로 이동됩니다.
              원본 데이터에 즉시 반영하지 않고, 격리 상태에서 추가 검토가 가능합니다.
            </Box>
          </div>
          <div>
            <Box variant="h4">안전장치 2: Snapshot</Box>
            <Box variant="small">
              보정 전 원본 데이터의 스냅샷이 S3에 저장됩니다.
              보정 결과가 잘못된 경우, 스냅샷을 기반으로 롤백할 수 있습니다.
            </Box>
          </div>
          <div>
            <Box variant="h4">안전장치 3: Audit Log</Box>
            <Box variant="small">
              모든 보정 작업은 record_id, 변경 전/후 값, 승인자, 타임스탬프를 포함하는
              감사 로그에 기록되어 완전한 추적이 가능합니다.
            </Box>
          </div>
        </ColumnLayout>

        <Alert type="info">
          참고: 보정 승인(Slack 인터랙티브 승인 및 자동 보정 적용) 기능은 현재 개발 예정 단계이며, 아직 구현되지 않았습니다.
        </Alert>
      </SpaceBetween>
    </ExpandableSection>
  );
}

function TechStackDetail() {
  return (
    <ExpandableSection
      headerText="기술 스택 & AWS 서비스 구성"
      variant="container"
      headerDescription="Amazon Bedrock AgentCore Runtime 기반 서버리스 아키텍처"
    >
      <SpaceBetween size="m">
        <Table
          variant="embedded"
          items={[
            { category: '에이전트 런타임', service: 'Amazon Bedrock AgentCore Runtime', purpose: '파이프라인의 서버리스 실행 환경. 인프라 관리 없이 에이전트를 배포하고 호출합니다.' },
            { category: 'LLM 모델', service: 'Claude Sonnet 4.6 (Bedrock Converse API)', purpose: '모델 ID: global.anthropic.claude-sonnet-4-6. 동적 규칙 생성, PRIMARY/REFLECTION 시맨틱 분석에 사용. max_tokens=16,384, batch_size=50.' },
            { category: '데이터 소스', service: 'Amazon DynamoDB', purpose: '택배 물류 원본 데이터 저장. Export to S3 또는 Rate-limited Scan으로 데이터 추출.' },
            { category: '중간 저장소', service: 'Amazon S3', purpose: '스테이징 데이터, 의심 항목(JSONL), 판정 결과, 리포트(Markdown), 스냅샷 저장.' },
            { category: '판정 캐시', service: 'Amazon DynamoDB', purpose: 'LLM 판정 결과 캐시. pattern_key 기반으로 동일 오류 패턴의 판정을 재활용.' },
            { category: '동적 규칙 캐시', service: 'Amazon S3', purpose: '스키마 fingerprint(SHA-256) 기반으로 LLM 생성 동적 규칙을 캐시. TTL 1시간. 동일 스키마 반복 검증 시 LLM 호출 2회(~40초)를 절감.' },
            { category: '알림', service: 'Slack API', purpose: '검증 결과 요약, 인터랙티브 승인 메시지 발송. Human-in-the-Loop 승인 게이트.' },
            { category: '에이전트 프레임워크', service: 'Strands Agents SDK', purpose: '@tool 데코레이터 기반 도구 정의, GraphBuilder로 DAG 파이프라인 구성.' },
          ]}
          columnDefinitions={[
            { id: 'category', header: '영역', cell: item => <Box fontWeight="bold">{item.category}</Box>, width: 130 },
            { id: 'service', header: 'AWS 서비스 / 기술', cell: item => <Badge color="blue">{item.service}</Badge>, width: 280 },
            { id: 'purpose', header: '용도', cell: item => <Box variant="small">{item.purpose}</Box>, width: 400 },
          ]}
        />

        <Box variant="h4">프로덕션 확장 시 고려 사항</Box>
        <ColumnLayout columns={2}>
          <SpaceBetween size="xs">
            <Box variant="p">
              <strong>스케줄 기반 배치 검증</strong>: EventBridge Scheduler로 주기적 파이프라인 트리거.
              체크포인트 관리로 증분 처리(Incremental Processing)를 지원합니다.
            </Box>
            <Box variant="p">
              <strong>이벤트 기반 실시간 검증</strong>: DynamoDB Streams → Lambda → AgentCore로
              데이터 변경 시 즉시 검증을 트리거할 수 있습니다.
            </Box>
          </SpaceBetween>
          <SpaceBetween size="xs">
            <Box variant="p">
              <strong>멀티 테이블 확장</strong>: Coordinator의 데이터 소스 설정과 규칙 레지스트리를
              테이블별로 분리하면, 하나의 에이전트로 여러 테이블을 검증할 수 있습니다.
            </Box>
            <Box variant="p">
              <strong>모니터링</strong>: AgentCore Observability로 파이프라인 실행 추적,
              CloudWatch Metrics로 건강도 점수 트렌드 대시보드를 구성할 수 있습니다.
            </Box>
          </SpaceBetween>
        </ColumnLayout>
      </SpaceBetween>
    </ExpandableSection>
  );
}

export default function AgentIntro() {
  return (
    <SpaceBetween size="l">
      <Container
        header={
          <Header
            variant="h2"
            description="LLM 기반 데이터 품질 모니터링 에이전트"
          >
            에이전트 구조 및 작동 흐름
          </Header>
        }
      >
        <SpaceBetween size="l">
          <Box variant="p">
            이 에이전트는 <strong>규칙 기반 검증</strong>(정적 결정론적 검사)과{' '}
            <strong>LLM 시맨틱 분석</strong>(Amazon Bedrock 기반 문맥 지능)을 결합한
            하이브리드 데이터 품질 파이프라인입니다.
            "이미 알고 있는 오류"는 정적 규칙으로 빠르게 탐지하고,
            "아직 모르는 오류"는 LLM이 데이터 패턴에서 자율적으로 발견합니다.
            Human-in-the-Loop 승인 하에 자동 보정까지 수행하는 엔드투엔드 파이프라인으로,
            <strong> Amazon Bedrock AgentCore Runtime</strong>에서 서버리스로 실행됩니다.
          </Box>

          <PipelineFlowDiagram />

          <ColumnLayout columns={5} variant="text-grid">
            <NodeCard
              title="1. Coordinator"
              type="데이터 추출"
              description="DynamoDB 또는 S3에서 택배 데이터를 추출하고, 검증을 위해 스테이징하며, 파이프라인 상태를 초기화합니다."
            />
            <NodeCard
              title="2. DQ Validator"
              type="규칙 기반 검증"
              description="스키마 추론, 검증 규칙(YAML) 로딩, LLM을 통한 동적 규칙 발견 후 결정론적 전체 스캔 검사(범위, 포맷, 시간순서, 크로스컬럼)를 수행합니다."
            />
            <NodeCard
              title="3. LLM Analyzer"
              type="시맨틱 AI 분석"
              description="의심 항목에 대해 PRIMARY LLM 분석 + REFLECTION 자기검증을 수행합니다. 정적 규칙으로 잡지 못하는 비즈니스 로직 오류를 탐지하며, 판정 캐시로 효율성을 높입니다."
            />
            <NodeCard
              title="4. Report & Notify"
              type="리포트 생성"
              description="건강도 점수를 산출하고, DQ 리포트(Markdown)를 S3에 생성하며, Slack으로 인터랙티브 승인 메시지를 발송합니다."
            />
            <NodeCard
              title="5. Correction"
              type="보정 처리"
              description="승인된 보정을 적용하고, 높은 신뢰도 오류를 격리(quarantine)하며, 스냅샷을 저장하고 감사 로그를 기록합니다."
            />
          </ColumnLayout>
        </SpaceBetween>
      </Container>

      <ArchitectureRationale />
      <StaticVsDynamicRules />
      <SuspectFilteringArchitecture />
      <LlmVerificationDetail />
      <HitlCorrectionDetail />
      <TechStackDetail />

      <Container
        header={
          <Header
            variant="h2"
            description="AI Agent 아이디어를 실현 가능한 구현 명세서로 변환하는 솔루션"
          >
            📃 AWS P.A.T.H Agent Designer
          </Header>
        }
      >
        <SpaceBetween size="m">
          <Box variant="p">
            이 에이전트의 아키텍처는{' '}
            <Link href="https://d21k0iabhuk0yx.cloudfront.net/" external>
              AWS P.A.T.H Agent Designer
            </Link>
            {' '}솔루션의 도움을 받아 설계되었습니다.
          </Box>
          <Box variant="p">
            AWS P.A.T.H Agent Designer는 여러분이 가진 AI Agent 아이디어를 입력하면,
            AI가 그 아이디어의 <strong>실현 가능성을 점검</strong>하고,
            <strong> 어떤 구조로 만들어야 하는지 분석</strong>한 뒤,
            개발팀이 바로 착수할 수 있는 <strong>구현 명세서까지 자동으로 생성</strong>해 주는 솔루션입니다.
          </Box>
          <Box variant="p">
            이 솔루션을 통해 만들어진 개발 명세서를 바탕으로 AI 주도 개발 라이프사이클(AI-DLC)을 통해 에이전트를 개발하고, AWS 환경에 배포하였습니다.
          </Box>
          <SpaceBetween size="xs">
            <Link href="https://d21k0iabhuk0yx.cloudfront.net/" external variant="primary" fontSize="body-m">
              P.A.T.H Agent Designer 바로가기
            </Link>
            <Link href="https://aws.amazon.com/ko/blogs/tech/ai-driven-development-life-cycle/" external variant="primary" fontSize="body-m">
              AI-DLC 방법론 소개 바로가기
            </Link>
          </SpaceBetween>
        </SpaceBetween>
      </Container>
    </SpaceBetween>
  );
}
