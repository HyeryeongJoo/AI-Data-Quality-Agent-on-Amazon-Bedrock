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
import Cards from '@cloudscape-design/components/cards';

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
        minWidth: 1100,
        fontFamily: 'monospace',
        fontSize: 13,
        lineHeight: 1.6,
      }}>
        {[
          { label: 'Coordinator', sub: '데이터 추출', color: '#0972d3' },
          { label: 'Rule Validator', sub: '규칙 기반 검증', color: '#037f0c' },
          { label: 'Anomaly Detector', sub: '이상치 탐지', color: '#9469d6' },
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
        v2 파이프라인 (6개 노드) | Directed Acyclic Graph(DAG, 방향성 비순환 그래프) 기반 순차 실행 | 조건부 라우팅: 의심 항목 0건 시 LLM Analyzer 건너뜀 | dry_run 시 Correction 건너뜀
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
              <Box variant="h4">동적 규칙 생성 과정 (Rule Validator 내부)</Box>
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

              <Alert type="info" header="왜 LLM을 2회 호출하는가?">
                1회 호출로 바로 규칙을 만들면, LLM은 샘플 5건만 보고 규칙을 생성하게 되어 전체 데이터의 실제 분포를 반영하지 못합니다.
                <br /><br />
                <strong>1회차 (프로파일 대상 발견)</strong>: LLM에게 스키마 + 샘플 5건을 보여주고 <strong>"뭘 조사해야 하는지"</strong>를 질문합니다.
                LLM은 "착불 결제인데 배송비가 0인 조합", "배송 거리 대비 배송 시간 비율" 등 검사할 크로스컬럼 조건을 제안합니다.
                <br />
                <strong>전체 프로파일링 (LLM 호출 없음)</strong>: 제안된 조건을 기반으로 전체 데이터를 스캔하여 컬럼별 통계(null율, 고유값 분포)와 크로스컬럼 조건 매칭률을 산출합니다.
                <br />
                <strong>2회차 (규칙 생성)</strong>: LLM에게 <strong>전체 프로파일링 결과</strong>를 보여주고 "중복되지 않는 새 규칙을 생성하라"고 요청합니다.
                이제 LLM은 샘플이 아닌 전체 데이터의 분포를 근거로 정확한 규칙을 생성할 수 있습니다.
              </Alert>

              <Box variant="small" color="text-body-secondary">
                동적 규칙은 AUTO-NNN 형태의 ID가 부여되며, 정적 규칙과 동일한 검증 도구(range_check, regex_validate 등)를 사용합니다.
                LLM이 발견하지만, 실제 검증은 결정론적 도구로 수행하여 재현 가능성을 보장합니다.
                생성된 동적 규칙은 스키마 fingerprint 기반으로 S3에 캐시되어, 동일 스키마의 반복 검증 시 LLM 2회 호출(~40초)을 건너뜁니다.
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

function FourLayerValidation() {
  return (
    <ExpandableSection
      defaultExpanded
      headerText="4단계 검증 레이어 — 왜 4번 검증하는가?"
      variant="container"
      headerDescription="정적 규칙 → 동적 규칙 → 이상치 탐지 → LLM 시맨틱 분석, 각 단계가 이전 단계의 한계를 보완"
    >
      <SpaceBetween size="m">
        <Alert type="info" header="기존 데이터 품질 솔루션의 한계">
          전통적인 DQ 솔루션은 사전에 정의된 규칙(null 체크, 범위 검사, 형식 검증)만으로 데이터를 검증합니다.
          이 방식은 <strong>"이미 알고 있는 오류"</strong>만 잡을 수 있으며,
          규칙에 없는 새로운 패턴의 오류, 데이터 분포상 이상치, 비즈니스 문맥을 고려한 판단은 불가능합니다.
          이 에이전트는 4단계 검증 레이어를 통해 각각의 한계를 순차적으로 보완합니다.
        </Alert>

        {/* 4-layer diagram */}
        <div style={{
          background: '#f2f3f3',
          borderRadius: 8,
          padding: '20px 16px',
          overflowX: 'auto',
        }}>
          <div style={{
            display: 'flex',
            flexDirection: 'column',
            gap: 0,
            fontFamily: 'monospace',
            fontSize: 13,
            maxWidth: 900,
            margin: '0 auto',
          }}>
            {[
              {
                layer: '1단계',
                title: '정적 규칙 검증',
                node: 'Rule Validator',
                color: '#037f0c',
                what: '사전 정의된 비즈니스 규칙으로 전수 스캔',
                catches: '형식 오류, 범위 초과, 시간순서 위반, 크로스컬럼 불일치',
                misses: '규칙에 정의되지 않은 새로운 패턴의 오류',
              },
              {
                layer: '2단계',
                title: '동적 규칙 검증',
                node: 'Rule Validator',
                color: '#0972d3',
                what: 'LLM이 데이터 프로파일링 결과를 보고 자동 생성한 규칙으로 전수 스캔',
                catches: '정적 규칙이 놓친 크로스컬럼 조건, 데이터 변화에 따른 새 패턴',
                misses: '규칙 형태(min/max, regex)로 표현할 수 없는 통계적 이상',
              },
              {
                layer: '3단계',
                title: '통계·문맥 이상치 탐지',
                node: 'Anomaly Detector',
                color: '#9469d6',
                what: '전체 데이터의 분포를 수학적으로 분석하여 이상치 탐지',
                catches: '데이터 분포 대비 극단값(Z-Score), 다변량 이상치(Isolation Forest), 카테고리 내 이상, 상관관계 이탈',
                misses: '통계적으로는 정상이지만 비즈니스적으로 잘못된 데이터',
              },
              {
                layer: '4단계',
                title: 'LLM 시맨틱 분석',
                node: 'LLM Analyzer',
                color: '#d13212',
                what: '1~3단계에서 수집된 의심 항목을 LLM이 비즈니스 문맥으로 최종 판정',
                catches: '오탐 제거, 비즈니스 예외 식별, 수정값 추론',
                misses: '-',
              },
            ].map((item, i, arr) => (
              <div key={item.layer}>
                <div style={{
                  display: 'flex',
                  alignItems: 'stretch',
                  gap: 0,
                }}>
                  {/* Layer badge */}
                  <div style={{
                    minWidth: 80,
                    background: item.color,
                    color: '#fff',
                    fontWeight: 700,
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    borderRadius: i === 0 ? '8px 0 0 0' : i === arr.length - 1 ? '0 0 0 8px' : 0,
                    padding: '8px 4px',
                    fontSize: 12,
                  }}>
                    <div>{item.layer}</div>
                    <div style={{ fontSize: 10, opacity: 0.8 }}>{item.node}</div>
                  </div>
                  {/* Content */}
                  <div style={{
                    flex: 1,
                    border: `2px solid ${item.color}`,
                    borderLeft: 'none',
                    borderRadius: i === 0 ? '0 8px 0 0' : i === arr.length - 1 ? '0 0 8px 0' : 0,
                    padding: '10px 16px',
                    background: '#fff',
                    borderTop: i > 0 ? 'none' : undefined,
                  }}>
                    <div style={{ fontWeight: 700, color: item.color, marginBottom: 4 }}>{item.title}</div>
                    <div style={{ fontSize: 12, lineHeight: 1.6 }}>
                      <span style={{ color: '#687078' }}>검증 방법: </span>{item.what}<br />
                      <span style={{ color: '#037f0c' }}>탐지 대상: </span>{item.catches}<br />
                      {item.misses !== '-' && (
                        <><span style={{ color: '#d13212' }}>한계: </span>{item.misses}</>
                      )}
                      {item.misses === '-' && (
                        <><span style={{ color: '#0972d3' }}>역할: </span>1~3단계의 의심 항목을 최종 검증하여 오탐을 제거하고, 실제 오류만 확정</>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
          <div style={{
            textAlign: 'center',
            marginTop: 12,
            fontSize: 12,
            color: '#687078',
          }}>
            각 단계가 이전 단계의 한계를 보완 | 1~3단계: 의심 항목 수집 (빠르고 저렴) → 4단계: LLM 최종 판정 (느리지만 정밀)
          </div>
        </div>

        <Box variant="h4">단계별 차이 비교</Box>
        <Table
          variant="embedded"
          items={[
            {
              dim: '탐지 접근법',
              static: '사전 정의 규칙 (YAML)',
              dynamic: 'LLM이 생성한 규칙',
              anomaly: '수학적 알고리즘',
              llm: 'LLM 문맥 이해',
            },
            {
              dim: '핵심 질문',
              static: '"이 값은 허용 범위 안인가?"',
              dynamic: '"이 데이터에서 추가로 검사할 패턴이 있는가?"',
              anomaly: '"이 값은 다른 데이터 대비 비정상적인가?"',
              llm: '"이 의심 항목은 실제 오류인가, 비즈니스 예외인가?"',
            },
            {
              dim: '택배 데이터 예시 — 탐지',
              isExample: true,
              static: '전화번호가 010-XXXX-XXXX 패턴에 불일치 → 탐지',
              dynamic: 'LLM이 "착불인데 배송비=0" 조건을 발견하여 규칙 생성 → 탐지',
              anomaly: '배송비 평균 5,000원인데 99,000원인 레코드 → Z-Score 이상치로 탐지',
              llm: '"중량 0.005kg은 서류 배송이므로 정상" → 오탐 제거',
            },
            {
              dim: '택배 데이터 예시 — 미탐지',
              isExample: true,
              static: '"착불인데 배송비=0"은 규칙에 없어서 통과',
              dynamic: '배송비 99,000원은 min/max(0~100,000) 범위 안이므로 통과',
              anomaly: '"착불인데 배송비=0"은 통계적으로는 흔한 조합이라 통과',
              llm: '(1~3단계에서 올라온 의심 항목만 분석 — 독자적 탐지 없음)',
            },
            {
              dim: '실행 방식',
              static: '결정론적 전수 스캔',
              dynamic: '결정론적 전수 스캔',
              anomaly: '통계 모델 전수 분석',
              llm: '의심 항목만 분석',
            },
            {
              dim: '검증 도구',
              static: 'range_check, regex_validate 등',
              dynamic: '동일 (range_check 등)',
              anomaly: 'Z-Score, IQR, Isolation Forest 등',
              llm: 'Bedrock Converse API (Claude)',
            },
            {
              dim: '비용',
              static: '무료 (로컬 연산)',
              dynamic: 'LLM 2회 호출 (규칙 생성 시에만, 캐시 재활용)',
              anomaly: '무료 (로컬 연산)',
              llm: 'LLM 1회 호출 (의심 항목 수에 비례)',
            },
            {
              dim: '속도',
              static: '수 초 (전수 스캔)',
              dynamic: '수 초 (전수 스캔, 캐시 HIT 시)',
              anomaly: '수 초 (전수 분석)',
              llm: '~90초 (LLM 응답 대기)',
            },
          ]}
          columnDefinitions={[
            { id: 'dim', header: '', cell: (item: any) => <Box fontWeight="bold" color={item.isExample ? 'text-status-info' : undefined}>{item.dim}</Box>, width: 130 },
            { id: 'static', header: '1. 정적 규칙', cell: (item: any) => <Box variant="small" color={item.isExample ? undefined : 'text-body-secondary'}>{item.static}</Box>, width: 190 },
            { id: 'dynamic', header: '2. 동적 규칙', cell: (item: any) => <Box variant="small" color={item.isExample ? undefined : 'text-body-secondary'}>{item.dynamic}</Box>, width: 190 },
            { id: 'anomaly', header: '3. 이상치 탐지', cell: (item: any) => <Box variant="small" color={item.isExample ? undefined : 'text-body-secondary'}>{item.anomaly}</Box>, width: 190 },
            { id: 'llm', header: '4. LLM 분석', cell: (item: any) => <Box variant="small" color={item.isExample ? undefined : 'text-body-secondary'}>{item.llm}</Box>, width: 190 },
          ]}
          stripedRows
        />

        <Alert type="warning" header="동적 규칙 vs 이상치 탐지 — 겹치지 않나?">
          동적 규칙의 <code>out_of_range</code>와 Anomaly Detector의 Z-Score/IQR 모두 "극단적인 값"을 잡을 수 있어 일부 겹칩니다.
          그러나 접근 방식이 근본적으로 다릅니다.
          동적 규칙은 LLM이 <strong>고정된 min/max 경계</strong>를 설정하여 규칙 형태로 검사하지만,
          Anomaly Detector는 <strong>전체 데이터의 실제 분포</strong>(평균, 표준편차, 사분위수)를 계산하여 통계적으로 벗어난 값을 찾습니다.
          예를 들어, 동적 규칙이 "배송비 0~100,000원"으로 설정하면 99,000원은 통과하지만,
          평균이 5,000원인 데이터에서 Anomaly Detector는 99,000원을 Z-Score 47의 이상치로 탐지합니다.
          또한 Isolation Forest의 다변량 분석이나 희귀 조합 탐지는 규칙 형태로는 표현 자체가 불가능합니다.
          중복 탐지된 레코드는 Anomaly Detector가 자동으로 제거합니다.
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
      headerDescription="Rule Validator → Anomaly Detector → LLM Analyzer 간 의심 항목(Suspect) 수집 및 필터링 아키텍처"
    >
      <SpaceBetween size="m">
        <Alert type="info" header="예시: 샘플 데이터 151건 기준으로 이해하기 (v2 파이프라인)">
          샘플 데이터 151건(정상 100건 + 이상 51건)을 예로 들겠습니다.
          먼저 Rule Validator가 151건 전체를 규칙 기반으로 전수 스캔하여 약 34건의 의심 항목(Suspect)을 탐지합니다.
          다음으로 Anomaly Detector가 151건 전체를 통계적/문맥적으로 분석하여,
          규칙으로는 잡히지 않았지만 이상한 레코드(예: 8건)를 추가로 발견하여 기존 suspects에 merge합니다.
          최종적으로 LLM Analyzer는 합산된 약 42건의 의심 항목만 정밀 분석하여 오류를 확정합니다.
          나머지 109건은 LLM 호출 없이 정상 처리됩니다.
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
            gap: 10,
            minWidth: 1100,
            fontFamily: 'monospace',
            fontSize: 13,
          }}>
            {/* Stage 1: Full Data */}
            <div style={{
              border: '2px solid #0972d3',
              borderRadius: 8,
              padding: '12px 14px',
              background: '#fff',
              textAlign: 'center',
              minWidth: 120,
            }}>
              <div style={{ fontWeight: 700, color: '#0972d3' }}>전체 데이터</div>
              <div style={{ fontSize: 28, fontWeight: 700, color: '#0972d3', margin: '4px 0' }}>151건</div>
              <div style={{ fontSize: 11, color: '#687078' }}>S3 스테이징</div>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', color: '#687078', fontSize: 18, fontWeight: 700 }}>&rarr;</div>

            {/* Stage 2: Rule Validator */}
            <div style={{
              border: '2px solid #037f0c',
              borderRadius: 8,
              padding: '12px 14px',
              background: '#fff',
              textAlign: 'center',
              minWidth: 170,
            }}>
              <div style={{ fontWeight: 700, color: '#037f0c' }}>Rule Validator</div>
              <div style={{ fontSize: 11, color: '#687078', margin: '4px 0' }}>결정론적 전수 스캔</div>
              <div style={{ display: 'flex', justifyContent: 'center', gap: 12, marginTop: 8 }}>
                <div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: '#037f0c' }}>117건</div>
                  <div style={{ fontSize: 11, color: '#037f0c' }}>정상 통과</div>
                </div>
                <div style={{ borderLeft: '1px solid #e9ebed', paddingLeft: 12 }}>
                  <div style={{ fontSize: 18, fontWeight: 700, color: '#d13212' }}>34건</div>
                  <div style={{ fontSize: 11, color: '#d13212' }}>의심 항목</div>
                </div>
              </div>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', color: '#687078', fontSize: 18, fontWeight: 700 }}>&rarr;</div>

            {/* Stage 3: Anomaly Detector */}
            <div style={{
              border: '2px solid #9469d6',
              borderRadius: 8,
              padding: '12px 14px',
              background: '#fff',
              textAlign: 'center',
              minWidth: 190,
            }}>
              <div style={{ fontWeight: 700, color: '#9469d6' }}>Anomaly Detector</div>
              <div style={{ fontSize: 11, color: '#687078', margin: '4px 0' }}>통계·문맥 이상치 탐지</div>
              <div style={{ display: 'flex', justifyContent: 'center', gap: 12, marginTop: 8 }}>
                <div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: '#9469d6' }}>+8건</div>
                  <div style={{ fontSize: 11, color: '#9469d6' }}>이상치 추가</div>
                </div>
                <div style={{ borderLeft: '1px solid #e9ebed', paddingLeft: 12 }}>
                  <div style={{ fontSize: 18, fontWeight: 700, color: '#d13212' }}>42건</div>
                  <div style={{ fontSize: 11, color: '#d13212' }}>합산 의심</div>
                </div>
              </div>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', flexDirection: 'column', justifyContent: 'center', gap: 2 }}>
              <div style={{ color: '#d13212', fontSize: 11, fontWeight: 700 }}>suspect만</div>
              <div style={{ color: '#d13212', fontSize: 18, fontWeight: 700 }}>&rarr;</div>
            </div>

            {/* Stage 4: LLM Analyzer */}
            <div style={{
              border: '2px solid #d13212',
              borderRadius: 8,
              padding: '12px 14px',
              background: '#fff',
              textAlign: 'center',
              minWidth: 170,
            }}>
              <div style={{ fontWeight: 700, color: '#d13212' }}>LLM Analyzer</div>
              <div style={{ fontSize: 11, color: '#687078', margin: '4px 0' }}>PRIMARY 분석 + 신뢰도 판정</div>
              <div style={{ display: 'flex', justifyContent: 'center', gap: 12, marginTop: 8 }}>
                <div>
                  <div style={{ fontSize: 18, fontWeight: 700, color: '#037f0c' }}>7건</div>
                  <div style={{ fontSize: 11, color: '#037f0c' }}>오탐 제거</div>
                </div>
                <div style={{ borderLeft: '1px solid #e9ebed', paddingLeft: 12 }}>
                  <div style={{ fontSize: 18, fontWeight: 700, color: '#d13212' }}>35건</div>
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
            [v2 파이프라인 151건 예시] 규칙 34건 + 이상치 8건 = 의심 42건만 LLM 분석 (전체의 약 28%) | Anomaly Detector는 전체 데이터를 별도 분석하여 규칙이 놓친 이상치를 추가 발견
          </div>
        </div>

        <Alert type="warning">
          <strong>중요: Suspect는 "오류 확정"이 아닙니다</strong> — 규칙 기반 검증이나 이상치 탐지에서 탐지된 의심 항목은 실제 오류가 아닐 수 있습니다.
          예를 들어 중량이 0.005kg인 레코드는 범위 초과(out_of_range)로 탐지되지만,
          서류 배송이라면 비즈니스적으로 정상입니다. Z-Score 이상치도 정상적인 변동일 수 있습니다.
          LLM Analyzer는 이런 문맥을 이해하여 오탐을 걸러내고 실제 오류만 확정합니다.
        </Alert>
      </SpaceBetween>
    </ExpandableSection>
  );
}

function LlmVerificationDetail() {
  return (
    <ExpandableSection
      defaultExpanded
      headerText="LLM 검증 (PRIMARY 분석 + 명시적 신뢰도 판정)"
      variant="container"
      headerDescription="명시적 신뢰도 기준으로 투명한 판정을 제공하는 LLM 분석 아키텍처"
    >
      <SpaceBetween size="m">
        <Alert type="info" header="v2.1 변경사항">
          <strong>REFLECTION 단계 제거:</strong> 기존에는 PRIMARY 분석 후 REFLECTION(자기 검증) 단계를 추가로 수행하여 오탐을 줄였습니다.
          그러나 REFLECTION으로 인해 LLM 호출이 2배로 증가하여 분석 시간이 2배 소요되는 병목이 발생했습니다.
          v2.1부터는 REFLECTION을 제거하여 분석 속도를 약 50% 개선했습니다.
          <br /><br />
          <strong>명시적 신뢰도 판정 기준 도입:</strong> 기존에는 LLM이 자체 판단으로 HIGH/MEDIUM/LOW를 결정하여
          동일 유형의 오류에도 배치마다 다른 신뢰도가 나올 수 있었습니다.
          v2.1부터는 시스템 프롬프트에 명확한 판정 기준을 정의하고, LOW confidence 판정도 자동 제외하지 않고
          모든 결과를 사용자에게 투명하게 제공합니다.
        </Alert>

        <Container header={<Header variant="h3">PRIMARY 분석</Header>}>
          <SpaceBetween size="xs">
            <Box variant="p">
              LLM(Claude Sonnet)이 의심 항목의 원본 데이터와 위반 사유를 분석하여
              <code>is_error</code>(실제 오류 여부)와 <code>confidence</code>(신뢰도: HIGH/MEDIUM/LOW)를 판정합니다.
            </Box>
            <Box variant="p">
              배치 단위(기본 50건)로 처리하여 LLM 호출 횟수를 최소화합니다.
              <strong>한 번의 호출에서</strong> <code>is_error</code>(오류 여부), <code>confidence</code>(신뢰도), <code>suggested_correction</code>(보정 제안)을 동시에 반환합니다 — 신뢰도 판정을 위한 별도 추가 호출은 없습니다.
              모든 신뢰도의 판정 결과가 사용자에게 투명하게 제공되어 직접 확인할 수 있습니다.
            </Box>
          </SpaceBetween>
        </Container>

        <Container header={<Header variant="h3">명시적 신뢰도(Confidence) 판정 기준</Header>}>
          <SpaceBetween size="xs">
            <Box variant="p">
              LLM이 일관된 기준으로 판정할 수 있도록, 시스템 프롬프트에 다음과 같은 명시적 판정 기준을 정의합니다.
            </Box>
            <Table
              variant="embedded"
              items={[
                { level: 'HIGH', criteria: '데이터만으로 오류 여부를 확실히 판단할 수 있는 경우', examples: '형식 오류(우편번호 자릿수, 전화번호 패턴), 명백한 범위 초과(음수 중량, 미래 날짜), 논리적 모순(배송완료 시간 < 접수 시간)' },
                { level: 'MEDIUM', criteria: '오류 가능성이 높지만 비즈니스 예외가 존재할 수 있는 경우', examples: '범위 경계값(최소/최대에 근접), 비표준이지만 유효할 수 있는 형식, 도메인 지식이 필요한 크로스컬럼 불일치' },
                { level: 'LOW', criteria: '오류인지 확신할 수 없는 경우', examples: '통계적으로 드문 값이지만 정상 범위일 수 있는 경우, 비즈니스 컨텍스트에 따라 정상/오류가 달라지는 경우' },
              ]}
              columnDefinitions={[
                { id: 'level', header: '신뢰도', cell: item => {
                  const color = item.level === 'HIGH' ? 'red' : item.level === 'MEDIUM' ? 'blue' : 'grey';
                  return <Badge color={color}>{item.level}</Badge>;
                }, width: 100 },
                { id: 'criteria', header: '판정 기준', cell: item => <Box fontWeight="bold">{item.criteria}</Box>, width: 300 },
                { id: 'examples', header: '해당 사례', cell: item => <Box variant="small">{item.examples}</Box>, width: 400 },
              ]}
            />
            <Box variant="small" color="text-body-secondary">
              이 기준은 LLM 시스템 프롬프트에 명시적으로 포함되어, LLM이 배치와 무관하게 일관된 판정을 하도록 유도합니다.
              모든 신뢰도(HIGH/MEDIUM/LOW)의 판정 결과가 사용자에게 제공되어, 사용자가 직접 판정의 적절성을 검토할 수 있습니다.
            </Box>
          </SpaceBetween>
        </Container>

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
            { category: 'LLM 모델', service: 'Claude Sonnet 5 (Bedrock Converse API)', purpose: '모델 ID: global.anthropic.claude-sonnet-5. 파이프라인 당 총 3회 호출 (캐시 MISS 기준): Rule Validator 2회 (①프로파일 대상 발견 + ②동적 규칙 생성) + LLM Analyzer ⌈의심항목÷50⌉회 (③배치 PRIMARY 분석). 캐시 HIT 시 1회로 단축. max_tokens=16,384, batch_size=50.' },
            { category: '데이터 소스', service: 'Amazon DynamoDB', purpose: '택배 물류 원본 데이터 저장. Export to S3 또는 Rate-limited Scan으로 데이터 추출.' },
            { category: '중간 저장소', service: 'Amazon S3', purpose: '스테이징 데이터, 의심 항목(JSONL), 판정 결과, 리포트(Markdown), 스냅샷 저장.' },
            { category: '동적 규칙 캐시', service: 'Amazon S3', purpose: '스키마 fingerprint(SHA-256) 기반으로 LLM 생성 동적 규칙을 캐시. TTL 1시간. 동일 스키마 반복 검증 시 LLM 호출 2회(~40초)를 절감.' },
            { category: '알림', service: 'Slack API', purpose: '검증 결과 요약, 인터랙티브 승인 메시지 발송. Human-in-the-Loop 승인 게이트.' },
            { category: '에이전트 프레임워크', service: 'Strands Agents SDK', purpose: '@tool 데코레이터 기반 도구 정의, GraphBuilder로 Directed Acyclic Graph(DAG, 방향성 비순환 그래프) 파이프라인 구성.' },
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

interface UpdateLogItem {
  version: string;
  date: string;
  title: string;
  changes: string[];
  isNew?: boolean;
}

function UpdateLog() {
  const updates: UpdateLogItem[] = [
    {
      version: 'v2.1',
      date: '2026-03-18',
      title: 'REFLECTION 제거 + 명시적 신뢰도 판정 기준 도입',
      isNew: true,
      changes: [
        'REFLECTION(자기 검증) 단계를 제거하고 PRIMARY 단일 분석으로 변경 — 분석 시간 약 50% 단축',
        '변경 이유: REFLECTION으로 인한 LLM 호출 2배 시간 소요가 실사용 시 병목으로 작용',
        '시스템 프롬프트에 명시적 신뢰도(HIGH/MEDIUM/LOW) 판정 기준을 정의하여 일관된 판정 유도',
        'LOW confidence 판정을 자동 제외하지 않고 모든 결과를 사용자에게 투명하게 제공',
        '사용자가 각 레코드의 신뢰도와 판정 기준을 직접 확인하고 검토 가능',
        '동적 규칙 생성 시 프로파일링 개선: 저카디널리티 컬럼(고유값 ≤50)은 전체 고유값을 LLM에 전달하여 희소 코드 누락 방지',
        '고카디널리티 컬럼은 Top-5 → Top-20으로 확대, all_values_included 플래그로 LLM이 전체값 여부를 인식',
        '규칙 생성 프롬프트에 allowed_values 보수적 생성 지시 추가 — 일부값만 제공된 컬럼은 pattern 기반 규칙 우선',
      ],
    },
    {
      version: 'v2.0',
      date: '2026-03-18',
      title: '이상치 탐지 (Anomaly Detection) 노드 추가',
      changes: [
        '새로운 Anomaly Detector 노드 추가 (6개 노드 파이프라인)',
        '통계적 이상치 탐지: Z-Score, IQR, Isolation Forest',
        '문맥적 이상치 탐지: 조건부 이상치, 상관관계 이탈, 희귀 조합',
        'UI에서 사용자가 탐지 기법을 선택 가능',
        'v1(기존)과 v2(이상치탐지) 파이프라인 버전 선택 지원',
        '규칙으로 잡히지 않지만, 통계적/문맥적으로 이상한 데이터 탐지',
      ],
    },
    {
      version: 'v1.0',
      date: '2026-02-24',
      title: '초기 릴리스',
      changes: [
        'Coordinator → Rule Validator → LLM Analyzer → Report & Notify → Correction (5개 노드)',
        '정적 규칙 + LLM 동적 규칙 하이브리드 검증',
        'PRIMARY + REFLECTION 2단계 LLM 자기 검증',
        'Human-in-the-Loop 승인 기반 보정 프로세스',
        'Amazon Bedrock AgentCore Runtime 서버리스 배포',
      ],
    },
  ];

  return (
    <Container
      header={
        <Header
          variant="h2"
          description="에이전트의 버전별 업데이트 내역"
        >
          2. 업데이트 로그
        </Header>
      }
    >
      <Cards
        items={updates}
        cardDefinition={{
          header: item => (
            <SpaceBetween direction="horizontal" size="xs">
              <Badge color={item.isNew ? 'green' : 'grey'}>{item.version}</Badge>
              {item.isNew && <Badge color="red">NEW</Badge>}
              <Box variant="small" color="text-body-secondary">{item.date}</Box>
            </SpaceBetween>
          ),
          sections: [
            {
              id: 'title',
              content: item => <Box variant="h4">{item.title}</Box>,
            },
            {
              id: 'changes',
              content: item => (
                <SpaceBetween size="xxs">
                  {item.changes.map((change, i) => (
                    <Box key={i} variant="small">
                      <StatusIndicator type={item.isNew ? 'success' : 'info'}>
                        {change}
                      </StatusIndicator>
                    </Box>
                  ))}
                </SpaceBetween>
              ),
            },
          ],
        }}
        cardsPerRow={[{ cards: 1 }, { minWidth: 600, cards: 2 }]}
      />
    </Container>
  );
}

export default function AgentIntro() {
  return (
    <SpaceBetween size="xl">
      {/* Section 1: 에이전트 아키텍처 */}
      <Container
        header={
          <Header
            variant="h2"
            description="LLM 기반 데이터 품질 모니터링 에이전트의 구조와 설계 원리"
          >
            1. 에이전트 아키텍처
          </Header>
        }
      >
        <SpaceBetween size="l">
          {/* 1.1 에이전트 구조 및 작동 흐름 */}
          <ExpandableSection
            defaultExpanded
            headerText="에이전트 구조 및 작동 흐름"
            variant="container"
            headerDescription="6개 노드로 구성된 Directed Acyclic Graph(DAG, 방향성 비순환 그래프) 기반 파이프라인 (v2)"
          >
            <SpaceBetween size="l">
              <Box variant="p">
                이 에이전트는 <strong>규칙 기반 검증</strong>(정적 결정론적 검사), <strong>이상치 탐지</strong>(통계적/문맥적 분석), 그리고{' '}
                <strong>LLM 시맨틱 분석</strong>(Amazon Bedrock 기반 문맥 지능)을 결합한
                하이브리드 데이터 품질 파이프라인입니다.
                "이미 알고 있는 오류"는 정적 규칙으로 빠르게 탐지하고,
                "규칙으로 잡히지 않지만 통계적/문맥적으로 이상한 데이터"는 이상치 탐지 노드가 발견하며,
                "아직 모르는 오류"는 LLM이 데이터 패턴에서 자율적으로 발견합니다.
                Human-in-the-Loop 승인 하에 자동 보정까지 수행하는 엔드투엔드 파이프라인으로,
                <strong> Amazon Bedrock AgentCore Runtime</strong>에서 서버리스로 실행됩니다.
              </Box>

              <PipelineFlowDiagram />

              <ColumnLayout columns={3} variant="text-grid">
                <NodeCard
                  title="1. Coordinator"
                  type="데이터 추출"
                  description="DynamoDB 또는 S3에서 데이터를 추출하고, 검증을 위해 스테이징하며, 파이프라인 상태를 초기화합니다."
                />
                <NodeCard
                  title="2. Rule Validator"
                  type="규칙 기반 검증"
                  description="스키마 추론, 검증 규칙(YAML) 로딩, LLM을 통한 동적 규칙 발견 후 결정론적 전체 스캔 검사(범위, 포맷, 시간순서, 크로스컬럼)를 수행합니다."
                />
                <NodeCard
                  title="3. Anomaly Detector"
                  type="이상치 탐지"
                  description="통계적 이상치(Z-Score, IQR, Isolation Forest)와 문맥적 이상치(조건부, 상관관계 이탈, 희귀 조합)를 탐지하여 규칙으로 잡히지 않는 의심 항목을 추가합니다."
                />
                <NodeCard
                  title="4. LLM Analyzer"
                  type="시맨틱 AI 분석"
                  description="의심 항목에 대해 명시적 신뢰도 기준(HIGH/MEDIUM/LOW)으로 PRIMARY LLM 분석을 수행합니다. 모든 신뢰도의 판정 결과를 사용자에게 투명하게 제공합니다."
                />
                <NodeCard
                  title="5. Report & Notify"
                  type="리포트 생성"
                  description="건강도 점수를 산출하고, DQ 리포트(Markdown)를 S3에 생성하며, Slack으로 인터랙티브 승인 메시지를 발송합니다."
                />
                <NodeCard
                  title="6. Correction"
                  type="보정 처리"
                  description="승인된 보정을 적용하고, 높은 신뢰도 오류를 격리(quarantine)하며, 스냅샷을 저장하고 감사 로그를 기록합니다."
                />
              </ColumnLayout>
            </SpaceBetween>
          </ExpandableSection>

          {/* 1.2 설계 근거: 왜 이 구조인가 */}
          <ArchitectureRationale />
          {/* 1.3 개념 프레임워크: 4단계 검증이 필요한 이유 */}
          <FourLayerValidation />
          {/* 1.4 Rule Validator 심화: 정적·동적 규칙 동작 방식 */}
          <StaticVsDynamicRules />
          {/* 1.5 파이프라인 흐름: 의심 항목 수집 → LLM 필터링 */}
          <SuspectFilteringArchitecture />
          {/* 1.6 LLM Analyzer 심화: PRIMARY 분석·신뢰도 판정 */}
          <LlmVerificationDetail />
          {/* 1.7 오류 확정 후 처리: HITL 보정 프로세스 */}
          <HitlCorrectionDetail />
          {/* 1.8 기술 스택 */}
          <TechStackDetail />
        </SpaceBetween>
      </Container>

      {/* Section 2: 업데이트 로그 */}
      <UpdateLog />

      {/* Section 3: AWS P.A.T.H Agent Designer */}
      <Container
        header={
          <Header
            variant="h2"
            description="AI Agent 아이디어를 실현 가능한 구현 명세서로 변환하는 솔루션"
          >
            3. AWS P.A.T.H Agent Designer
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
