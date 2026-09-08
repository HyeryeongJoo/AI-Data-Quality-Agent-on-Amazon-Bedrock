import { useState } from 'react';
import TopNavigation from '@cloudscape-design/components/top-navigation';
import AppLayout from '@cloudscape-design/components/app-layout';
import ContentLayout from '@cloudscape-design/components/content-layout';
import Header from '@cloudscape-design/components/header';
import SpaceBetween from '@cloudscape-design/components/space-between';
import SideNavigation from '@cloudscape-design/components/side-navigation';
import BreadcrumbGroup from '@cloudscape-design/components/breadcrumb-group';
import Flashbar from '@cloudscape-design/components/flashbar';
import { useNotifications } from './hooks/useNotifications';
import AgentIntro from './components/AgentIntro';
import SampleDataTable from './components/SampleDataTable';
import ValidationRunner from './components/ValidationRunner';
import ValidationResults from './components/ValidationResults';
import type { DataRecord, ValidationResult } from './types';

const NAV_ITEMS = [
  { type: 'link' as const, text: '에이전트 아키텍처', href: '/architecture' },
  { type: 'divider' as const },
  { type: 'link' as const, text: '규칙 + AI 검증', href: '/validation-v1' },
  { type: 'link' as const, text: '규칙 + 이상치 + AI', href: '/validation-v2' },
];

const PAGE_META: Record<string, { title: string; description: string }> = {
  '/architecture': {
    title: '에이전트 아키텍처',
    description: 'Amazon Bedrock AgentCore 기반 AI 데이터 퀄리티 검증 에이전트의 구조와 동작 원리',
  },
  '/validation-v1': {
    title: '규칙 + AI 검증',
    description: '정적·동적 규칙으로 의심 항목을 수집하고 LLM이 비즈니스 문맥으로 최종 판정하는 5개 노드 파이프라인',
  },
  '/validation-v2': {
    title: '규칙 + 이상치 + AI',
    description: '규칙 기반 탐지에 통계적·문맥적 이상치 탐지를 더해 의심 항목을 확장한 뒤 LLM이 최종 판정하는 6개 노드 파이프라인',
  },
};

export default function App() {
  const [records, setRecords] = useState<DataRecord[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [dataSource, setDataSource] = useState<string>('s3://dq-agent-staging-dev-joohyery/sample/data.jsonl');
  const [validationResults, setValidationResults] = useState<Record<string, ValidationResult | null>>({});
  const [activeHref, setActiveHref] = useState('/architecture');
  const { notifications, notifySuccess, notifyError } = useNotifications();

  const pipelineVersion = activeHref === '/validation-v2' ? 'v2' : 'v1';
  const validationResult = validationResults[pipelineVersion] ?? null;

  const handleDataLoaded = (recs: DataRecord[], cols: string[]) => {
    setRecords(recs);
    setColumns(cols);
    setValidationResults({});
    notifySuccess(`S3에서 ${recs.length}건의 레코드를 로드했습니다.`);
  };

  const handleValidationComplete = (result: ValidationResult) => {
    setValidationResults(prev => ({ ...prev, [pipelineVersion]: result }));
    if (result.status === 'completed') {
      const statusKr = result.health_status === 'healthy' ? '정상' : result.health_status === 'warning' ? '주의' : '위험';
      notifySuccess(
        `검증 완료: ${result.total_records}건 스캔, ` +
        `${result.violation_count}건 위반 발견 (건강도: ${statusKr})`
      );
    }
  };

  const meta = PAGE_META[activeHref];

  return (
    <>
      <TopNavigation
        identity={{
          href: '#',
          title: 'AI 데이터 퀄리티 검증 에이전트 데모',
        }}
        utilities={[
          {
            type: 'button',
            text: 'Amazon Bedrock AgentCore',
            external: true,
            href: 'https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/',
          },
        ]}
      />
      <AppLayout
        toolsHide
        navigation={
          <SideNavigation
            header={{ text: 'AI 데이터 퀄리티 검증 에이전트', href: '#' }}
            activeHref={activeHref}
            onFollow={(e) => {
              e.preventDefault();
              if (e.detail.href !== '#') setActiveHref(e.detail.href);
            }}
            items={NAV_ITEMS}
          />
        }
        notifications={<Flashbar items={notifications} />}
        breadcrumbs={
          <BreadcrumbGroup
            items={[
              { text: 'AI 데이터 퀄리티 검증 에이전트', href: '#' },
              { text: meta.title, href: activeHref },
            ]}
            onFollow={(e) => e.preventDefault()}
          />
        }
        content={
          <ContentLayout
            header={
              <Header variant="h1" description={meta.description}>
                {meta.title}
              </Header>
            }
          >
            {activeHref === '/architecture' && <AgentIntro />}
            {(activeHref === '/validation-v1' || activeHref === '/validation-v2') && (
              <SpaceBetween size="l">
                <SampleDataTable
                  records={records}
                  columns={columns}
                  onDataLoaded={handleDataLoaded}
                  onSourceChange={setDataSource}
                  loading={loading}
                  setLoading={setLoading}
                  onError={notifyError}
                />
                <ValidationRunner
                  key={pipelineVersion}
                  hasData={records.length > 0}
                  s3DataPath={dataSource}
                  pipelineVersion={pipelineVersion}
                  onValidationComplete={handleValidationComplete}
                  onError={notifyError}
                />
                {validationResult && validationResult.status !== 'running' && (
                  <ValidationResults result={validationResult} />
                )}
              </SpaceBetween>
            )}
          </ContentLayout>
        }
      />
    </>
  );
}
