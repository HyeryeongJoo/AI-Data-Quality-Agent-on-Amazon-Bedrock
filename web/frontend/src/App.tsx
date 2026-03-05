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
  { type: 'link' as const, text: '데이터 검증 실행', href: '/validation' },
];

const PAGE_META: Record<string, { title: string; description: string }> = {
  '/architecture': {
    title: '에이전트 아키텍처',
    description: 'Amazon Bedrock AgentCore 기반 AI 데이터 퀄리티 검증 에이전트의 구조와 동작 원리',
  },
  '/validation': {
    title: '데이터 검증 실행',
    description: '데이터를 로드하고 AI 에이전트로 데이터 품질을 검증합니다',
  },
};

export default function App() {
  const [records, setRecords] = useState<DataRecord[]>([]);
  const [columns, setColumns] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [dataSource, setDataSource] = useState<string>('');
  const [validationResult, setValidationResult] = useState<ValidationResult | null>(null);
  const [activeHref, setActiveHref] = useState('/architecture');
  const { notifications, notifySuccess, notifyError } = useNotifications();

  const handleDataLoaded = (recs: DataRecord[], cols: string[]) => {
    setRecords(recs);
    setColumns(cols);
    setValidationResult(null);
    notifySuccess(`S3에서 ${recs.length}건의 레코드를 로드했습니다.`);
  };

  const handleValidationComplete = (result: ValidationResult) => {
    setValidationResult(result);
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
            {activeHref === '/validation' && (
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
                  hasData={records.length > 0}
                  s3DataPath={dataSource}
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
