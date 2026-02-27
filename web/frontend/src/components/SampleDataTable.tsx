import { useState } from 'react';
import Table from '@cloudscape-design/components/table';
import Header from '@cloudscape-design/components/header';
import Button from '@cloudscape-design/components/button';
import Box from '@cloudscape-design/components/box';
import Pagination from '@cloudscape-design/components/pagination';
import TextFilter from '@cloudscape-design/components/text-filter';
import SpaceBetween from '@cloudscape-design/components/space-between';
import FileUpload from '@cloudscape-design/components/file-upload';
import { useCollection } from '@cloudscape-design/collection-hooks';
import type { DataRecord } from '../types';
import { loadSampleData, uploadCsvFile } from '../api/client';

interface Props {
  records: DataRecord[];
  columns: string[];
  onDataLoaded: (records: DataRecord[], columns: string[]) => void;
  onSourceChange?: (s3Path: string) => void;
  loading: boolean;
  setLoading: (v: boolean) => void;
  onError: (msg: string) => void;
}

export default function SampleDataTable({ records, columns, onDataLoaded, onSourceChange, loading, setLoading, onError }: Props) {
  const [filterText, setFilterText] = useState('');
  const [uploadFiles, setUploadFiles] = useState<File[]>([]);
  const [uploading, setUploading] = useState(false);

  const { items, collectionProps, paginationProps } = useCollection(records, {
    filtering: {
      filteringFunction: (item) => {
        if (!filterText) return true;
        const text = filterText.toLowerCase();
        return Object.values(item).some(v =>
          v !== null && String(v).toLowerCase().includes(text)
        );
      },
    },
    pagination: { pageSize: 20 },
    sorting: {},
  });

  const handleLoad = async () => {
    setLoading(true);
    try {
      const data = await loadSampleData();
      onDataLoaded(data.records, data.columns);
      onSourceChange?.(data.source);
    } catch (e: any) {
      onError(e.message || '샘플 데이터 로드에 실패했습니다.');
    } finally {
      setLoading(false);
    }
  };

  const handleFileChange = async (files: File[]) => {
    setUploadFiles(files);
    if (files.length === 0) return;
    const file = files[0];
    setUploading(true);
    try {
      const data = await uploadCsvFile(file);
      onDataLoaded(data.records, data.columns);
      onSourceChange?.(data.source);
      setUploadFiles([]);
    } catch (e: any) {
      onError(e.message || 'CSV 업로드에 실패했습니다.');
    } finally {
      setUploading(false);
    }
  };

  const columnDefs = columns.map(col => ({
    id: col,
    header: col,
    cell: (item: DataRecord) => {
      const val = item[col];
      if (val === null || val === undefined) {
        return <Box color="text-status-inactive">null</Box>;
      }
      return String(val);
    },
    sortingField: col,
    width: col === 'address' || col === 'item_description' ? 200 : undefined,
  }));

  return (
    <Table
      {...collectionProps}
      variant="container"
      header={
        <Header
          variant="h2"
          counter={records.length > 0 ? `(${records.length}건)` : undefined}
          actions={
            <SpaceBetween direction="horizontal" size="s">
              <Button
                variant="primary"
                onClick={handleLoad}
                loading={loading}
                iconName="download"
              >
                샘플 데이터 조회
              </Button>
              <FileUpload
                value={uploadFiles}
                onChange={({ detail }) => handleFileChange(detail.value as File[])}
                accept=".csv"
                i18nStrings={{
                  uploadButtonText: () => uploading ? '업로드 중...' : 'CSV 파일 업로드',
                  dropzoneText: () => 'CSV 파일을 드래그하세요',
                  removeFileAriaLabel: () => '파일 제거',
                  errorIconAriaLabel: '오류',
                  warningIconAriaLabel: '경고',
                  limitShowFewer: '간략히',
                  limitShowMore: '더보기',
                }}
                constraintText="CSV 파일만 지원 (최대 10MB)"
                showFileSize
                showFileThumbnail={false}
              />
            </SpaceBetween>
          }
          description="택배 샘플 데이터를 조회하거나, 직접 로컬 CSV 파일을 업로드하여 검증 데이터를 준비합니다"
        >
          데이터
        </Header>
      }
      items={items}
      columnDefinitions={columnDefs}
      resizableColumns
      stickyHeader
      stripedRows
      wrapLines
      loading={loading}
      loadingText="S3에서 샘플 데이터를 로드하는 중..."
      empty={
        <Box textAlign="center" color="inherit" padding="l">
          <SpaceBetween size="m">
            <b>데이터가 로드되지 않았습니다</b>
            <Box variant="p" color="inherit">
              "샘플 데이터 조회" 버튼으로 S3에서 택배 레코드를 로드하거나, CSV 파일을 직접 업로드하세요.
            </Box>
          </SpaceBetween>
        </Box>
      }
      filter={
        records.length > 0 ? (
          <TextFilter
            filteringText={filterText}
            onChange={({ detail }) => setFilterText(detail.filteringText)}
            filteringPlaceholder="레코드 검색..."
          />
        ) : undefined
      }
      pagination={records.length > 0 ? <Pagination {...paginationProps} /> : undefined}
    />
  );
}
