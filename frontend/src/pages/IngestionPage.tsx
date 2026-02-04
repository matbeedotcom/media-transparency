/**
 * Ingestion Management page for MITDS
 *
 * Universal interface for searching, configuring, triggering,
 * and monitoring data ingestion across all sources.
 */

import { useState, useEffect, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getIngestionStatus,
  searchCompanies,
  triggerIngestion,
  getIngestionRuns,
  getIngestionRunLogs,
  listJobs,
  cancelJob,
  getJobResult,
  type CompanySearchResult,
  type JobStatus as JobStatusFull,
  type IngestionRun,
} from '@/api';

type Tab = 'search' | 'configure' | 'jobs' | 'history';

interface SourceConfig {
  incremental: boolean;
  startYear: string;
  endYear: string;
  limit: string;
}

// Local interface for selected entities with a flattened identifier
interface SelectedEntity {
  name: string;
  source: string;
  identifier: string;
  identifier_type: string;
  jurisdiction?: string;
  status?: string;
  address?: string;
  match_score?: number;
}

// Helper to extract primary identifier from CompanySearchResult
function extractPrimaryIdentifier(result: CompanySearchResult): { identifier: string; identifier_type: string } {
  // The search API returns identifier and identifier_type directly
  if (result.identifier && result.identifier_type) {
    return { identifier: result.identifier, identifier_type: result.identifier_type };
  }
  // Fallback for legacy format with identifiers dict (if present)
  const legacyResult = result as { identifiers?: Record<string, string> };
  if (legacyResult.identifiers) {
    const entries = Object.entries(legacyResult.identifiers);
    if (entries.length > 0) {
      const [key, value] = entries[0];
      return { identifier: String(value ?? ''), identifier_type: key };
    }
  }
  // Last resort: use name
  return { identifier: result.name ?? '', identifier_type: 'name' };
}

// Convert CompanySearchResult to SelectedEntity
function toSelectedEntity(result: CompanySearchResult): SelectedEntity {
  const { identifier, identifier_type } = extractPrimaryIdentifier(result);
  return {
    name: result.name ?? '',
    source: result.source ?? '',
    identifier,
    identifier_type,
    jurisdiction: result.jurisdiction,
    status: result.status,
    address: result.address,
    match_score: result.match_score,
  };
}

const SOURCE_LABELS: Record<string, string> = {
  sec_edgar: 'SEC EDGAR',
  irs990: 'IRS 990',
  cra: 'CRA Charities',
  canada_corps: 'Canada Corps',
  opencorporates: 'OpenCorporates',
  meta_ads: 'Meta Ads',
};

const STATUS_COLORS: Record<string, string> = {
  healthy: 'var(--color-success)',
  running: 'var(--color-primary)',
  degraded: 'var(--color-warning)',
  failed: 'var(--color-danger)',
  stale: 'var(--color-warning)',
  disabled: 'var(--text-muted)',
  never_run: 'var(--text-muted)',
};

const STATUS_ICONS: Record<string, string> = {
  healthy: '\u2705',
  running: '\uD83D\uDD04',
  degraded: '\u26A0\uFE0F',
  failed: '\u274C',
  stale: '\u23F0',
  disabled: '\u23F8\uFE0F',
  never_run: '\uD83C\uDD95',
};

export default function IngestionPage() {
  const queryClient = useQueryClient();

  // Tab
  const [activeTab, setActiveTab] = useState<Tab>('search');

  // Search & Ingest state
  const [searchInputValue, setSearchInputValue] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [searchTimeout, setSearchTimeout] = useState<ReturnType<typeof setTimeout> | null>(null);
  const [sourceFilter, setSourceFilter] = useState('');
  const [selectedEntities, setSelectedEntities] = useState<SelectedEntity[]>([]);
  const [targetIncremental, setTargetIncremental] = useState(true);
  const [targetStartYear, setTargetStartYear] = useState('');
  const [targetEndYear, setTargetEndYear] = useState('');
  const [targetLimit, setTargetLimit] = useState('');

  // Configure & Run state
  const [sourceConfigs, setSourceConfigs] = useState<Record<string, SourceConfig>>({});

  // Jobs state
  const [expandedJobId, setExpandedJobId] = useState<string | null>(null);

  // History state
  const [historySourceFilter, setHistorySourceFilter] = useState('');
  const [historyStatusFilter, setHistoryStatusFilter] = useState('');
  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);

  // Log viewer state
  const [logLines, setLogLines] = useState<string[]>([]);
  const [logOffset, setLogOffset] = useState(0);
  const logEndRef = useRef<HTMLDivElement>(null);

  // ========================
  // Queries
  // ========================

  const { data: statusData, isLoading: statusLoading } = useQuery({
    queryKey: ['ingestion-status'],
    queryFn: ({ signal }) => getIngestionStatus(signal),
    refetchInterval: 30000,
  });

  const { data: searchData, isLoading: searchLoading } = useQuery({
    queryKey: ['ingestion-search', searchQuery, sourceFilter],
    queryFn: () => searchCompanies({
      q: searchQuery,
      sources: sourceFilter || undefined,
      limit: 25,
    }),
    enabled: activeTab === 'search' && searchQuery.length >= 2,
    staleTime: 60000,
  });

  const { data: jobsData } = useQuery({
    queryKey: ['jobs-list'],
    queryFn: () => listJobs({ limit: 20 }),
    refetchInterval: (query) => {
      const jobs = query.state.data?.jobs;
      if (jobs?.some((j: JobStatusFull) => j.status === 'pending' || j.status === 'running')) {
        return 5000;
      }
      return activeTab === 'jobs' ? 30000 : 60000;
    },
  });

  const { data: jobResultData } = useQuery({
    queryKey: ['job-result', expandedJobId],
    queryFn: () => getJobResult(expandedJobId!),
    enabled: !!expandedJobId,
  });

  const { data: runsData } = useQuery({
    queryKey: ['ingestion-runs', historySourceFilter, historyStatusFilter],
    queryFn: () => getIngestionRuns({
      source: historySourceFilter || undefined,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      status: (historyStatusFilter || undefined) as any,
      limit: 25,
    }),
    enabled: activeTab === 'history',
  });

  // Log viewer query (polls while live)
  const { data: logData } = useQuery({
    queryKey: ['run-logs', expandedRunId, logOffset],
    queryFn: () => getIngestionRunLogs(expandedRunId!, { offset: logOffset }),
    enabled: !!expandedRunId,
    refetchInterval: (query) => {
      return query.state.data?.is_live ? 2000 : false;
    },
  });

  // Accumulate new log lines when data arrives
  useEffect(() => {
    if (logData && logData.lines && logData.lines.length > 0) {
      setLogLines((prev) => [...prev, ...(logData.lines ?? [])]);
      setLogOffset(logData.total_lines ?? 0);
    }
  }, [logData]);

  // Reset log state when expanded run changes
  useEffect(() => {
    setLogLines([]);
    setLogOffset(0);
  }, [expandedRunId]);

  // Auto-scroll to bottom when new log lines arrive
  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logLines]);

  // ========================
  // Mutations
  // ========================

  type SourceType = "irs990" | "cra" | "sec_edgar" | "canada_corps" | "sedar" | "alberta-nonprofits" | "meta_ads";
  
  const triggerMutation = useMutation({
    mutationFn: ({ source, options }: { source: SourceType; options?: Parameters<typeof triggerIngestion>[1] }) =>
      triggerIngestion(source, options ?? {}),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ingestion-status'] });
      queryClient.invalidateQueries({ queryKey: ['jobs-list'] });
      queryClient.invalidateQueries({ queryKey: ['ingestion-runs'] });
    },
  });

  const cancelMutation = useMutation({
    mutationFn: (jobId: string) => cancelJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['jobs-list'] });
    },
  });

  // ========================
  // Handlers
  // ========================

  const handleSearchInput = (value: string) => {
    setSearchInputValue(value);
    if (searchTimeout) clearTimeout(searchTimeout);
    const timeout = setTimeout(() => setSearchQuery(value), 300);
    setSearchTimeout(timeout);
  };

  const handleSelectEntity = (result: CompanySearchResult) => {
    const entity = toSelectedEntity(result);
    setSelectedEntities((prev) => {
      const exists = prev.some(
        (e) => e.identifier === entity.identifier && e.source === entity.source
      );
      if (exists) {
        return prev.filter(
          (e) => !(e.identifier === entity.identifier && e.source === entity.source)
        );
      }
      return [...prev, entity];
    });
  };

  const handleTriggerSelected = () => {
    if (triggerMutation.isPending || selectedEntities.length === 0) return;

    const bySource = selectedEntities.reduce((acc, entity) => {
      (acc[entity.source] ??= []).push(entity.identifier);
      return acc;
    }, {} as Record<string, string[]>);

    const startYear = targetStartYear ? parseInt(targetStartYear) : undefined;
    const endYear = targetEndYear ? parseInt(targetEndYear) : undefined;
    const limit = targetLimit ? parseInt(targetLimit) : undefined;

    Object.entries(bySource).forEach(([source, identifiers]) => {
      triggerMutation.mutate({
        source: source as SourceType,
        options: {
          incremental: targetIncremental,
          start_year: startYear,
          end_year: endYear,
          limit,
          target_entities: identifiers,
        },
      });
    });

    setSelectedEntities([]);
    setActiveTab('jobs');
  };

  const getSourceConfig = (source: string): SourceConfig =>
    sourceConfigs[source] ?? { incremental: true, startYear: '', endYear: '', limit: '' };

  const updateSourceConfig = (source: string, updates: Partial<SourceConfig>) => {
    setSourceConfigs((prev) => ({
      ...prev,
      [source]: { ...getSourceConfig(source), ...updates },
    }));
  };

  const handleTriggerSource = (source: string) => {
    if (triggerMutation.isPending) return;
    const cfg = getSourceConfig(source);
    triggerMutation.mutate({
      source: source as SourceType,
      options: {
        incremental: cfg.incremental,
        start_year: cfg.startYear ? parseInt(cfg.startYear) : undefined,
        end_year: cfg.endYear ? parseInt(cfg.endYear) : undefined,
        limit: cfg.limit ? parseInt(cfg.limit) : undefined,
      },
    });
  };

  const handleTriggerAll = () => {
    if (triggerMutation.isPending) return;
    const enabled = (statusData?.sources ?? []).filter(
      (s: { status?: string; source?: string }) => s.status !== 'disabled' && s.status !== 'running'
    );
    enabled.forEach((s: { source?: string }) => {
      if (s.source) {
        triggerMutation.mutate({ source: s.source as SourceType, options: { incremental: true } });
      }
    });
    setActiveTab('jobs');
  };

  const handleQuickRun = (source: string) => {
    if (triggerMutation.isPending) return;
    triggerMutation.mutate({ source: source as SourceType, options: { incremental: true } });
  };

  // Group selections by source for summary
  const selectionSummary = selectedEntities.reduce((acc, e) => {
    acc[e.source] = (acc[e.source] ?? 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const enabledSources = (statusData?.sources ?? []).filter(
    (s: { status?: string }) => s.status !== 'disabled'
  );

  const activeJobs = (jobsData?.jobs ?? []).filter(
    (j: JobStatusFull) => j.status === 'pending' || j.status === 'running'
  );

  const recentJobs = (jobsData?.jobs ?? []).filter(
    (j: JobStatusFull) => j.status !== 'pending' && j.status !== 'running'
  );

  // ========================
  // Render
  // ========================

  return (
    <div className="ingestion-page">
      <header className="page-header">
        <h1>Ingestion Management</h1>
        <p>Search companies, configure sources, trigger ingestion, and monitor jobs</p>
      </header>

      {/* Source Status Cards */}
      <div className="source-cards">
        {statusLoading ? (
          <div className="loading"><div className="spinner" /> Loading sources...</div>
        ) : (
          enabledSources.map((source) => (
            <div key={source.source ?? 'unknown'} className="source-card card">
              <div className="source-card-header">
                <strong>{SOURCE_LABELS[source.source ?? ''] ?? source.source ?? ''}</strong>
                <span
                  className="status-badge"
                  style={{ background: STATUS_COLORS[source.status ?? ''] ?? 'var(--text-muted)' }}
                >
                  {STATUS_ICONS[source.status ?? ''] ?? '?'} {source.status ?? ''}
                </span>
              </div>
              <div className="source-card-stats">
                <span>{(source.records_processed ?? 0).toLocaleString()} records</span>
                <span className="text-muted">
                  {source.last_successful_run
                    ? new Date(source.last_successful_run).toLocaleDateString()
                    : 'Never run'}
                </span>
              </div>
              <button
                type="button"
                className="btn btn-secondary btn-sm"
                disabled={source.status === 'running' || triggerMutation.isPending}
                onClick={() => source.source && handleQuickRun(source.source)}
              >
                Quick Run
              </button>
            </div>
          ))
        )}
      </div>

      {/* Tab Bar */}
      <div className="tab-bar">
        {([
          ['search', 'Search & Ingest'],
          ['configure', 'Configure & Run'],
          ['jobs', 'Jobs'],
          ['history', 'History'],
        ] as [Tab, string][]).map(([key, label]) => (
          <button
            key={key}
            type="button"
            className={`tab ${activeTab === key ? 'tab-active' : ''}`}
            onClick={() => setActiveTab(key)}
          >
            {label}
            {key === 'jobs' && activeJobs.length > 0 && (
              <span className="tab-badge">{activeJobs.length}</span>
            )}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      <div className="tab-content">
        {/* ================================ */}
        {/* TAB: Search & Ingest             */}
        {/* ================================ */}
        {activeTab === 'search' && (
          <div className="search-tab">
            <div className="search-row">
              <input
                type="text"
                className="search-input"
                placeholder="Search companies across all sources..."
                value={searchInputValue}
                onChange={(e) => handleSearchInput(e.target.value)}
              />
              <select
                className="source-select"
                value={sourceFilter}
                onChange={(e) => setSourceFilter(e.target.value)}
              >
                <option value="">All Sources</option>
                {enabledSources.map((s) => (
                  <option key={s.source ?? 'unknown'} value={s.source ?? ''}>
                    {SOURCE_LABELS[s.source ?? ''] ?? s.source ?? ''}
                  </option>
                ))}
              </select>
            </div>

            {/* Selected chips */}
            {selectedEntities.length > 0 && (
              <div className="selected-chips">
                {selectedEntities.map((entity) => (
                  <span
                    key={`${entity.source}-${entity.identifier}`}
                    className="entity-chip"
                  >
                    {entity.name}
                    <span className="chip-source">{SOURCE_LABELS[entity.source] ?? entity.source}</span>
                    <button
                      type="button"
                      className="chip-remove"
                      onClick={() => setSelectedEntities((prev) => 
                        prev.filter((e) => !(e.identifier === entity.identifier && e.source === entity.source))
                      )}
                      title="Remove"
                    >
                      x
                    </button>
                  </span>
                ))}
              </div>
            )}

            {/* Search results */}
            {searchLoading && searchQuery.length >= 2 && (
              <div className="search-status">Searching across sources...</div>
            )}

            {searchData && searchData.results && searchData.results.length > 0 && (
              <div className="card search-results-card">
                <table>
                  <thead>
                    <tr>
                      <th style={{ width: 32 }}></th>
                      <th>Name</th>
                      <th>Source</th>
                      <th>Identifier</th>
                      <th>Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {searchData.results.map((result) => {
                      const { identifier, identifier_type } = extractPrimaryIdentifier(result);
                      const isSelected = selectedEntities.some(
                        (e) => e.identifier === identifier && e.source === result.source
                      );
                      return (
                        <tr
                          key={`${result.source ?? 'unknown'}-${identifier}`}
                          className={`result-row ${isSelected ? 'result-row-selected' : ''}`}
                          onClick={() => handleSelectEntity(result)}
                        >
                          <td>
                            <input type="checkbox" checked={isSelected} readOnly />
                          </td>
                          <td><strong>{result.name ?? ''}</strong></td>
                          <td>
                            <span className="source-tag">
                              {SOURCE_LABELS[result.source ?? ''] ?? result.source ?? ''}
                            </span>
                          </td>
                          <td className="text-muted">
                            {identifier_type}: {identifier}
                          </td>
                          <td className="result-details">
                            {result.jurisdiction && (
                              <span className="detail-tag">{result.jurisdiction}</span>
                            )}
                            {result.status && (
                              <span className="detail-tag">{result.status}</span>
                            )}
                            {result.address && (
                              <span className="detail-tag">{result.address}</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
                {searchData.sources_failed && searchData.sources_failed.length > 0 && (
                  <div className="search-status text-warning">
                    Search failed for: {searchData.sources_failed.join(', ')}
                  </div>
                )}
              </div>
            )}

            {searchData && searchQuery.length >= 2 && (!searchData.results || searchData.results.length === 0) && !searchLoading && (
              <div className="search-status text-muted">
                No results found for &quot;{searchQuery}&quot;
              </div>
            )}

            {/* Ingest Selected Panel */}
            {selectedEntities.length > 0 && (
              <div className="card ingest-panel">
                <h3>Ingest Selected Entities</h3>
                <p className="text-muted ingest-summary">
                  {Object.entries(selectionSummary)
                    .map(([src, count]) => `${count} from ${SOURCE_LABELS[src] ?? src}`)
                    .join(', ')}
                </p>
                <div className="config-row">
                  <label className="config-label">
                    <input
                      type="checkbox"
                      checked={targetIncremental}
                      onChange={(e) => setTargetIncremental(e.target.checked)}
                    />
                    Incremental
                  </label>
                  <label className="config-label">
                    Start Year:
                    <input
                      type="number"
                      className="config-input"
                      min="2000"
                      max="2030"
                      value={targetStartYear}
                      onChange={(e) => setTargetStartYear(e.target.value)}
                      placeholder="e.g. 2020"
                    />
                  </label>
                  <label className="config-label">
                    End Year:
                    <input
                      type="number"
                      className="config-input"
                      min="2000"
                      max="2030"
                      value={targetEndYear}
                      onChange={(e) => setTargetEndYear(e.target.value)}
                      placeholder="e.g. 2024"
                    />
                  </label>
                  <label className="config-label">
                    Limit:
                    <input
                      type="number"
                      className="config-input"
                      min="1"
                      value={targetLimit}
                      onChange={(e) => setTargetLimit(e.target.value)}
                      placeholder="max"
                    />
                  </label>
                </div>
                <button
                  type="button"
                  className="btn btn-primary"
                  onClick={handleTriggerSelected}
                  disabled={triggerMutation.isPending}
                >
                  {triggerMutation.isPending
                    ? 'Starting...'
                    : `Ingest ${selectedEntities.length} Selected`}
                </button>
              </div>
            )}
          </div>
        )}

        {/* ================================ */}
        {/* TAB: Configure & Run             */}
        {/* ================================ */}
        {activeTab === 'configure' && (
          <div className="configure-tab">
            {enabledSources.map((source) => {
              const sourceKey = source.source ?? '';
              const cfg = getSourceConfig(sourceKey);
              return (
                <div key={sourceKey} className="card config-card">
                  <div className="config-card-header">
                    <div>
                      <h3>{SOURCE_LABELS[sourceKey] ?? sourceKey}</h3>
                      <span
                        className="status-badge-sm"
                        style={{ background: STATUS_COLORS[source.status ?? ''] ?? 'var(--text-muted)' }}
                      >
                        {source.status ?? ''}
                      </span>
                      <span className="text-muted config-meta">
                        Last: {source.last_successful_run
                          ? new Date(source.last_successful_run).toLocaleString()
                          : 'Never'}
                        {' \u00B7 '}
                        {(source.records_processed ?? 0).toLocaleString()} records
                      </span>
                    </div>
                  </div>
                  <div className="config-row">
                    <label className="config-label">
                      <input
                        type="checkbox"
                        checked={cfg.incremental}
                        onChange={(e) => updateSourceConfig(sourceKey, { incremental: e.target.checked })}
                      />
                      Incremental
                    </label>
                    <label className="config-label">
                      Start Year:
                      <input
                        type="number"
                        className="config-input"
                        min="2000"
                        max="2030"
                        value={cfg.startYear}
                        onChange={(e) => updateSourceConfig(sourceKey, { startYear: e.target.value })}
                        placeholder="e.g. 2020"
                      />
                    </label>
                    <label className="config-label">
                      End Year:
                      <input
                        type="number"
                        className="config-input"
                        min="2000"
                        max="2030"
                        value={cfg.endYear}
                        onChange={(e) => updateSourceConfig(sourceKey, { endYear: e.target.value })}
                        placeholder="e.g. 2024"
                      />
                    </label>
                    <label className="config-label">
                      Limit:
                      <input
                        type="number"
                        className="config-input"
                        min="1"
                        value={cfg.limit}
                        onChange={(e) => updateSourceConfig(sourceKey, { limit: e.target.value })}
                        placeholder="max"
                      />
                    </label>
                    <button
                      type="button"
                      className="btn btn-primary"
                      disabled={source.status === 'running' || triggerMutation.isPending}
                      onClick={() => handleTriggerSource(sourceKey)}
                    >
                      {source.status === 'running' ? 'Running...' : 'Run Full Ingestion'}
                    </button>
                  </div>
                </div>
              );
            })}

            <div className="bulk-actions">
              <button
                type="button"
                className="btn btn-secondary"
                onClick={handleTriggerAll}
                disabled={triggerMutation.isPending}
              >
                Trigger All Sources (Incremental)
              </button>
            </div>
          </div>
        )}

        {/* ================================ */}
        {/* TAB: Jobs                        */}
        {/* ================================ */}
        {activeTab === 'jobs' && (
          <div className="jobs-tab">
            {/* Active jobs */}
            {activeJobs.length > 0 && (
              <div className="active-jobs-section">
                <h3>Active Jobs</h3>
                {activeJobs.map((job: JobStatusFull) => (
                  <div key={job.job_id} className="card active-job-card">
                    <div className="active-job-header">
                      <strong>{job.job_type}</strong>
                      <span className={`job-badge job-badge-${job.status}`}>{job.status}</span>
                    </div>
                    <div className="active-job-meta text-muted">
                      Started: {job.started_at ? new Date(job.started_at).toLocaleString() : 'Pending'}
                    </div>
                    {job.progress != null && (
                      <div className="progress-bar">
                        <div className="progress-fill" style={{ width: `${job.progress * 100}%` }} />
                        <span className="progress-text">{(job.progress * 100).toFixed(0)}%</span>
                      </div>
                    )}
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      onClick={() => cancelMutation.mutate(job.job_id)}
                      disabled={cancelMutation.isPending}
                    >
                      Cancel
                    </button>
                  </div>
                ))}
              </div>
            )}

            {/* Recent jobs */}
            <h3>Recent Jobs</h3>
            {recentJobs.length > 0 ? (
              <div className="card">
                <table>
                  <thead>
                    <tr>
                      <th>Type</th>
                      <th>Status</th>
                      <th>Created</th>
                      <th>Progress</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recentJobs.map((job: JobStatusFull) => (
                      <>
                        <tr key={job.job_id}>
                          <td><strong>{job.job_type}</strong></td>
                          <td>
                            <span className={`job-badge job-badge-${job.status}`}>
                              {job.status}
                            </span>
                          </td>
                          <td>{new Date(job.created_at).toLocaleString()}</td>
                          <td>
                            {job.progress != null
                              ? `${(job.progress * 100).toFixed(0)}%`
                              : '\u2014'}
                          </td>
                          <td>
                            {job.status === 'completed' && (
                              <button
                                type="button"
                                className="btn btn-secondary btn-sm"
                                onClick={() => setExpandedJobId(expandedJobId === job.job_id ? null : job.job_id)}
                              >
                                {expandedJobId === job.job_id ? 'Hide' : 'View Result'}
                              </button>
                            )}
                            {job.status === 'failed' && job.error && (
                              <span className="text-danger" title={job.error}>Error</span>
                            )}
                          </td>
                        </tr>
                        {expandedJobId === job.job_id && (
                          <tr key={`${job.job_id}-detail`}>
                            <td colSpan={5}>
                              <div className="job-detail">
                                {jobResultData ? (
                                  <pre>{JSON.stringify(jobResultData.result ?? jobResultData, null, 2)}</pre>
                                ) : (
                                  <span className="text-muted">Loading result...</span>
                                )}
                              </div>
                            </td>
                          </tr>
                        )}
                      </>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="card empty-state">
                <p>No completed jobs yet.</p>
                <p className="text-muted">Jobs appear here after ingestion or analysis runs.</p>
              </div>
            )}
          </div>
        )}

        {/* ================================ */}
        {/* TAB: History                     */}
        {/* ================================ */}
        {activeTab === 'history' && (
          <div className="history-tab">
            <div className="filter-row">
              <select
                value={historySourceFilter}
                onChange={(e) => setHistorySourceFilter(e.target.value)}
              >
                <option value="">All Sources</option>
                {enabledSources.map((s) => (
                  <option key={s.source ?? 'unknown'} value={s.source ?? ''}>
                    {SOURCE_LABELS[s.source ?? ''] ?? s.source ?? ''}
                  </option>
                ))}
              </select>
              <select
                value={historyStatusFilter}
                onChange={(e) => setHistoryStatusFilter(e.target.value)}
              >
                <option value="">All Statuses</option>
                <option value="completed">Completed</option>
                <option value="failed">Failed</option>
                <option value="partial">Partial</option>
                <option value="running">Running</option>
              </select>
            </div>

            {runsData?.runs && runsData.runs.length > 0 ? (
              <div className="card">
                <table>
                  <thead>
                    <tr>
                      <th>Source</th>
                      <th>Status</th>
                      <th>Started</th>
                      <th>Completed</th>
                      <th>Processed</th>
                      <th>Created</th>
                      <th>Errors</th>
                    </tr>
                  </thead>
                  <tbody>
                    {runsData.runs.map((run: IngestionRun) => (
                      <>
                        <tr
                          key={run.run_id ?? 'unknown'}
                          className="history-row"
                          onClick={() => setExpandedRunId(expandedRunId === run.run_id ? null : (run.run_id ?? null))}
                        >
                          <td><strong>{SOURCE_LABELS[run.source ?? ''] ?? run.source ?? ''}</strong></td>
                          <td>
                            <span className={
                              run.status === 'COMPLETED' ? 'text-success'
                                : run.status === 'FAILED' ? 'text-danger'
                                : ''
                            }>
                              {run.status}
                            </span>
                          </td>
                          <td>{run.started_at ? new Date(run.started_at).toLocaleString() : '\u2014'}</td>
                          <td>{run.completed_at ? new Date(run.completed_at).toLocaleString() : '\u2014'}</td>
                          <td>{run.records_processed ?? 0}</td>
                          <td>{run.records_created ?? 0}</td>
                          <td>{run.errors?.length ?? 0}</td>
                        </tr>
                        {expandedRunId === run.run_id && (
                          <tr key={`${run.run_id}-detail`}>
                            <td colSpan={7}>
                              <div className="run-detail">
                                <div className="run-detail-grid">
                                  <div><strong>Run ID:</strong> {run.run_id}</div>
                                  <div><strong>Records Updated:</strong> {run.records_updated ?? 0}</div>
                                  <div><strong>Duplicates Found:</strong> {run.duplicates_found ?? 0}</div>
                                </div>
                                {run.errors && run.errors.length > 0 && (
                                  <div className="run-errors">
                                    <strong>Errors:</strong>
                                    <ul>
                                      {run.errors.map((err, i) => (
                                        <li key={i} className="text-danger">
                                          {typeof err === 'object' ? JSON.stringify(err) : String(err)}
                                        </li>
                                      ))}
                                    </ul>
                                  </div>
                                )}
                                <div className="run-log-viewer">
                                  <div className="run-log-header">
                                    <strong>Log Output</strong>
                                    {logData?.is_live && (
                                      <span className="log-live-indicator">Live</span>
                                    )}
                                  </div>
                                  {logLines.length > 0 ? (
                                    <pre className="log-output">
                                      {logLines.join('\n')}
                                      <div ref={logEndRef} />
                                    </pre>
                                  ) : (
                                    <p className="text-muted" style={{ fontSize: '0.8125rem' }}>
                                      {logData === undefined ? 'Loading logs...' : 'No log output available'}
                                    </p>
                                  )}
                                </div>
                              </div>
                            </td>
                          </tr>
                        )}
                      </>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="card empty-state">
                <p>No ingestion runs found.</p>
                <p className="text-muted">Runs will appear here after ingestion is triggered.</p>
              </div>
            )}
          </div>
        )}
      </div>

      <style>{`
        .ingestion-page {
          max-width: 100%;
        }

        .page-header {
          margin-bottom: var(--spacing-lg);
        }

        .page-header h1 {
          margin-bottom: var(--spacing-xs);
        }

        /* Source Status Cards */
        .source-cards {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
          margin-bottom: var(--spacing-lg);
        }

        .source-card {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
        }

        .source-card-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
        }

        .source-card-stats {
          display: flex;
          justify-content: space-between;
          font-size: 0.8125rem;
        }

        .status-badge {
          display: inline-flex;
          align-items: center;
          gap: 4px;
          padding: 2px 8px;
          border-radius: 12px;
          font-size: 0.6875rem;
          font-weight: 600;
          color: white;
        }

        .status-badge-sm {
          display: inline-block;
          padding: 1px 6px;
          border-radius: 8px;
          font-size: 0.625rem;
          font-weight: 600;
          color: white;
          margin-left: var(--spacing-sm);
        }

        /* Tab Bar */
        .tab-bar {
          display: flex;
          gap: var(--spacing-xs);
          border-bottom: 2px solid var(--border-color);
          margin-bottom: var(--spacing-lg);
        }

        .tab {
          padding: var(--spacing-sm) var(--spacing-md);
          background: none;
          border: none;
          border-bottom: 2px solid transparent;
          margin-bottom: -2px;
          cursor: pointer;
          font-size: 0.875rem;
          font-weight: 500;
          color: var(--text-secondary);
          transition: all 0.2s;
          position: relative;
        }

        .tab:hover {
          color: var(--text-primary);
        }

        .tab-active {
          color: var(--color-primary);
          border-bottom-color: var(--color-primary);
          font-weight: 600;
        }

        .tab-badge {
          position: relative;
          top: -1px;
          margin-left: 6px;
          background: var(--color-primary);
          color: white;
          border-radius: 10px;
          padding: 1px 6px;
          font-size: 0.6875rem;
          font-weight: 600;
        }

        .tab-content {
          min-height: 400px;
        }

        /* Search Tab */
        .search-row {
          display: flex;
          gap: var(--spacing-sm);
          margin-bottom: var(--spacing-md);
        }

        .search-row .search-input {
          flex: 1 1 0%;
          width: auto;
          min-width: 0;
          padding: 10px 14px;
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-size: 0.9375rem;
        }

        .search-row .search-input:focus {
          outline: none;
          border-color: var(--color-primary);
          box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.15);
        }

        .search-row .source-select {
          flex: 0 0 auto;
          width: auto;
          padding: 10px 14px;
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-size: 0.875rem;
          min-width: 160px;
        }

        .selected-chips {
          display: flex;
          flex-wrap: wrap;
          gap: var(--spacing-xs);
          margin-bottom: var(--spacing-md);
        }

        .entity-chip {
          display: inline-flex;
          align-items: center;
          gap: 6px;
          padding: 4px 10px;
          background: var(--color-primary);
          color: white;
          border-radius: 16px;
          font-size: 0.75rem;
          font-weight: 500;
        }

        .chip-source {
          opacity: 0.75;
          font-size: 0.625rem;
        }

        .chip-remove {
          background: none;
          border: none;
          color: white;
          cursor: pointer;
          font-size: 14px;
          padding: 0 2px;
          opacity: 0.7;
          line-height: 1;
        }

        .chip-remove:hover {
          opacity: 1;
        }

        .search-results-card {
          max-height: 400px;
          overflow-y: auto;
          margin-bottom: var(--spacing-md);
        }

        .result-row {
          cursor: pointer;
        }

        .result-row:hover {
          background: var(--bg-tertiary);
        }

        .result-row input[type="checkbox"] {
          width: auto;
        }

        .result-row-selected {
          background: rgba(59, 130, 246, 0.08);
        }

        .result-details {
          display: flex;
          flex-wrap: wrap;
          gap: 4px;
        }

        .detail-tag {
          display: inline-block;
          padding: 2px 6px;
          background: var(--bg-tertiary);
          border-radius: 4px;
          font-size: 0.6875rem;
          color: var(--text-secondary);
        }

        .source-tag {
          display: inline-block;
          padding: 2px 6px;
          background: var(--bg-secondary);
          border: 1px solid var(--border-color);
          border-radius: 4px;
          font-size: 0.6875rem;
          font-weight: 600;
        }

        .search-status {
          font-size: 0.8125rem;
          padding: var(--spacing-sm);
        }

        /* Ingest Panel */
        .ingest-panel {
          margin-top: var(--spacing-md);
        }

        .ingest-panel h3 {
          margin-bottom: var(--spacing-xs);
        }

        .ingest-summary {
          margin-bottom: var(--spacing-md);
          font-size: 0.8125rem;
        }

        /* Config shared styles */
        .config-row {
          display: flex;
          align-items: center;
          gap: var(--spacing-md);
          flex-wrap: wrap;
          margin-bottom: var(--spacing-md);
        }

        .config-label {
          display: flex;
          align-items: center;
          gap: var(--spacing-xs);
          font-size: 0.875rem;
          font-weight: 500;
        }

        .config-label input[type="checkbox"] {
          width: auto;
        }

        .config-row .config-input {
          width: 100px;
          padding: 4px 8px;
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-size: 0.875rem;
        }

        /* Configure Tab */
        .config-card {
          margin-bottom: var(--spacing-md);
        }

        .config-card h3 {
          display: inline;
          font-size: 1rem;
        }

        .config-card-header {
          margin-bottom: var(--spacing-md);
        }

        .config-meta {
          display: block;
          font-size: 0.8125rem;
          margin-top: var(--spacing-xs);
        }

        .bulk-actions {
          margin-top: var(--spacing-md);
          text-align: center;
        }

        /* Jobs Tab */
        .active-jobs-section {
          margin-bottom: var(--spacing-lg);
        }

        .active-jobs-section h3 {
          margin-bottom: var(--spacing-sm);
        }

        .active-job-card {
          margin-bottom: var(--spacing-sm);
        }

        .active-job-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: var(--spacing-xs);
        }

        .active-job-meta {
          font-size: 0.8125rem;
          margin-bottom: var(--spacing-sm);
        }

        .job-badge {
          display: inline-block;
          padding: 2px 8px;
          border-radius: 4px;
          font-size: 0.75rem;
          font-weight: 600;
        }

        .job-badge-pending { background: var(--bg-tertiary); color: var(--text-secondary); }
        .job-badge-running { background: rgba(37, 99, 235, 0.1); color: var(--color-primary); }
        .job-badge-completed { background: rgba(34, 197, 94, 0.1); color: var(--color-success); }
        .job-badge-failed { background: rgba(239, 68, 68, 0.1); color: var(--color-danger); }
        .job-badge-cancelled { background: var(--bg-tertiary); color: var(--text-muted); }

        .progress-bar {
          position: relative;
          height: 20px;
          background: var(--bg-tertiary);
          border-radius: 10px;
          overflow: hidden;
          margin-bottom: var(--spacing-sm);
        }

        .progress-fill {
          position: absolute;
          top: 0;
          left: 0;
          bottom: 0;
          background: var(--color-primary);
          border-radius: 10px;
          transition: width 0.3s ease;
        }

        .progress-text {
          position: absolute;
          top: 0;
          left: 0;
          right: 0;
          text-align: center;
          font-size: 0.625rem;
          font-weight: 600;
          line-height: 20px;
          color: var(--text-primary);
        }

        .job-detail {
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
        }

        .job-detail pre {
          margin: 0;
          white-space: pre-wrap;
          word-break: break-all;
          font-size: 0.75rem;
          max-height: 300px;
          overflow-y: auto;
        }

        .jobs-tab h3 {
          margin-bottom: var(--spacing-sm);
        }

        /* History Tab */
        .filter-row {
          display: flex;
          gap: var(--spacing-sm);
          margin-bottom: var(--spacing-md);
        }

        .filter-row select {
          width: auto;
          padding: 8px 12px;
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-size: 0.875rem;
        }

        .history-row {
          cursor: pointer;
        }

        .history-row:hover {
          background: var(--bg-tertiary);
        }

        .run-detail {
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
        }

        .run-detail-grid {
          display: flex;
          gap: var(--spacing-lg);
          flex-wrap: wrap;
          margin-bottom: var(--spacing-sm);
          font-size: 0.8125rem;
        }

        .run-errors {
          margin-top: var(--spacing-sm);
          font-size: 0.8125rem;
        }

        .run-errors ul {
          margin: var(--spacing-xs) 0 0 var(--spacing-md);
          padding: 0;
        }

        .run-errors li {
          margin-bottom: var(--spacing-xs);
        }

        .run-log-viewer {
          margin-top: var(--spacing-md);
        }

        .run-log-header {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          margin-bottom: var(--spacing-sm);
          font-size: 0.8125rem;
        }

        .log-output {
          background: #1e1e1e;
          color: #d4d4d4;
          padding: var(--spacing-md);
          border-radius: var(--border-radius);
          font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
          font-size: 0.75rem;
          line-height: 1.6;
          max-height: 400px;
          overflow-y: auto;
          white-space: pre-wrap;
          word-break: break-all;
          margin: 0;
        }

        .log-live-indicator {
          display: inline-block;
          padding: 2px 8px;
          background: rgba(34, 197, 94, 0.15);
          color: var(--color-success);
          border-radius: 4px;
          font-size: 0.6875rem;
          font-weight: 600;
          animation: pulse 1.5s infinite;
        }

        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.5; }
        }

        /* Shared */
        .loading {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: var(--spacing-md);
          padding: var(--spacing-xl);
        }

        .empty-state {
          text-align: center;
          padding: var(--spacing-xl);
        }

        .btn-sm {
          padding: 4px 8px;
          font-size: 0.75rem;
        }

        @media (max-width: 768px) {
          .source-cards {
            grid-template-columns: repeat(2, 1fr);
          }

          .search-row {
            flex-direction: column;
          }

          .source-select {
            min-width: auto;
          }

          .config-row {
            flex-direction: column;
            align-items: flex-start;
          }
        }
      `}</style>
    </div>
  );
}
