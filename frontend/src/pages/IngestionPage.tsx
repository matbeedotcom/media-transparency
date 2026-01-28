/**
 * Ingestion Management page for MITDS
 *
 * Universal interface for searching, configuring, triggering,
 * and monitoring data ingestion across all sources.
 */

import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getIngestionStatus,
  searchIngestionSources,
  triggerIngestion,
  getIngestionRuns,
  listJobs,
  cancelJob,
  getJobResult,
  type CompanySearchResult,
  type JobStatusFull,
  type IngestionRun,
} from '../services/api';

type Tab = 'search' | 'configure' | 'jobs' | 'history';

interface SourceConfig {
  incremental: boolean;
  startYear: string;
  endYear: string;
  limit: string;
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
  const [selectedEntities, setSelectedEntities] = useState<CompanySearchResult[]>([]);
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

  // ========================
  // Queries
  // ========================

  const { data: statusData, isLoading: statusLoading } = useQuery({
    queryKey: ['ingestion-status'],
    queryFn: getIngestionStatus,
    refetchInterval: 30000,
  });

  const { data: searchData, isLoading: searchLoading } = useQuery({
    queryKey: ['ingestion-search', searchQuery, sourceFilter],
    queryFn: () => searchIngestionSources({
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
      status: historyStatusFilter || undefined,
      limit: 25,
    }),
    enabled: activeTab === 'history',
  });

  // ========================
  // Mutations
  // ========================

  const triggerMutation = useMutation({
    mutationFn: ({ source, options }: { source: string; options?: Parameters<typeof triggerIngestion>[1] }) =>
      triggerIngestion(source, options),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['ingestion-status'] });
      queryClient.invalidateQueries({ queryKey: ['jobs-list'] });
      queryClient.invalidateQueries({ queryKey: ['ingestion-runs'] });
    },
  });

  const cancelMutation = useMutation({
    mutationFn: cancelJob,
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
    setSelectedEntities((prev) => {
      const exists = prev.some(
        (e) => e.identifier === result.identifier && e.source === result.source
      );
      if (exists) {
        return prev.filter(
          (e) => !(e.identifier === result.identifier && e.source === result.source)
        );
      }
      return [...prev, result];
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
        source,
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
      source,
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
      (s) => s.status !== 'disabled' && s.status !== 'running'
    );
    enabled.forEach((s) => {
      triggerMutation.mutate({ source: s.source, options: { incremental: true } });
    });
    setActiveTab('jobs');
  };

  const handleQuickRun = (source: string) => {
    if (triggerMutation.isPending) return;
    triggerMutation.mutate({ source, options: { incremental: true } });
  };

  // Group selections by source for summary
  const selectionSummary = selectedEntities.reduce((acc, e) => {
    acc[e.source] = (acc[e.source] ?? 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  const enabledSources = (statusData?.sources ?? []).filter(
    (s) => s.status !== 'disabled'
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
            <div key={source.source} className="source-card card">
              <div className="source-card-header">
                <strong>{SOURCE_LABELS[source.source] ?? source.source}</strong>
                <span
                  className="status-badge"
                  style={{ background: STATUS_COLORS[source.status] ?? 'var(--text-muted)' }}
                >
                  {STATUS_ICONS[source.status] ?? '?'} {source.status}
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
                onClick={() => handleQuickRun(source.source)}
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
                  <option key={s.source} value={s.source}>
                    {SOURCE_LABELS[s.source] ?? s.source}
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
                      onClick={() => handleSelectEntity(entity)}
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

            {searchData && searchData.results.length > 0 && (
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
                      const isSelected = selectedEntities.some(
                        (e) => e.identifier === result.identifier && e.source === result.source
                      );
                      return (
                        <tr
                          key={`${result.source}-${result.identifier}`}
                          className={`result-row ${isSelected ? 'result-row-selected' : ''}`}
                          onClick={() => handleSelectEntity(result)}
                        >
                          <td>
                            <input type="checkbox" checked={isSelected} readOnly />
                          </td>
                          <td><strong>{result.name}</strong></td>
                          <td>
                            <span className="source-tag">
                              {SOURCE_LABELS[result.source] ?? result.source}
                            </span>
                          </td>
                          <td className="text-muted">
                            {result.identifier_type}: {result.identifier}
                          </td>
                          <td className="result-details">
                            {Array.isArray(result.details.tickers) && (
                              <span className="detail-tag">
                                {(result.details.tickers as string[]).join(', ')}
                              </span>
                            )}
                            {typeof result.details.form_type === 'string' && (
                              <span className="detail-tag">Form {result.details.form_type}</span>
                            )}
                            {typeof result.details.province === 'string' && (
                              <span className="detail-tag">{result.details.province}</span>
                            )}
                            {typeof result.details.status === 'string' && (
                              <span className="detail-tag">{result.details.status}</span>
                            )}
                            {typeof result.details.operating_name === 'string' && (
                              <span className="detail-tag">aka {result.details.operating_name}</span>
                            )}
                            {typeof result.details.corporation_type === 'string' && (
                              <span className="detail-tag">{result.details.corporation_type}</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
                {searchData.sources_failed.length > 0 && (
                  <div className="search-status text-warning">
                    Search failed for: {searchData.sources_failed.join(', ')}
                  </div>
                )}
              </div>
            )}

            {searchData && searchQuery.length >= 2 && searchData.results.length === 0 && !searchLoading && (
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
              const cfg = getSourceConfig(source.source);
              return (
                <div key={source.source} className="card config-card">
                  <div className="config-card-header">
                    <div>
                      <h3>{SOURCE_LABELS[source.source] ?? source.source}</h3>
                      <span
                        className="status-badge-sm"
                        style={{ background: STATUS_COLORS[source.status] ?? 'var(--text-muted)' }}
                      >
                        {source.status}
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
                        onChange={(e) => updateSourceConfig(source.source, { incremental: e.target.checked })}
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
                        onChange={(e) => updateSourceConfig(source.source, { startYear: e.target.value })}
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
                        onChange={(e) => updateSourceConfig(source.source, { endYear: e.target.value })}
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
                        onChange={(e) => updateSourceConfig(source.source, { limit: e.target.value })}
                        placeholder="max"
                      />
                    </label>
                    <button
                      type="button"
                      className="btn btn-primary"
                      disabled={source.status === 'running' || triggerMutation.isPending}
                      onClick={() => handleTriggerSource(source.source)}
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
                  <option key={s.source} value={s.source}>
                    {SOURCE_LABELS[s.source] ?? s.source}
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
                          key={run.run_id}
                          className="history-row"
                          onClick={() => setExpandedRunId(expandedRunId === run.run_id ? null : run.run_id)}
                        >
                          <td><strong>{SOURCE_LABELS[run.source] ?? run.source}</strong></td>
                          <td>
                            <span className={
                              run.status === 'completed' ? 'text-success'
                                : run.status === 'failed' ? 'text-danger'
                                : ''
                            }>
                              {run.status}
                            </span>
                          </td>
                          <td>{run.started_at ? new Date(run.started_at).toLocaleString() : '\u2014'}</td>
                          <td>{run.completed_at ? new Date(run.completed_at).toLocaleString() : '\u2014'}</td>
                          <td>{run.records_processed}</td>
                          <td>{run.records_created}</td>
                          <td>{run.errors.length}</td>
                        </tr>
                        {expandedRunId === run.run_id && (
                          <tr key={`${run.run_id}-detail`}>
                            <td colSpan={7}>
                              <div className="run-detail">
                                <div className="run-detail-grid">
                                  <div><strong>Run ID:</strong> {run.run_id}</div>
                                  <div><strong>Records Updated:</strong> {run.records_updated}</div>
                                  <div><strong>Duplicates Found:</strong> {run.duplicates_found}</div>
                                </div>
                                {run.errors.length > 0 && (
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

        .search-input {
          flex: 1;
          padding: 10px 14px;
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-size: 0.9375rem;
        }

        .search-input:focus {
          outline: none;
          border-color: var(--color-primary);
          box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.15);
        }

        .source-select {
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

        .config-input {
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
