/**
 * Dashboard page for MITDS
 *
 * Shows system overview, recent activity, and key metrics.
 */

import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getIngestionStatus,
  searchEntities,
  triggerIngestion,
  listJobs,
  cancelJob,
  getIngestionRuns,
  searchIngestionSources,
  type IngestionStatus,
  type JobStatusFull,
  type CompanySearchResult,
} from '../services/api';

export default function Dashboard() {
  // Ingestion config state: track which source has its config panel expanded
  const [expandedSource, setExpandedSource] = useState<string | null>(null);
  const [ingestionIncremental, setIngestionIncremental] = useState(true);
  const [ingestionStartYear, setIngestionStartYear] = useState('');
  const [ingestionEndYear, setIngestionEndYear] = useState('');
  const [ingestionLimit, setIngestionLimit] = useState('');

  // Company search state
  const [searchQuery, setSearchQuery] = useState('');
  const [searchTimeout, setSearchTimeout] = useState<ReturnType<typeof setTimeout> | null>(null);
  const [selectedEntities, setSelectedEntities] = useState<CompanySearchResult[]>([]);

  // Job result viewer
  const [viewingJobId, setViewingJobId] = useState<string | null>(null);

  const { data: ingestionData, isLoading: ingestionLoading } = useQuery({
    queryKey: ['ingestion-status'],
    queryFn: getIngestionStatus,
    refetchInterval: 60000, // Refresh every minute
  });

  // Fetch entity counts by type
  const { data: orgData } = useQuery({
    queryKey: ['entity-count', 'ORGANIZATION'],
    queryFn: () => searchEntities({ type: 'ORGANIZATION', limit: 1 }),
  });

  const { data: personData } = useQuery({
    queryKey: ['entity-count', 'PERSON'],
    queryFn: () => searchEntities({ type: 'PERSON', limit: 1 }),
  });

  const { data: outletData } = useQuery({
    queryKey: ['entity-count', 'OUTLET'],
    queryFn: () => searchEntities({ type: 'OUTLET', limit: 1 }),
  });

  const { data: allData } = useQuery({
    queryKey: ['entity-count', 'ALL'],
    queryFn: () => searchEntities({ limit: 1 }),
  });

  const queryClient = useQueryClient();

  const triggerMutation = useMutation({
    mutationFn: (source: string) => triggerIngestion(source),
    onSuccess: () => {
      // Refetch ingestion status after triggering
      queryClient.invalidateQueries({ queryKey: ['ingestion-status'] });
    },
  });

  // Jobs list - auto-refresh when any job is running
  const { data: jobsData } = useQuery({
    queryKey: ['jobs-list'],
    queryFn: () => listJobs({ limit: 10 }),
    refetchInterval: (query) => {
      const jobs = query.state.data?.jobs;
      if (jobs?.some((j: JobStatusFull) => j.status === 'pending' || j.status === 'running')) {
        return 10000; // 10s when jobs are active
      }
      return 60000;
    },
  });

  // Ingestion runs for expanded source
  const { data: ingestionRunsData } = useQuery({
    queryKey: ['ingestion-runs', expandedSource],
    queryFn: () => getIngestionRuns({ source: expandedSource!, limit: 5 }),
    enabled: !!expandedSource,
  });

  // Cancel job mutation
  const cancelMutation = useMutation({
    mutationFn: cancelJob,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['jobs-list'] });
    },
  });

  // Company search query (debounced)
  const { data: searchData, isLoading: searchLoading } = useQuery({
    queryKey: ['ingestion-search', searchQuery, expandedSource],
    queryFn: () => searchIngestionSources({
      q: searchQuery,
      sources: expandedSource || undefined,
      limit: 15,
    }),
    enabled: searchQuery.length >= 2,
    staleTime: 60000,
  });

  const handleTrigger = (source: string) => {
    if (triggerMutation.isPending) return;
    triggerMutation.mutate(source);
  };

  const handleSearchInput = (value: string) => {
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

  const handleTriggerWithOptions = (source: string) => {
    if (triggerMutation.isPending) return;

    // Build target entities for this source from selections
    const sourceTargets = selectedEntities
      .filter((e) => e.source === source)
      .map((e) => e.identifier);

    const startYear = ingestionStartYear ? parseInt(ingestionStartYear) : undefined;
    const endYear = ingestionEndYear ? parseInt(ingestionEndYear) : undefined;
    const limit = ingestionLimit ? parseInt(ingestionLimit) : undefined;

    triggerIngestion(source, {
      incremental: ingestionIncremental,
      start_year: startYear,
      end_year: endYear,
      limit,
      target_entities: sourceTargets.length > 0 ? sourceTargets : undefined,
    }).then(() => {
      queryClient.invalidateQueries({ queryKey: ['ingestion-status'] });
      queryClient.invalidateQueries({ queryKey: ['jobs-list'] });
      setExpandedSource(null);
      // Clear selections for this source after trigger
      setSelectedEntities((prev) => prev.filter((e) => e.source !== source));
    });
  };

  const getStatusColor = (status: IngestionStatus['status']) => {
    switch (status) {
      case 'healthy':
        return 'text-success';
      case 'running':
        return 'text-primary';
      case 'degraded':
        return 'text-warning';
      case 'failed':
        return 'text-danger';
      case 'stale':
        return 'text-warning';
      case 'disabled':
        return 'text-muted';
      case 'never_run':
        return 'text-muted';
      default:
        return 'text-muted';
    }
  };

  const getStatusIcon = (status: IngestionStatus['status']) => {
    switch (status) {
      case 'healthy':
        return '✅';
      case 'running':
        return '🔄';
      case 'degraded':
        return '⚠️';
      case 'failed':
        return '❌';
      case 'stale':
        return '⏰';
      case 'disabled':
        return '⏸️';
      case 'never_run':
        return '🆕';
      default:
        return '❓';
    }
  };

  return (
    <div className="dashboard">
      <header className="page-header">
        <h1>Dashboard</h1>
        <p>Media Influence Topology & Detection System Overview</p>
      </header>

      {/* Stats Grid */}
      <div className="stats-grid">
        <div className="card stat-card">
          <div className="stat-icon">🏢</div>
          <div className="stat-content">
            <div className="stat-value">{(orgData?.total ?? 0).toLocaleString()}</div>
            <div className="stat-label">Organizations</div>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-icon">👤</div>
          <div className="stat-content">
            <div className="stat-value">{(personData?.total ?? 0).toLocaleString()}</div>
            <div className="stat-label">Persons</div>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-icon">📰</div>
          <div className="stat-content">
            <div className="stat-value">{(outletData?.total ?? 0).toLocaleString()}</div>
            <div className="stat-label">Outlets</div>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-icon">🔗</div>
          <div className="stat-content">
            <div className="stat-value">{(allData?.total ?? 0).toLocaleString()}</div>
            <div className="stat-label">Total Entities</div>
          </div>
        </div>
      </div>

      {/* Data Sources Status */}
      <section className="section">
        <h2>Data Sources</h2>
        <div className="card">
          {ingestionLoading ? (
            <div className="loading">
              <div className="spinner" />
              <span>Loading status...</span>
            </div>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Source</th>
                  <th>Status</th>
                  <th>Last Run</th>
                  <th>Records</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {ingestionData?.sources.map((source) => (
                  <React.Fragment key={source.source}>
                  <tr>
                    <td>
                      <strong>{source.source.toUpperCase()}</strong>
                    </td>
                    <td className={getStatusColor(source.status)}>
                      {getStatusIcon(source.status)} {source.status}
                    </td>
                    <td>
                      {source.last_successful_run
                        ? new Date(source.last_successful_run).toLocaleString()
                        : 'Never'}
                    </td>
                    <td>{(source.records_processed ?? 0).toLocaleString()}</td>
                    <td>
                      <div className="source-actions">
                        <button
                          type="button"
                          className="btn btn-secondary"
                          disabled={source.status === 'disabled' || source.status === 'running' || triggerMutation.isPending}
                          onClick={() => handleTrigger(source.source)}
                        >
                          {source.status === 'running' ? 'Running...' : triggerMutation.isPending ? 'Starting...' : 'Trigger'}
                        </button>
                        <button
                          type="button"
                          className="btn-icon"
                          title={expandedSource === source.source ? 'Hide options' : 'Show options'}
                          onClick={() => setExpandedSource(expandedSource === source.source ? null : source.source)}
                        >
                          {expandedSource === source.source ? '▲' : '▼'}
                        </button>
                      </div>
                    </td>
                  </tr>
                  {/* Expandable config panel */}
                  {expandedSource === source.source && (
                    <tr>
                      <td colSpan={5}>
                        <div className="ingestion-config">
                          {/* Company Search */}
                          <div className="search-section">
                            <h4>Target Specific Companies</h4>
                            <p className="search-hint">
                              Search by company name to ingest only specific entities instead of the full dataset.
                            </p>
                            <input
                              type="text"
                              className="search-input"
                              placeholder={`Search ${source.source.toUpperCase()} companies...`}
                              onChange={(e) => handleSearchInput(e.target.value)}
                            />

                            {/* Selected entities chips */}
                            {selectedEntities.filter((e) => e.source === source.source).length > 0 && (
                              <div className="selected-chips">
                                {selectedEntities
                                  .filter((e) => e.source === source.source)
                                  .map((entity) => (
                                    <span
                                      key={`${entity.source}-${entity.identifier}`}
                                      className="entity-chip"
                                    >
                                      {entity.name}
                                      <span className="chip-id">{entity.identifier_type}: {entity.identifier}</span>
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
                              <div className="search-status">Searching...</div>
                            )}
                            {searchData && searchData.results.length > 0 && (
                              <div className="search-results">
                                <table className="runs-table">
                                  <thead>
                                    <tr>
                                      <th></th>
                                      <th>Name</th>
                                      <th>ID</th>
                                      <th>Details</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {searchData.results
                                      .filter((r) => r.source === source.source)
                                      .map((result) => {
                                        const isSelected = selectedEntities.some(
                                          (e) => e.identifier === result.identifier && e.source === result.source
                                        );
                                        return (
                                          <tr
                                            key={`${result.source}-${result.identifier}`}
                                            className={`search-result-row ${isSelected ? 'selected' : ''}`}
                                            onClick={() => handleSelectEntity(result)}
                                          >
                                            <td>
                                              <input
                                                type="checkbox"
                                                checked={isSelected}
                                                readOnly
                                              />
                                            </td>
                                            <td><strong>{result.name}</strong></td>
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
                                                <span className="detail-tag">
                                                  {'Form ' + result.details.form_type}
                                                </span>
                                              )}
                                              {typeof result.details.province === 'string' && (
                                                <span className="detail-tag">
                                                  {result.details.province}
                                                </span>
                                              )}
                                              {typeof result.details.status === 'string' && (
                                                <span className="detail-tag">
                                                  {result.details.status}
                                                </span>
                                              )}
                                              {typeof result.details.operating_name === 'string' && (
                                                <span className="detail-tag">
                                                  {'aka ' + result.details.operating_name}
                                                </span>
                                              )}
                                              {typeof result.details.corporation_type === 'string' && (
                                                <span className="detail-tag">
                                                  {result.details.corporation_type}
                                                </span>
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
                            {searchData && searchQuery.length >= 2 &&
                              searchData.results.filter((r) => r.source === source.source).length === 0 &&
                              !searchLoading && (
                                <div className="search-status text-muted">
                                  No results found for "{searchQuery}" in {source.source.toUpperCase()}
                                </div>
                              )}
                          </div>

                          {/* Ingestion options */}
                          <div className="config-row">
                            <label className="config-label">
                              <input
                                type="checkbox"
                                checked={ingestionIncremental}
                                onChange={(e) => setIngestionIncremental(e.target.checked)}
                              />
                              Incremental (only new records)
                            </label>
                            <label className="config-label">
                              Start Year:
                              <input
                                type="number"
                                className="config-input"
                                min="2000"
                                max="2030"
                                value={ingestionStartYear}
                                onChange={(e) => setIngestionStartYear(e.target.value)}
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
                                value={ingestionEndYear}
                                onChange={(e) => setIngestionEndYear(e.target.value)}
                                placeholder="e.g. 2024"
                              />
                            </label>
                            <label className="config-label">
                              Limit:
                              <input
                                type="number"
                                className="config-input"
                                min="1"
                                value={ingestionLimit}
                                onChange={(e) => setIngestionLimit(e.target.value)}
                                placeholder="max records"
                              />
                            </label>
                            <button
                              type="button"
                              className="btn btn-primary"
                              onClick={() => handleTriggerWithOptions(source.source)}
                            >
                              {selectedEntities.filter((e) => e.source === source.source).length > 0
                                ? `Ingest ${selectedEntities.filter((e) => e.source === source.source).length} Selected`
                                : 'Run Full Ingestion'}
                            </button>
                          </div>
                          {/* Recent runs for this source */}
                          {ingestionRunsData?.runs && ingestionRunsData.runs.length > 0 && (
                            <div className="recent-runs">
                              <h4>Recent Runs</h4>
                              <table className="runs-table">
                                <thead>
                                  <tr>
                                    <th>Status</th>
                                    <th>Started</th>
                                    <th>Records</th>
                                    <th>Created</th>
                                    <th>Errors</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {ingestionRunsData.runs.map((run) => (
                                    <tr key={run.run_id}>
                                      <td className={run.status === 'completed' ? 'text-success' : run.status === 'failed' ? 'text-danger' : ''}>
                                        {run.status}
                                      </td>
                                      <td>{run.started_at ? new Date(run.started_at).toLocaleString() : '—'}</td>
                                      <td>{run.records_processed}</td>
                                      <td>{run.records_created}</td>
                                      <td>{run.errors.length}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          )}
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </section>

      {/* Jobs Panel */}
      <section className="section">
        <h2>Jobs</h2>
        <div className="card">
          {jobsData?.jobs && jobsData.jobs.length > 0 ? (
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
                {jobsData.jobs.map((job: JobStatusFull) => (
                  <tr key={job.job_id}>
                    <td><strong>{job.job_type}</strong></td>
                    <td>
                      <span className={`job-status job-status-${job.status}`}>
                        {job.status}
                      </span>
                    </td>
                    <td>{new Date(job.created_at).toLocaleString()}</td>
                    <td>
                      {job.progress != null ? (
                        <div className="progress-bar">
                          <div className="progress-fill" style={{ width: `${job.progress * 100}%` }} />
                          <span className="progress-text">{(job.progress * 100).toFixed(0)}%</span>
                        </div>
                      ) : (
                        <span className="text-muted">—</span>
                      )}
                    </td>
                    <td>
                      {(job.status === 'pending' || job.status === 'running') && (
                        <button
                          type="button"
                          className="btn btn-secondary btn-sm"
                          onClick={() => cancelMutation.mutate(job.job_id)}
                          disabled={cancelMutation.isPending}
                        >
                          Cancel
                        </button>
                      )}
                      {job.status === 'completed' && (
                        <button
                          type="button"
                          className="btn btn-secondary btn-sm"
                          onClick={() => setViewingJobId(viewingJobId === job.job_id ? null : job.job_id)}
                        >
                          {viewingJobId === job.job_id ? 'Hide' : 'View'}
                        </button>
                      )}
                      {job.status === 'failed' && job.error && (
                        <span className="text-danger" title={job.error}>Error</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="empty-state">
              <p>No jobs to display.</p>
              <p className="text-muted">
                Jobs will appear here when you trigger ingestion, detection, or report generation.
              </p>
            </div>
          )}
        </div>
      </section>

      {/* Quick Actions */}
      <section className="section">
        <h2>Quick Actions</h2>
        <div className="actions-grid">
          <div className="card action-card">
            <h3>🔍 Search Entities</h3>
            <p>Explore organizations, persons, and media outlets</p>
            <a href="/entities" className="btn btn-primary">
              Go to Explorer
            </a>
          </div>
          <div className="card action-card">
            <h3>🎯 Run Detection</h3>
            <p>Analyze temporal coordination patterns</p>
            <a href="/detection" className="btn btn-primary">
              Start Analysis
            </a>
          </div>
          <div className="card action-card">
            <h3>📄 Generate Report</h3>
            <p>Create structural risk reports</p>
            <a href="/reports" className="btn btn-primary">
              Create Report
            </a>
          </div>
        </div>
      </section>

      <style>{`
        .page-header {
          margin-bottom: var(--spacing-lg);
        }

        .page-header h1 {
          margin-bottom: var(--spacing-xs);
        }

        .stats-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
          margin-bottom: var(--spacing-lg);
        }

        .stat-card {
          display: flex;
          align-items: center;
          gap: var(--spacing-md);
        }

        .stat-icon {
          font-size: 2rem;
        }

        .stat-value {
          font-size: 2rem;
          font-weight: 700;
          color: var(--text-primary);
        }

        .stat-label {
          font-size: 0.875rem;
          color: var(--text-secondary);
        }

        .section {
          margin-bottom: var(--spacing-lg);
        }

        .section h2 {
          margin-bottom: var(--spacing-md);
        }

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

        .actions-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
          gap: var(--spacing-md);
        }

        .action-card h3 {
          margin-bottom: var(--spacing-sm);
        }

        .action-card p {
          margin-bottom: var(--spacing-md);
        }

        .source-actions {
          display: flex;
          gap: var(--spacing-xs);
          align-items: center;
        }

        .btn-icon {
          background: none;
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          cursor: pointer;
          padding: 4px 8px;
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .btn-icon:hover {
          background: var(--bg-tertiary);
          color: var(--text-primary);
        }

        .btn-sm {
          padding: 4px 8px;
          font-size: 0.75rem;
        }

        .ingestion-config {
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
        }

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

        .recent-runs {
          margin-top: var(--spacing-md);
          padding-top: var(--spacing-md);
          border-top: 1px solid var(--border-color);
        }

        .recent-runs h4 {
          margin-bottom: var(--spacing-sm);
          font-size: 0.875rem;
        }

        .runs-table {
          width: 100%;
          border-collapse: collapse;
          font-size: 0.8125rem;
        }

        .runs-table th,
        .runs-table td {
          padding: var(--spacing-xs) var(--spacing-sm);
          text-align: left;
          border-bottom: 1px solid var(--border-color);
        }

        .runs-table th {
          font-weight: 600;
        }

        .job-status {
          padding: 2px 8px;
          border-radius: 4px;
          font-size: 0.75rem;
          font-weight: 500;
        }

        .job-status-pending {
          background: rgba(245, 158, 11, 0.15);
          color: #D97706;
        }

        .job-status-running {
          background: rgba(59, 130, 246, 0.15);
          color: #2563EB;
        }

        .job-status-completed {
          background: rgba(16, 185, 129, 0.15);
          color: #059669;
        }

        .job-status-failed {
          background: rgba(220, 38, 38, 0.15);
          color: #DC2626;
        }

        .job-status-cancelled {
          background: var(--bg-tertiary);
          color: var(--text-muted);
        }

        .progress-bar {
          position: relative;
          height: 20px;
          background: var(--bg-tertiary);
          border-radius: 10px;
          overflow: hidden;
          min-width: 80px;
        }

        .progress-fill {
          height: 100%;
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

        .search-section {
          margin-bottom: var(--spacing-md);
          padding-bottom: var(--spacing-md);
          border-bottom: 1px solid var(--border-color);
        }

        .search-section h4 {
          margin-bottom: var(--spacing-xs);
          font-size: 0.875rem;
        }

        .search-hint {
          font-size: 0.8125rem;
          color: var(--text-muted);
          margin-bottom: var(--spacing-sm);
        }

        .search-input {
          width: 100%;
          padding: 8px 12px;
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-size: 0.875rem;
          margin-bottom: var(--spacing-sm);
        }

        .search-input:focus {
          outline: none;
          border-color: var(--color-primary);
          box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.15);
        }

        .selected-chips {
          display: flex;
          flex-wrap: wrap;
          gap: var(--spacing-xs);
          margin-bottom: var(--spacing-sm);
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

        .chip-id {
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

        .search-results {
          max-height: 280px;
          overflow-y: auto;
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          margin-bottom: var(--spacing-sm);
        }

        .search-result-row {
          cursor: pointer;
        }

        .search-result-row:hover {
          background: var(--bg-tertiary);
        }

        .search-result-row.selected {
          background: rgba(59, 130, 246, 0.08);
        }

        .search-status {
          font-size: 0.8125rem;
          padding: var(--spacing-sm);
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
      `}</style>
    </div>
  );
}
