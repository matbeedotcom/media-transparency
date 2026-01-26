/**
 * Validation Dashboard page for MITDS
 *
 * Track detection accuracy against golden datasets with
 * comprehensive metrics visualization and validation controls.
 */

import { useState, useCallback } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';

// =========================
// Types
// =========================

interface ValidationMetrics {
  recall: number;
  precision: number;
  f1_score: number;
  false_positive_rate: number;
  accuracy: number;
  total_cases: number;
  passed_cases: number;
  failed_cases: number;
  last_run_at: string | null;
  meets_targets: boolean;
  target_recall: number;
  target_max_fpr: number;
}

interface DashboardData {
  summary: {
    current: {
      recall: number;
      precision: number;
      f1: number;
      false_positive_rate: number;
    };
    targets: {
      recall: number;
      max_fpr: number;
    };
    status: {
      meets_targets: boolean;
      message: string;
    };
    changes: {
      recall: number;
      precision: number;
      fpr: number;
    };
    cases: {
      total: number;
      positive: number;
      negative: number;
      passed: number;
      failed: number;
    };
    last_run: {
      at: string | null;
      id: string | null;
    };
  };
  history: {
    recall: TimeSeriesPoint[];
    precision: TimeSeriesPoint[];
    false_positive_rate: TimeSeriesPoint[];
    f1: TimeSeriesPoint[];
  };
  by_case_type: Record<string, { count: number; recall: number; precision: number; false_positive_rate: number }>;
  signal_performance: Record<string, { detection_rate: number; true_positive_rate: number; false_positive_rate: number }>;
  recent_failures: FailureRecord[];
  health_checks: HealthCheck[];
}

interface TimeSeriesPoint {
  timestamp: string;
  value: number;
  label?: string;
}

interface FailureRecord {
  case_id: string;
  case_name: string;
  failure_type: 'false_positive' | 'false_negative';
  score: number;
  expected_label: string;
  detected: boolean;
  signals_found: string[];
  signals_missing: string[];
}

interface HealthCheck {
  name: string;
  status: 'pass' | 'fail' | 'warning';
  message: string;
  value?: number;
  target?: number;
}

interface GoldenDataset {
  id: string;
  name: string;
  version: string;
  description: string;
  case_count: number;
  positive_cases: number;
  negative_cases: number;
}

interface ValidationRunResponse {
  job_id: string;
  status: string;
  status_url: string;
  estimated_cases: number;
}

// =========================
// API Functions
// =========================

async function fetchDashboard(): Promise<DashboardData> {
  const response = await fetch('/api/validation/dashboard');
  if (!response.ok) throw new Error('Failed to fetch dashboard');
  return response.json();
}

async function fetchDatasets(): Promise<GoldenDataset[]> {
  const response = await fetch('/api/validation/datasets');
  if (!response.ok) throw new Error('Failed to fetch datasets');
  return response.json();
}

async function runValidation(params: {
  dataset_id: string;
  include_synthetic: boolean;
  threshold: number;
}): Promise<ValidationRunResponse> {
  const response = await fetch('/api/validation/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(params),
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || 'Failed to run validation');
  }
  return response.json();
}

async function fetchJobStatus(jobId: string): Promise<{ status: string; results?: Record<string, number> }> {
  const response = await fetch(`/api/validation/jobs/${jobId}`);
  if (!response.ok) throw new Error('Failed to fetch job status');
  return response.json();
}

// =========================
// Component
// =========================

export default function ValidationDashboard() {
  const queryClient = useQueryClient();

  // Form state
  const [selectedDataset, setSelectedDataset] = useState('sample');
  const [includeSynthetic, setIncludeSynthetic] = useState(true);
  const [threshold, setThreshold] = useState(0.45);

  // Job tracking
  const [currentJobId, setCurrentJobId] = useState<string | null>(null);

  // Fetch dashboard data
  const { data: dashboard, isLoading: dashboardLoading } = useQuery({
    queryKey: ['validationDashboard'],
    queryFn: fetchDashboard,
    refetchInterval: currentJobId ? 2000 : false,
  });

  // Fetch datasets
  const { data: datasets = [] } = useQuery({
    queryKey: ['goldenDatasets'],
    queryFn: fetchDatasets,
  });

  // Run validation mutation
  const runMutation = useMutation({
    mutationFn: runValidation,
    onSuccess: (data) => {
      setCurrentJobId(data.job_id);
      // Poll for completion
      pollJobStatus(data.job_id);
    },
  });

  const pollJobStatus = useCallback(async (jobId: string) => {
    const poll = async () => {
      try {
        const status = await fetchJobStatus(jobId);
        if (status.status === 'completed' || status.status === 'failed') {
          setCurrentJobId(null);
          queryClient.invalidateQueries({ queryKey: ['validationDashboard'] });
        } else {
          setTimeout(poll, 2000);
        }
      } catch {
        setCurrentJobId(null);
      }
    };
    poll();
  }, [queryClient]);

  const handleRunValidation = useCallback((e: React.FormEvent) => {
    e.preventDefault();
    runMutation.mutate({
      dataset_id: selectedDataset,
      include_synthetic: includeSynthetic,
      threshold,
    });
  }, [selectedDataset, includeSynthetic, threshold, runMutation]);

  const formatPercent = (value: number): string => `${(value * 100).toFixed(1)}%`;
  const formatChange = (value: number): string => {
    const sign = value >= 0 ? '+' : '';
    return `${sign}${(value * 100).toFixed(1)}%`;
  };

  const getMetricColor = (value: number, target: number, isMax: boolean = false): string => {
    if (isMax) {
      return value <= target ? 'var(--color-success)' : 'var(--color-danger)';
    }
    return value >= target ? 'var(--color-success)' : 'var(--color-danger)';
  };

  const getStatusIcon = (status: string): string => {
    switch (status) {
      case 'pass': return '✓';
      case 'fail': return '✗';
      case 'warning': return '⚠';
      default: return '?';
    }
  };

  const getStatusColor = (status: string): string => {
    switch (status) {
      case 'pass': return 'var(--color-success)';
      case 'fail': return 'var(--color-danger)';
      case 'warning': return 'var(--color-warning)';
      default: return 'var(--text-muted)';
    }
  };

  const summary = dashboard?.summary;

  return (
    <div className="validation-dashboard">
      <header className="page-header">
        <h1>Validation Dashboard</h1>
        <p>Track detection accuracy and system performance metrics</p>
      </header>

      {/* Metrics Overview */}
      <div className="metrics-grid">
        <div className="card metric-card">
          <h3>Recall</h3>
          <div
            className="metric-value"
            style={{ color: summary ? getMetricColor(summary.current.recall, summary.targets.recall) : undefined }}
          >
            {summary ? formatPercent(summary.current.recall) : '--'}
          </div>
          <div className="metric-target">Target: ≥{summary ? formatPercent(summary.targets.recall) : '85%'}</div>
          {summary && summary.changes.recall !== 0 && (
            <div className={`metric-change ${summary.changes.recall >= 0 ? 'positive' : 'negative'}`}>
              {formatChange(summary.changes.recall)}
            </div>
          )}
        </div>

        <div className="card metric-card">
          <h3>Precision</h3>
          <div className="metric-value">
            {summary ? formatPercent(summary.current.precision) : '--'}
          </div>
          <div className="metric-target">Higher is better</div>
          {summary && summary.changes.precision !== 0 && (
            <div className={`metric-change ${summary.changes.precision >= 0 ? 'positive' : 'negative'}`}>
              {formatChange(summary.changes.precision)}
            </div>
          )}
        </div>

        <div className="card metric-card">
          <h3>False Positive Rate</h3>
          <div
            className="metric-value"
            style={{ color: summary ? getMetricColor(summary.current.false_positive_rate, summary.targets.max_fpr, true) : undefined }}
          >
            {summary ? formatPercent(summary.current.false_positive_rate) : '--'}
          </div>
          <div className="metric-target">Target: ≤{summary ? formatPercent(summary.targets.max_fpr) : '5%'}</div>
          {summary && summary.changes.fpr !== 0 && (
            <div className={`metric-change ${summary.changes.fpr <= 0 ? 'positive' : 'negative'}`}>
              {formatChange(summary.changes.fpr)}
            </div>
          )}
        </div>

        <div className="card metric-card">
          <h3>Last Validation</h3>
          <div className="metric-value" style={{ fontSize: '1.25rem' }}>
            {summary?.last_run.at ? new Date(summary.last_run.at).toLocaleDateString() : 'Never'}
          </div>
          <div className="metric-target">
            {summary?.cases.total ? `${summary.cases.total} cases` : 'Run validation below'}
          </div>
        </div>
      </div>

      {/* Status Banner */}
      {summary && (
        <div className={`status-banner ${summary.status.meets_targets ? 'success' : 'warning'}`}>
          <span className="status-icon">{summary.status.meets_targets ? '✓' : '⚠'}</span>
          <span className="status-message">{summary.status.message || 'All targets met'}</span>
        </div>
      )}

      {/* Run Validation */}
      <section className="section">
        <h2>Run Validation</h2>
        <div className="card">
          <p className="section-description">
            Validate detection accuracy against documented influence operations
            (golden dataset) and synthetic coordination patterns.
          </p>

          <form className="validation-form" onSubmit={handleRunValidation}>
            <div className="form-row">
              <div className="form-group">
                <label htmlFor="dataset-select">Golden Dataset</label>
                <select
                  id="dataset-select"
                  value={selectedDataset}
                  onChange={(e) => setSelectedDataset(e.target.value)}
                >
                  {datasets.map((ds) => (
                    <option key={ds.id} value={ds.id}>
                      {ds.name} ({ds.case_count} cases)
                    </option>
                  ))}
                </select>
              </div>

              <div className="form-group">
                <label htmlFor="threshold-input">Detection Threshold</label>
                <input
                  id="threshold-input"
                  type="number"
                  min="0"
                  max="1"
                  step="0.05"
                  value={threshold}
                  onChange={(e) => setThreshold(parseFloat(e.target.value))}
                />
              </div>

              <div className="form-group">
                <label>Options</label>
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={includeSynthetic}
                    onChange={(e) => setIncludeSynthetic(e.target.checked)}
                  />
                  Include synthetic patterns
                </label>
              </div>
            </div>

            <button
              type="submit"
              className="btn btn-primary"
              disabled={runMutation.isPending || !!currentJobId}
            >
              {currentJobId ? 'Running...' : runMutation.isPending ? 'Starting...' : 'Run Validation'}
            </button>
            {runMutation.isError && (
              <span className="error-text">{(runMutation.error as Error).message}</span>
            )}
          </form>
        </div>
      </section>

      {/* Health Checks */}
      {dashboard?.health_checks && dashboard.health_checks.length > 0 && (
        <section className="section">
          <h2>Health Checks</h2>
          <div className="health-checks-grid">
            {dashboard.health_checks.map((check, idx) => (
              <div key={idx} className={`health-check-card ${check.status}`}>
                <span className="check-icon" style={{ color: getStatusColor(check.status) }}>
                  {getStatusIcon(check.status)}
                </span>
                <div className="check-content">
                  <span className="check-name">{check.name}</span>
                  <span className="check-message">{check.message}</span>
                </div>
              </div>
            ))}
          </div>
        </section>
      )}

      {/* Performance by Case Type */}
      {dashboard?.by_case_type && Object.keys(dashboard.by_case_type).length > 0 && (
        <section className="section">
          <h2>Performance by Case Type</h2>
          <div className="card">
            <table className="metrics-table">
              <thead>
                <tr>
                  <th>Case Type</th>
                  <th>Count</th>
                  <th>Recall</th>
                  <th>Precision</th>
                  <th>FPR</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(dashboard.by_case_type).map(([type, metrics]) => (
                  <tr key={type}>
                    <td>{type}</td>
                    <td>{metrics.count}</td>
                    <td>{formatPercent(metrics.recall)}</td>
                    <td>{formatPercent(metrics.precision)}</td>
                    <td>{formatPercent(metrics.false_positive_rate)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Recent Failures */}
      {dashboard?.recent_failures && dashboard.recent_failures.length > 0 && (
        <section className="section">
          <h2>Recent Failures</h2>
          <div className="card">
            <div className="failures-list">
              {dashboard.recent_failures.map((failure, idx) => (
                <div key={idx} className={`failure-item ${failure.failure_type}`}>
                  <div className="failure-header">
                    <span className="failure-type">
                      {failure.failure_type === 'false_positive' ? 'False Positive' : 'False Negative'}
                    </span>
                    <span className="failure-score">Score: {failure.score.toFixed(2)}</span>
                  </div>
                  <div className="failure-name">{failure.case_name}</div>
                  {failure.signals_missing.length > 0 && (
                    <div className="failure-signals">
                      Missing: {failure.signals_missing.join(', ')}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        </section>
      )}

      {/* Signal Performance */}
      {dashboard?.signal_performance && Object.keys(dashboard.signal_performance).length > 0 && (
        <section className="section">
          <h2>Signal Performance</h2>
          <div className="card">
            <table className="metrics-table">
              <thead>
                <tr>
                  <th>Signal Type</th>
                  <th>Detection Rate</th>
                  <th>True Positive Rate</th>
                  <th>False Positive Rate</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(dashboard.signal_performance).map(([signal, perf]) => (
                  <tr key={signal}>
                    <td>{signal}</td>
                    <td>{formatPercent(perf.detection_rate)}</td>
                    <td>{formatPercent(perf.true_positive_rate)}</td>
                    <td>{formatPercent(perf.false_positive_rate)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Empty State */}
      {!dashboardLoading && (!dashboard || !summary?.last_run.at) && (
        <section className="section">
          <div className="card">
            <div className="empty-state">
              <p>No validation runs yet.</p>
              <p className="text-muted">
                Run a validation above to track accuracy over time.
              </p>
            </div>
          </div>
        </section>
      )}

      <style>{`
        .validation-dashboard {
          max-width: 1200px;
          margin: 0 auto;
        }

        .metrics-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
          margin-bottom: var(--spacing-lg);
        }

        .metric-card {
          text-align: center;
          position: relative;
        }

        .metric-card h3 {
          font-size: 0.875rem;
          color: var(--text-secondary);
          margin-bottom: var(--spacing-sm);
        }

        .metric-value {
          font-size: 2.5rem;
          font-weight: 700;
          color: var(--text-primary);
        }

        .metric-target {
          font-size: 0.75rem;
          color: var(--text-muted);
          margin-top: var(--spacing-xs);
        }

        .metric-change {
          font-size: 0.75rem;
          margin-top: var(--spacing-xs);
        }

        .metric-change.positive {
          color: var(--color-success);
        }

        .metric-change.negative {
          color: var(--color-danger);
        }

        .status-banner {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          padding: var(--spacing-md);
          border-radius: var(--border-radius);
          margin-bottom: var(--spacing-lg);
        }

        .status-banner.success {
          background: rgba(16, 185, 129, 0.1);
          border: 1px solid var(--color-success);
        }

        .status-banner.warning {
          background: rgba(245, 158, 11, 0.1);
          border: 1px solid var(--color-warning);
        }

        .status-icon {
          font-size: 1.25rem;
        }

        .section {
          margin-bottom: var(--spacing-lg);
        }

        .section h2 {
          margin-bottom: var(--spacing-md);
        }

        .section-description {
          color: var(--text-muted);
          margin-bottom: var(--spacing-md);
        }

        .validation-form {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
        }

        .form-row {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
        }

        .form-group {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-xs);
        }

        .checkbox-label {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          font-weight: normal;
          cursor: pointer;
        }

        .checkbox-label input {
          width: auto;
        }

        .error-text {
          color: var(--color-danger);
          font-size: 0.875rem;
        }

        .health-checks-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
          gap: var(--spacing-sm);
        }

        .health-check-card {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          padding: var(--spacing-sm) var(--spacing-md);
          background: var(--bg-secondary);
          border-radius: var(--border-radius);
          border-left: 3px solid;
        }

        .health-check-card.pass {
          border-color: var(--color-success);
        }

        .health-check-card.fail {
          border-color: var(--color-danger);
        }

        .health-check-card.warning {
          border-color: var(--color-warning);
        }

        .check-icon {
          font-size: 1.25rem;
        }

        .check-content {
          display: flex;
          flex-direction: column;
        }

        .check-name {
          font-weight: 500;
          font-size: 0.875rem;
        }

        .check-message {
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .metrics-table {
          width: 100%;
          border-collapse: collapse;
        }

        .metrics-table th,
        .metrics-table td {
          padding: var(--spacing-sm);
          text-align: left;
          border-bottom: 1px solid var(--border-color);
        }

        .metrics-table th {
          font-weight: 600;
          font-size: 0.75rem;
          text-transform: uppercase;
          color: var(--text-muted);
        }

        .failures-list {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
        }

        .failure-item {
          padding: var(--spacing-sm);
          background: var(--bg-secondary);
          border-radius: var(--border-radius);
          border-left: 3px solid;
        }

        .failure-item.false_positive {
          border-color: var(--color-warning);
        }

        .failure-item.false_negative {
          border-color: var(--color-danger);
        }

        .failure-header {
          display: flex;
          justify-content: space-between;
          margin-bottom: var(--spacing-xs);
        }

        .failure-type {
          font-size: 0.75rem;
          font-weight: 500;
          text-transform: uppercase;
        }

        .failure-score {
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .failure-name {
          font-size: 0.875rem;
        }

        .failure-signals {
          font-size: 0.75rem;
          color: var(--text-muted);
          margin-top: var(--spacing-xs);
        }

        .empty-state {
          text-align: center;
          padding: var(--spacing-xl);
        }
      `}</style>
    </div>
  );
}
