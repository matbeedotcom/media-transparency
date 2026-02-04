/**
 * Case detail page for MITDS
 *
 * Shows case status, progress, and report.
 */

import { useParams, Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getCase,
  getCaseReport,
  getCaseProcessing,
  startCase,
  pauseCase,
  resumeCase,
  type CaseReportResponse,
} from '@/api';
import { CaseReportComponent } from '../components/cases/CaseReport';

// Type definitions for incomplete OpenAPI schema types
interface CaseStats {
  entity_count?: number;
  relationship_count?: number;
  leads_processed?: number;
  leads_pending?: number;
  pending_matches?: number;
}

interface ProcessingDetails {
  current_phase?: string;
  progress_percent?: number;
  leads_total?: number;
  leads_completed?: number;
  leads_failed?: number;
  elapsed_seconds?: number;
  recent_entities?: string[];
  recent_leads?: string[];
}

const statusColors: Record<string, string> = {
  initializing: 'text-muted',
  processing: 'text-primary',
  paused: 'text-warning',
  completed: 'text-success',
  failed: 'text-danger',
};

const statusIcons: Record<string, string> = {
  initializing: '🔄',
  processing: '⚙️',
  paused: '⏸️',
  completed: '✅',
  failed: '❌',
};

function formatElapsedTime(seconds: number): string {
  if (seconds < 60) {
    return `${Math.floor(seconds)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = Math.floor(seconds % 60);
  if (minutes < 60) {
    return `${minutes}m ${remainingSeconds}s`;
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `${hours}h ${remainingMinutes}m`;
}

export default function CaseDetail() {
  const { id } = useParams<{ id: string }>();
  const queryClient = useQueryClient();

  const { data: caseData, isLoading: caseLoading } = useQuery({
    queryKey: ['case', id],
    queryFn: () => getCase(id!),
    refetchInterval: (query) => {
      const data = query.state.data;
      if (data?.status === 'PROCESSING') return 5000;
      return false;
    },
    enabled: !!id,
  });

  const { data: reportData, isLoading: reportLoading } = useQuery({
    queryKey: ['case-report', id],
    queryFn: ({ signal }) => getCaseReport(id!, { format: 'json' }, signal),
    enabled: !!id && caseData?.status === 'COMPLETED',
  });

  const { data: processingDetails } = useQuery({
    queryKey: ['case-processing', id],
    queryFn: () => getCaseProcessing(id!),
    refetchInterval: caseData?.status === 'PROCESSING' ? 2000 : false,
    enabled: !!id && caseData?.status === 'PROCESSING',
  });

  const startMutation = useMutation({
    mutationFn: () => startCase(id!),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['case', id] }),
  });

  const pauseMutation = useMutation({
    mutationFn: () => pauseCase(id!),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['case', id] }),
  });

  const resumeMutation = useMutation({
    mutationFn: () => resumeCase(id!),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ['case', id] }),
  });

  if (caseLoading) {
    return (
      <div className="loading">
        <div className="spinner" />
        <span>Loading case...</span>
      </div>
    );
  }

  if (!caseData) {
    return (
      <div className="error-message">
        <h1>Case Not Found</h1>
        <p>The case you're looking for doesn't exist.</p>
        <Link to="/cases" className="btn btn-primary">
          Back to Cases
        </Link>
      </div>
    );
  }

  return (
    <div className="case-detail">
      <header className="page-header">
        <div className="header-content">
          <h1>{caseData.name}</h1>
          <span className={`status-badge ${statusColors[caseData.status]}`}>
            {statusIcons[caseData.status]} {caseData.status}
          </span>
        </div>
        <div className="header-actions">
          {caseData.status === 'INITIALIZING' && (
            <button
              className="btn btn-primary"
              onClick={() => startMutation.mutate()}
              disabled={startMutation.isPending}
            >
              Start Processing
            </button>
          )}
          {caseData.status === 'PROCESSING' && (
            <button
              className="btn btn-warning"
              onClick={() => pauseMutation.mutate()}
              disabled={pauseMutation.isPending}
            >
              Pause
            </button>
          )}
          {caseData.status === 'PAUSED' && (
            <button
              className="btn btn-primary"
              onClick={() => resumeMutation.mutate()}
              disabled={resumeMutation.isPending}
            >
              Resume
            </button>
          )}
          {((caseData.stats as CaseStats | undefined)?.pending_matches ?? 0) > 0 && (
            <Link to={`/cases/${id}/review`} className="btn btn-secondary">
              Review Matches ({(caseData.stats as CaseStats | undefined)?.pending_matches ?? 0})
            </Link>
          )}
        </div>
      </header>

      {/* Entry Point Info */}
      <div className="card">
        <h2>Entry Point</h2>
        <div className="entry-point-info">
          <div className="info-row">
            <span className="label">Type:</span>
            <span className="value">{caseData.entry_point_type}</span>
          </div>
          <div className="info-row">
            <span className="label">Value:</span>
            <span className="value">{caseData.entry_point_value}</span>
          </div>
          <div className="info-row">
            <span className="label">Created:</span>
            <span className="value">{caseData.created_at ? new Date(caseData.created_at).toLocaleString() : 'N/A'}</span>
          </div>
          {caseData.completed_at && (
            <div className="info-row">
              <span className="label">Completed:</span>
              <span className="value">{new Date(caseData.completed_at).toLocaleString()}</span>
            </div>
          )}
        </div>
      </div>

      {/* Stats */}
      <div className="stats-grid">
        <div className="card stat-card">
          <div className="stat-value">{(caseData.stats as CaseStats | undefined)?.entity_count ?? 0}</div>
          <div className="stat-label">Entities</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{(caseData.stats as CaseStats | undefined)?.relationship_count ?? 0}</div>
          <div className="stat-label">Relationships</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{(caseData.stats as CaseStats | undefined)?.leads_processed ?? 0}</div>
          <div className="stat-label">Leads Processed</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{(caseData.stats as CaseStats | undefined)?.leads_pending ?? 0}</div>
          <div className="stat-label">Leads Pending</div>
        </div>
      </div>

      {/* Report */}
      {caseData.status === 'COMPLETED' && (
        <div className="card">
          <h2>Case Report</h2>
          {reportLoading ? (
            <div className="loading">
              <div className="spinner" />
              <span>Loading report...</span>
            </div>
          ) : reportData && typeof reportData === 'object' ? (
            <CaseReportComponent report={reportData as CaseReportResponse} />
          ) : (
            <p>Report not available</p>
          )}
        </div>
      )}

      {/* Processing indicator */}
      {caseData.status === 'PROCESSING' && (() => {
        const pd = processingDetails as ProcessingDetails | undefined;
        return (
        <div className="card processing-card">
          <div className="processing-header">
            <div className="processing-indicator">
              <div className="spinner" />
              <div>
                <h3>Processing...</h3>
                <p className="phase-text">
                  {pd?.current_phase === 'processing_leads' && 'Processing leads'}
                  {pd?.current_phase === 'initializing' && 'Initializing research session'}
                  {pd?.current_phase === 'finalizing' && 'Finalizing results'}
                  {!pd && 'Starting...'}
                </p>
              </div>
            </div>
            {pd && (pd.progress_percent ?? 0) > 0 && (
              <div className="progress-badge">
                {(pd.progress_percent ?? 0).toFixed(0)}%
              </div>
            )}
          </div>
          
          {/* Progress bar */}
          {pd && (pd.leads_total ?? 0) > 0 && (
            <div className="progress-section">
              <div className="progress-bar-container">
                <div 
                  className="progress-bar-fill" 
                  style={{ width: `${pd.progress_percent ?? 0}%` }}
                />
              </div>
              <div className="progress-stats">
                <span>{pd.leads_completed ?? 0} / {pd.leads_total ?? 0} leads</span>
                {(pd.leads_failed ?? 0) > 0 && (
                  <span className="failed-count">{pd.leads_failed} failed</span>
                )}
                {(pd.elapsed_seconds ?? 0) > 0 && (
                  <span className="elapsed-time">
                    {formatElapsedTime(pd.elapsed_seconds ?? 0)}
                  </span>
                )}
              </div>
            </div>
          )}
          
          {/* Recent activity */}
          {pd && ((pd.recent_entities?.length ?? 0) > 0 || (pd.recent_leads?.length ?? 0) > 0) && (
            <div className="recent-activity">
              {(pd.recent_entities?.length ?? 0) > 0 && (
                <div className="activity-section">
                  <h4>Recently Discovered</h4>
                  <ul className="activity-list">
                    {(pd.recent_entities ?? []).map((name, idx) => (
                      <li key={idx} className="entity-item">{name}</li>
                    ))}
                  </ul>
                </div>
              )}
              {(pd.recent_leads?.length ?? 0) > 0 && (
                <div className="activity-section">
                  <h4>Currently Processing</h4>
                  <ul className="activity-list">
                    {(pd.recent_leads ?? []).map((lead, idx) => (
                      <li key={idx} className="lead-item">{lead}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}
        </div>
        );
      })()}

      <style>{`
        .page-header {
          display: flex;
          justify-content: space-between;
          align-items: flex-start;
          margin-bottom: var(--spacing-lg);
          flex-wrap: wrap;
          gap: var(--spacing-md);
        }

        .header-content {
          display: flex;
          align-items: center;
          gap: var(--spacing-md);
        }

        .status-badge {
          padding: var(--spacing-xs) var(--spacing-sm);
          border-radius: var(--radius);
          font-size: 0.875rem;
          font-weight: 500;
        }

        .header-actions {
          display: flex;
          gap: var(--spacing-sm);
        }

        .card {
          margin-bottom: var(--spacing-md);
        }

        .card h2 {
          margin-bottom: var(--spacing-md);
          font-size: 1.25rem;
        }

        .entry-point-info {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
        }

        .info-row {
          display: flex;
          gap: var(--spacing-md);
        }

        .info-row .label {
          font-weight: 500;
          min-width: 100px;
        }

        .info-row .value {
          color: var(--text-secondary);
        }

        .stats-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: var(--spacing-md);
          margin-bottom: var(--spacing-lg);
        }

        .stat-card {
          text-align: center;
          padding: var(--spacing-lg);
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

        .processing-card {
          background: var(--bg-secondary);
        }

        .processing-header {
          display: flex;
          justify-content: space-between;
          align-items: flex-start;
          margin-bottom: var(--spacing-md);
        }

        .processing-indicator {
          display: flex;
          align-items: center;
          gap: var(--spacing-md);
        }

        .processing-indicator h3 {
          margin-bottom: var(--spacing-xs);
        }

        .processing-indicator p {
          color: var(--text-secondary);
          margin: 0;
        }

        .phase-text {
          font-size: 0.875rem;
        }

        .progress-badge {
          font-size: 1.5rem;
          font-weight: 700;
          color: var(--primary);
        }

        .progress-section {
          margin-bottom: var(--spacing-md);
        }

        .progress-bar-container {
          width: 100%;
          height: 8px;
          background: var(--border-color);
          border-radius: 4px;
          overflow: hidden;
          margin-bottom: var(--spacing-sm);
        }

        .progress-bar-fill {
          height: 100%;
          background: var(--primary);
          border-radius: 4px;
          transition: width 0.3s ease;
        }

        .progress-stats {
          display: flex;
          justify-content: space-between;
          font-size: 0.875rem;
          color: var(--text-secondary);
        }

        .failed-count {
          color: #ef4444;
        }

        .elapsed-time {
          font-variant-numeric: tabular-nums;
        }

        .recent-activity {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
          padding-top: var(--spacing-md);
          border-top: 1px solid var(--border-color);
        }

        .activity-section h4 {
          font-size: 0.875rem;
          font-weight: 600;
          margin-bottom: var(--spacing-sm);
          color: var(--text-secondary);
        }

        .activity-list {
          list-style: none;
          padding: 0;
          margin: 0;
        }

        .activity-list li {
          padding: var(--spacing-xs) 0;
          font-size: 0.875rem;
          border-bottom: 1px solid var(--border-color);
        }

        .activity-list li:last-child {
          border-bottom: none;
        }

        .entity-item {
          color: var(--text-primary);
        }

        .lead-item {
          color: var(--text-secondary);
          font-family: monospace;
          font-size: 0.8rem;
        }

        .loading {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: var(--spacing-md);
          padding: var(--spacing-xl);
        }

        .error-message {
          text-align: center;
          padding: var(--spacing-xl);
        }

        .text-muted { color: var(--text-secondary); }
        .text-primary { color: var(--primary); }
        .text-warning { color: #f59e0b; }
        .text-success { color: #10b981; }
        .text-danger { color: #ef4444; }
      `}</style>
    </div>
  );
}
