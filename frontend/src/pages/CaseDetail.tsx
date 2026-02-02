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
  startCase,
  pauseCase,
  resumeCase,
  type CaseResponse,
  type CaseReport,
} from '../services/api';
import { CaseReportComponent } from '../components/cases/CaseReport';

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

export default function CaseDetail() {
  const { id } = useParams<{ id: string }>();
  const queryClient = useQueryClient();

  const { data: caseData, isLoading: caseLoading } = useQuery({
    queryKey: ['case', id],
    queryFn: () => getCase(id!),
    refetchInterval: (query) => {
      const data = query.state.data;
      if (data?.status === 'processing') return 5000;
      return false;
    },
    enabled: !!id,
  });

  const { data: reportData, isLoading: reportLoading } = useQuery({
    queryKey: ['case-report', id],
    queryFn: () => getCaseReport(id!, 'json') as Promise<CaseReport>,
    enabled: !!id && caseData?.status === 'completed',
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
          {caseData.status === 'initializing' && (
            <button
              className="btn btn-primary"
              onClick={() => startMutation.mutate()}
              disabled={startMutation.isPending}
            >
              Start Processing
            </button>
          )}
          {caseData.status === 'processing' && (
            <button
              className="btn btn-warning"
              onClick={() => pauseMutation.mutate()}
              disabled={pauseMutation.isPending}
            >
              Pause
            </button>
          )}
          {caseData.status === 'paused' && (
            <button
              className="btn btn-primary"
              onClick={() => resumeMutation.mutate()}
              disabled={resumeMutation.isPending}
            >
              Resume
            </button>
          )}
          {caseData.stats.pending_matches > 0 && (
            <Link to={`/cases/${id}/review`} className="btn btn-secondary">
              Review Matches ({caseData.stats.pending_matches})
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
            <span className="value">{new Date(caseData.created_at).toLocaleString()}</span>
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
          <div className="stat-value">{caseData.stats.entity_count}</div>
          <div className="stat-label">Entities</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{caseData.stats.relationship_count}</div>
          <div className="stat-label">Relationships</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{caseData.stats.leads_processed}</div>
          <div className="stat-label">Leads Processed</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{caseData.stats.leads_pending}</div>
          <div className="stat-label">Leads Pending</div>
        </div>
      </div>

      {/* Report */}
      {caseData.status === 'completed' && (
        <div className="card">
          <h2>Case Report</h2>
          {reportLoading ? (
            <div className="loading">
              <div className="spinner" />
              <span>Loading report...</span>
            </div>
          ) : reportData ? (
            <CaseReportComponent report={reportData} />
          ) : (
            <p>Report not available</p>
          )}
        </div>
      )}

      {/* Processing indicator */}
      {caseData.status === 'processing' && (
        <div className="card processing-card">
          <div className="processing-indicator">
            <div className="spinner" />
            <div>
              <h3>Processing...</h3>
              <p>
                {caseData.stats.leads_processed} leads processed,{' '}
                {caseData.stats.leads_pending} remaining
              </p>
            </div>
          </div>
        </div>
      )}

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
