/**
 * Entity Resolution page for MITDS
 *
 * Review and manage entity resolution candidates:
 * - View resolution statistics
 * - Trigger resolution runs
 * - Review, merge, and reject candidate pairs
 */

import { useState, useCallback } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getResolutionCandidates,
  getResolutionStats,
  mergeCandidate,
  rejectCandidate,
  triggerResolution,
  type ResolutionCandidate,
  type ListCandidatesStatus,
  type EntityType,
} from '@/api';

export default function EntityResolution() {
  const queryClient = useQueryClient();

  // Filter state
  const [statusFilter, setStatusFilter] = useState<string>('pending');
  const [priorityFilter, setPriorityFilter] = useState<string>('');
  const [strategyFilter, setStrategyFilter] = useState<string>('');
  const [page, setPage] = useState(0);
  const pageSize = 20;

  // Trigger form state
  const [triggerEntityType, setTriggerEntityType] = useState<string>('');
  const [dryRun, setDryRun] = useState(false);

  // Expanded candidate for review
  const [expandedId, setExpandedId] = useState<string | null>(null);

  // Fetch stats
  const { data: stats } = useQuery({
    queryKey: ['resolution-stats'],
    queryFn: ({ signal }) => getResolutionStats(signal),
    refetchInterval: 30000,
  });

  // Fetch candidates
  const { data: candidatesData, isLoading: candidatesLoading } = useQuery({
    queryKey: ['resolution-candidates', statusFilter, priorityFilter, strategyFilter, page],
    queryFn: () =>
      getResolutionCandidates({
        status: (statusFilter || undefined) as ListCandidatesStatus | undefined,
        limit: pageSize,
        offset: page * pageSize,
      }),
  });

  // Mutations
  const mergeMutation = useMutation({
    mutationFn: (candidateId: string) => mergeCandidate(candidateId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['resolution-candidates'] });
      queryClient.invalidateQueries({ queryKey: ['resolution-stats'] });
      setExpandedId(null);
    },
  });

  const rejectMutation = useMutation({
    mutationFn: (candidateId: string) => rejectCandidate(candidateId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['resolution-candidates'] });
      queryClient.invalidateQueries({ queryKey: ['resolution-stats'] });
      setExpandedId(null);
    },
  });

  const triggerMutation = useMutation({
    mutationFn: () =>
      triggerResolution({
        entity_type: (triggerEntityType || undefined) as EntityType | undefined,
        dry_run: dryRun,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['resolution-candidates'] });
      queryClient.invalidateQueries({ queryKey: ['resolution-stats'] });
    },
  });

  const handleMerge = useCallback(
    (id: string) => {
      if (confirm('Merge these entities? This action cannot be undone.')) {
        mergeMutation.mutate(id);
      }
    },
    [mergeMutation]
  );

  const handleReject = useCallback(
    (id: string) => {
      rejectMutation.mutate(id);
    },
    [rejectMutation]
  );

  const handleTrigger = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault();
      triggerMutation.mutate();
    },
    [triggerMutation]
  );

  const candidates = candidatesData?.results || [];
  const totalCandidates = candidatesData?.total || 0;
  const totalPages = Math.ceil(totalCandidates / pageSize);

  const getConfidenceColor = (confidence: number): string => {
    if (confidence >= 0.9) return '#10B981';
    if (confidence >= 0.7) return '#3B82F6';
    if (confidence >= 0.5) return '#F59E0B';
    return '#DC2626';
  };

  const getStrategyLabel = (strategy: string): string => {
    switch (strategy) {
      case 'deterministic':
        return 'Exact Match';
      case 'fuzzy':
        return 'Fuzzy Match';
      case 'embedding':
        return 'Semantic Match';
      default:
        return strategy;
    }
  };

  return (
    <div className="entity-resolution">
      <header className="page-header">
        <h1>Entity Resolution</h1>
        <p>Review and manage duplicate entity candidates across data sources</p>
      </header>

      {/* Stats Overview */}
      <div className="stats-grid">
        <div className="card stat-card">
          <div className="stat-value" style={{ color: '#F59E0B' }}>
            {stats?.total_pending ?? '--'}
          </div>
          <div className="stat-label">Pending</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value" style={{ color: '#3B82F6' }}>
            {stats?.total_in_progress ?? '--'}
          </div>
          <div className="stat-label">In Progress</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value" style={{ color: '#10B981' }}>
            {stats?.total_approved ?? '--'}
          </div>
          <div className="stat-label">Approved</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value" style={{ color: '#DC2626' }}>
            {stats?.total_rejected ?? '--'}
          </div>
          <div className="stat-label">Rejected</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">{stats?.total_merged ?? '--'}</div>
          <div className="stat-label">Merged</div>
        </div>
        <div className="card stat-card">
          <div className="stat-value">
            {stats?.avg_confidence != null
              ? `${(stats.avg_confidence * 100).toFixed(0)}%`
              : '--'}
          </div>
          <div className="stat-label">Avg Confidence</div>
        </div>
      </div>

      {/* Strategy Breakdown */}
      {stats?.by_strategy && Object.keys(stats.by_strategy).length > 0 && (
        <div className="card strategy-breakdown">
          <h3>By Strategy</h3>
          <div className="strategy-chips">
            {Object.entries(stats.by_strategy).map(([strategy, count]) => (
              <span key={strategy} className="strategy-chip">
                {getStrategyLabel(strategy)}: {String(count ?? 0)}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Trigger Resolution */}
      <div className="card trigger-section">
        <h2>Run Resolution</h2>
        <p className="text-muted">
          Scan for duplicate entities across data sources using deterministic, fuzzy, and semantic matching.
        </p>
        <form className="trigger-form" onSubmit={handleTrigger}>
          <div className="form-row">
            <div className="form-group">
              <label>Entity Type</label>
              <select
                value={triggerEntityType}
                onChange={(e) => setTriggerEntityType(e.target.value)}
              >
                <option value="">All Types</option>
                <option value="Organization">Organization</option>
                <option value="Person">Person</option>
                <option value="Outlet">Outlet</option>
              </select>
            </div>
            <div className="form-group">
              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={dryRun}
                  onChange={(e) => setDryRun(e.target.checked)}
                />
                Dry run (find candidates without creating records)
              </label>
            </div>
          </div>
          <button
            type="submit"
            className="btn btn-primary"
            disabled={triggerMutation.isPending}
          >
            {triggerMutation.isPending ? 'Running...' : 'Run Resolution'}
          </button>
          {triggerMutation.isSuccess && triggerMutation.data && (
            <div className="trigger-result">
              Found {triggerMutation.data.candidates_found} candidate
              {triggerMutation.data.candidates_found !== 1 ? 's' : ''}
              {triggerMutation.data.dry_run ? ' (dry run)' : ''}
            </div>
          )}
          {triggerMutation.isError && (
            <div className="error-message">
              {(triggerMutation.error as Error).message}
            </div>
          )}
        </form>
      </div>

      {/* Candidate Queue */}
      <div className="candidates-section">
        <h2>Resolution Candidates</h2>

        {/* Filters */}
        <div className="filters-row">
          <div className="form-group">
            <label>Status</label>
            <select
              value={statusFilter}
              onChange={(e) => {
                setStatusFilter(e.target.value);
                setPage(0);
              }}
            >
              <option value="pending">Pending</option>
              <option value="in_progress">In Progress</option>
              <option value="">All</option>
            </select>
          </div>
          <div className="form-group">
            <label>Priority</label>
            <select
              value={priorityFilter}
              onChange={(e) => {
                setPriorityFilter(e.target.value);
                setPage(0);
              }}
            >
              <option value="">All</option>
              <option value="high">High</option>
              <option value="medium">Medium</option>
              <option value="low">Low</option>
            </select>
          </div>
          <div className="form-group">
            <label>Strategy</label>
            <select
              value={strategyFilter}
              onChange={(e) => {
                setStrategyFilter(e.target.value);
                setPage(0);
              }}
            >
              <option value="">All</option>
              <option value="deterministic">Exact Match</option>
              <option value="fuzzy">Fuzzy Match</option>
              <option value="embedding">Semantic Match</option>
            </select>
          </div>
          <div className="form-group">
            <span className="results-count">
              {totalCandidates} result{totalCandidates !== 1 ? 's' : ''}
            </span>
          </div>
        </div>

        {/* Candidate List */}
        {candidatesLoading ? (
          <div className="card">
            <div className="empty-state">Loading candidates...</div>
          </div>
        ) : candidates.length === 0 ? (
          <div className="card">
            <div className="empty-state">
              <p>No candidates found.</p>
              <p className="text-muted">
                Run a resolution scan or change your filters.
              </p>
            </div>
          </div>
        ) : (
          <div className="candidates-list">
            {candidates.map((candidate: ResolutionCandidate) => (
              <div
                key={candidate.id ?? 'unknown'}
                className={`card candidate-card ${expandedId === candidate.id ? 'expanded' : ''}`}
              >
                <div
                  className="candidate-summary"
                  onClick={() =>
                    setExpandedId(expandedId === candidate.id ? null : candidate.id ?? null)
                  }
                >
                  <div className="candidate-entities">
                    <div className="entity-side">
                      <span className="entity-type-badge">
                        {candidate.source_entity_type}
                      </span>
                      <span className="entity-name">
                        {candidate.source_entity_name}
                      </span>
                    </div>
                    <span className="merge-arrow">&#8596;</span>
                    <div className="entity-side">
                      <span className="entity-type-badge">
                        {candidate.candidate_entity_type}
                      </span>
                      <span className="entity-name">
                        {candidate.candidate_entity_name}
                      </span>
                    </div>
                  </div>
                  <div className="candidate-meta">
                    <span className="strategy-tag">
                      {getStrategyLabel(candidate.match_strategy ?? '')}
                    </span>
                    <span
                      className="confidence-badge"
                      style={{
                        color: getConfidenceColor(candidate.match_confidence ?? 0),
                      }}
                    >
                      {((candidate.match_confidence ?? 0) * 100).toFixed(0)}%
                    </span>
                    <span className={`priority-tag priority-${candidate.priority}`}>
                      {candidate.priority}
                    </span>
                  </div>
                </div>

                {/* Expanded Review Panel */}
                {expandedId === candidate.id && (
                  <div className="candidate-review">
                    <div className="review-comparison">
                      <div className="comparison-side">
                        <h4>Source Entity</h4>
                        <dl>
                          <dt>Name</dt>
                          <dd>{candidate.source_entity_name}</dd>
                          <dt>Type</dt>
                          <dd>{candidate.source_entity_type}</dd>
                          <dt>ID</dt>
                          <dd className="entity-id">{candidate.source_entity_id}</dd>
                        </dl>
                      </div>
                      <div className="comparison-side">
                        <h4>Candidate Entity</h4>
                        <dl>
                          <dt>Name</dt>
                          <dd>{candidate.candidate_entity_name}</dd>
                          <dt>Type</dt>
                          <dd>{candidate.candidate_entity_type}</dd>
                          <dt>ID</dt>
                          <dd className="entity-id">{candidate.candidate_entity_id}</dd>
                        </dl>
                      </div>
                    </div>

                    {/* Match Details */}
                    {candidate.match_details &&
                      Object.keys(candidate.match_details).length > 0 && (
                        <div className="match-details">
                          <h4>Match Details</h4>
                          <dl>
                            {Object.entries(candidate.match_details).map(
                              ([key, value]) => (
                                <div key={key} className="detail-row">
                                  <dt>{key.replace(/_/g, ' ')}</dt>
                                  <dd>
                                    {typeof value === 'number'
                                      ? value.toFixed(3)
                                      : String(value)}
                                  </dd>
                                </div>
                              )
                            )}
                          </dl>
                        </div>
                      )}

                    {/* Confidence Bar */}
                    <div className="confidence-bar-container">
                      <label>Confidence</label>
                      <div className="confidence-bar">
                        <div
                          className="confidence-fill"
                          style={{
                            width: `${(candidate.match_confidence ?? 0) * 100}%`,
                            backgroundColor: getConfidenceColor(
                              candidate.match_confidence ?? 0
                            ),
                          }}
                        />
                      </div>
                      <span>{((candidate.match_confidence ?? 0) * 100).toFixed(1)}%</span>
                    </div>

                    {/* Actions */}
                    <div className="review-actions">
                      <button
                        className="btn btn-merge"
                        onClick={() => handleMerge(candidate.id ?? '')}
                        disabled={
                          mergeMutation.isPending || rejectMutation.isPending
                        }
                      >
                        {mergeMutation.isPending ? 'Merging...' : 'Merge'}
                      </button>
                      <button
                        className="btn btn-reject"
                        onClick={() => handleReject(candidate.id ?? '')}
                        disabled={
                          mergeMutation.isPending || rejectMutation.isPending
                        }
                      >
                        {rejectMutation.isPending ? 'Rejecting...' : 'Reject'}
                      </button>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="pagination">
            <button
              className="btn btn-secondary"
              disabled={page === 0}
              onClick={() => setPage((p) => Math.max(0, p - 1))}
            >
              Previous
            </button>
            <span className="page-info">
              Page {page + 1} of {totalPages}
            </span>
            <button
              className="btn btn-secondary"
              disabled={page >= totalPages - 1}
              onClick={() => setPage((p) => p + 1)}
            >
              Next
            </button>
          </div>
        )}
      </div>

      <style>{`
        .entity-resolution {
          max-width: 1200px;
          margin: 0 auto;
        }

        .stats-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
          gap: var(--spacing-md);
          margin-bottom: var(--spacing-lg);
        }

        .stat-card {
          text-align: center;
          padding: var(--spacing-md);
        }

        .stat-card .stat-value {
          font-size: 2rem;
          font-weight: 700;
        }

        .stat-card .stat-label {
          font-size: 0.75rem;
          color: var(--text-muted);
          margin-top: var(--spacing-xs);
        }

        .strategy-breakdown {
          margin-bottom: var(--spacing-lg);
        }

        .strategy-breakdown h3 {
          margin-bottom: var(--spacing-sm);
          font-size: 0.875rem;
          color: var(--text-secondary);
        }

        .strategy-chips {
          display: flex;
          gap: var(--spacing-sm);
          flex-wrap: wrap;
        }

        .strategy-chip {
          padding: 4px 12px;
          background: var(--bg-tertiary);
          border-radius: 16px;
          font-size: 0.8125rem;
        }

        .trigger-section {
          margin-bottom: var(--spacing-lg);
        }

        .trigger-section h2 {
          margin-bottom: var(--spacing-xs);
        }

        .trigger-form {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
          margin-top: var(--spacing-md);
        }

        .form-row {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
          align-items: end;
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
          cursor: pointer;
        }

        .checkbox-label input {
          width: auto;
        }

        .trigger-result {
          padding: var(--spacing-sm);
          background: rgba(16, 185, 129, 0.1);
          border-radius: var(--border-radius);
          color: #10B981;
          font-weight: 500;
        }

        .error-message {
          color: #DC2626;
          padding: var(--spacing-sm);
          background: rgba(220, 38, 38, 0.1);
          border-radius: var(--border-radius);
        }

        .candidates-section h2 {
          margin-bottom: var(--spacing-md);
        }

        .filters-row {
          display: flex;
          gap: var(--spacing-md);
          align-items: end;
          margin-bottom: var(--spacing-md);
          flex-wrap: wrap;
        }

        .filters-row .form-group {
          min-width: 140px;
        }

        .results-count {
          font-size: 0.875rem;
          color: var(--text-muted);
          padding-bottom: 4px;
        }

        .candidates-list {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
        }

        .candidate-card {
          cursor: pointer;
          transition: border-color 0.2s ease;
        }

        .candidate-card:hover {
          border-color: var(--color-primary);
        }

        .candidate-card.expanded {
          border-color: var(--color-primary);
          cursor: default;
        }

        .candidate-summary {
          display: flex;
          justify-content: space-between;
          align-items: center;
          gap: var(--spacing-md);
          flex-wrap: wrap;
        }

        .candidate-entities {
          display: flex;
          align-items: center;
          gap: var(--spacing-md);
          flex: 1;
        }

        .entity-side {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
        }

        .entity-type-badge {
          font-size: 10px;
          padding: 2px 6px;
          border-radius: 4px;
          background: var(--bg-tertiary);
          color: var(--text-muted);
          text-transform: uppercase;
        }

        .entity-name {
          font-weight: 500;
        }

        .merge-arrow {
          font-size: 1.25rem;
          color: var(--text-muted);
        }

        .candidate-meta {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
        }

        .strategy-tag {
          font-size: 0.75rem;
          padding: 2px 8px;
          background: var(--bg-tertiary);
          border-radius: 4px;
        }

        .confidence-badge {
          font-weight: 700;
          font-size: 0.875rem;
        }

        .priority-tag {
          font-size: 0.625rem;
          padding: 2px 6px;
          border-radius: 4px;
          text-transform: uppercase;
          font-weight: 600;
        }

        .priority-high {
          background: rgba(220, 38, 38, 0.15);
          color: #DC2626;
        }

        .priority-medium {
          background: rgba(245, 158, 11, 0.15);
          color: #D97706;
        }

        .priority-low {
          background: rgba(107, 114, 128, 0.15);
          color: #6B7280;
        }

        .candidate-review {
          border-top: 1px solid var(--border-color);
          margin-top: var(--spacing-md);
          padding-top: var(--spacing-md);
        }

        .review-comparison {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: var(--spacing-lg);
          margin-bottom: var(--spacing-md);
        }

        .comparison-side {
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
        }

        .comparison-side h4 {
          margin-bottom: var(--spacing-sm);
          font-size: 0.875rem;
          color: var(--text-secondary);
        }

        .comparison-side dl {
          margin: 0;
          display: grid;
          grid-template-columns: auto 1fr;
          gap: var(--spacing-xs) var(--spacing-sm);
          font-size: 0.875rem;
        }

        .comparison-side dt {
          font-weight: 500;
          color: var(--text-muted);
        }

        .entity-id {
          font-family: monospace;
          font-size: 0.75rem;
          word-break: break-all;
        }

        .match-details {
          margin-bottom: var(--spacing-md);
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
        }

        .match-details h4 {
          margin-bottom: var(--spacing-sm);
          font-size: 0.875rem;
        }

        .match-details dl {
          margin: 0;
        }

        .detail-row {
          display: flex;
          justify-content: space-between;
          padding: var(--spacing-xs) 0;
          border-bottom: 1px solid var(--border-color);
          font-size: 0.8125rem;
        }

        .detail-row dt {
          text-transform: capitalize;
          color: var(--text-secondary);
        }

        .confidence-bar-container {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          margin-bottom: var(--spacing-md);
        }

        .confidence-bar-container label {
          font-size: 0.8125rem;
          color: var(--text-muted);
          min-width: 80px;
        }

        .confidence-bar {
          flex: 1;
          height: 8px;
          background: var(--bg-tertiary);
          border-radius: 4px;
          overflow: hidden;
        }

        .confidence-fill {
          height: 100%;
          border-radius: 4px;
          transition: width 0.3s ease;
        }

        .review-actions {
          display: flex;
          gap: var(--spacing-sm);
        }

        .btn-merge {
          background: #10B981;
          color: white;
          border: none;
          padding: var(--spacing-sm) var(--spacing-lg);
          border-radius: var(--border-radius);
          cursor: pointer;
          font-weight: 500;
        }

        .btn-merge:hover:not(:disabled) {
          background: #059669;
        }

        .btn-reject {
          background: #DC2626;
          color: white;
          border: none;
          padding: var(--spacing-sm) var(--spacing-lg);
          border-radius: var(--border-radius);
          cursor: pointer;
          font-weight: 500;
        }

        .btn-reject:hover:not(:disabled) {
          background: #B91C1C;
        }

        .btn-merge:disabled,
        .btn-reject:disabled {
          opacity: 0.5;
          cursor: not-allowed;
        }

        .pagination {
          display: flex;
          justify-content: center;
          align-items: center;
          gap: var(--spacing-md);
          margin-top: var(--spacing-lg);
        }

        .page-info {
          font-size: 0.875rem;
          color: var(--text-muted);
        }

        .empty-state {
          text-align: center;
          padding: var(--spacing-xl);
        }
      `}</style>
    </div>
  );
}
