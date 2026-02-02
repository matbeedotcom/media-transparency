/**
 * Entity match review queue page
 *
 * Allows researchers to review and approve/reject entity matches.
 */

import { useParams, Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import {
  getCase,
  listCaseMatches,
  approveMatch,
  rejectMatch,
  deferMatch,
  type EntityMatchResponse,
} from '../services/api';
import { MatchReview } from '../components/cases/MatchReview';

export default function ReviewQueue() {
  const { id } = useParams<{ id: string }>();
  const queryClient = useQueryClient();
  const [selectedMatch, setSelectedMatch] = useState<EntityMatchResponse | null>(null);
  const [reviewNotes, setReviewNotes] = useState('');

  const { data: caseData } = useQuery({
    queryKey: ['case', id],
    queryFn: () => getCase(id!),
    enabled: !!id,
  });

  const { data: matchesData, isLoading } = useQuery({
    queryKey: ['case-matches', id],
    queryFn: () => listCaseMatches(id!),
    enabled: !!id,
  });

  const approveMutation = useMutation({
    mutationFn: (matchId: string) => approveMatch(matchId, reviewNotes),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['case-matches', id] });
      queryClient.invalidateQueries({ queryKey: ['case', id] });
      setSelectedMatch(null);
      setReviewNotes('');
    },
  });

  const rejectMutation = useMutation({
    mutationFn: (matchId: string) => rejectMatch(matchId, reviewNotes),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['case-matches', id] });
      queryClient.invalidateQueries({ queryKey: ['case', id] });
      setSelectedMatch(null);
      setReviewNotes('');
    },
  });

  const deferMutation = useMutation({
    mutationFn: (matchId: string) => deferMatch(matchId, reviewNotes),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['case-matches', id] });
      setSelectedMatch(null);
      setReviewNotes('');
    },
  });

  const handleApprove = () => {
    if (selectedMatch) {
      approveMutation.mutate(selectedMatch.id);
    }
  };

  const handleReject = () => {
    if (selectedMatch) {
      rejectMutation.mutate(selectedMatch.id);
    }
  };

  const handleDefer = () => {
    if (selectedMatch) {
      deferMutation.mutate(selectedMatch.id);
    }
  };

  const isActioning = approveMutation.isPending || rejectMutation.isPending || deferMutation.isPending;

  return (
    <div className="review-queue">
      <header className="page-header">
        <div>
          <h1>Review Entity Matches</h1>
          <p>
            Case: <Link to={`/cases/${id}`}>{caseData?.name || 'Loading...'}</Link>
          </p>
        </div>
        <Link to={`/cases/${id}`} className="btn btn-secondary">
          Back to Case
        </Link>
      </header>

      {isLoading ? (
        <div className="loading">
          <div className="spinner" />
          <span>Loading matches...</span>
        </div>
      ) : matchesData?.items.length === 0 ? (
        <div className="empty-state">
          <h2>No Pending Matches</h2>
          <p>All entity matches have been reviewed.</p>
          <Link to={`/cases/${id}`} className="btn btn-primary">
            Back to Case
          </Link>
        </div>
      ) : (
        <div className="review-layout">
          {/* Match List */}
          <div className="card match-list">
            <h2>Pending Matches ({matchesData?.pending_count})</h2>
            <div className="matches">
              {matchesData?.items.map((match) => (
                <button
                  key={match.id}
                  className={`match-item ${selectedMatch?.id === match.id ? 'selected' : ''}`}
                  onClick={() => setSelectedMatch(match)}
                >
                  <div className="match-summary">
                    <span className="source">{match.source_entity.name}</span>
                    <span className="arrow">→</span>
                    <span className="target">{match.target_entity.name}</span>
                  </div>
                  <div className="match-confidence">
                    {(match.confidence * 100).toFixed(0)}% confidence
                  </div>
                </button>
              ))}
            </div>
          </div>

          {/* Match Detail */}
          <div className="card match-detail">
            {selectedMatch ? (
              <>
                <h2>Match Details</h2>
                <MatchReview match={selectedMatch} />

                <div className="review-actions">
                  <div className="form-group">
                    <label htmlFor="notes">Review Notes (optional)</label>
                    <textarea
                      id="notes"
                      value={reviewNotes}
                      onChange={(e) => setReviewNotes(e.target.value)}
                      placeholder="Add notes about this decision..."
                      rows={3}
                    />
                  </div>

                  <div className="action-buttons">
                    <button
                      className="btn btn-success"
                      onClick={handleApprove}
                      disabled={isActioning}
                    >
                      ✓ Approve
                    </button>
                    <button
                      className="btn btn-danger"
                      onClick={handleReject}
                      disabled={isActioning}
                    >
                      ✗ Reject
                    </button>
                    <button
                      className="btn btn-secondary"
                      onClick={handleDefer}
                      disabled={isActioning}
                    >
                      ⏸ Defer
                    </button>
                  </div>
                </div>
              </>
            ) : (
              <div className="no-selection">
                <p>Select a match from the list to review</p>
              </div>
            )}
          </div>
        </div>
      )}

      <style>{`
        .page-header {
          display: flex;
          justify-content: space-between;
          align-items: flex-start;
          margin-bottom: var(--spacing-lg);
        }

        .page-header p {
          color: var(--text-secondary);
        }

        .review-layout {
          display: grid;
          grid-template-columns: 1fr 2fr;
          gap: var(--spacing-md);
        }

        @media (max-width: 768px) {
          .review-layout {
            grid-template-columns: 1fr;
          }
        }

        .match-list h2,
        .match-detail h2 {
          margin-bottom: var(--spacing-md);
          font-size: 1.1rem;
        }

        .matches {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
          max-height: 500px;
          overflow-y: auto;
        }

        .match-item {
          width: 100%;
          padding: var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--radius);
          background: var(--bg-primary);
          cursor: pointer;
          text-align: left;
          transition: all 0.2s;
        }

        .match-item:hover {
          border-color: var(--primary);
        }

        .match-item.selected {
          border-color: var(--primary);
          background: var(--bg-secondary);
        }

        .match-summary {
          display: flex;
          align-items: center;
          gap: var(--spacing-xs);
          font-weight: 500;
        }

        .match-summary .arrow {
          color: var(--text-secondary);
        }

        .match-confidence {
          font-size: 0.875rem;
          color: var(--text-secondary);
          margin-top: var(--spacing-xs);
        }

        .no-selection {
          display: flex;
          align-items: center;
          justify-content: center;
          min-height: 300px;
          color: var(--text-secondary);
        }

        .review-actions {
          margin-top: var(--spacing-lg);
          border-top: 1px solid var(--border-color);
          padding-top: var(--spacing-lg);
        }

        .form-group {
          margin-bottom: var(--spacing-md);
        }

        .form-group label {
          display: block;
          margin-bottom: var(--spacing-xs);
          font-weight: 500;
        }

        .form-group textarea {
          width: 100%;
          padding: var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--radius);
          font-family: inherit;
        }

        .action-buttons {
          display: flex;
          gap: var(--spacing-sm);
        }

        .btn-success {
          background: #10b981;
          color: white;
        }

        .btn-success:hover {
          background: #059669;
        }

        .btn-danger {
          background: #ef4444;
          color: white;
        }

        .btn-danger:hover {
          background: #dc2626;
        }

        .empty-state {
          text-align: center;
          padding: var(--spacing-xl);
        }

        .empty-state h2 {
          margin-bottom: var(--spacing-md);
        }

        .empty-state p {
          color: var(--text-secondary);
          margin-bottom: var(--spacing-md);
        }

        .loading {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: var(--spacing-md);
          padding: var(--spacing-xl);
        }
      `}</style>
    </div>
  );
}
