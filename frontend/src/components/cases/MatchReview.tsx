/**
 * Match review component
 *
 * Displays entity match details for review.
 */

import { type EntityMatchResponse } from '@/api';

interface MatchReviewProps {
  match: EntityMatchResponse;
}

export function MatchReview({ match }: MatchReviewProps) {
  const sourceEntity = match.source_entity;
  const targetEntity = match.target_entity;
  const signals = match.match_signals;
  const sourceIdentifiers = sourceEntity?.identifiers ?? {};
  const targetIdentifiers = targetEntity?.identifiers ?? {};

  return (
    <div className="match-review">
      {/* Entities */}
      <div className="entities-comparison">
        <div className="entity-card source">
          <h4>Source Entity</h4>
          <div className="entity-name">{sourceEntity?.name ?? 'Unknown'}</div>
          <div className="entity-meta">
            <span className="badge">{sourceEntity?.entity_type ?? 'Unknown'}</span>
            {sourceEntity?.jurisdiction && (
              <span className="badge">{sourceEntity.jurisdiction}</span>
            )}
          </div>
          {Object.keys(sourceIdentifiers).length > 0 && (
            <div className="identifiers">
              {Object.entries(sourceIdentifiers).map(([key, value]) => (
                <div key={key} className="identifier">
                  <span className="id-key">{key}:</span>
                  <span className="id-value">{value}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="match-arrow">
          <span className="confidence-score">
            {((match.confidence ?? 0) * 100).toFixed(0)}%
          </span>
          <span className="arrow">⟷</span>
        </div>

        <div className="entity-card target">
          <h4>Target Entity</h4>
          <div className="entity-name">{targetEntity?.name ?? 'Unknown'}</div>
          <div className="entity-meta">
            <span className="badge">{targetEntity?.entity_type ?? 'Unknown'}</span>
            {targetEntity?.jurisdiction && (
              <span className="badge">{targetEntity.jurisdiction}</span>
            )}
          </div>
          {Object.keys(targetIdentifiers).length > 0 && (
            <div className="identifiers">
              {Object.entries(targetIdentifiers).map(([key, value]) => (
                <div key={key} className="identifier">
                  <span className="id-key">{key}:</span>
                  <span className="id-value">{value}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Match Signals */}
      {signals && (
      <div className="match-signals">
        <h4>Match Signals</h4>
        <div className="signals-grid">
          {signals.name_similarity != null && (
            <div className="signal">
              <span className="signal-label">Name Similarity</span>
              <span className="signal-value">
                {((signals.name_similarity ?? 0) * 100).toFixed(0)}%
              </span>
            </div>
          )}
          {signals.identifier_match && (
            <div className="signal">
              <span className="signal-label">Identifier Match</span>
              <span className="signal-value">
                {signals.identifier_match.type ?? 'Unknown'}:{' '}
                {signals.identifier_match.matched ? '✓' : '✗'}
              </span>
            </div>
          )}
          <div className="signal">
            <span className="signal-label">Jurisdiction Match</span>
            <span className="signal-value">
              {signals.jurisdiction_match ? '✓ Yes' : '✗ No'}
            </span>
          </div>
          {signals.address_overlap && (
            <div className="signal">
              <span className="signal-label">Address Overlap</span>
              <span className="signal-value">
                City: {signals.address_overlap.city ? '✓' : '✗'},{' '}
                Postal: {signals.address_overlap.postal_fsa ? '✓' : '✗'}
              </span>
            </div>
          )}
          {(signals.shared_directors?.length ?? 0) > 0 && (
            <div className="signal">
              <span className="signal-label">Shared Directors</span>
              <span className="signal-value">
                {(signals.shared_directors ?? []).join(', ')}
              </span>
            </div>
          )}
        </div>
      </div>
      )}

      <style>{`
        .match-review {
          margin-top: var(--spacing-md);
        }

        .entities-comparison {
          display: flex;
          align-items: stretch;
          gap: var(--spacing-md);
          margin-bottom: var(--spacing-lg);
        }

        .entity-card {
          flex: 1;
          padding: var(--spacing-md);
          border: 1px solid var(--border-color);
          border-radius: var(--radius);
          background: var(--bg-secondary);
        }

        .entity-card h4 {
          font-size: 0.875rem;
          color: var(--text-secondary);
          margin-bottom: var(--spacing-sm);
        }

        .entity-name {
          font-size: 1.25rem;
          font-weight: 600;
          margin-bottom: var(--spacing-sm);
        }

        .entity-meta {
          display: flex;
          gap: var(--spacing-xs);
          flex-wrap: wrap;
          margin-bottom: var(--spacing-sm);
        }

        .badge {
          padding: 2px 8px;
          background: var(--bg-primary);
          border-radius: 12px;
          font-size: 0.75rem;
          color: var(--text-secondary);
        }

        .identifiers {
          margin-top: var(--spacing-sm);
          padding-top: var(--spacing-sm);
          border-top: 1px dashed var(--border-color);
        }

        .identifier {
          display: flex;
          gap: var(--spacing-xs);
          font-size: 0.875rem;
        }

        .id-key {
          color: var(--text-secondary);
        }

        .match-arrow {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          min-width: 80px;
        }

        .confidence-score {
          font-size: 1.5rem;
          font-weight: 700;
          color: var(--primary);
        }

        .arrow {
          font-size: 1.5rem;
          color: var(--text-secondary);
        }

        .match-signals h4 {
          margin-bottom: var(--spacing-md);
        }

        .signals-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
        }

        .signal {
          display: flex;
          flex-direction: column;
        }

        .signal-label {
          font-size: 0.875rem;
          color: var(--text-secondary);
        }

        .signal-value {
          font-weight: 500;
        }
      `}</style>
    </div>
  );
}
