/**
 * EvidencePanel component for MITDS
 *
 * Displays evidence supporting an entity or relationship with source links.
 */

import { useQuery } from '@tanstack/react-query';
import { format } from 'date-fns';
import { getEntityEvidence, type Evidence } from '../../services/api';
import SourceLink from './SourceLink';

interface EvidencePanelProps {
  entityId: string;
  title?: string;
  maxItems?: number;
  showHeader?: boolean;
}

// Evidence type display configuration
const EVIDENCE_TYPE_CONFIG: Record<string, { label: string; icon: string; color: string }> = {
  IRS_990: {
    label: 'IRS Form 990',
    icon: '📋',
    color: '#3B82F6',
  },
  CRA_T3010: {
    label: 'CRA T3010',
    icon: '🍁',
    color: '#DC2626',
  },
  OPENCORPORATES: {
    label: 'OpenCorporates',
    icon: '🏢',
    color: '#8B5CF6',
  },
  MANUAL: {
    label: 'Manual Entry',
    icon: '✏️',
    color: '#6B7280',
  },
  WEB_ARCHIVE: {
    label: 'Web Archive',
    icon: '🌐',
    color: '#059669',
  },
  DEFAULT: {
    label: 'Unknown',
    icon: '📄',
    color: '#9CA3AF',
  },
};

export default function EvidencePanel({
  entityId,
  title = 'Evidence',
  maxItems = 10,
  showHeader = true,
}: EvidencePanelProps) {
  const {
    data: evidenceData,
    isLoading,
    error,
  } = useQuery({
    queryKey: ['entity-evidence', entityId],
    queryFn: () => getEntityEvidence(entityId),
    enabled: !!entityId,
  });

  const getTypeConfig = (type: string) => {
    return EVIDENCE_TYPE_CONFIG[type] || EVIDENCE_TYPE_CONFIG.DEFAULT;
  };

  const formatDate = (dateString: string): string => {
    try {
      return format(new Date(dateString), 'MMM d, yyyy HH:mm');
    } catch {
      return dateString;
    }
  };

  if (isLoading) {
    return (
      <div className="evidence-panel">
        {showHeader && <h3 className="panel-title">{title}</h3>}
        <div className="evidence-loading">
          <div className="spinner" />
          <span>Loading evidence...</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="evidence-panel">
        {showHeader && <h3 className="panel-title">{title}</h3>}
        <div className="evidence-error">
          <span>Failed to load evidence</span>
        </div>
      </div>
    );
  }

  const evidence = evidenceData?.evidence || [];
  const displayEvidence = evidence.slice(0, maxItems);
  const hasMore = evidence.length > maxItems;

  if (evidence.length === 0) {
    return (
      <div className="evidence-panel">
        {showHeader && <h3 className="panel-title">{title}</h3>}
        <div className="evidence-empty">
          <p>No evidence available for this entity.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="evidence-panel">
      {showHeader && (
        <h3 className="panel-title">
          {title}
          <span className="evidence-count">{evidence.length}</span>
        </h3>
      )}

      <ul className="evidence-list">
        {displayEvidence.map((item: Evidence) => {
          const config = getTypeConfig(item.evidence_type);
          return (
            <li key={item.id} className="evidence-item">
              <div className="evidence-header">
                <span
                  className="evidence-type-badge"
                  style={{ backgroundColor: config.color }}
                  title={config.label}
                >
                  <span className="type-icon">{config.icon}</span>
                  {config.label}
                </span>
                <span className="evidence-date" title={item.retrieved_at}>
                  {formatDate(item.retrieved_at)}
                </span>
              </div>
              <div className="evidence-source">
                <SourceLink
                  url={item.source_url}
                  archiveUrl={item.archive_url}
                />
              </div>
            </li>
          );
        })}
      </ul>

      {hasMore && (
        <div className="evidence-more">
          <button className="btn btn-text">
            View all {evidence.length} sources
          </button>
        </div>
      )}

      <style>{`
        .evidence-panel {
          background: var(--bg-primary);
          border-radius: var(--border-radius);
        }

        .panel-title {
          display: flex;
          align-items: center;
          gap: 8px;
          font-size: 1rem;
          margin-bottom: var(--spacing-md);
        }

        .evidence-count {
          font-size: 0.75rem;
          padding: 2px 8px;
          border-radius: 10px;
          background: var(--bg-tertiary);
          color: var(--text-muted);
          font-weight: normal;
        }

        .evidence-list {
          list-style: none;
          margin: 0;
          padding: 0;
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
        }

        .evidence-item {
          padding: var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          background: var(--bg-secondary);
        }

        .evidence-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: var(--spacing-xs);
        }

        .evidence-type-badge {
          display: inline-flex;
          align-items: center;
          gap: 4px;
          font-size: 0.625rem;
          padding: 2px 8px;
          border-radius: 4px;
          color: white;
          text-transform: uppercase;
          font-weight: 500;
        }

        .type-icon {
          font-size: 0.75rem;
        }

        .evidence-date {
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .evidence-source {
          margin-top: var(--spacing-xs);
        }

        .evidence-loading,
        .evidence-empty,
        .evidence-error {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: var(--spacing-sm);
          padding: var(--spacing-lg);
          color: var(--text-muted);
          text-align: center;
        }

        .evidence-error {
          color: var(--color-danger);
        }

        .evidence-more {
          margin-top: var(--spacing-sm);
          text-align: center;
        }

        .btn-text {
          background: none;
          border: none;
          color: var(--color-primary);
          cursor: pointer;
          font-size: 0.875rem;
          padding: var(--spacing-xs);
        }

        .btn-text:hover {
          text-decoration: underline;
        }
      `}</style>
    </div>
  );
}
