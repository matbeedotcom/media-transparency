/**
 * Case report display component
 *
 * Shows ranked entities, relationships, and cross-border flags.
 */

import { type CaseReportResponse } from '@/api';

// Type alias for backward compatibility
type CaseReport = CaseReportResponse;

interface CaseReportComponentProps {
  report: CaseReport;
}

export function CaseReportComponent({ report }: CaseReportComponentProps) {
  const summary = report.summary;
  const crossBorderFlags = report.cross_border_flags ?? [];
  const topEntities = report.top_entities ?? [];
  const topRelationships = report.top_relationships ?? [];
  const unknowns = report.unknowns ?? [];

  return (
    <div className="case-report">
      {/* Summary */}
      {summary && (
      <div className="report-section">
        <h3>Summary</h3>
        <div className="summary-grid">
          <div className="summary-item">
            <span className="label">Entry Point</span>
            <span className="value">{summary.entry_point ?? 'N/A'}</span>
          </div>
          <div className="summary-item">
            <span className="label">Entities</span>
            <span className="value">{summary.entity_count ?? 0}</span>
          </div>
          <div className="summary-item">
            <span className="label">Relationships</span>
            <span className="value">{summary.relationship_count ?? 0}</span>
          </div>
          <div className="summary-item">
            <span className="label">Cross-Border</span>
            <span className="value">{summary.cross_border_count ?? 0}</span>
          </div>
          <div className="summary-item">
            <span className="label">Processing Time</span>
            <span className="value">{(summary.processing_time_seconds ?? 0).toFixed(1)}s</span>
          </div>
        </div>
        {summary.has_unresolved_matches && (
          <div className="warning-badge">
            ⚠️ Has unresolved entity matches requiring review
          </div>
        )}
      </div>
      )}

      {/* Cross-Border Flags */}
      {crossBorderFlags.length > 0 && (
        <div className="report-section">
          <h3>⚠️ Cross-Border Connections</h3>
          <table>
            <thead>
              <tr>
                <th>US Entity</th>
                <th>CA Entity</th>
                <th>Relationship</th>
                <th>Amount</th>
              </tr>
            </thead>
            <tbody>
              {crossBorderFlags.map((flag, idx) => (
                <tr key={idx}>
                  <td>{flag.us_entity_name ?? 'Unknown'}</td>
                  <td>{flag.ca_entity_name ?? 'Unknown'}</td>
                  <td>{flag.relationship_type ?? 'Unknown'}</td>
                  <td>{flag.amount ? `$${flag.amount.toLocaleString()}` : 'N/A'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Top Entities */}
      {topEntities.length > 0 && (
        <div className="report-section">
          <h3>Top Entities</h3>
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>Name</th>
                <th>Type</th>
                <th>Jurisdiction</th>
                <th>Relevance</th>
              </tr>
            </thead>
            <tbody>
              {topEntities.slice(0, 10).map((entity, idx) => (
                <tr key={entity.entity_id ?? idx}>
                  <td>{idx + 1}</td>
                  <td>{entity.name ?? 'Unknown'}</td>
                  <td>{entity.entity_type ?? 'Unknown'}</td>
                  <td>{entity.jurisdiction ?? 'Unknown'}</td>
                  <td>{(entity.relevance_score ?? 0).toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Top Relationships */}
      {topRelationships.length > 0 && (
        <div className="report-section">
          <h3>Key Relationships</h3>
          <table>
            <thead>
              <tr>
                <th>Source</th>
                <th>Target</th>
                <th>Type</th>
                <th>Amount</th>
                <th>Significance</th>
              </tr>
            </thead>
            <tbody>
              {topRelationships.slice(0, 10).map((rel, idx) => (
                <tr key={idx}>
                  <td>{rel.source_name ?? 'Unknown'}</td>
                  <td>{rel.target_name ?? 'Unknown'}</td>
                  <td>{rel.relationship_type ?? 'Unknown'}</td>
                  <td>{rel.amount ? `$${rel.amount.toLocaleString()}` : 'N/A'}</td>
                  <td>{(rel.significance_score ?? 0).toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Unknowns */}
      {unknowns.length > 0 && (
        <div className="report-section">
          <h3>Unknowns</h3>
          <p className="section-description">
            The following entities could not be fully traced:
          </p>
          <ul className="unknowns-list">
            {unknowns.map((unknown, idx) => (
              <li key={idx}>
                <strong>{unknown.entity_name ?? 'Unknown'}</strong>: {unknown.reason ?? 'Unknown reason'}
                {(unknown.attempted_sources?.length ?? 0) > 0 && (
                  <span className="attempted-sources">
                    (Attempted: {(unknown.attempted_sources ?? []).join(', ')})
                  </span>
                )}
              </li>
            ))}
          </ul>
        </div>
      )}

      <style>{`
        .case-report {
          margin-top: var(--spacing-md);
        }

        .report-section {
          margin-bottom: var(--spacing-lg);
        }

        .report-section h3 {
          margin-bottom: var(--spacing-md);
          font-size: 1.1rem;
        }

        .summary-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
          gap: var(--spacing-md);
          margin-bottom: var(--spacing-md);
        }

        .summary-item {
          display: flex;
          flex-direction: column;
        }

        .summary-item .label {
          font-size: 0.875rem;
          color: var(--text-secondary);
        }

        .summary-item .value {
          font-size: 1.25rem;
          font-weight: 600;
        }

        .warning-badge {
          padding: var(--spacing-sm);
          background: #fef3c7;
          border: 1px solid #fcd34d;
          border-radius: var(--radius);
          color: #92400e;
        }

        table {
          width: 100%;
          border-collapse: collapse;
        }

        th, td {
          padding: var(--spacing-sm);
          text-align: left;
          border-bottom: 1px solid var(--border-color);
        }

        th {
          font-weight: 600;
          background: var(--bg-secondary);
        }

        .section-description {
          color: var(--text-secondary);
          margin-bottom: var(--spacing-md);
        }

        .unknowns-list {
          list-style: disc;
          padding-left: var(--spacing-lg);
        }

        .unknowns-list li {
          margin-bottom: var(--spacing-sm);
        }

        .attempted-sources {
          color: var(--text-secondary);
          font-size: 0.875rem;
          margin-left: var(--spacing-xs);
        }
      `}</style>
    </div>
  );
}
