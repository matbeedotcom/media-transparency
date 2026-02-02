/**
 * Case report display component
 *
 * Shows ranked entities, relationships, and cross-border flags.
 */

import { type CaseReport } from '../../services/api';

interface CaseReportComponentProps {
  report: CaseReport;
}

export function CaseReportComponent({ report }: CaseReportComponentProps) {
  return (
    <div className="case-report">
      {/* Summary */}
      <div className="report-section">
        <h3>Summary</h3>
        <div className="summary-grid">
          <div className="summary-item">
            <span className="label">Entry Point</span>
            <span className="value">{report.summary.entry_point}</span>
          </div>
          <div className="summary-item">
            <span className="label">Entities</span>
            <span className="value">{report.summary.entity_count}</span>
          </div>
          <div className="summary-item">
            <span className="label">Relationships</span>
            <span className="value">{report.summary.relationship_count}</span>
          </div>
          <div className="summary-item">
            <span className="label">Cross-Border</span>
            <span className="value">{report.summary.cross_border_count}</span>
          </div>
          <div className="summary-item">
            <span className="label">Processing Time</span>
            <span className="value">{report.summary.processing_time_seconds.toFixed(1)}s</span>
          </div>
        </div>
        {report.summary.has_unresolved_matches && (
          <div className="warning-badge">
            ⚠️ Has unresolved entity matches requiring review
          </div>
        )}
      </div>

      {/* Cross-Border Flags */}
      {report.cross_border_flags.length > 0 && (
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
              {report.cross_border_flags.map((flag, idx) => (
                <tr key={idx}>
                  <td>{flag.us_entity_name}</td>
                  <td>{flag.ca_entity_name}</td>
                  <td>{flag.relationship_type}</td>
                  <td>{flag.amount ? `$${flag.amount.toLocaleString()}` : 'N/A'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Top Entities */}
      {report.top_entities.length > 0 && (
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
              {report.top_entities.slice(0, 10).map((entity, idx) => (
                <tr key={entity.entity_id}>
                  <td>{idx + 1}</td>
                  <td>{entity.name}</td>
                  <td>{entity.entity_type}</td>
                  <td>{entity.jurisdiction || 'Unknown'}</td>
                  <td>{entity.relevance_score.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Top Relationships */}
      {report.top_relationships.length > 0 && (
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
              {report.top_relationships.slice(0, 10).map((rel, idx) => (
                <tr key={idx}>
                  <td>{rel.source_name}</td>
                  <td>{rel.target_name}</td>
                  <td>{rel.relationship_type}</td>
                  <td>{rel.amount ? `$${rel.amount.toLocaleString()}` : 'N/A'}</td>
                  <td>{rel.significance_score.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Unknowns */}
      {report.unknowns.length > 0 && (
        <div className="report-section">
          <h3>Unknowns</h3>
          <p className="section-description">
            The following entities could not be fully traced:
          </p>
          <ul className="unknowns-list">
            {report.unknowns.map((unknown, idx) => (
              <li key={idx}>
                <strong>{unknown.entity_name}</strong>: {unknown.reason}
                {unknown.attempted_sources.length > 0 && (
                  <span className="attempted-sources">
                    (Attempted: {unknown.attempted_sources.join(', ')})
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
