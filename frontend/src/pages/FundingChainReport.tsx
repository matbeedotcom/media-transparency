/**
 * Funding Chain Report page for MITDS
 *
 * Displays funding chain visualization tracing paths from political ads
 * to corporate funders with confidence scores and evidence links.
 */

import { useParams, Link } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { axiosInstance } from '@/api/axios-instance';
import { FundingChain } from '../components/FundingChain';
import { ObfuscationScore } from '../components/ObfuscationScore';

interface FundingChainLink {
  from_entity: string;
  to_entity: string;
  relationship_type: string;
  confidence: number;
  evidence_type: 'proven' | 'corroborated' | 'inferred';
  evidence_sources: string[];
}

interface FundingChainData {
  chain_id: string;
  overall_confidence: number;
  corroboration_count: number;
  links: FundingChainLink[];
}

interface CrossBorderFlag {
  us_entity_id: string;
  us_entity_name: string;
  ca_entity_id: string;
  ca_entity_name: string;
  relationship_type: string;
  amount?: number;
  confidence: number;
}

interface EvidenceItem {
  source: string;
  type: string;
  url?: string;
  retrieved_at?: string;
}

interface FundingChainReport {
  case_id: string;
  generated_at: string;
  summary: {
    entry_point: string;
    total_entities: number;
    total_relationships: number;
    sources_queried: string[];
    sources_with_results: string[];
    sources_without_results: string[];
  };
  funding_chains: FundingChainData[];
  cross_border_flags: CrossBorderFlag[];
  evidence_index: EvidenceItem[];
}

async function fetchFundingChainReport(
  caseId: string,
  format: 'json' | 'markdown' = 'json'
): Promise<FundingChainReport> {
  const response = await axiosInstance.get(
    `/api/v1/reports/funding-chain/${caseId}`,
    { params: { format } }
  );
  return response.data;
}

export default function FundingChainReport() {
  const { caseId } = useParams<{ caseId: string }>();

  const { data: report, isLoading, error } = useQuery({
    queryKey: ['funding-chain-report', caseId],
    queryFn: () => fetchFundingChainReport(caseId!),
    enabled: !!caseId,
  });

  if (isLoading) {
    return (
      <div className="loading">
        <div className="spinner" />
        <span>Loading funding chain report...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="error-message">
        <h1>Error Loading Report</h1>
        <p>Failed to load funding chain report: {error instanceof Error ? error.message : 'Unknown error'}</p>
        <Link to={`/cases/${caseId}`} className="btn btn-primary">
          Back to Case
        </Link>
      </div>
    );
  }

  if (!report) {
    return (
      <div className="error-message">
        <h1>Report Not Found</h1>
        <p>The funding chain report could not be found.</p>
        <Link to={`/cases/${caseId}`} className="btn btn-primary">
          Back to Case
        </Link>
      </div>
    );
  }

  return (
    <div className="funding-chain-report">
      <header className="page-header">
        <div className="header-content">
          <h1>Funding Chain Report</h1>
          <Link to={`/cases/${caseId}`} className="btn btn-secondary">
            ← Back to Case
          </Link>
        </div>
        <div className="header-meta">
          <span className="meta-item">
            Generated: {new Date(report.generated_at).toLocaleString()}
          </span>
        </div>
      </header>

      {/* Summary Section */}
      <div className="card summary-card">
        <h2>Summary</h2>
        <div className="summary-grid">
          <div className="summary-item">
            <div className="summary-label">Entry Point</div>
            <div className="summary-value">{report.summary.entry_point}</div>
          </div>
          <div className="summary-item">
            <div className="summary-label">Total Entities</div>
            <div className="summary-value">{report.summary.total_entities}</div>
          </div>
          <div className="summary-item">
            <div className="summary-label">Total Relationships</div>
            <div className="summary-value">{report.summary.total_relationships}</div>
          </div>
        </div>

        <div className="sources-section">
          <h3>Data Sources</h3>
          <div className="sources-list">
            <div className="source-group">
              <span className="source-label">Queried:</span>
              <span className="source-values">
                {report.summary.sources_queried.length > 0
                  ? report.summary.sources_queried.join(', ')
                  : 'None'}
              </span>
            </div>
            <div className="source-group">
              <span className="source-label">With Results:</span>
              <span className="source-values success">
                {report.summary.sources_with_results.length > 0
                  ? report.summary.sources_with_results.join(', ')
                  : 'None'}
              </span>
            </div>
            {report.summary.sources_without_results.length > 0 && (
              <div className="source-group">
                <span className="source-label">Without Results:</span>
                <span className="source-values muted">
                  {report.summary.sources_without_results.join(', ')}
                </span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Funding Chains */}
      <div className="card chains-card">
        <h2>Funding Chains</h2>
        {report.funding_chains.length === 0 ? (
          <p className="empty-state">No funding chains found.</p>
        ) : (
          <div className="chains-list">
            {report.funding_chains.map((chain) => (
              <div key={chain.chain_id} className="chain-item">
                <div className="chain-header">
                  <h3>{chain.chain_id}</h3>
                  <div className="chain-metrics">
                    <ObfuscationScore
                      overall_score={chain.overall_confidence}
                      signals={[]}
                      is_flagged={chain.overall_confidence > 0.7}
                    />
                    <span className="corroboration-badge">
                      {chain.corroboration_count} source{chain.corroboration_count !== 1 ? 's' : ''}
                    </span>
                  </div>
                </div>
                <FundingChain chain={chain} />
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Cross-Border Flags */}
      {report.cross_border_flags.length > 0 && (
        <div className="card cross-border-card">
          <h2>Cross-Border Flags</h2>
          <div className="flags-list">
            {report.cross_border_flags.map((flag, idx) => (
              <div key={idx} className="flag-item">
                <div className="flag-header">
                  <span className="flag-entity">{flag.us_entity_name}</span>
                  <span className="flag-arrow">→</span>
                  <span className="flag-entity">{flag.ca_entity_name}</span>
                </div>
                <div className="flag-details">
                  <span className="flag-type">{flag.relationship_type}</span>
                  {flag.amount && (
                    <span className="flag-amount">${flag.amount.toLocaleString()}</span>
                  )}
                  <span className="flag-confidence">
                    Confidence: {(flag.confidence * 100).toFixed(0)}%
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Evidence Index */}
      {report.evidence_index.length > 0 && (
        <div className="card evidence-card">
          <h2>Evidence Index</h2>
          <div className="evidence-list">
            {report.evidence_index.map((evidence, idx) => (
              <div key={idx} className="evidence-item">
                <div className="evidence-source">
                  <strong>{evidence.source}</strong>
                  <span className="evidence-type">({evidence.type})</span>
                </div>
                {evidence.url && (
                  <a href={evidence.url} target="_blank" rel="noopener noreferrer" className="evidence-link">
                    {evidence.url}
                  </a>
                )}
                {evidence.retrieved_at && (
                  <span className="evidence-date">
                    Retrieved: {new Date(evidence.retrieved_at).toLocaleString()}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      <style>{`
        .funding-chain-report {
          padding: var(--spacing-lg);
          max-width: 1200px;
          margin: 0 auto;
        }

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

        .header-meta {
          display: flex;
          gap: var(--spacing-md);
          font-size: 0.875rem;
          color: var(--text-secondary);
        }

        .card {
          background: var(--bg-primary);
          border: 1px solid var(--border-color);
          border-radius: var(--radius);
          padding: var(--spacing-lg);
          margin-bottom: var(--spacing-lg);
        }

        .card h2 {
          margin-top: 0;
          margin-bottom: var(--spacing-md);
          font-size: 1.5rem;
        }

        .summary-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
          margin-bottom: var(--spacing-lg);
        }

        .summary-item {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-xs);
        }

        .summary-label {
          font-size: 0.875rem;
          color: var(--text-secondary);
          font-weight: 500;
        }

        .summary-value {
          font-size: 1.5rem;
          font-weight: 700;
          color: var(--text-primary);
        }

        .sources-section {
          margin-top: var(--spacing-lg);
          padding-top: var(--spacing-lg);
          border-top: 1px solid var(--border-color);
        }

        .sources-section h3 {
          font-size: 1rem;
          margin-bottom: var(--spacing-sm);
        }

        .sources-list {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
        }

        .source-group {
          display: flex;
          gap: var(--spacing-sm);
        }

        .source-label {
          font-weight: 500;
          min-width: 120px;
        }

        .source-values {
          color: var(--text-secondary);
        }

        .source-values.success {
          color: var(--success);
        }

        .source-values.muted {
          color: var(--text-muted);
        }

        .chains-list {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-lg);
        }

        .chain-item {
          border: 1px solid var(--border-color);
          border-radius: var(--radius);
          padding: var(--spacing-md);
        }

        .chain-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: var(--spacing-md);
        }

        .chain-header h3 {
          margin: 0;
          font-size: 1.25rem;
        }

        .chain-metrics {
          display: flex;
          align-items: center;
          gap: var(--spacing-md);
        }

        .corroboration-badge {
          padding: var(--spacing-xs) var(--spacing-sm);
          background: var(--bg-secondary);
          border-radius: var(--radius);
          font-size: 0.875rem;
        }

        .flags-list {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
        }

        .flag-item {
          padding: var(--spacing-md);
          background: var(--bg-secondary);
          border-radius: var(--radius);
        }

        .flag-header {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          margin-bottom: var(--spacing-xs);
        }

        .flag-entity {
          font-weight: 600;
        }

        .flag-arrow {
          color: var(--text-secondary);
        }

        .flag-details {
          display: flex;
          gap: var(--spacing-md);
          font-size: 0.875rem;
          color: var(--text-secondary);
        }

        .evidence-list {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
        }

        .evidence-item {
          padding: var(--spacing-sm);
          border-bottom: 1px solid var(--border-color);
        }

        .evidence-item:last-child {
          border-bottom: none;
        }

        .evidence-source {
          margin-bottom: var(--spacing-xs);
        }

        .evidence-type {
          color: var(--text-secondary);
          font-size: 0.875rem;
        }

        .evidence-link {
          display: block;
          color: var(--primary);
          text-decoration: none;
          word-break: break-all;
          margin-bottom: var(--spacing-xs);
        }

        .evidence-link:hover {
          text-decoration: underline;
        }

        .evidence-date {
          font-size: 0.875rem;
          color: var(--text-secondary);
        }

        .empty-state {
          color: var(--text-secondary);
          font-style: italic;
          text-align: center;
          padding: var(--spacing-xl);
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

        .btn {
          padding: var(--spacing-sm) var(--spacing-md);
          border-radius: var(--radius);
          border: none;
          cursor: pointer;
          text-decoration: none;
          display: inline-block;
          font-size: 0.875rem;
        }

        .btn-primary {
          background: var(--primary);
          color: white;
        }

        .btn-secondary {
          background: var(--bg-secondary);
          color: var(--text-primary);
          border: 1px solid var(--border-color);
        }
      `}</style>
    </div>
  );
}
