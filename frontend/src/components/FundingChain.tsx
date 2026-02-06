/**
 * FundingChain component
 *
 * Visual chain diagram showing entity→entity links with color-coded confidence.
 */

interface FundingChainLink {
  from_entity: string;
  to_entity: string;
  relationship_type: string;
  confidence: number;
  evidence_type: 'proven' | 'corroborated' | 'inferred';
  evidence_sources: string[];
}

interface FundingChainProps {
  chain: {
    chain_id: string;
    overall_confidence: number;
    corroboration_count: number;
    links: FundingChainLink[];
  };
}

export function FundingChain({ chain }: FundingChainProps) {
  const getConfidenceColor = (confidence: number): string => {
    if (confidence >= 0.8) return '#10b981'; // green
    if (confidence >= 0.6) return '#f59e0b'; // yellow
    return '#ef4444'; // red
  };

  const getEvidenceTypeColor = (evidenceType: string): string => {
    switch (evidenceType) {
      case 'proven':
        return '#10b981';
      case 'corroborated':
        return '#f59e0b';
      case 'inferred':
        return '#6b7280';
      default:
        return '#6b7280';
    }
  };

  const getEvidenceTypeLabel = (evidenceType: string): string => {
    switch (evidenceType) {
      case 'proven':
        return 'Proven';
      case 'corroborated':
        return 'Corroborated';
      case 'inferred':
        return 'Inferred';
      default:
        return 'Unknown';
    }
  };

  if (!chain.links || chain.links.length === 0) {
    return (
      <div className="funding-chain-empty">
        <p>No links found in this chain.</p>
      </div>
    );
  }

  return (
    <div className="funding-chain">
      <div className="chain-links">
        {chain.links.map((link, index) => (
          <div key={index} className="chain-link-item">
            <div className="link-visual">
              <div className="link-node from-node">
                <span className="node-label">{link.from_entity.substring(0, 8)}...</span>
              </div>
              <div
                className="link-connector"
                style={{
                  backgroundColor: getConfidenceColor(link.confidence),
                  opacity: link.confidence,
                }}
              >
                <div className="connector-label">
                  {link.relationship_type}
                </div>
              </div>
              <div className="link-node to-node">
                <span className="node-label">{link.to_entity.substring(0, 8)}...</span>
              </div>
            </div>
            <div className="link-details">
              <div className="detail-row">
                <span className="detail-label">From:</span>
                <span className="detail-value">{link.from_entity}</span>
              </div>
              <div className="detail-row">
                <span className="detail-label">To:</span>
                <span className="detail-value">{link.to_entity}</span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Type:</span>
                <span className="detail-value">{link.relationship_type}</span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Confidence:</span>
                <span
                  className="detail-value confidence-badge"
                  style={{ color: getConfidenceColor(link.confidence) }}
                >
                  {(link.confidence * 100).toFixed(0)}%
                </span>
              </div>
              <div className="detail-row">
                <span className="detail-label">Evidence:</span>
                <span
                  className="detail-value evidence-badge"
                  style={{
                    backgroundColor: getEvidenceTypeColor(link.evidence_type),
                    color: 'white',
                  }}
                >
                  {getEvidenceTypeLabel(link.evidence_type)}
                </span>
              </div>
              {link.evidence_sources && link.evidence_sources.length > 0 && (
                <div className="detail-row">
                  <span className="detail-label">Sources:</span>
                  <span className="detail-value sources-list">
                    {link.evidence_sources.join(', ')}
                  </span>
                </div>
              )}
            </div>
          </div>
        ))}
      </div>

      <style>{`
        .funding-chain {
          width: 100%;
        }

        .funding-chain-empty {
          padding: var(--spacing-lg);
          text-align: center;
          color: var(--text-secondary);
        }

        .chain-links {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
        }

        .chain-link-item {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
          padding: var(--spacing-md);
          background: var(--bg-secondary);
          border-radius: var(--radius);
        }

        .link-visual {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          margin-bottom: var(--spacing-sm);
        }

        .link-node {
          padding: var(--spacing-xs) var(--spacing-sm);
          background: var(--bg-primary);
          border: 2px solid var(--border-color);
          border-radius: var(--radius);
          font-size: 0.75rem;
          font-family: monospace;
          min-width: 80px;
          text-align: center;
        }

        .link-connector {
          flex: 1;
          height: 3px;
          position: relative;
          border-radius: 2px;
          min-width: 100px;
        }

        .connector-label {
          position: absolute;
          top: -20px;
          left: 50%;
          transform: translateX(-50%);
          font-size: 0.75rem;
          color: var(--text-secondary);
          white-space: nowrap;
          background: var(--bg-primary);
          padding: 2px 6px;
          border-radius: 4px;
        }

        .link-details {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-xs);
          font-size: 0.875rem;
        }

        .detail-row {
          display: flex;
          gap: var(--spacing-xs);
        }

        .detail-label {
          font-weight: 500;
          color: var(--text-secondary);
          min-width: 80px;
        }

        .detail-value {
          color: var(--text-primary);
          word-break: break-word;
        }

        .confidence-badge {
          font-weight: 600;
        }

        .evidence-badge {
          display: inline-block;
          padding: 2px 8px;
          border-radius: 4px;
          font-size: 0.75rem;
          font-weight: 500;
        }

        .sources-list {
          font-size: 0.8rem;
          color: var(--text-secondary);
        }

        @media (max-width: 768px) {
          .link-visual {
            flex-direction: column;
            align-items: flex-start;
          }

          .link-connector {
            width: 3px;
            height: 40px;
            min-width: 3px;
          }

          .connector-label {
            left: 10px;
            top: 50%;
            transform: translateY(-50%);
          }

          .link-details {
            grid-template-columns: 1fr;
          }
        }
      `}</style>
    </div>
  );
}
