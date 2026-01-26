/**
 * InfrastructureOverlap component for MITDS
 *
 * Visualizes shared infrastructure between domains/outlets using
 * a matrix view and signal breakdown.
 */

import { useState, useCallback } from 'react';
import { useMutation } from '@tanstack/react-query';

interface InfraSignal {
  type: string;
  value: string;
  weight: number;
  description: string;
}

interface InfraMatch {
  domain_a: string;
  domain_b: string;
  total_score: number;
  confidence: number;
  signals: InfraSignal[];
  sharing_category: string | null;
}

interface InfrastructureOverlapProps {
  initialDomains?: string[];
  onMatchSelect?: (match: InfraMatch) => void;
  height?: string;
}

// Signal type colors and labels
const SIGNAL_COLORS: Record<string, string> = {
  same_analytics: '#DC2626', // Red - highest signal
  same_gtm: '#DC2626',
  same_adsense: '#B91C1C', // Darker red
  same_pixel: '#EA580C', // Orange
  same_ip: '#D97706', // Amber
  ssl_san_overlap: '#CA8A04', // Yellow
  same_nameserver: '#65A30D', // Lime
  same_registrar: '#0D9488', // Teal
  same_asn: '#0284C7', // Sky
  same_hosting: '#6366F1', // Indigo
  same_cdn: '#8B5CF6', // Violet
  same_cms: '#A855F7', // Purple
  same_ssl_issuer: '#6B7280', // Gray
};

const SIGNAL_LABELS: Record<string, string> = {
  same_analytics: 'Same Google Analytics',
  same_gtm: 'Same GTM Container',
  same_adsense: 'Same AdSense Publisher',
  same_pixel: 'Same Facebook Pixel',
  same_ip: 'Same IP Address',
  ssl_san_overlap: 'SSL Certificate Overlap',
  same_nameserver: 'Shared Nameserver',
  same_registrar: 'Same Registrar',
  same_asn: 'Same ASN',
  same_hosting: 'Same Hosting Provider',
  same_cdn: 'Same CDN',
  same_cms: 'Same CMS',
  same_ssl_issuer: 'Same SSL Issuer',
};

export default function InfrastructureOverlap({
  initialDomains = [],
  onMatchSelect,
  height = '600px',
}: InfrastructureOverlapProps) {
  const [domains, setDomains] = useState<string[]>(initialDomains);
  const [domainInput, setDomainInput] = useState('');
  const [minScore, setMinScore] = useState(1.0);
  const [matches, setMatches] = useState<InfraMatch[]>([]);
  const [selectedMatch, setSelectedMatch] = useState<InfraMatch | null>(null);

  // Analyze domains mutation
  const analyzeMutation = useMutation({
    mutationFn: async (domainList: string[]) => {
      const response = await fetch(
        `/api/relationships/shared-infrastructure?domains=${domainList.join(',')}&min_score=${minScore}`
      );
      if (!response.ok) {
        throw new Error('Failed to analyze infrastructure');
      }
      return response.json();
    },
    onSuccess: (data) => {
      setMatches(data.matches || []);
    },
  });

  const handleAddDomain = useCallback(() => {
    const trimmed = domainInput.trim().toLowerCase();
    if (trimmed && !domains.includes(trimmed)) {
      setDomains([...domains, trimmed]);
      setDomainInput('');
    }
  }, [domainInput, domains]);

  const handleRemoveDomain = useCallback((domain: string) => {
    setDomains(domains.filter((d) => d !== domain));
  }, [domains]);

  const handleAnalyze = useCallback(() => {
    if (domains.length >= 2) {
      analyzeMutation.mutate(domains);
    }
  }, [domains, analyzeMutation]);

  const handleMatchClick = useCallback(
    (match: InfraMatch) => {
      setSelectedMatch(match);
      onMatchSelect?.(match);
    },
    [onMatchSelect]
  );

  const getConfidenceColor = (confidence: number): string => {
    if (confidence >= 0.7) return 'var(--color-danger)';
    if (confidence >= 0.4) return 'var(--color-warning)';
    return 'var(--color-success)';
  };

  return (
    <div className="infrastructure-overlap" style={{ height }}>
      {/* Domain Input Section */}
      <div className="input-section">
        <h3>Analyze Infrastructure Sharing</h3>
        <p className="description">
          Enter domain names to detect shared infrastructure like analytics IDs,
          hosting providers, and SSL certificates.
        </p>

        <div className="domain-input-row">
          <input
            type="text"
            value={domainInput}
            onChange={(e) => setDomainInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleAddDomain()}
            placeholder="Enter domain (e.g., example.com)"
            aria-label="Domain name input"
          />
          <button type="button" className="btn btn-secondary" onClick={handleAddDomain}>
            Add
          </button>
        </div>

        {/* Domain Tags */}
        {domains.length > 0 && (
          <div className="domain-tags">
            {domains.map((domain) => (
              <span key={domain} className="domain-tag">
                {domain}
                <button
                  type="button"
                  className="tag-remove"
                  onClick={() => handleRemoveDomain(domain)}
                  aria-label={`Remove ${domain}`}
                >
                  &times;
                </button>
              </span>
            ))}
          </div>
        )}

        {/* Controls */}
        <div className="controls-row">
          <label className="score-control">
            <span>Min Score:</span>
            <input
              type="range"
              min="0"
              max="5"
              step="0.5"
              value={minScore}
              onChange={(e) => setMinScore(Number(e.target.value))}
              aria-label="Minimum score threshold"
            />
            <span className="score-value">{minScore.toFixed(1)}</span>
          </label>

          <button
            type="button"
            className="btn btn-primary"
            onClick={handleAnalyze}
            disabled={domains.length < 2 || analyzeMutation.isPending}
          >
            {analyzeMutation.isPending ? 'Analyzing...' : 'Analyze'}
          </button>
        </div>
      </div>

      {/* Results Section */}
      {matches.length > 0 && (
        <div className="results-section">
          <h4>Detected Overlaps ({matches.length})</h4>

          <div className="results-grid">
            {/* Match List */}
            <div className="match-list">
              {matches.map((match, idx) => (
                <div
                  key={`${match.domain_a}-${match.domain_b}-${idx}`}
                  className={`match-item ${selectedMatch === match ? 'selected' : ''}`}
                  onClick={() => handleMatchClick(match)}
                  onKeyDown={(e) => e.key === 'Enter' && handleMatchClick(match)}
                  role="button"
                  tabIndex={0}
                >
                  <div className="match-domains">
                    <span className="domain">{match.domain_a}</span>
                    <span className="connector">↔</span>
                    <span className="domain">{match.domain_b}</span>
                  </div>
                  <div className="match-meta">
                    <span
                      className="confidence-badge"
                      style={{ backgroundColor: getConfidenceColor(match.confidence) }}
                    >
                      {(match.confidence * 100).toFixed(0)}%
                    </span>
                    <span className="signal-count">{match.signals.length} signals</span>
                    {match.sharing_category && (
                      <span className="category-badge">{match.sharing_category}</span>
                    )}
                  </div>
                </div>
              ))}
            </div>

            {/* Signal Detail */}
            {selectedMatch && (
              <div className="signal-detail">
                <h5>
                  Signals: {selectedMatch.domain_a} ↔ {selectedMatch.domain_b}
                </h5>

                <div className="signal-list">
                  {selectedMatch.signals.map((signal, idx) => (
                    <div
                      key={`${signal.type}-${signal.value}-${idx}`}
                      className="signal-item"
                    >
                      <div
                        className="signal-indicator"
                        style={{ backgroundColor: SIGNAL_COLORS[signal.type] || '#6B7280' }}
                      />
                      <div className="signal-content">
                        <div className="signal-header">
                          <span className="signal-type">
                            {SIGNAL_LABELS[signal.type] || signal.type}
                          </span>
                          <span className="signal-weight">
                            Weight: {signal.weight.toFixed(1)}
                          </span>
                        </div>
                        <div className="signal-value">{signal.value}</div>
                        {signal.description && (
                          <div className="signal-description">{signal.description}</div>
                        )}
                      </div>
                    </div>
                  ))}
                </div>

                <div className="score-summary">
                  <div className="summary-item">
                    <span className="label">Total Score</span>
                    <span className="value">{selectedMatch.total_score.toFixed(2)}</span>
                  </div>
                  <div className="summary-item">
                    <span className="label">Confidence</span>
                    <span
                      className="value"
                      style={{ color: getConfidenceColor(selectedMatch.confidence) }}
                    >
                      {(selectedMatch.confidence * 100).toFixed(0)}%
                    </span>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Empty State */}
      {!analyzeMutation.isPending && matches.length === 0 && domains.length >= 2 && (
        <div className="empty-state">
          <p>Click &quot;Analyze&quot; to detect shared infrastructure between domains.</p>
        </div>
      )}

      {domains.length < 2 && (
        <div className="empty-state">
          <p>Add at least 2 domains to analyze infrastructure sharing.</p>
        </div>
      )}

      <style>{`
        .infrastructure-overlap {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
          overflow: hidden;
        }

        .input-section {
          padding: var(--spacing-md);
          background: var(--bg-secondary);
          border-radius: var(--border-radius);
        }

        .input-section h3 {
          margin: 0 0 var(--spacing-xs) 0;
          font-size: 1rem;
        }

        .description {
          margin: 0 0 var(--spacing-md) 0;
          color: var(--text-muted);
          font-size: 0.875rem;
        }

        .domain-input-row {
          display: flex;
          gap: var(--spacing-sm);
        }

        .domain-input-row input {
          flex: 1;
        }

        .domain-tags {
          display: flex;
          flex-wrap: wrap;
          gap: var(--spacing-xs);
          margin-top: var(--spacing-sm);
        }

        .domain-tag {
          display: inline-flex;
          align-items: center;
          gap: var(--spacing-xs);
          padding: 4px 8px;
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
          font-size: 0.875rem;
        }

        .tag-remove {
          background: none;
          border: none;
          color: var(--text-muted);
          cursor: pointer;
          padding: 0;
          font-size: 1rem;
          line-height: 1;
        }

        .tag-remove:hover {
          color: var(--color-danger);
        }

        .controls-row {
          display: flex;
          align-items: center;
          justify-content: space-between;
          margin-top: var(--spacing-md);
        }

        .score-control {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          font-size: 0.875rem;
        }

        .score-control input[type="range"] {
          width: 120px;
        }

        .score-value {
          min-width: 30px;
          text-align: center;
          font-weight: 500;
        }

        .results-section {
          flex: 1;
          overflow: hidden;
          display: flex;
          flex-direction: column;
        }

        .results-section h4 {
          margin: 0 0 var(--spacing-sm) 0;
          font-size: 0.875rem;
        }

        .results-grid {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: var(--spacing-md);
          flex: 1;
          overflow: hidden;
        }

        .match-list {
          overflow-y: auto;
          display: flex;
          flex-direction: column;
          gap: var(--spacing-xs);
        }

        .match-item {
          padding: var(--spacing-sm);
          background: var(--bg-secondary);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          cursor: pointer;
          transition: all 0.15s ease;
        }

        .match-item:hover {
          background: var(--bg-tertiary);
        }

        .match-item.selected {
          border-color: var(--color-primary);
          background: var(--bg-tertiary);
        }

        .match-domains {
          display: flex;
          align-items: center;
          gap: var(--spacing-xs);
          font-weight: 500;
        }

        .connector {
          color: var(--text-muted);
        }

        .match-meta {
          display: flex;
          align-items: center;
          gap: var(--spacing-xs);
          margin-top: var(--spacing-xs);
          font-size: 0.75rem;
        }

        .confidence-badge {
          padding: 2px 6px;
          border-radius: 4px;
          color: white;
          font-weight: 500;
        }

        .signal-count {
          color: var(--text-muted);
        }

        .category-badge {
          padding: 2px 6px;
          background: var(--bg-tertiary);
          border-radius: 4px;
          text-transform: uppercase;
        }

        .signal-detail {
          overflow-y: auto;
          padding: var(--spacing-md);
          background: var(--bg-secondary);
          border-radius: var(--border-radius);
        }

        .signal-detail h5 {
          margin: 0 0 var(--spacing-md) 0;
          font-size: 0.875rem;
        }

        .signal-list {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
        }

        .signal-item {
          display: flex;
          gap: var(--spacing-sm);
          padding: var(--spacing-sm);
          background: var(--bg-primary);
          border-radius: var(--border-radius);
        }

        .signal-indicator {
          width: 4px;
          border-radius: 2px;
          flex-shrink: 0;
        }

        .signal-content {
          flex: 1;
          min-width: 0;
        }

        .signal-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: 2px;
        }

        .signal-type {
          font-weight: 500;
          font-size: 0.875rem;
        }

        .signal-weight {
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .signal-value {
          font-family: ui-monospace, monospace;
          font-size: 0.75rem;
          color: var(--text-secondary);
          word-break: break-all;
        }

        .signal-description {
          font-size: 0.75rem;
          color: var(--text-muted);
          margin-top: 2px;
        }

        .score-summary {
          display: flex;
          gap: var(--spacing-md);
          margin-top: var(--spacing-md);
          padding-top: var(--spacing-md);
          border-top: 1px solid var(--border-color);
        }

        .summary-item {
          display: flex;
          flex-direction: column;
        }

        .summary-item .label {
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .summary-item .value {
          font-size: 1.25rem;
          font-weight: 600;
        }

        .empty-state {
          display: flex;
          align-items: center;
          justify-content: center;
          padding: var(--spacing-lg);
          color: var(--text-muted);
          text-align: center;
        }

        @media (max-width: 768px) {
          .results-grid {
            grid-template-columns: 1fr;
          }
        }
      `}</style>
    </div>
  );
}
