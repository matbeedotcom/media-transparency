/**
 * ObfuscationScore component
 *
 * Score gauge display with signal breakdown showing overall score
 * and visual indicator based on score level.
 */

interface Signal {
  signal_type?: string;
  category?: string;
  score?: number;
  weight?: number;
}

interface ObfuscationScoreProps {
  overall_score: number;
  signals?: Signal[];
  is_flagged?: boolean;
}

export function ObfuscationScore({
  overall_score,
  signals = [],
  is_flagged = false,
}: ObfuscationScoreProps) {
  const getScoreColor = (score: number): string => {
    if (score >= 0.8) return '#ef4444'; // red - high risk
    if (score >= 0.6) return '#f59e0b'; // yellow - medium risk
    if (score >= 0.4) return '#3b82f6'; // blue - low risk
    return '#10b981'; // green - minimal risk
  };

  const getScoreLabel = (score: number): string => {
    if (score >= 0.8) return 'High Risk';
    if (score >= 0.6) return 'Medium Risk';
    if (score >= 0.4) return 'Low Risk';
    return 'Minimal Risk';
  };

  const getScoreIcon = (score: number): string => {
    if (score >= 0.8) return '🔴';
    if (score >= 0.6) return '🟡';
    if (score >= 0.4) return '🔵';
    return '🟢';
  };

  const scoreColor = getScoreColor(overall_score);
  const scoreLabel = getScoreLabel(overall_score);
  const scoreIcon = getScoreIcon(overall_score);
  const scorePercent = Math.round(overall_score * 100);

  return (
    <div className="obfuscation-score">
      <div className="score-gauge">
        <div className="gauge-container">
          <div
            className="gauge-fill"
            style={{
              width: `${scorePercent}%`,
              backgroundColor: scoreColor,
            }}
          />
          <div className="gauge-label">
            <span className="gauge-icon">{scoreIcon}</span>
            <span className="gauge-value">{scorePercent}%</span>
          </div>
        </div>
        <div className="score-info">
          <div className="score-label" style={{ color: scoreColor }}>
            {scoreLabel}
          </div>
          {is_flagged && (
            <div className="flagged-badge">
              ⚠️ Flagged
            </div>
          )}
        </div>
      </div>

      {signals && signals.length > 0 && (
        <div className="signals-breakdown">
          <div className="signals-header">Signal Breakdown</div>
          <div className="signals-list">
            {signals.map((signal, index) => (
              <div key={index} className="signal-item">
                <div className="signal-header">
                  <span className="signal-type">
                    {signal.signal_type || `Signal ${index + 1}`}
                  </span>
                  {signal.category && (
                    <span className="signal-category">{signal.category}</span>
                  )}
                </div>
                <div className="signal-details">
                  <div className="signal-score-bar">
                    <div
                      className="signal-score-fill"
                      style={{
                        width: `${(signal.score || 0) * 100}%`,
                        backgroundColor: getScoreColor(signal.score || 0),
                      }}
                    />
                  </div>
                  <span className="signal-score-value">
                    {Math.round((signal.score || 0) * 100)}%
                  </span>
                  {signal.weight !== undefined && (
                    <span className="signal-weight">
                      (weight: {signal.weight.toFixed(2)})
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <style>{`
        .obfuscation-score {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
        }

        .score-gauge {
          display: flex;
          align-items: center;
          gap: var(--spacing-md);
        }

        .gauge-container {
          position: relative;
          width: 200px;
          height: 40px;
          background: var(--bg-secondary);
          border-radius: 20px;
          overflow: hidden;
          border: 2px solid var(--border-color);
        }

        .gauge-fill {
          position: absolute;
          top: 0;
          left: 0;
          height: 100%;
          transition: width 0.3s ease;
          border-radius: 20px;
        }

        .gauge-label {
          position: absolute;
          top: 50%;
          left: 50%;
          transform: translate(-50%, -50%);
          display: flex;
          align-items: center;
          gap: var(--spacing-xs);
          z-index: 1;
          font-weight: 700;
          color: var(--text-primary);
          text-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
        }

        .gauge-icon {
          font-size: 1.2rem;
        }

        .gauge-value {
          font-size: 1rem;
        }

        .score-info {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-xs);
        }

        .score-label {
          font-weight: 600;
          font-size: 0.875rem;
        }

        .flagged-badge {
          padding: 2px 8px;
          background: #fef3c7;
          color: #92400e;
          border-radius: 4px;
          font-size: 0.75rem;
          font-weight: 500;
        }

        .signals-breakdown {
          margin-top: var(--spacing-sm);
          padding-top: var(--spacing-sm);
          border-top: 1px solid var(--border-color);
        }

        .signals-header {
          font-weight: 600;
          font-size: 0.875rem;
          margin-bottom: var(--spacing-sm);
          color: var(--text-secondary);
        }

        .signals-list {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-xs);
        }

        .signal-item {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-xs);
          padding: var(--spacing-xs);
          background: var(--bg-secondary);
          border-radius: var(--radius);
        }

        .signal-header {
          display: flex;
          align-items: center;
          gap: var(--spacing-xs);
          font-size: 0.875rem;
        }

        .signal-type {
          font-weight: 500;
          color: var(--text-primary);
        }

        .signal-category {
          padding: 2px 6px;
          background: var(--bg-primary);
          border-radius: 4px;
          font-size: 0.75rem;
          color: var(--text-secondary);
        }

        .signal-details {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          font-size: 0.75rem;
        }

        .signal-score-bar {
          flex: 1;
          height: 8px;
          background: var(--bg-primary);
          border-radius: 4px;
          overflow: hidden;
        }

        .signal-score-fill {
          height: 100%;
          transition: width 0.3s ease;
        }

        .signal-score-value {
          min-width: 40px;
          font-weight: 600;
          color: var(--text-primary);
        }

        .signal-weight {
          color: var(--text-secondary);
          font-size: 0.7rem;
        }

        @media (max-width: 768px) {
          .score-gauge {
            flex-direction: column;
            align-items: flex-start;
          }

          .gauge-container {
            width: 100%;
          }
        }
      `}</style>
    </div>
  );
}
