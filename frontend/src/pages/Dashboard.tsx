/**
 * Dashboard page for MITDS
 *
 * Shows system overview, recent activity, and key metrics.
 */

import { useQuery } from '@tanstack/react-query';
import { getIngestionStatus, type IngestionStatus } from '../services/api';

export default function Dashboard() {
  const { data: ingestionData, isLoading: ingestionLoading } = useQuery({
    queryKey: ['ingestion-status'],
    queryFn: getIngestionStatus,
    refetchInterval: 60000, // Refresh every minute
  });

  const getStatusColor = (status: IngestionStatus['status']) => {
    switch (status) {
      case 'healthy':
        return 'text-success';
      case 'degraded':
        return 'text-warning';
      case 'failed':
        return 'text-danger';
      case 'stale':
        return 'text-warning';
      case 'disabled':
        return 'text-muted';
      default:
        return 'text-muted';
    }
  };

  const getStatusIcon = (status: IngestionStatus['status']) => {
    switch (status) {
      case 'healthy':
        return '✅';
      case 'degraded':
        return '⚠️';
      case 'failed':
        return '❌';
      case 'stale':
        return '⏰';
      case 'disabled':
        return '⏸️';
      default:
        return '❓';
    }
  };

  return (
    <div className="dashboard">
      <header className="page-header">
        <h1>Dashboard</h1>
        <p>Media Influence Topology & Detection System Overview</p>
      </header>

      {/* Stats Grid */}
      <div className="stats-grid">
        <div className="card stat-card">
          <div className="stat-icon">🏢</div>
          <div className="stat-content">
            <div className="stat-value">0</div>
            <div className="stat-label">Organizations</div>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-icon">👤</div>
          <div className="stat-content">
            <div className="stat-value">0</div>
            <div className="stat-label">Persons</div>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-icon">📰</div>
          <div className="stat-content">
            <div className="stat-value">0</div>
            <div className="stat-label">Outlets</div>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-icon">🔗</div>
          <div className="stat-content">
            <div className="stat-value">0</div>
            <div className="stat-label">Relationships</div>
          </div>
        </div>
      </div>

      {/* Data Sources Status */}
      <section className="section">
        <h2>Data Sources</h2>
        <div className="card">
          {ingestionLoading ? (
            <div className="loading">
              <div className="spinner" />
              <span>Loading status...</span>
            </div>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Source</th>
                  <th>Status</th>
                  <th>Last Run</th>
                  <th>Records</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {ingestionData?.sources.map((source) => (
                  <tr key={source.source}>
                    <td>
                      <strong>{source.source.toUpperCase()}</strong>
                    </td>
                    <td className={getStatusColor(source.status)}>
                      {getStatusIcon(source.status)} {source.status}
                    </td>
                    <td>
                      {source.last_successful_run
                        ? new Date(source.last_successful_run).toLocaleString()
                        : 'Never'}
                    </td>
                    <td>{source.records_count.toLocaleString()}</td>
                    <td>
                      <button
                        className="btn btn-secondary"
                        disabled={source.status === 'disabled'}
                      >
                        Trigger
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </section>

      {/* Recent Activity */}
      <section className="section">
        <h2>Recent Activity</h2>
        <div className="card">
          <div className="empty-state">
            <p>No recent activity to display.</p>
            <p className="text-muted">
              Activity will appear here once data ingestion begins.
            </p>
          </div>
        </div>
      </section>

      {/* Quick Actions */}
      <section className="section">
        <h2>Quick Actions</h2>
        <div className="actions-grid">
          <div className="card action-card">
            <h3>🔍 Search Entities</h3>
            <p>Explore organizations, persons, and media outlets</p>
            <a href="/entities" className="btn btn-primary">
              Go to Explorer
            </a>
          </div>
          <div className="card action-card">
            <h3>🎯 Run Detection</h3>
            <p>Analyze temporal coordination patterns</p>
            <a href="/detection" className="btn btn-primary">
              Start Analysis
            </a>
          </div>
          <div className="card action-card">
            <h3>📄 Generate Report</h3>
            <p>Create structural risk reports</p>
            <a href="/reports" className="btn btn-primary">
              Create Report
            </a>
          </div>
        </div>
      </section>

      <style>{`
        .page-header {
          margin-bottom: var(--spacing-lg);
        }

        .page-header h1 {
          margin-bottom: var(--spacing-xs);
        }

        .stats-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
          margin-bottom: var(--spacing-lg);
        }

        .stat-card {
          display: flex;
          align-items: center;
          gap: var(--spacing-md);
        }

        .stat-icon {
          font-size: 2rem;
        }

        .stat-value {
          font-size: 2rem;
          font-weight: 700;
          color: var(--text-primary);
        }

        .stat-label {
          font-size: 0.875rem;
          color: var(--text-secondary);
        }

        .section {
          margin-bottom: var(--spacing-lg);
        }

        .section h2 {
          margin-bottom: var(--spacing-md);
        }

        .loading {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: var(--spacing-md);
          padding: var(--spacing-xl);
        }

        .empty-state {
          text-align: center;
          padding: var(--spacing-xl);
        }

        .actions-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
          gap: var(--spacing-md);
        }

        .action-card h3 {
          margin-bottom: var(--spacing-sm);
        }

        .action-card p {
          margin-bottom: var(--spacing-md);
        }
      `}</style>
    </div>
  );
}
