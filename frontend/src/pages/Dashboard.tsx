/**
 * Dashboard page for MITDS
 *
 * Shows system overview with entity counts and source health.
 * Ingestion management has moved to the dedicated /ingestion page.
 */

import { useQuery } from '@tanstack/react-query';
import { Link } from 'react-router-dom';
import {
  getIngestionStatus,
  searchEntities,
  type IngestionStatus,
} from '../services/api';

export default function Dashboard() {
  const { data: ingestionData, isLoading: ingestionLoading } = useQuery({
    queryKey: ['ingestion-status'],
    queryFn: getIngestionStatus,
    refetchInterval: 60000,
  });

  const { data: orgData } = useQuery({
    queryKey: ['entity-count', 'ORGANIZATION'],
    queryFn: () => searchEntities({ type: 'ORGANIZATION', limit: 1 }),
  });

  const { data: personData } = useQuery({
    queryKey: ['entity-count', 'PERSON'],
    queryFn: () => searchEntities({ type: 'PERSON', limit: 1 }),
  });

  const { data: outletData } = useQuery({
    queryKey: ['entity-count', 'OUTLET'],
    queryFn: () => searchEntities({ type: 'OUTLET', limit: 1 }),
  });

  const { data: allData } = useQuery({
    queryKey: ['entity-count', 'ALL'],
    queryFn: () => searchEntities({ limit: 1 }),
  });

  const getStatusColor = (status: IngestionStatus['status']) => {
    switch (status) {
      case 'healthy':
        return 'text-success';
      case 'running':
        return 'text-primary';
      case 'degraded':
      case 'stale':
        return 'text-warning';
      case 'failed':
        return 'text-danger';
      case 'disabled':
      case 'never_run':
      default:
        return 'text-muted';
    }
  };

  const getStatusIcon = (status: IngestionStatus['status']) => {
    switch (status) {
      case 'healthy':
        return '✅';
      case 'running':
        return '🔄';
      case 'degraded':
        return '⚠️';
      case 'failed':
        return '❌';
      case 'stale':
        return '⏰';
      case 'disabled':
        return '⏸️';
      case 'never_run':
        return '🆕';
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
            <div className="stat-value">{(orgData?.total ?? 0).toLocaleString()}</div>
            <div className="stat-label">Organizations</div>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-icon">👤</div>
          <div className="stat-content">
            <div className="stat-value">{(personData?.total ?? 0).toLocaleString()}</div>
            <div className="stat-label">Persons</div>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-icon">📰</div>
          <div className="stat-content">
            <div className="stat-value">{(outletData?.total ?? 0).toLocaleString()}</div>
            <div className="stat-label">Outlets</div>
          </div>
        </div>
        <div className="card stat-card">
          <div className="stat-icon">🔗</div>
          <div className="stat-content">
            <div className="stat-value">{(allData?.total ?? 0).toLocaleString()}</div>
            <div className="stat-label">Total Entities</div>
          </div>
        </div>
      </div>

      {/* Data Sources Status (read-only) */}
      <section className="section">
        <div className="section-header">
          <h2>Data Sources</h2>
          <Link to="/ingestion" className="btn btn-secondary btn-sm">
            Manage Ingestion
          </Link>
        </div>
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
                    <td>{(source.records_processed ?? 0).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </section>

      {/* Quick Actions */}
      <section className="section">
        <h2>Quick Actions</h2>
        <div className="actions-grid">
          <div className="card action-card">
            <h3>📥 Manage Ingestion</h3>
            <p>Search, configure, and trigger data ingestion</p>
            <Link to="/ingestion" className="btn btn-primary">
              Open Ingestion
            </Link>
          </div>
          <div className="card action-card">
            <h3>🔍 Search Entities</h3>
            <p>Explore organizations, persons, and media outlets</p>
            <Link to="/entities" className="btn btn-primary">
              Go to Explorer
            </Link>
          </div>
          <div className="card action-card">
            <h3>🎯 Run Detection</h3>
            <p>Analyze temporal coordination patterns</p>
            <Link to="/detection" className="btn btn-primary">
              Start Analysis
            </Link>
          </div>
          <div className="card action-card">
            <h3>📄 Generate Report</h3>
            <p>Create structural risk reports</p>
            <Link to="/reports" className="btn btn-primary">
              Create Report
            </Link>
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

        .section-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          margin-bottom: var(--spacing-md);
        }

        .section-header h2 {
          margin-bottom: 0;
        }

        .btn-sm {
          padding: 4px 8px;
          font-size: 0.75rem;
        }

        .loading {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: var(--spacing-md);
          padding: var(--spacing-xl);
        }

        .actions-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
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
