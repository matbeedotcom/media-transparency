/**
 * Settings Page for MITDS
 *
 * Displays system configuration, data source status, and connection health.
 * All settings are display-only (configured via environment variables).
 */

import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  getSettings,
  getConnectionsStatus,
  getMetaAuthUrl,
  getMetaAuthStatus,
  disconnectMeta,
  type SettingsResponse,
  type ConnectionInfo,
  type DataSourceInfo,
  type ConnectionStatusType,
} from '../services/api';

type TabType = 'sources' | 'connections' | 'api';

export default function SettingsPage() {
  const [activeTab, setActiveTab] = useState<TabType>('sources');

  const { data: settings, isLoading, error, refetch } = useQuery({
    queryKey: ['settings'],
    queryFn: getSettings,
    staleTime: 30_000, // 30 seconds
  });

  const { refetch: refetchConnections, isFetching: isRefetchingConnections } = useQuery({
    queryKey: ['connections'],
    queryFn: getConnectionsStatus,
    enabled: false, // Only fetch on demand
  });

  const handleRefreshConnections = async () => {
    await refetchConnections();
    await refetch();
  };

  if (isLoading) {
    return (
      <div className="settings-page">
        <div className="loading-container">
          <div className="spinner" />
          <p>Loading settings...</p>
        </div>
        <style>{styles}</style>
      </div>
    );
  }

  if (error) {
    return (
      <div className="settings-page">
        <div className="error-container">
          <h2>Error Loading Settings</h2>
          <p>{error instanceof Error ? error.message : 'An error occurred'}</p>
          <button className="btn btn-primary" onClick={() => refetch()}>
            Retry
          </button>
        </div>
        <style>{styles}</style>
      </div>
    );
  }

  return (
    <div className="settings-page">
      <header className="page-header">
        <div className="page-title">
          <h1>Settings</h1>
          <p className="subtitle">System configuration and data source status</p>
        </div>
        <div className="header-actions">
          <span className="env-badge" data-env={settings?.api.environment}>
            {settings?.api.environment}
          </span>
        </div>
      </header>

      {/* Tabs */}
      <div className="tabs">
        <button
          className={`tab ${activeTab === 'sources' ? 'active' : ''}`}
          onClick={() => setActiveTab('sources')}
        >
          Data Sources
        </button>
        <button
          className={`tab ${activeTab === 'connections' ? 'active' : ''}`}
          onClick={() => setActiveTab('connections')}
        >
          Connections
        </button>
        <button
          className={`tab ${activeTab === 'api' ? 'active' : ''}`}
          onClick={() => setActiveTab('api')}
        >
          API Configuration
        </button>
      </div>

      {/* Tab Content */}
      <div className="tab-content">
        {activeTab === 'sources' && settings && (
          <DataSourcesTab sources={settings.data_sources} />
        )}
        {activeTab === 'connections' && settings && (
          <ConnectionsTab
            connections={settings.connections}
            onRefresh={handleRefreshConnections}
            isRefreshing={isRefetchingConnections}
          />
        )}
        {activeTab === 'api' && settings && (
          <APIConfigTab config={settings.api} />
        )}
      </div>

      <style>{styles}</style>
    </div>
  );
}

// =========================
// Data Sources Tab
// =========================

function DataSourcesTab({ sources }: { sources: DataSourceInfo[] }) {
  const enabledCount = sources.filter((s) => s.enabled).length;
  const disabledCount = sources.length - enabledCount;

  return (
    <div className="tab-panel">
      <div className="panel-header">
        <h2>Data Sources</h2>
        <div className="panel-stats">
          <span className="stat enabled">{enabledCount} enabled</span>
          <span className="stat disabled">{disabledCount} disabled</span>
        </div>
      </div>

      <p className="panel-description">
        Data sources are configured via environment variables. Sources requiring API keys
        must have valid credentials set before they can be enabled.
      </p>

      {/* Meta/Facebook Connection Card */}
      <MetaConnectionCard />

      <div className="sources-grid">
        {sources.map((source) => (
          <DataSourceCard key={source.id} source={source} />
        ))}
      </div>
    </div>
  );
}

// =========================
// Meta Connection Card
// =========================

function MetaConnectionCard() {
  const queryClient = useQueryClient();
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  // Check for OAuth callback params in URL
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const metaSuccess = params.get('meta_success');
    const metaError = params.get('meta_error');
    const metaUser = params.get('meta_user');
    const metaErrorDesc = params.get('meta_error_description');

    if (metaSuccess === 'true') {
      setSuccessMessage(`Successfully connected Facebook account${metaUser ? ` for ${metaUser}` : ''}`);
      // Clean up URL
      window.history.replaceState({}, '', window.location.pathname);
      // Refresh status
      queryClient.invalidateQueries({ queryKey: ['meta-auth-status'] });
    } else if (metaError) {
      setErrorMessage(metaErrorDesc || `OAuth error: ${metaError}`);
      window.history.replaceState({}, '', window.location.pathname);
    }
  }, [queryClient]);

  // Fetch Meta auth status
  const { data: metaStatus, isLoading: isLoadingStatus } = useQuery({
    queryKey: ['meta-auth-status'],
    queryFn: getMetaAuthStatus,
    staleTime: 30_000,
  });

  // Connect mutation
  const connectMutation = useMutation({
    mutationFn: getMetaAuthUrl,
    onSuccess: (data) => {
      // Redirect to Facebook OAuth
      window.location.href = data.auth_url;
    },
    onError: (error: Error) => {
      setErrorMessage(error.message || 'Failed to initiate Facebook login');
    },
  });

  // Disconnect mutation
  const disconnectMutation = useMutation({
    mutationFn: disconnectMeta,
    onSuccess: () => {
      setSuccessMessage('Facebook account disconnected');
      queryClient.invalidateQueries({ queryKey: ['meta-auth-status'] });
    },
    onError: (error: Error) => {
      setErrorMessage(error.message || 'Failed to disconnect');
    },
  });

  const handleConnect = () => {
    setErrorMessage(null);
    setSuccessMessage(null);
    connectMutation.mutate();
  };

  const handleDisconnect = () => {
    setErrorMessage(null);
    setSuccessMessage(null);
    if (window.confirm('Are you sure you want to disconnect your Facebook account?')) {
      disconnectMutation.mutate();
    }
  };

  const isConnecting = connectMutation.isPending;
  const isDisconnecting = disconnectMutation.isPending;

  return (
    <div className="meta-connection-card">
      <div className="meta-card-header">
        <div className="meta-title">
          <span className="meta-icon">
            <svg viewBox="0 0 24 24" width="24" height="24" fill="currentColor">
              <path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"/>
            </svg>
          </span>
          <div>
            <h3>Meta/Facebook Connection</h3>
            <p className="meta-subtitle">Required for Meta Ad Library API access</p>
          </div>
        </div>
        <div className="meta-status">
          {isLoadingStatus ? (
            <span className="status-badge loading">Checking...</span>
          ) : metaStatus?.connected ? (
            <span className="status-badge connected">Connected</span>
          ) : (
            <span className="status-badge disconnected">Not Connected</span>
          )}
        </div>
      </div>

      {/* Success/Error Messages */}
      {successMessage && (
        <div className="meta-message success">
          {successMessage}
          <button className="close-btn" onClick={() => setSuccessMessage(null)}>×</button>
        </div>
      )}
      {errorMessage && (
        <div className="meta-message error">
          {errorMessage}
          <button className="close-btn" onClick={() => setErrorMessage(null)}>×</button>
        </div>
      )}

      <div className="meta-card-content">
        {metaStatus?.connected ? (
          <>
            <div className="meta-details">
              <div className="meta-detail-row">
                <span className="detail-label">Connected As</span>
                <span className="detail-value">{metaStatus.fb_user_name || 'Unknown'}</span>
              </div>
              {metaStatus.expires_at && (
                <div className="meta-detail-row">
                  <span className="detail-label">Token Expires</span>
                  <span className={`detail-value ${metaStatus.expires_soon ? 'warning' : ''}`}>
                    {metaStatus.days_until_expiry !== null
                      ? `${metaStatus.days_until_expiry} days`
                      : new Date(metaStatus.expires_at).toLocaleDateString()}
                    {metaStatus.expires_soon && ' (Expiring soon!)'}
                  </span>
                </div>
              )}
              {metaStatus.scopes && metaStatus.scopes.length > 0 && (
                <div className="meta-detail-row">
                  <span className="detail-label">Permissions</span>
                  <span className="detail-value scopes">
                    {metaStatus.scopes.join(', ')}
                  </span>
                </div>
              )}
            </div>
            <div className="meta-actions">
              {metaStatus.expires_soon && (
                <button
                  className="btn btn-primary"
                  onClick={handleConnect}
                  disabled={isConnecting}
                >
                  {isConnecting ? 'Connecting...' : 'Reconnect'}
                </button>
              )}
              <button
                className="btn btn-secondary"
                onClick={handleDisconnect}
                disabled={isDisconnecting}
              >
                {isDisconnecting ? 'Disconnecting...' : 'Disconnect'}
              </button>
            </div>
          </>
        ) : (
          <>
            <p className="meta-description">
              Connect your Facebook account to enable Meta Ad Library ingestion.
              This is required to fetch political and social issue ads data.
            </p>
            <div className="meta-actions">
              <button
                className="btn btn-primary btn-facebook"
                onClick={handleConnect}
                disabled={isConnecting}
              >
                {isConnecting ? 'Connecting...' : 'Connect Facebook'}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function DataSourceCard({ source }: { source: DataSourceInfo }) {
  const statusIcon = source.enabled ? '✅' : '⏸️';
  const statusText = source.enabled ? 'Enabled' : 'Disabled';

  return (
    <div className={`source-card ${source.enabled ? 'enabled' : 'disabled'}`}>
      <div className="source-header">
        <div className="source-title">
          <span className="source-icon">{statusIcon}</span>
          <h3>{source.name}</h3>
        </div>
        <span className={`status-badge ${source.enabled ? 'enabled' : 'disabled'}`}>
          {statusText}
        </span>
      </div>

      <p className="source-description">{source.description}</p>

      <div className="source-details">
        {source.requires_api_key && (
          <div className="detail-row">
            <span className="detail-label">API Key</span>
            <span className={`detail-value ${source.has_api_key ? 'success' : 'warning'}`}>
              {source.has_api_key ? 'Configured' : 'Not configured'}
            </span>
          </div>
        )}

        {source.api_key_env_var && (
          <div className="detail-row">
            <span className="detail-label">Environment Variable</span>
            <code className="detail-value code">{source.api_key_env_var}</code>
          </div>
        )}

        {source.feature_flag && (
          <div className="detail-row">
            <span className="detail-label">Feature Flag</span>
            <code className="detail-value code">{source.feature_flag}</code>
          </div>
        )}

        <div className="detail-row">
          <span className="detail-label">Last Run</span>
          <span className="detail-value">
            {source.last_successful_run
              ? new Date(source.last_successful_run).toLocaleString()
              : 'Never'}
          </span>
        </div>

        <div className="detail-row">
          <span className="detail-label">Records</span>
          <span className="detail-value">{source.records_total.toLocaleString()}</span>
        </div>
      </div>
    </div>
  );
}

// =========================
// Connections Tab
// =========================

function ConnectionsTab({
  connections,
  onRefresh,
  isRefreshing,
}: {
  connections: ConnectionInfo[];
  onRefresh: () => void;
  isRefreshing: boolean;
}) {
  const allHealthy = connections.every((c) => c.status === 'healthy');

  return (
    <div className="tab-panel">
      <div className="panel-header">
        <h2>Service Connections</h2>
        <div className="panel-actions">
          <span className={`health-badge ${allHealthy ? 'healthy' : 'unhealthy'}`}>
            {allHealthy ? 'All Healthy' : 'Issues Detected'}
          </span>
          <button
            className="btn btn-secondary btn-sm"
            onClick={onRefresh}
            disabled={isRefreshing}
          >
            {isRefreshing ? 'Checking...' : 'Refresh'}
          </button>
        </div>
      </div>

      <p className="panel-description">
        Connection settings are configured via environment variables. Restart the server
        after changing connection parameters.
      </p>

      <div className="connections-list">
        {connections.map((conn) => (
          <ConnectionCard key={conn.name} connection={conn} />
        ))}
      </div>
    </div>
  );
}

function ConnectionCard({ connection }: { connection: ConnectionInfo }) {
  const statusConfig: Record<ConnectionStatusType, { icon: string; color: string }> = {
    healthy: { icon: '✅', color: 'var(--color-success)' },
    unhealthy: { icon: '❌', color: 'var(--color-danger)' },
    unknown: { icon: '❓', color: 'var(--color-warning)' },
  };

  const { icon, color } = statusConfig[connection.status];

  return (
    <div className={`connection-card ${connection.status}`}>
      <div className="connection-status" style={{ borderLeftColor: color }}>
        <span className="status-icon">{icon}</span>
        <div className="connection-info">
          <h3>{connection.name}</h3>
          <p className="connection-host">
            {connection.host}
            {connection.port && `:${connection.port}`}
          </p>
        </div>
        <div className="connection-metrics">
          {connection.latency_ms !== null && (
            <span className="latency">{connection.latency_ms}ms</span>
          )}
        </div>
      </div>
      {connection.error && (
        <div className="connection-error">
          <strong>Error:</strong> {connection.error}
        </div>
      )}
    </div>
  );
}

// =========================
// API Configuration Tab
// =========================

function APIConfigTab({ config }: { config: SettingsResponse['api'] }) {
  return (
    <div className="tab-panel">
      <div className="panel-header">
        <h2>API Configuration</h2>
      </div>

      <p className="panel-description">
        These settings are configured via environment variables and cannot be changed at runtime.
      </p>

      <div className="config-table">
        <table>
          <tbody>
            <tr>
              <td className="config-label">Environment</td>
              <td className="config-value">
                <span className="env-badge" data-env={config.environment}>
                  {config.environment}
                </span>
              </td>
            </tr>
            <tr>
              <td className="config-label">API Host</td>
              <td className="config-value">
                <code>{config.api_host}</code>
              </td>
            </tr>
            <tr>
              <td className="config-label">API Port</td>
              <td className="config-value">
                <code>{config.api_port}</code>
              </td>
            </tr>
            <tr>
              <td className="config-label">Debug Mode</td>
              <td className="config-value">
                <span className={`bool-badge ${config.debug_mode ? 'true' : 'false'}`}>
                  {config.debug_mode ? 'Enabled' : 'Disabled'}
                </span>
              </td>
            </tr>
            <tr>
              <td className="config-label">Log Level</td>
              <td className="config-value">
                <code>{config.log_level}</code>
              </td>
            </tr>
            <tr>
              <td className="config-label">CORS Origins</td>
              <td className="config-value">
                <div className="origins-list">
                  {config.cors_origins.map((origin, i) => (
                    <code key={i}>{origin}</code>
                  ))}
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

// =========================
// Styles
// =========================

const styles = `
  .settings-page {
    max-width: 1200px;
    margin: 0 auto;
  }

  .page-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: var(--spacing-lg);
  }

  .page-title h1 {
    margin: 0;
    font-size: 1.75rem;
    color: var(--text-primary);
  }

  .page-title .subtitle {
    margin: var(--spacing-xs) 0 0;
    color: var(--text-muted);
    font-size: 0.875rem;
  }

  .header-actions {
    display: flex;
    align-items: center;
    gap: var(--spacing-md);
  }

  .env-badge {
    padding: var(--spacing-xs) var(--spacing-sm);
    border-radius: var(--border-radius);
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
  }

  .env-badge[data-env="development"] {
    background: var(--color-warning-bg, #fef3c7);
    color: var(--color-warning-text, #92400e);
  }

  .env-badge[data-env="staging"] {
    background: var(--color-info-bg, #dbeafe);
    color: var(--color-info-text, #1e40af);
  }

  .env-badge[data-env="production"] {
    background: var(--color-success-bg, #d1fae5);
    color: var(--color-success-text, #065f46);
  }

  .loading-container,
  .error-container {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: var(--spacing-xl);
    text-align: center;
    min-height: 300px;
  }

  .spinner {
    width: 40px;
    height: 40px;
    border: 3px solid var(--border-color);
    border-top-color: var(--color-primary);
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }

  /* Tabs */
  .tabs {
    display: flex;
    gap: var(--spacing-xs);
    border-bottom: 1px solid var(--border-color);
    margin-bottom: var(--spacing-lg);
  }

  .tab {
    padding: var(--spacing-sm) var(--spacing-md);
    background: none;
    border: none;
    border-bottom: 2px solid transparent;
    color: var(--text-secondary);
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease;
  }

  .tab:hover {
    color: var(--text-primary);
    background: var(--bg-tertiary);
  }

  .tab.active {
    color: var(--color-primary);
    border-bottom-color: var(--color-primary);
  }

  /* Tab Panel */
  .tab-panel {
    animation: fadeIn 0.2s ease;
  }

  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(4px); }
    to { opacity: 1; transform: translateY(0); }
  }

  .panel-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: var(--spacing-md);
  }

  .panel-header h2 {
    margin: 0;
    font-size: 1.25rem;
    color: var(--text-primary);
  }

  .panel-stats {
    display: flex;
    gap: var(--spacing-md);
  }

  .panel-stats .stat {
    font-size: 0.875rem;
    font-weight: 500;
  }

  .panel-stats .stat.enabled {
    color: var(--color-success);
  }

  .panel-stats .stat.disabled {
    color: var(--text-muted);
  }

  .panel-actions {
    display: flex;
    align-items: center;
    gap: var(--spacing-md);
  }

  .panel-description {
    color: var(--text-secondary);
    font-size: 0.875rem;
    margin-bottom: var(--spacing-lg);
    max-width: 700px;
  }

  /* Health Badge */
  .health-badge {
    padding: var(--spacing-xs) var(--spacing-sm);
    border-radius: var(--border-radius);
    font-size: 0.75rem;
    font-weight: 600;
  }

  .health-badge.healthy {
    background: var(--color-success-bg, #d1fae5);
    color: var(--color-success-text, #065f46);
  }

  .health-badge.unhealthy {
    background: var(--color-danger-bg, #fee2e2);
    color: var(--color-danger-text, #991b1b);
  }

  /* Sources Grid */
  .sources-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: var(--spacing-md);
  }

  .source-card {
    background: var(--bg-primary);
    border: 1px solid var(--border-color);
    border-radius: var(--border-radius);
    padding: var(--spacing-md);
    transition: box-shadow 0.2s ease;
  }

  .source-card:hover {
    box-shadow: var(--shadow-sm);
  }

  .source-card.disabled {
    opacity: 0.7;
  }

  .source-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: var(--spacing-sm);
  }

  .source-title {
    display: flex;
    align-items: center;
    gap: var(--spacing-sm);
  }

  .source-icon {
    font-size: 1.25rem;
  }

  .source-title h3 {
    margin: 0;
    font-size: 1rem;
    color: var(--text-primary);
  }

  .status-badge {
    padding: 2px var(--spacing-sm);
    border-radius: var(--border-radius);
    font-size: 0.75rem;
    font-weight: 500;
  }

  .status-badge.enabled {
    background: var(--color-success-bg, #d1fae5);
    color: var(--color-success-text, #065f46);
  }

  .status-badge.disabled {
    background: var(--bg-tertiary);
    color: var(--text-muted);
  }

  .source-description {
    font-size: 0.875rem;
    color: var(--text-secondary);
    margin-bottom: var(--spacing-md);
  }

  .source-details {
    display: flex;
    flex-direction: column;
    gap: var(--spacing-xs);
    padding-top: var(--spacing-sm);
    border-top: 1px solid var(--border-color);
  }

  .detail-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.8125rem;
  }

  .detail-label {
    color: var(--text-muted);
  }

  .detail-value {
    color: var(--text-primary);
  }

  .detail-value.success {
    color: var(--color-success);
  }

  .detail-value.warning {
    color: var(--color-warning);
  }

  .detail-value.code {
    font-family: monospace;
    font-size: 0.75rem;
    background: var(--bg-tertiary);
    padding: 2px var(--spacing-xs);
    border-radius: 3px;
  }

  /* Connections List */
  .connections-list {
    display: flex;
    flex-direction: column;
    gap: var(--spacing-md);
  }

  .connection-card {
    background: var(--bg-primary);
    border: 1px solid var(--border-color);
    border-radius: var(--border-radius);
    overflow: hidden;
  }

  .connection-status {
    display: flex;
    align-items: center;
    gap: var(--spacing-md);
    padding: var(--spacing-md);
    border-left: 4px solid;
  }

  .connection-card.healthy .connection-status {
    border-left-color: var(--color-success);
  }

  .connection-card.unhealthy .connection-status {
    border-left-color: var(--color-danger);
  }

  .connection-card.unknown .connection-status {
    border-left-color: var(--color-warning);
  }

  .status-icon {
    font-size: 1.5rem;
  }

  .connection-info {
    flex: 1;
  }

  .connection-info h3 {
    margin: 0;
    font-size: 1rem;
    color: var(--text-primary);
  }

  .connection-host {
    margin: var(--spacing-xs) 0 0;
    font-size: 0.875rem;
    color: var(--text-muted);
    font-family: monospace;
  }

  .connection-metrics {
    display: flex;
    align-items: center;
    gap: var(--spacing-md);
  }

  .latency {
    font-size: 0.875rem;
    color: var(--text-secondary);
    font-family: monospace;
  }

  .connection-error {
    padding: var(--spacing-sm) var(--spacing-md);
    background: var(--color-danger-bg, #fee2e2);
    color: var(--color-danger-text, #991b1b);
    font-size: 0.8125rem;
  }

  /* Config Table */
  .config-table {
    background: var(--bg-primary);
    border: 1px solid var(--border-color);
    border-radius: var(--border-radius);
    overflow: hidden;
  }

  .config-table table {
    width: 100%;
    border-collapse: collapse;
  }

  .config-table tr:not(:last-child) {
    border-bottom: 1px solid var(--border-color);
  }

  .config-table td {
    padding: var(--spacing-md);
  }

  .config-label {
    width: 200px;
    font-weight: 500;
    color: var(--text-secondary);
    background: var(--bg-secondary);
  }

  .config-value {
    color: var(--text-primary);
  }

  .config-value code {
    font-family: monospace;
    font-size: 0.875rem;
    background: var(--bg-tertiary);
    padding: 2px var(--spacing-xs);
    border-radius: 3px;
  }

  .bool-badge {
    padding: 2px var(--spacing-sm);
    border-radius: var(--border-radius);
    font-size: 0.75rem;
    font-weight: 500;
  }

  .bool-badge.true {
    background: var(--color-success-bg, #d1fae5);
    color: var(--color-success-text, #065f46);
  }

  .bool-badge.false {
    background: var(--bg-tertiary);
    color: var(--text-muted);
  }

  .origins-list {
    display: flex;
    flex-wrap: wrap;
    gap: var(--spacing-xs);
  }

  /* Button Styles */
  .btn-sm {
    padding: var(--spacing-xs) var(--spacing-sm);
    font-size: 0.8125rem;
  }

  /* Meta Connection Card */
  .meta-connection-card {
    background: var(--bg-primary);
    border: 1px solid var(--border-color);
    border-radius: var(--border-radius);
    padding: var(--spacing-lg);
    margin-bottom: var(--spacing-lg);
  }

  .meta-card-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: var(--spacing-md);
  }

  .meta-title {
    display: flex;
    align-items: center;
    gap: var(--spacing-md);
  }

  .meta-icon {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 40px;
    height: 40px;
    border-radius: var(--border-radius);
    background: #1877f2;
    color: white;
  }

  .meta-title h3 {
    margin: 0;
    font-size: 1.125rem;
    color: var(--text-primary);
  }

  .meta-subtitle {
    margin: var(--spacing-xs) 0 0;
    font-size: 0.875rem;
    color: var(--text-muted);
  }

  .meta-status .status-badge {
    padding: var(--spacing-xs) var(--spacing-sm);
    border-radius: var(--border-radius);
    font-size: 0.75rem;
    font-weight: 600;
  }

  .meta-status .status-badge.connected {
    background: var(--color-success-bg, #d1fae5);
    color: var(--color-success-text, #065f46);
  }

  .meta-status .status-badge.disconnected {
    background: var(--bg-tertiary);
    color: var(--text-muted);
  }

  .meta-status .status-badge.loading {
    background: var(--color-info-bg, #dbeafe);
    color: var(--color-info-text, #1e40af);
  }

  .meta-message {
    padding: var(--spacing-sm) var(--spacing-md);
    border-radius: var(--border-radius);
    margin-bottom: var(--spacing-md);
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.875rem;
  }

  .meta-message.success {
    background: var(--color-success-bg, #d1fae5);
    color: var(--color-success-text, #065f46);
  }

  .meta-message.error {
    background: var(--color-danger-bg, #fee2e2);
    color: var(--color-danger-text, #991b1b);
  }

  .meta-message .close-btn {
    background: none;
    border: none;
    font-size: 1.25rem;
    cursor: pointer;
    color: inherit;
    opacity: 0.7;
    padding: 0;
    line-height: 1;
  }

  .meta-message .close-btn:hover {
    opacity: 1;
  }

  .meta-card-content {
    display: flex;
    flex-direction: column;
    gap: var(--spacing-md);
  }

  .meta-description {
    font-size: 0.875rem;
    color: var(--text-secondary);
    margin: 0;
  }

  .meta-details {
    display: flex;
    flex-direction: column;
    gap: var(--spacing-sm);
    padding: var(--spacing-md);
    background: var(--bg-secondary);
    border-radius: var(--border-radius);
  }

  .meta-detail-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.875rem;
  }

  .meta-detail-row .detail-label {
    color: var(--text-muted);
  }

  .meta-detail-row .detail-value {
    color: var(--text-primary);
    font-weight: 500;
  }

  .meta-detail-row .detail-value.warning {
    color: var(--color-warning);
  }

  .meta-detail-row .detail-value.scopes {
    font-family: monospace;
    font-size: 0.75rem;
    background: var(--bg-tertiary);
    padding: 2px var(--spacing-xs);
    border-radius: 3px;
  }

  .meta-actions {
    display: flex;
    gap: var(--spacing-sm);
  }

  .btn-facebook {
    background: #1877f2;
    border-color: #1877f2;
  }

  .btn-facebook:hover {
    background: #166fe5;
    border-color: #166fe5;
  }

  @media (max-width: 768px) {
    .page-header {
      flex-direction: column;
      gap: var(--spacing-md);
    }

    .sources-grid {
      grid-template-columns: 1fr;
    }

    .config-table td {
      display: block;
      width: 100%;
    }

    .config-label {
      width: 100%;
      padding-bottom: var(--spacing-xs);
    }

    .config-value {
      padding-top: 0;
    }
  }
`;
