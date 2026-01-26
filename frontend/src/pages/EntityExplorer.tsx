/**
 * Entity Explorer page for MITDS
 *
 * Search and explore entities with graph visualization and evidence.
 */

import { useState, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useParams, useSearchParams, useNavigate } from 'react-router-dom';
import {
  searchEntities,
  getEntity,
  getEntityRelationships,
  type Entity,
  type Relationship,
} from '../services/api';
import { EntityGraph, InfrastructureOverlap } from '../components/graph';
import { EvidencePanel } from '../components/evidence';

type ViewMode = 'detail' | 'graph' | 'evidence' | 'infrastructure';

export default function EntityExplorer() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const [searchQuery, setSearchQuery] = useState(searchParams.get('q') || '');
  const [viewMode, setViewMode] = useState<ViewMode>('detail');
  const [selectedRelationship, setSelectedRelationship] = useState<Relationship | null>(null);

  // Temporal state for timeline slider
  const [temporalEnabled, setTemporalEnabled] = useState(false);
  const [temporalDate, setTemporalDate] = useState<string>(() => {
    // Default to current date
    return new Date().toISOString().split('T')[0];
  });
  const [timelineMin] = useState(() => {
    // Default min: 5 years ago
    const d = new Date();
    d.setFullYear(d.getFullYear() - 5);
    return d.toISOString().split('T')[0];
  });
  const [timelineMax] = useState(() => {
    // Max: today
    return new Date().toISOString().split('T')[0];
  });

  // Search entities
  const { data: searchResults, isLoading: searchLoading } = useQuery({
    queryKey: ['entities', searchParams.get('q'), searchParams.get('type')],
    queryFn: () =>
      searchEntities({
        q: searchParams.get('q') || undefined,
        type: searchParams.get('type') || undefined,
        limit: 20,
        offset: 0,
      }),
    enabled: !!searchParams.get('q'),
  });

  // Get selected entity details
  const { data: selectedEntity, isLoading: entityLoading } = useQuery({
    queryKey: ['entity', id],
    queryFn: () => getEntity(id!),
    enabled: !!id,
  });

  // Get entity relationships for detail view
  const { data: relationships } = useQuery({
    queryKey: ['entity-relationships-list', id],
    queryFn: () => getEntityRelationships(id!, { direction: 'both' }),
    enabled: !!id && viewMode === 'detail',
  });

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setSearchParams({ q: searchQuery });
  };

  const handleEntitySelect = useCallback(
    (entityId: string) => {
      navigate(`/entities/${entityId}?${searchParams.toString()}`);
      setViewMode('detail');
      setSelectedRelationship(null);
    },
    [navigate, searchParams]
  );

  const handleNodeClick = useCallback(
    (nodeId: string) => {
      handleEntitySelect(nodeId);
    },
    [handleEntitySelect]
  );

  const handleEdgeClick = useCallback((rel: Relationship) => {
    setSelectedRelationship(rel);
  }, []);

  const handleTypeFilter = (type: string) => {
    const newParams: Record<string, string> = {};
    searchParams.forEach((value, key) => {
      newParams[key] = value;
    });
    if (type) {
      newParams.type = type;
    } else {
      delete newParams.type;
    }
    setSearchParams(newParams);
  };

  const formatProperties = (props: Record<string, unknown> | undefined): string[] => {
    if (!props) return [];
    return Object.entries(props)
      .filter(([, v]) => v != null && v !== '')
      .map(([k, v]) => `${k}: ${String(v)}`);
  };

  return (
    <div className="entity-explorer">
      <header className="page-header">
        <h1>Entity Explorer</h1>
        <p>Search and explore organizations, persons, and media outlets</p>
      </header>

      {/* Search Form */}
      <div className="card mb-lg">
        <form onSubmit={handleSearch} className="search-form">
          <div className="search-input-group">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Search by name, alias, or identifier..."
            />
            <button type="submit" className="btn btn-primary">
              Search
            </button>
          </div>
          <div className="search-filters">
            <select
              value={searchParams.get('type') || ''}
              onChange={(e) => handleTypeFilter(e.target.value)}
              aria-label="Filter by entity type"
              title="Filter by entity type"
            >
              <option value="">All Types</option>
              <option value="ORGANIZATION">Organizations</option>
              <option value="PERSON">Persons</option>
              <option value="OUTLET">Outlets</option>
              <option value="SPONSOR">Sponsors</option>
            </select>
          </div>
        </form>
      </div>

      <div className="explorer-layout">
        {/* Search Results Panel */}
        <div className="results-panel">
          <h2>
            Results
            {searchResults && ` (${searchResults.total})`}
          </h2>
          <div className="card">
            {searchLoading ? (
              <div className="loading">
                <div className="spinner" />
                <span>Searching...</span>
              </div>
            ) : searchResults?.results.length ? (
              <ul className="entity-list">
                {searchResults.results.map((entity: Entity) => (
                  <li
                    key={entity.id}
                    className={`entity-item ${id === entity.id ? 'selected' : ''}`}
                  >
                    <a
                      href={`/entities/${entity.id}?${searchParams.toString()}`}
                      onClick={(e) => {
                        e.preventDefault();
                        handleEntitySelect(entity.id);
                      }}
                    >
                      <span className="entity-type-badge">{entity.entity_type}</span>
                      <span className="entity-name">{entity.name}</span>
                      <span className="entity-confidence">
                        {(entity.confidence * 100).toFixed(0)}%
                      </span>
                    </a>
                  </li>
                ))}
              </ul>
            ) : searchParams.get('q') ? (
              <div className="empty-state">
                <p>No entities found matching your search.</p>
              </div>
            ) : (
              <div className="empty-state">
                <p>Enter a search query to find entities.</p>
              </div>
            )}
          </div>
        </div>

        {/* Main Content Area */}
        <div className="main-panel">
          {/* View Mode Tabs */}
          {id && (
            <div className="view-tabs">
              <button
                type="button"
                className={`tab ${viewMode === 'detail' ? 'active' : ''}`}
                onClick={() => setViewMode('detail')}
              >
                Details
              </button>
              <button
                type="button"
                className={`tab ${viewMode === 'graph' ? 'active' : ''}`}
                onClick={() => setViewMode('graph')}
              >
                Graph View
              </button>
              <button
                type="button"
                className={`tab ${viewMode === 'evidence' ? 'active' : ''}`}
                onClick={() => setViewMode('evidence')}
              >
                Evidence
              </button>
              <button
                type="button"
                className={`tab ${viewMode === 'infrastructure' ? 'active' : ''}`}
                onClick={() => setViewMode('infrastructure')}
              >
                Infrastructure
              </button>
            </div>
          )}

          <div className="view-content card">
            {entityLoading ? (
              <div className="loading">
                <div className="spinner" />
                <span>Loading entity...</span>
              </div>
            ) : !id ? (
              <div className="empty-state">
                <p>Select an entity to view details.</p>
              </div>
            ) : viewMode === 'detail' && selectedEntity ? (
              <div className="entity-detail">
                <div className="detail-header">
                  <h3>{selectedEntity.name}</h3>
                  <span className="entity-type-badge large">
                    {selectedEntity.entity_type}
                  </span>
                </div>

                <dl className="entity-properties">
                  <dt>ID</dt>
                  <dd className="monospace">{selectedEntity.id}</dd>

                  <dt>Confidence</dt>
                  <dd>{(selectedEntity.confidence * 100).toFixed(0)}%</dd>

                  <dt>Created</dt>
                  <dd>{new Date(selectedEntity.created_at).toLocaleString()}</dd>

                  {selectedEntity.updated_at && (
                    <>
                      <dt>Updated</dt>
                      <dd>{new Date(selectedEntity.updated_at).toLocaleString()}</dd>
                    </>
                  )}

                  {selectedEntity.aliases && selectedEntity.aliases.length > 0 && (
                    <>
                      <dt>Aliases</dt>
                      <dd>{selectedEntity.aliases.join(', ')}</dd>
                    </>
                  )}

                  {selectedEntity.properties &&
                    formatProperties(selectedEntity.properties).map((prop) => {
                      const [key, value] = prop.split(': ');
                      return (
                        <div key={key} className="property-row">
                          <dt>{key}</dt>
                          <dd>{value}</dd>
                        </div>
                      );
                    })}
                </dl>

                {/* Relationships Summary */}
                {relationships?.relationships && relationships.relationships.length > 0 && (
                  <div className="relationships-summary">
                    <h4>Relationships ({relationships.total})</h4>
                    <ul className="relationship-list">
                      {relationships.relationships.slice(0, 5).map((rel) => (
                        <li key={rel.id} className="relationship-item">
                          <span className="rel-type-badge">{rel.rel_type}</span>
                          <span className="rel-direction">
                            {rel.source_entity.id === id ? '→' : '←'}
                          </span>
                          <a
                            href={`/entities/${
                              rel.source_entity.id === id
                                ? rel.target_entity.id
                                : rel.source_entity.id
                            }`}
                            onClick={(e) => {
                              e.preventDefault();
                              handleEntitySelect(
                                rel.source_entity.id === id
                                  ? rel.target_entity.id
                                  : rel.source_entity.id
                              );
                            }}
                          >
                            {rel.source_entity.id === id
                              ? rel.target_entity.name
                              : rel.source_entity.name}
                          </a>
                        </li>
                      ))}
                    </ul>
                    {relationships.total > 5 && (
                      <button
                        type="button"
                        className="btn btn-text"
                        onClick={() => setViewMode('graph')}
                      >
                        View all in graph →
                      </button>
                    )}
                  </div>
                )}
              </div>
            ) : viewMode === 'graph' && id ? (
              <div className="graph-view">
                {/* Timeline Slider Controls */}
                <div className="timeline-controls">
                  <label className="timeline-toggle">
                    <input
                      type="checkbox"
                      checked={temporalEnabled}
                      onChange={(e) => setTemporalEnabled(e.target.checked)}
                    />
                    <span>Historical View</span>
                  </label>
                  {temporalEnabled && (
                    <div className="timeline-slider-container">
                      <span className="timeline-label">{timelineMin}</span>
                      <input
                        type="range"
                        className="timeline-slider"
                        aria-label="Timeline date slider"
                        title="Select historical date"
                        min={new Date(timelineMin).getTime()}
                        max={new Date(timelineMax).getTime()}
                        value={new Date(temporalDate).getTime()}
                        onChange={(e) => {
                          const date = new Date(Number(e.target.value));
                          setTemporalDate(date.toISOString().split('T')[0]);
                        }}
                      />
                      <span className="timeline-label">{timelineMax}</span>
                      <input
                        type="date"
                        className="timeline-date-input"
                        aria-label="Select date"
                        title="Select specific date"
                        value={temporalDate}
                        min={timelineMin}
                        max={timelineMax}
                        onChange={(e) => setTemporalDate(e.target.value)}
                      />
                    </div>
                  )}
                  {temporalEnabled && (
                    <div className="timeline-info">
                      Showing graph as of: <strong>{new Date(temporalDate).toLocaleDateString()}</strong>
                    </div>
                  )}
                </div>
                <EntityGraph
                  entityId={id}
                  onNodeClick={handleNodeClick}
                  onEdgeClick={handleEdgeClick}
                  height="500px"
                  asOf={temporalEnabled ? temporalDate : undefined}
                />
                {selectedRelationship && (
                  <div className="relationship-detail card">
                    <h4>Selected Relationship</h4>
                    <dl className="rel-properties">
                      <dt>Type</dt>
                      <dd>{selectedRelationship.rel_type}</dd>
                      <dt>From</dt>
                      <dd>{selectedRelationship.source_entity.name}</dd>
                      <dt>To</dt>
                      <dd>{selectedRelationship.target_entity.name}</dd>
                      {selectedRelationship.confidence && (
                        <>
                          <dt>Confidence</dt>
                          <dd>{(selectedRelationship.confidence * 100).toFixed(0)}%</dd>
                        </>
                      )}
                      {selectedRelationship.evidence_count && (
                        <>
                          <dt>Evidence</dt>
                          <dd>{selectedRelationship.evidence_count} sources</dd>
                        </>
                      )}
                    </dl>
                  </div>
                )}
              </div>
            ) : viewMode === 'evidence' && id ? (
              <div className="evidence-view">
                <EvidencePanel entityId={id} showHeader={false} maxItems={20} />
              </div>
            ) : viewMode === 'infrastructure' ? (
              <div className="infrastructure-view">
                <InfrastructureOverlap
                  initialDomains={
                    selectedEntity?.entity_type === 'OUTLET' && selectedEntity?.properties?.domains
                      ? (selectedEntity.properties.domains as string[])
                      : []
                  }
                  height="500px"
                />
              </div>
            ) : null}
          </div>
        </div>
      </div>

      <style>{`
        .search-form {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
        }

        .search-input-group {
          display: flex;
          gap: var(--spacing-sm);
        }

        .search-input-group input {
          flex: 1;
        }

        .search-filters {
          display: flex;
          gap: var(--spacing-sm);
        }

        .search-filters select {
          width: auto;
        }

        .explorer-layout {
          display: grid;
          grid-template-columns: 300px 1fr;
          gap: var(--spacing-md);
          min-height: 600px;
        }

        .results-panel h2,
        .main-panel h2 {
          margin-bottom: var(--spacing-sm);
          font-size: 1rem;
        }

        .entity-list {
          list-style: none;
          margin: 0;
          padding: 0;
          max-height: 550px;
          overflow-y: auto;
        }

        .entity-item {
          border-bottom: 1px solid var(--border-color);
        }

        .entity-item:last-child {
          border-bottom: none;
        }

        .entity-item a {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          padding: var(--spacing-sm);
          color: var(--text-primary);
          text-decoration: none;
        }

        .entity-item a:hover {
          background-color: var(--bg-tertiary);
        }

        .entity-item.selected a {
          background-color: var(--color-primary);
          color: white;
        }

        .entity-type-badge {
          font-size: 0.625rem;
          padding: 2px 6px;
          border-radius: 4px;
          background-color: var(--bg-tertiary);
          text-transform: uppercase;
        }

        .entity-type-badge.large {
          font-size: 0.75rem;
          padding: 4px 8px;
        }

        .entity-item.selected .entity-type-badge {
          background-color: rgba(255, 255, 255, 0.2);
        }

        .entity-name {
          flex: 1;
          font-weight: 500;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
        }

        .entity-confidence {
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .view-tabs {
          display: flex;
          gap: 4px;
          margin-bottom: var(--spacing-sm);
        }

        .tab {
          padding: var(--spacing-sm) var(--spacing-md);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius) var(--border-radius) 0 0;
          background: var(--bg-secondary);
          color: var(--text-secondary);
          cursor: pointer;
          font-size: 0.875rem;
          transition: all 0.15s ease;
        }

        .tab:hover {
          background: var(--bg-tertiary);
        }

        .tab.active {
          background: var(--bg-primary);
          border-bottom-color: var(--bg-primary);
          color: var(--text-primary);
          font-weight: 500;
        }

        .view-content {
          min-height: 500px;
        }

        .detail-header {
          display: flex;
          align-items: center;
          gap: var(--spacing-md);
          margin-bottom: var(--spacing-md);
        }

        .detail-header h3 {
          margin: 0;
          flex: 1;
        }

        .entity-properties {
          display: grid;
          grid-template-columns: 120px 1fr;
          gap: var(--spacing-xs) var(--spacing-md);
          margin: var(--spacing-md) 0;
        }

        .entity-properties dt {
          font-weight: 500;
          color: var(--text-secondary);
        }

        .entity-properties dd {
          color: var(--text-primary);
          word-break: break-word;
        }

        .monospace {
          font-family: ui-monospace, monospace;
          font-size: 0.875rem;
        }

        .relationships-summary {
          margin-top: var(--spacing-lg);
          padding-top: var(--spacing-lg);
          border-top: 1px solid var(--border-color);
        }

        .relationships-summary h4 {
          margin-bottom: var(--spacing-sm);
        }

        .relationship-list {
          list-style: none;
          margin: 0;
          padding: 0;
        }

        .relationship-item {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          padding: var(--spacing-xs) 0;
        }

        .rel-type-badge {
          font-size: 0.625rem;
          padding: 2px 6px;
          border-radius: 4px;
          background-color: var(--bg-tertiary);
          text-transform: uppercase;
        }

        .rel-direction {
          color: var(--text-muted);
        }

        .relationship-item a {
          color: var(--color-primary);
          text-decoration: none;
        }

        .relationship-item a:hover {
          text-decoration: underline;
        }

        .graph-view {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
        }

        .timeline-controls {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
          margin-bottom: var(--spacing-sm);
        }

        .timeline-toggle {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          cursor: pointer;
          font-weight: 500;
        }

        .timeline-toggle input[type="checkbox"] {
          width: 18px;
          height: 18px;
          cursor: pointer;
        }

        .timeline-slider-container {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          flex-wrap: wrap;
        }

        .timeline-slider {
          flex: 1;
          min-width: 200px;
          height: 8px;
          cursor: pointer;
          accent-color: var(--color-primary);
        }

        .timeline-label {
          font-size: 0.75rem;
          color: var(--text-muted);
          white-space: nowrap;
        }

        .timeline-date-input {
          padding: 4px 8px;
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-size: 0.875rem;
          background: var(--bg-primary);
          color: var(--text-primary);
        }

        .timeline-info {
          font-size: 0.875rem;
          color: var(--text-secondary);
          padding: var(--spacing-xs) 0;
        }

        .timeline-info strong {
          color: var(--color-primary);
        }

        .relationship-detail {
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
        }

        .relationship-detail h4 {
          margin-bottom: var(--spacing-sm);
        }

        .rel-properties {
          display: grid;
          grid-template-columns: 80px 1fr;
          gap: var(--spacing-xs) var(--spacing-sm);
          font-size: 0.875rem;
        }

        .evidence-view {
          padding: var(--spacing-sm);
        }

        .infrastructure-view {
          padding: var(--spacing-sm);
        }

        .btn-text {
          background: none;
          border: none;
          color: var(--color-primary);
          cursor: pointer;
          font-size: 0.875rem;
          padding: 0;
          margin-top: var(--spacing-sm);
        }

        .btn-text:hover {
          text-decoration: underline;
        }

        .loading {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: var(--spacing-md);
          padding: var(--spacing-lg);
          min-height: 200px;
        }

        .empty-state {
          text-align: center;
          padding: var(--spacing-lg);
          color: var(--text-muted);
          min-height: 200px;
          display: flex;
          align-items: center;
          justify-content: center;
        }

        @media (max-width: 900px) {
          .explorer-layout {
            grid-template-columns: 1fr;
          }

          .entity-list {
            max-height: 300px;
          }
        }
      `}</style>
    </div>
  );
}
