/**
 * Entity Explorer page for MITDS
 *
 * Search and explore entities with graph visualization and evidence.
 */

import { useState, useCallback } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
import { useParams, useSearchParams, useNavigate } from 'react-router-dom';
import {
  searchEntities,
  getEntity,
  getEntityRelationships,
  getBoardInterlocks,
  getRelationshipChanges,
  getRelationshipTimeline,
  getFundingPaths,
  getFundingRecipients,
  getFundingSources,
  findConnectingEntities,
  getSharedFunders,
  quickIngest,
  triggerLinkedInIngestion,
  getLinkedInStatus,
  type Entity,
  type Relationship,
  type BoardInterlock,
  type EntitySummary,
  type LinkedInStatusResponse,
  type EntityType,
  type QuickIngestResponse,
  type QuickIngestResponseExternalMatchesItem,
} from '@/api';
import { EntityGraph, InfrastructureOverlap } from '../components/graph';
import { EvidencePanel } from '../components/evidence';

type ViewMode = 'detail' | 'graph' | 'evidence' | 'infrastructure' | 'interlocks' | 'funding' | 'changes';

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

  // Pagination state
  const [offset, setOffset] = useState(0);
  const pageSize = 20;

  // Funding tab state
  const [fundingFiscalYear, setFundingFiscalYear] = useState<number | undefined>(undefined);

  // Relationship changes tab state
  const [changesFromDate, setChangesFromDate] = useState(() => {
    const d = new Date();
    d.setFullYear(d.getFullYear() - 1);
    return d.toISOString().split('T')[0];
  });
  const [changesToDate, setChangesToDate] = useState(() => new Date().toISOString().split('T')[0]);

  // Relationship timeline state
  const [timelineRelationship, setTimelineRelationship] = useState<Relationship | null>(null);

  // Connecting entities state
  const [connectorEntityIds, setConnectorEntityIds] = useState<EntitySummary[]>([]);
  const [connectorSearchQuery, setConnectorSearchQuery] = useState('');
  const [connectorMaxHops, setConnectorMaxHops] = useState(2);
  const [showConnectorPanel, setShowConnectorPanel] = useState(false);

  // Shared funders state (used in funding tab when multiple entities selected)
  const [sharedFunderIds, setSharedFunderIds] = useState<EntitySummary[]>([]);
  const [sharedFunderSearch, setSharedFunderSearch] = useState('');
  const [showSharedFunders, setShowSharedFunders] = useState(false);

  // Quick ingest state
  const [quickIngestJurisdiction, setQuickIngestJurisdiction] = useState<string>('');
  const [quickIngestResult, setQuickIngestResult] = useState<QuickIngestResponse | null>(null);
  const [discoverExecutives, setDiscoverExecutives] = useState<boolean>(false);
  
  // LinkedIn discovery modal state
  const [showLinkedInModal, setShowLinkedInModal] = useState(false);
  const [linkedInMethod, setLinkedInMethod] = useState<'csv' | 'scrape'>('csv');
  const [linkedInCsvFile, setLinkedInCsvFile] = useState<File | null>(null);
  const [linkedInSessionCookie, setLinkedInSessionCookie] = useState('');
  const [linkedInTitlesFilter, setLinkedInTitlesFilter] = useState('CEO,CFO,Director,President,VP,Board');
  const [linkedInStatus, setLinkedInStatus] = useState<LinkedInStatusResponse | null>(null);

  // Search entities
  const { data: searchResults, isLoading: searchLoading } = useQuery({
    queryKey: ['entities', searchParams.get('q'), searchParams.get('type'), offset],
    queryFn: () =>
      searchEntities({
        q: searchParams.get('q') || undefined,
        type: (searchParams.get('type') as EntityType) || undefined,
        limit: pageSize,
        offset,
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

  // Get board interlocks
  const { data: interlockData, isLoading: interlocksLoading } = useQuery({
    queryKey: ['entity-interlocks', id],
    queryFn: () => getBoardInterlocks(id!),
    enabled: !!id && viewMode === 'interlocks',
  });

  // Funding sources
  const { data: fundingSources, isLoading: fundingSourcesLoading } = useQuery({
    queryKey: ['funding-sources', id, fundingFiscalYear],
    queryFn: () => getFundingSources(id!, { fiscal_year: fundingFiscalYear, limit: 50 }),
    enabled: !!id && viewMode === 'funding',
  });

  // Funding recipients
  const { data: fundingRecipients, isLoading: fundingRecipientsLoading } = useQuery({
    queryKey: ['funding-recipients', id, fundingFiscalYear],
    queryFn: () => getFundingRecipients(id!, { fiscal_year: fundingFiscalYear, limit: 50 }),
    enabled: !!id && viewMode === 'funding',
  });

  // Funding paths
  const { data: fundingPaths, isLoading: fundingPathsLoading } = useQuery({
    queryKey: ['funding-paths', id],
    queryFn: () => getFundingPaths(id!, { max_hops: 3 }),
    enabled: !!id && viewMode === 'funding',
  });

  // Relationship changes
  const { data: relationshipChanges, isLoading: changesLoading } = useQuery({
    queryKey: ['relationship-changes', id, changesFromDate, changesToDate],
    queryFn: () => getRelationshipChanges({ entity_id: id!, from_date: changesFromDate, to_date: changesToDate }),
    enabled: !!id && viewMode === 'changes',
  });

  // Relationship timeline (for a specific relationship)
  const { data: relTimeline } = useQuery({
    queryKey: ['relationship-timeline', timelineRelationship?.source_entity?.id, timelineRelationship?.target_entity?.id, timelineRelationship?.rel_type],
    queryFn: () => getRelationshipTimeline({
      source_id: timelineRelationship?.source_entity?.id ?? '',
      target_id: timelineRelationship?.target_entity?.id ?? '',
      rel_type: timelineRelationship?.rel_type || undefined,
    }),
    enabled: !!timelineRelationship && !!timelineRelationship.source_entity?.id && !!timelineRelationship.target_entity?.id,
  });

  // Connecting entities search
  const { data: connectorSearchResults, isLoading: isConnectorSearching } = useQuery({
    queryKey: ['connector-search', connectorSearchQuery],
    queryFn: () => searchEntities({ q: connectorSearchQuery, limit: 10 }),
    enabled: connectorSearchQuery.length >= 2,
  });

  // Connecting entities query
  const { data: connectingData, isLoading: connectingLoading } = useQuery({
    queryKey: ['connecting-entities', connectorEntityIds.map((e) => e.id).join(','), connectorMaxHops],
    queryFn: () => findConnectingEntities({
      entity_ids: connectorEntityIds.map((e) => e.id),
    }),
    enabled: showConnectorPanel && connectorEntityIds.length >= 2,
  });

  // Shared funders entity search
  const { data: sharedFunderSearchResults, isLoading: isSharedFunderSearching } = useQuery({
    queryKey: ['shared-funder-search', sharedFunderSearch],
    queryFn: () => searchEntities({ q: sharedFunderSearch, limit: 10 }),
    enabled: sharedFunderSearch.length >= 2,
  });

  // Shared funders query
  const { data: sharedFundersData, isLoading: sharedFundersLoading } = useQuery({
    queryKey: ['shared-funders', sharedFunderIds.map((e) => e.id).join(',')],
    queryFn: () => getSharedFunders({
      entity_ids: sharedFunderIds.map((e) => e.id),
    }),
    enabled: showSharedFunders && sharedFunderIds.length >= 2,
  });

  // Quick ingest mutation
  const quickIngestMutation = useMutation({
    mutationFn: (data: Parameters<typeof quickIngest>[0]) => quickIngest(data),
    onSuccess: (data) => {
      setQuickIngestResult(data);
      if (data.found && data.entity_id) {
        // Navigate to the newly ingested entity
        handleEntitySelect(data.entity_id);
      }
    },
  });

  // LinkedIn ingestion mutation
  const [linkedInResult, setLinkedInResult] = useState<{ run_id?: string; status?: string; message?: string; profiles_found?: number; executives_found?: number } | null>(null);
  const linkedInMutation = useMutation({
    mutationFn: (data: Parameters<typeof triggerLinkedInIngestion>[0]) => triggerLinkedInIngestion(data),
    onSuccess: (data) => {
      setLinkedInResult({
        run_id: data.run_id,
        status: data.run_id ? 'running' : 'completed',
        message: 'LinkedIn ingestion triggered',
      });
      // Refresh the entity to show new relationships
      if (data.run_id && quickIngestResult?.entity_id) {
        handleEntitySelect(quickIngestResult.entity_id);
      }
    },
  });

  // Handle LinkedIn CSV file selection
  const handleLinkedInCsvSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      setLinkedInCsvFile(file);
    }
  };

  // Fetch LinkedIn status when modal opens
  const handleOpenLinkedInModal = async () => {
    setShowLinkedInModal(true);
    try {
      // Pass entity ID if available, otherwise use a placeholder
      const entityIdToCheck = id || quickIngestResult?.entity_id || 'status-check';
      const status = await getLinkedInStatus(entityIdToCheck);
      setLinkedInStatus(status);
      // If cookie is configured on server, default to scrape method
      if (status.configured) {
        setLinkedInMethod('scrape');
      }
    } catch {
      // If API fails, assume no cookie configured
      setLinkedInStatus({ configured: false, message: 'Could not check status', methods_available: ['csv_import'] });
    }
  };

  // Submit LinkedIn ingestion
  const handleLinkedInSubmit = async () => {
    if (!quickIngestResult?.entity_name) return;

    const titlesArray = linkedInTitlesFilter
      .split(',')
      .map(t => t.trim())
      .filter(t => t.length > 0);

    if (linkedInMethod === 'csv' && linkedInCsvFile) {
      // Read file as base64
      const reader = new FileReader();
      reader.onload = () => {
        const base64 = (reader.result as string).split(',')[1];
        linkedInMutation.mutate({
          company_name: quickIngestResult.entity_name!,
          company_entity_id: quickIngestResult.entity_id || undefined,
          csv_data: base64,
          titles_filter: titlesArray,
        });
      };
      reader.readAsDataURL(linkedInCsvFile);
    } else if (linkedInMethod === 'scrape') {
      // Cookie may be configured on server, or provided in UI
      linkedInMutation.mutate({
        company_name: quickIngestResult.entity_name!,
        company_entity_id: quickIngestResult.entity_id || undefined,
        scrape: true,
        session_cookie: linkedInSessionCookie || undefined, // Server will use env var if not provided
        titles_filter: titlesArray,
        limit: 50,
      });
    }
  };

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setOffset(0);
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
              <option value="AD">Ads</option>
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
            ) : searchResults?.results && searchResults.results.length > 0 ? (
              <ul className="entity-list">
                {/* Deduplicate results by ID to prevent duplicate key warnings */}
                {Array.from(
                  new Map(searchResults.results.map((e: Entity) => [e.id, e])).values()
                ).map((entity: Entity) => (
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
                
                {/* Quick Ingest Option */}
                <div className="quick-ingest-panel">
                  <h4>Not in the database?</h4>
                  <p>Search public records to add this corporation:</p>
                  
                  <div className="quick-ingest-form">
                    <select
                      value={quickIngestJurisdiction}
                      onChange={(e) => setQuickIngestJurisdiction(e.target.value)}
                      aria-label="Select jurisdiction"
                    >
                      <option value="">Any jurisdiction</option>
                      <option value="CA">Canada (Federal)</option>
                      <option value="US">United States</option>
                      <option value="ON">Ontario</option>
                      <option value="BC">British Columbia</option>
                      <option value="AB">Alberta</option>
                      <option value="QC">Quebec</option>
                    </select>
                    
                    <button
                      type="button"
                      className="btn btn-primary"
                      onClick={() => quickIngestMutation.mutate({
                        name: searchParams.get('q') || '',
                        jurisdiction: quickIngestJurisdiction || undefined,
                        discover_executives: discoverExecutives,
                      })}
                      disabled={quickIngestMutation.isPending}
                    >
                      {quickIngestMutation.isPending ? 'Searching...' : 'Search & Ingest'}
                    </button>
                  </div>
                  
                  <label className="discover-executives-toggle">
                    <input
                      type="checkbox"
                      checked={discoverExecutives}
                      onChange={(e) => setDiscoverExecutives(e.target.checked)}
                    />
                    <span>Also discover executives via LinkedIn</span>
                  </label>

                  {/* Quick Ingest Results */}
                  {quickIngestResult && (
                    <div className={`quick-ingest-result ${quickIngestResult.found ? 'success' : 'info'}`}>
                      <p>{quickIngestResult.message}</p>
                      
                      {quickIngestResult.found && quickIngestResult.entity_id && (
                        <div className="ingest-actions">
                          <button
                            type="button"
                            className="btn btn-link"
                            onClick={() => handleEntitySelect(quickIngestResult.entity_id!)}
                          >
                            View {quickIngestResult.entity_name} →
                          </button>
                          
                          {quickIngestResult.linkedin_available && (
                            <button
                              type="button"
                              className="btn btn-linkedin"
                              onClick={handleOpenLinkedInModal}
                            >
                              Discover Executives
                            </button>
                          )}
                        </div>
                      )}
                      
                      {quickIngestResult.sources_searched && quickIngestResult.sources_searched.length > 0 && (
                        <p className="text-muted sources-list">
                          Searched: {quickIngestResult.sources_searched.join(', ')}
                        </p>
                      )}
                      
                      {/* Show external matches if no direct ingestion */}
                      {!quickIngestResult.found && quickIngestResult.external_matches && quickIngestResult.external_matches.length > 0 && (
                        <div className="external-matches">
                          <p>Possible matches found:</p>
                          <ul>
                            {quickIngestResult.external_matches.map((match: QuickIngestResponseExternalMatchesItem, i: number) => {
                              const jurisdiction = match.jurisdiction as string | undefined;
                              return (
                                <li key={i}>
                                  <strong>{String(match.name || '')}</strong>
                                  <span className="match-source">({String(match.source || '')})</span>
                                  {jurisdiction && (
                                    <span className="match-jurisdiction">{jurisdiction}</span>
                                  )}
                                </li>
                              );
                            })}
                          </ul>
                        </div>
                      )}
                    </div>
                  )}
                  
                  {quickIngestMutation.isError && (
                    <div className="quick-ingest-result error">
                      <p>Error searching public records. Please try again.</p>
                    </div>
                  )}
                </div>
              </div>
            ) : (
              <div className="empty-state">
                <p>Enter a search query to find entities.</p>
              </div>
            )}

            {/* Pagination */}
            {searchResults && (searchResults.total ?? 0) > pageSize && (
              <div className="pagination-controls">
                <button
                  type="button"
                  className="btn btn-secondary btn-sm"
                  disabled={offset === 0}
                  onClick={() => setOffset(Math.max(0, offset - pageSize))}
                >
                  Previous
                </button>
                <span className="pagination-info">
                  {offset + 1}–{Math.min(offset + pageSize, searchResults.total ?? 0)} of {searchResults.total ?? 0}
                </span>
                <button
                  type="button"
                  className="btn btn-secondary btn-sm"
                  disabled={offset + pageSize >= (searchResults.total ?? 0)}
                  onClick={() => setOffset(offset + pageSize)}
                >
                  Next
                </button>
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
              <button
                type="button"
                className={`tab ${viewMode === 'funding' ? 'active' : ''}`}
                onClick={() => setViewMode('funding')}
              >
                Funding
              </button>
              <button
                type="button"
                className={`tab ${viewMode === 'changes' ? 'active' : ''}`}
                onClick={() => setViewMode('changes')}
              >
                Changes
              </button>
              {selectedEntity?.entity_type === 'ORGANIZATION' && (
                <button
                  type="button"
                  className={`tab ${viewMode === 'interlocks' ? 'active' : ''}`}
                  onClick={() => setViewMode('interlocks')}
                >
                  Board Interlocks
                </button>
              )}
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
                    <h4>Relationships ({relationships.total ?? 0})</h4>
                    <ul className="relationship-list">
                      {relationships.relationships.slice(0, 5).map((rel) => (
                        <li key={rel.id} className="relationship-item">
                          <span className="rel-type-badge" data-type={rel.rel_type}>{rel.rel_type}</span>
                          <span className="rel-direction">
                            {rel.source_entity?.id === id ? '→' : '←'}
                          </span>
                          <a
                            href={`/entities/${
                              rel.source_entity?.id === id
                                ? rel.target_entity?.id
                                : rel.source_entity?.id
                            }`}
                            onClick={(e) => {
                              e.preventDefault();
                              handleEntitySelect(
                                rel.source_entity?.id === id
                                  ? (rel.target_entity?.id ?? '')
                                  : (rel.source_entity?.id ?? '')
                              );
                            }}
                          >
                            {rel.source_entity?.id === id
                              ? rel.target_entity?.name
                              : rel.source_entity?.name}
                          </a>
                          <button
                            type="button"
                            className="btn-timeline"
                            onClick={() => setTimelineRelationship(
                              timelineRelationship?.id === rel.id ? null : rel
                            )}
                            title="View relationship history"
                          >
                            {timelineRelationship?.id === rel.id ? 'Hide' : 'Timeline'}
                          </button>
                        </li>
                      ))}
                    </ul>

                    {/* Relationship Timeline */}
                    {timelineRelationship && relTimeline && (
                      <div className="rel-timeline-panel">
                        <h5>
                          Timeline: {timelineRelationship.rel_type} —{' '}
                          {timelineRelationship.source_entity?.name} → {timelineRelationship.target_entity?.name}
                        </h5>
                        {relTimeline.periods && relTimeline.periods.length > 0 ? (
                          <ul className="timeline-periods">
                            {relTimeline.periods.map((period, i) => (
                              <li key={i} className="timeline-period">
                                <span className="period-range">
                                  {period.valid_from || '?'} — {period.valid_to || 'present'}
                                </span>
                                {period.is_current && (
                                  <span className="badge-current">Current</span>
                                )}
                                {period.properties && Object.keys(period.properties).length > 0 && (
                                  <span className="text-muted period-details">
                                    {Object.entries(period.properties)
                                      .map(([k, v]) => `${k}: ${String(v)}`)
                                      .join(', ')}
                                  </span>
                                )}
                              </li>
                            ))}
                          </ul>
                        ) : (
                          <p className="text-muted">No historical periods found.</p>
                        )}
                      </div>
                    )}
                    {(relationships.total ?? 0) > 5 && (
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
                  enablePathFinding={true}
                  asOf={temporalEnabled ? temporalDate : undefined}
                />

                {/* Find Connectors Panel */}
                <div className="connector-section">
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => setShowConnectorPanel(!showConnectorPanel)}
                  >
                    {showConnectorPanel ? 'Hide Connectors' : 'Find Connectors'}
                  </button>
                  {showConnectorPanel && (
                    <div className="connector-panel">
                      <p className="text-muted connector-desc">
                        Find entities that connect multiple targets in the graph.
                      </p>
                      <div className="connector-search">
                        <input
                          type="text"
                          value={connectorSearchQuery}
                          onChange={(e) => setConnectorSearchQuery(e.target.value)}
                          placeholder="Search entities to add..."
                        />
                        {isConnectorSearching && <span className="text-muted">Searching...</span>}
                        {connectorSearchResults?.results && connectorSearchQuery && (
                          <div className="connector-dropdown">
                            {connectorSearchResults.results.map((entity) => (
                              <button
                                key={entity.id}
                                type="button"
                                className="connector-result"
                                onClick={() => {
                                  if (!connectorEntityIds.find((e) => e.id === entity.id)) {
                                    setConnectorEntityIds([...connectorEntityIds, entity]);
                                  }
                                  setConnectorSearchQuery('');
                                }}
                              >
                                <span className="entity-type-badge">{entity.entity_type}</span>
                                {entity.name}
                              </button>
                            ))}
                          </div>
                        )}
                      </div>
                      <div className="connector-chips">
                        {connectorEntityIds.map((e) => (
                          <span key={e.id} className="connector-chip">
                            {e.name}
                            <button type="button" onClick={() => setConnectorEntityIds(connectorEntityIds.filter((x) => x.id !== e.id))}>x</button>
                          </span>
                        ))}
                      </div>
                      <div className="connector-controls">
                        <label>
                          Max hops:
                          <input
                            type="range"
                            min={1}
                            max={5}
                            value={connectorMaxHops}
                            onChange={(e) => setConnectorMaxHops(Number(e.target.value))}
                          />
                          <span>{connectorMaxHops}</span>
                        </label>
                      </div>
                      {connectingLoading && <div className="loading"><div className="spinner" /><span>Finding connectors...</span></div>}
                      {connectingData?.connecting_entities && connectingData.connecting_entities.length > 0 && (
                        <div className="connector-results">
                          <h5>Connecting Entities ({connectingData.total_connectors ?? 0})</h5>
                          <ul className="connector-list">
                            {connectingData.connecting_entities.map((ce) => (
                              <li key={ce.id} className="connector-item">
                                <a
                                  href={`/entities/${ce.id}`}
                                  onClick={(e) => { e.preventDefault(); handleEntitySelect(ce.id ?? ''); }}
                                >
                                  {ce.name}
                                </a>
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                      {connectingData && connectingData.connecting_entities && connectingData.connecting_entities.length === 0 && (
                        <p className="text-muted">No connecting entities found.</p>
                      )}
                    </div>
                  )}
                </div>

                {selectedRelationship && (
                  <div className="relationship-detail card">
                    <h4>Selected Relationship</h4>
                    <dl className="rel-properties">
                      <dt>Type</dt>
                      <dd>{selectedRelationship.rel_type}</dd>
                      <dt>From</dt>
                      <dd>{selectedRelationship.source_entity?.name}</dd>
                      <dt>To</dt>
                      <dd>{selectedRelationship.target_entity?.name}</dd>
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
                      {selectedRelationship.properties?.ownership_percentage != null && (
                        <>
                          <dt>Ownership</dt>
                          <dd>{String(selectedRelationship.properties.ownership_percentage)}%</dd>
                        </>
                      )}
                      {selectedRelationship.properties?.form_type != null && (
                        <>
                          <dt>Filing</dt>
                          <dd>{String(selectedRelationship.properties.form_type)}</dd>
                        </>
                      )}
                      {selectedRelationship.properties?.filing_date != null && (
                        <>
                          <dt>Filed</dt>
                          <dd>{String(selectedRelationship.properties.filing_date)}</dd>
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
            ) : viewMode === 'funding' && id ? (
              <div className="funding-view">
                <div className="funding-filter">
                  <label>
                    Fiscal Year:
                    <input
                      type="number"
                      min="2000"
                      max="2030"
                      value={fundingFiscalYear ?? ''}
                      onChange={(e) => setFundingFiscalYear(e.target.value ? parseInt(e.target.value) : undefined)}
                      placeholder="All years"
                    />
                  </label>
                </div>

                {/* Funding Sources */}
                <div className="funding-section">
                  <h4>Funding Sources</h4>
                  {fundingSourcesLoading ? (
                    <div className="loading"><div className="spinner" /><span>Loading...</span></div>
                  ) : fundingSources?.funders && fundingSources.funders.length > 0 ? (
                    <table className="funding-table">
                      <thead>
                        <tr><th>Funder</th><th>Amount</th></tr>
                      </thead>
                      <tbody>
                        {fundingSources.funders.map((f, i) => (
                          <tr key={i}>
                            <td>
                              <a
                                href={`/entities/${f.entity?.id}`}
                                onClick={(e) => { e.preventDefault(); handleEntitySelect(f.entity?.id ?? ''); }}
                              >
                                {f.entity?.name}
                              </a>
                            </td>
                            <td>${(f.amount ?? 0).toLocaleString()}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <p className="text-muted">No funding sources found.</p>
                  )}
                </div>

                {/* Funding Recipients */}
                <div className="funding-section">
                  <h4>Funding Recipients</h4>
                  {fundingRecipientsLoading ? (
                    <div className="loading"><div className="spinner" /><span>Loading...</span></div>
                  ) : fundingRecipients?.recipients && fundingRecipients.recipients.length > 0 ? (
                    <table className="funding-table">
                      <thead>
                        <tr><th>Recipient</th><th>Amount</th></tr>
                      </thead>
                      <tbody>
                        {fundingRecipients.recipients.map((r, i) => (
                          <tr key={i}>
                            <td>
                              <a
                                href={`/entities/${r.entity?.id}`}
                                onClick={(e) => { e.preventDefault(); handleEntitySelect(r.entity?.id ?? ''); }}
                              >
                                {r.entity?.name}
                              </a>
                            </td>
                            <td>${(r.amount ?? 0).toLocaleString()}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <p className="text-muted">No funding recipients found.</p>
                  )}
                </div>

                {/* Funding Paths */}
                <div className="funding-section">
                  <h4>Multi-Hop Funding Paths</h4>
                  {fundingPathsLoading ? (
                    <div className="loading"><div className="spinner" /><span>Loading...</span></div>
                  ) : fundingPaths?.paths && fundingPaths.paths.length > 0 ? (
                    <ul className="funding-paths-list">
                      {fundingPaths.paths.map((path, i) => (
                        <li key={i} className="funding-path-item">
                          {path.intermediaries && path.intermediaries.length > 0 ? (
                            <div className="path-chain">
                              {path.intermediaries.map((entity, j) => (
                                <span key={j}>
                                  {j > 0 && <span className="path-arrow">→</span>}
                                  <a
                                    href={`/entities/${entity.id}`}
                                    onClick={(e) => { e.preventDefault(); handleEntitySelect(entity.id ?? ''); }}
                                  >
                                    {entity.name}
                                  </a>
                                </span>
                              ))}
                              {path.recipient && (
                                <span>
                                  <span className="path-arrow">→</span>
                                  <a
                                    href={`/entities/${path.recipient.id}`}
                                    onClick={(e) => { e.preventDefault(); handleEntitySelect(path.recipient?.id ?? ''); }}
                                  >
                                    {path.recipient.name}
                                  </a>
                                </span>
                              )}
                            </div>
                          ) : path.recipient ? (
                            <div className="path-chain">
                              <a
                                href={`/entities/${path.recipient.id}`}
                                onClick={(e) => { e.preventDefault(); handleEntitySelect(path.recipient?.id ?? ''); }}
                              >
                                {path.recipient.name}
                              </a>
                              <span className="text-muted"> ({path.hops ?? 0} hops, ${(path.total_amount ?? 0).toLocaleString()})</span>
                            </div>
                          ) : (
                            <span className="text-muted">Path {i + 1}</span>
                          )}
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="text-muted">No multi-hop funding paths found.</p>
                  )}
                </div>

                {/* Shared Funders */}
                <div className="funding-section">
                  <h4>Shared Funders Analysis</h4>
                  <p className="text-muted">Find funders shared between multiple entities.</p>
                  <div className="connector-search">
                    <input
                      type="text"
                      value={sharedFunderSearch}
                      onChange={(e) => setSharedFunderSearch(e.target.value)}
                      placeholder="Search entities to compare..."
                    />
                    {isSharedFunderSearching && <span className="text-muted">Searching...</span>}
                    {sharedFunderSearchResults?.results && sharedFunderSearch && (
                      <div className="connector-dropdown">
                        {sharedFunderSearchResults.results.map((entity) => (
                          <button
                            key={entity.id}
                            type="button"
                            className="connector-result"
                            onClick={() => {
                              if (!sharedFunderIds.find((e) => e.id === entity.id)) {
                                setSharedFunderIds([...sharedFunderIds, entity]);
                              }
                              setSharedFunderSearch('');
                            }}
                          >
                            <span className="entity-type-badge">{entity.entity_type}</span>
                            {entity.name}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                  <div className="connector-chips">
                    {sharedFunderIds.map((e) => (
                      <span key={e.id} className="connector-chip">
                        {e.name}
                        <button type="button" onClick={() => setSharedFunderIds(sharedFunderIds.filter((x) => x.id !== e.id))}>x</button>
                      </span>
                    ))}
                  </div>
                  {sharedFunderIds.length >= 2 && (
                    <button
                      type="button"
                      className="btn btn-secondary"
                      onClick={() => setShowSharedFunders(true)}
                      disabled={sharedFundersLoading}
                    >
                      {sharedFundersLoading ? 'Searching...' : 'Find Shared Funders'}
                    </button>
                  )}
                  {sharedFundersData?.shared_funders && sharedFundersData.shared_funders.length > 0 && (
                    <table className="funding-table" style={{ marginTop: 'var(--spacing-md)' }}>
                      <thead>
                        <tr>
                          <th>Funder</th>
                          <th>Shared Recipients</th>
                          <th>Total Funding</th>
                          <th>Concentration</th>
                        </tr>
                      </thead>
                      <tbody>
                        {sharedFundersData.shared_funders.map((sf, i) => (
                          <tr key={i}>
                            <td>
                              <a
                                href={`/entities/${sf.funder?.id}`}
                                onClick={(e) => { e.preventDefault(); handleEntitySelect(sf.funder?.id ?? ''); }}
                              >
                                {sf.funder?.name}
                              </a>
                            </td>
                            <td>{sf.shared_count ?? 0}</td>
                            <td>${(sf.total_funding ?? 0).toLocaleString()}</td>
                            <td>{((sf.funding_concentration ?? 0) * 100).toFixed(1)}%</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                  {showSharedFunders && sharedFundersData?.shared_funders && sharedFundersData.shared_funders.length === 0 && (
                    <p className="text-muted">No shared funders found.</p>
                  )}
                </div>
              </div>
            ) : viewMode === 'changes' && id ? (
              <div className="changes-view">
                <div className="changes-filter">
                  <label>
                    From:
                    <input type="date" value={changesFromDate} onChange={(e) => setChangesFromDate(e.target.value)} />
                  </label>
                  <label>
                    To:
                    <input type="date" value={changesToDate} onChange={(e) => setChangesToDate(e.target.value)} />
                  </label>
                </div>

                {changesLoading ? (
                  <div className="loading"><div className="spinner" /><span>Loading changes...</span></div>
                ) : relationshipChanges ? (
                  <div className="changes-content">
                    {/* Added */}
                    {relationshipChanges.added_relationships && relationshipChanges.added_relationships.length > 0 && (
                      <div className="changes-section">
                        <h4 className="changes-added">Added ({relationshipChanges.added_relationships.length})</h4>
                        <ul className="changes-list">
                          {(relationshipChanges.added_relationships as unknown as Array<Record<string, unknown>>).map((rel, i) => (
                            <li key={i} className="change-item added">
                              <span className="rel-type-badge">{String(rel.rel_type || 'UNKNOWN')}</span>
                              <span>
                                {String(rel.target_name || rel.target_entity_id || 'Unknown')}
                              </span>
                              {rel.valid_from ? (
                                <span className="text-muted">from {String(rel.valid_from)}</span>
                              ) : null}
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {/* Removed */}
                    {relationshipChanges.removed_relationships && relationshipChanges.removed_relationships.length > 0 && (
                      <div className="changes-section">
                        <h4 className="changes-removed">Removed ({relationshipChanges.removed_relationships.length})</h4>
                        <ul className="changes-list">
                          {(relationshipChanges.removed_relationships as unknown as Array<Record<string, unknown>>).map((rel, i) => (
                            <li key={i} className="change-item removed">
                              <span className="rel-type-badge">{String(rel.rel_type || 'UNKNOWN')}</span>
                              <span>
                                {String(rel.target_name || rel.target_entity_id || 'Unknown')}
                              </span>
                              {rel.valid_until ? (
                                <span className="text-muted">until {String(rel.valid_until)}</span>
                              ) : null}
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {/* Modified */}
                    {relationshipChanges.modified_relationships &&
                      (relationshipChanges.modified_relationships as unknown[]).length > 0 && (
                      <div className="changes-section">
                        <h4 className="changes-modified">
                          Modified ({(relationshipChanges.modified_relationships as unknown[]).length})
                        </h4>
                        <ul className="changes-list">
                          {(relationshipChanges.modified_relationships as unknown as Array<Record<string, unknown>>).map(
                            (rel, i) => (
                              <li key={i} className="change-item modified">
                                <span className="rel-type-badge">{String(rel.rel_type || 'UNKNOWN')}</span>
                                <span>{String(rel.target_name || rel.target_entity_id || 'Unknown')}</span>
                              </li>
                            )
                          )}
                        </ul>
                      </div>
                    )}

                    {(!relationshipChanges.added_relationships || relationshipChanges.added_relationships.length === 0) &&
                      (!relationshipChanges.removed_relationships || relationshipChanges.removed_relationships.length === 0) &&
                      !(relationshipChanges.modified_relationships as unknown[] | undefined)?.length && (
                      <div className="empty-state">
                        <p>No relationship changes in this period.</p>
                      </div>
                    )}
                  </div>
                ) : (
                  <div className="empty-state">
                    <p>Select a date range to view relationship changes.</p>
                  </div>
                )}
              </div>
            ) : viewMode === 'interlocks' && id ? (
              <div className="interlocks-view">
                {interlocksLoading ? (
                  <div className="loading">
                    <div className="spinner" />
                    <span>Loading board interlocks...</span>
                  </div>
                ) : interlockData?.interlocks && interlockData.interlocks.length > 0 ? (
                  <>
                    <h3 className="interlocks-title">
                      Shared Directors ({interlockData.total ?? 0})
                    </h3>
                    <p className="interlocks-description">
                      Directors of this organization who also serve on other boards.
                    </p>
                    <ul className="interlocks-list">
                      {interlockData.interlocks.map((interlock: BoardInterlock, idx: number) => (
                        <li key={interlock.director?.id ?? idx} className="interlock-item">
                          <div className="interlock-director">
                            <span className="entity-type-badge">PERSON</span>
                            <a
                              href={`/entities/${interlock.director?.id}`}
                              onClick={(e) => {
                                e.preventDefault();
                                handleEntitySelect(interlock.director?.id ?? '');
                              }}
                            >
                              {interlock.director?.name}
                            </a>
                          </div>
                          <div className="interlock-orgs">
                            <span className="interlock-orgs-label">Also directs:</span>
                            {interlock.organizations?.map((org) => (
                              <a
                                key={org.id}
                                href={`/entities/${org.id}`}
                                className="interlock-org-link"
                                onClick={(e) => {
                                  e.preventDefault();
                                  handleEntitySelect(org.id ?? '');
                                }}
                              >
                                {org.name}
                              </a>
                            ))}
                          </div>
                        </li>
                      ))}
                    </ul>
                  </>
                ) : (
                  <div className="empty-state">
                    <p>No board interlocks found for this organization.</p>
                  </div>
                )}
              </div>
            ) : null}
          </div>
        </div>
      </div>

      {/* LinkedIn Discovery Modal */}
      {showLinkedInModal && (
        <div className="modal-overlay" onClick={() => setShowLinkedInModal(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Discover Executives via LinkedIn</h3>
              <button
                type="button"
                className="modal-close"
                onClick={() => setShowLinkedInModal(false)}
              >
                ×
              </button>
            </div>
            
            <div className="modal-body">
              <p className="modal-description">
                Import LinkedIn data for <strong>{quickIngestResult?.entity_name}</strong> to discover
                executives, board members, and key personnel.
              </p>

              {/* Method Selection */}
              <div className="linkedin-method-tabs">
                <button
                  type="button"
                  className={`method-tab ${linkedInMethod === 'csv' ? 'active' : ''}`}
                  onClick={() => setLinkedInMethod('csv')}
                >
                  CSV Import
                </button>
                <button
                  type="button"
                  className={`method-tab ${linkedInMethod === 'scrape' ? 'active' : ''}`}
                  onClick={() => setLinkedInMethod('scrape')}
                >
                  Browser Scrape
                </button>
              </div>

              {linkedInMethod === 'csv' ? (
                <div className="linkedin-csv-section">
                  <p className="method-help">
                    Upload a CSV exported from LinkedIn Sales Navigator or company page.
                    <br />
                    <span className="text-muted">Required columns: Name, Title, LinkedIn URL</span>
                  </p>
                  <input
                    type="file"
                    accept=".csv"
                    onChange={handleLinkedInCsvSelect}
                    className="csv-input"
                  />
                  {linkedInCsvFile && (
                    <p className="file-selected">Selected: {linkedInCsvFile.name}</p>
                  )}
                </div>
              ) : (
                <div className="linkedin-scrape-section">
                  {linkedInStatus?.configured ? (
                    <div className="cookie-configured">
                      <span className="status-badge success">Cookie Configured</span>
                      <p className="method-help">
                        Your LinkedIn session cookie is configured on the server.
                        <br />
                        <span className="text-muted">Ready to scrape company profiles.</span>
                      </p>
                    </div>
                  ) : (
                    <>
                      <p className="method-help">
                        Scrape public profiles from the company page.
                        <br />
                        <span className="text-muted">Requires your LinkedIn session cookie (li_at)</span>
                      </p>
                      <label>
                        Session Cookie (li_at):
                        <input
                          type="password"
                          value={linkedInSessionCookie}
                          onChange={(e) => setLinkedInSessionCookie(e.target.value)}
                          placeholder="AQED..."
                          className="cookie-input"
                        />
                      </label>
                      <p className="cookie-help">
                        <strong>How to get your cookie:</strong><br />
                        1. Go to linkedin.com (logged in)<br />
                        2. Press F12 → Application tab → Cookies<br />
                        3. Find <code>li_at</code> and copy its value<br />
                        <br />
                        <em>Tip: Add to <code>.env</code> file as LINKEDIN_SESSION_COOKIE for persistence</em>
                      </p>
                    </>
                  )}
                </div>
              )}

              {/* Titles Filter */}
              <div className="titles-filter">
                <label>
                  Filter by titles (comma-separated):
                  <input
                    type="text"
                    value={linkedInTitlesFilter}
                    onChange={(e) => setLinkedInTitlesFilter(e.target.value)}
                    placeholder="CEO, CFO, Director, VP..."
                  />
                </label>
              </div>

              {/* Results */}
              {linkedInResult && (
                <div className={`linkedin-result ${linkedInResult.status === 'completed' ? 'success' : linkedInResult.status === 'running' ? 'info' : 'error'}`}>
                  <p>{linkedInResult.message}</p>
                  {linkedInResult.profiles_found !== undefined && (
                    <p>Profiles found: {linkedInResult.profiles_found}</p>
                  )}
                  {linkedInResult.executives_found !== undefined && (
                    <p>Executives matched: {linkedInResult.executives_found}</p>
                  )}
                </div>
              )}

              {linkedInMutation.isError && (
                <div className="linkedin-result error">
                  <p>Error: {(linkedInMutation.error as Error)?.message || 'Failed to ingest LinkedIn data'}</p>
                </div>
              )}
            </div>

            <div className="modal-footer">
              <button
                type="button"
                className="btn btn-secondary"
                onClick={() => setShowLinkedInModal(false)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="btn btn-primary"
                onClick={handleLinkedInSubmit}
                disabled={
                  linkedInMutation.isPending ||
                  (linkedInMethod === 'csv' && !linkedInCsvFile) ||
                  (linkedInMethod === 'scrape' && !linkedInSessionCookie && !linkedInStatus?.configured)
                }
              >
                {linkedInMutation.isPending ? 'Importing...' : 'Import Executives'}
              </button>
            </div>
          </div>
        </div>
      )}

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

        .rel-type-badge[data-type="OWNS"] {
          background-color: #E11D48;
          color: white;
        }

        .rel-type-badge[data-type="FUNDED_BY"] {
          background-color: #10B981;
          color: white;
        }

        .rel-type-badge[data-type="DIRECTOR_OF"] {
          background-color: #8B5CF6;
          color: white;
        }

        .rel-type-badge[data-type="EMPLOYED_BY"] {
          background-color: #3B82F6;
          color: white;
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

        .interlocks-view {
          padding: var(--spacing-sm);
        }

        .interlocks-title {
          margin-bottom: var(--spacing-xs);
        }

        .interlocks-description {
          font-size: 0.875rem;
          color: var(--text-muted);
          margin-bottom: var(--spacing-md);
        }

        .interlocks-list {
          list-style: none;
          margin: 0;
          padding: 0;
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
        }

        .interlock-item {
          padding: var(--spacing-md);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          background: var(--bg-secondary);
        }

        .interlock-director {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          margin-bottom: var(--spacing-sm);
          font-weight: 500;
        }

        .interlock-director a {
          color: var(--color-primary);
          text-decoration: none;
        }

        .interlock-director a:hover {
          text-decoration: underline;
        }

        .interlock-orgs {
          display: flex;
          flex-wrap: wrap;
          align-items: center;
          gap: var(--spacing-xs);
          font-size: 0.875rem;
        }

        .interlock-orgs-label {
          color: var(--text-muted);
        }

        .interlock-org-link {
          padding: 2px 8px;
          border-radius: 4px;
          background: var(--bg-tertiary);
          color: var(--color-primary);
          text-decoration: none;
          font-size: 0.8125rem;
        }

        .interlock-org-link:hover {
          background: var(--color-primary);
          color: white;
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
          flex-direction: column;
          align-items: center;
          justify-content: center;
          gap: var(--spacing-md);
        }

        .quick-ingest-panel {
          margin-top: var(--spacing-md);
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
          text-align: left;
          width: 100%;
          max-width: 400px;
        }

        .quick-ingest-panel h4 {
          margin: 0 0 var(--spacing-xs);
          color: var(--text-primary);
          font-size: 0.9rem;
        }

        .quick-ingest-panel > p {
          margin: 0 0 var(--spacing-sm);
          font-size: 0.8rem;
        }

        .quick-ingest-form {
          display: flex;
          gap: var(--spacing-sm);
          margin-bottom: var(--spacing-sm);
        }

        .quick-ingest-form select {
          flex: 1;
          padding: var(--spacing-xs) var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          background: var(--bg-primary);
          font-size: 0.8rem;
        }

        .quick-ingest-result {
          padding: var(--spacing-sm);
          border-radius: var(--border-radius);
          font-size: 0.8rem;
          margin-top: var(--spacing-sm);
        }

        .quick-ingest-result.success {
          background: rgba(16, 185, 129, 0.1);
          border: 1px solid #10B981;
          color: #065F46;
        }

        .quick-ingest-result.info {
          background: rgba(59, 130, 246, 0.1);
          border: 1px solid #3B82F6;
          color: #1E40AF;
        }

        .quick-ingest-result.error {
          background: rgba(239, 68, 68, 0.1);
          border: 1px solid #EF4444;
          color: #991B1B;
        }

        .quick-ingest-result p {
          margin: 0 0 var(--spacing-xs);
        }

        .quick-ingest-result .sources-list {
          font-size: 0.7rem;
          margin-top: var(--spacing-xs);
        }

        .external-matches {
          margin-top: var(--spacing-sm);
        }

        .external-matches ul {
          margin: var(--spacing-xs) 0 0;
          padding-left: var(--spacing-md);
        }

        .external-matches li {
          margin-bottom: var(--spacing-xs);
        }

        .match-source {
          font-size: 0.7rem;
          color: var(--text-muted);
          margin-left: var(--spacing-xs);
        }

        .match-jurisdiction {
          font-size: 0.65rem;
          padding: 2px 4px;
          background: var(--bg-secondary);
          border-radius: 4px;
          margin-left: var(--spacing-xs);
        }

        .btn-link {
          background: none;
          border: none;
          color: var(--color-primary);
          cursor: pointer;
          padding: 0;
          font-size: 0.8rem;
          text-decoration: underline;
        }

        .btn-link:hover {
          color: var(--color-primary-dark);
        }

        .discover-executives-toggle {
          display: flex;
          align-items: center;
          gap: var(--spacing-xs);
          font-size: 0.75rem;
          color: var(--text-secondary);
          cursor: pointer;
          margin-top: var(--spacing-xs);
        }

        .discover-executives-toggle input {
          width: 14px;
          height: 14px;
          cursor: pointer;
        }

        .ingest-actions {
          display: flex;
          gap: var(--spacing-sm);
          align-items: center;
          flex-wrap: wrap;
        }

        .linkedin-link {
          font-size: 0.75rem;
          color: #0A66C2;
          text-decoration: none;
          padding: 2px 6px;
          background: rgba(10, 102, 194, 0.1);
          border-radius: 4px;
        }

        .linkedin-link:hover {
          background: rgba(10, 102, 194, 0.2);
        }

        .executives-hint {
          font-size: 0.7rem;
          margin-top: var(--spacing-xs);
          padding: var(--spacing-xs);
          background: rgba(139, 92, 246, 0.1);
          border-radius: 4px;
          color: #6D28D9;
        }

        .executives-hint strong {
          color: #5B21B6;
        }

        .btn-linkedin {
          background: #0A66C2;
          color: white;
          border: none;
          padding: 4px 10px;
          border-radius: 4px;
          font-size: 0.75rem;
          cursor: pointer;
        }

        .btn-linkedin:hover {
          background: #004182;
        }

        /* Modal Styles */
        .modal-overlay {
          position: fixed;
          top: 0;
          left: 0;
          right: 0;
          bottom: 0;
          background: rgba(0, 0, 0, 0.5);
          display: flex;
          align-items: center;
          justify-content: center;
          z-index: 1000;
        }

        .modal-content {
          background: var(--bg-primary);
          border-radius: var(--border-radius);
          box-shadow: var(--shadow-lg);
          width: 90%;
          max-width: 500px;
          max-height: 90vh;
          overflow-y: auto;
        }

        .modal-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: var(--spacing-md);
          border-bottom: 1px solid var(--border-color);
        }

        .modal-header h3 {
          margin: 0;
          font-size: 1.1rem;
        }

        .modal-close {
          background: none;
          border: none;
          font-size: 1.5rem;
          cursor: pointer;
          color: var(--text-muted);
          padding: 0;
          line-height: 1;
        }

        .modal-close:hover {
          color: var(--text-primary);
        }

        .modal-body {
          padding: var(--spacing-md);
        }

        .modal-description {
          margin: 0 0 var(--spacing-md);
          font-size: 0.9rem;
          color: var(--text-secondary);
        }

        .modal-footer {
          display: flex;
          justify-content: flex-end;
          gap: var(--spacing-sm);
          padding: var(--spacing-md);
          border-top: 1px solid var(--border-color);
        }

        .linkedin-method-tabs {
          display: flex;
          gap: 4px;
          margin-bottom: var(--spacing-md);
        }

        .method-tab {
          flex: 1;
          padding: var(--spacing-sm);
          border: 1px solid var(--border-color);
          background: var(--bg-secondary);
          cursor: pointer;
          font-size: 0.85rem;
          transition: all 0.15s;
        }

        .method-tab:first-child {
          border-radius: var(--border-radius) 0 0 var(--border-radius);
        }

        .method-tab:last-child {
          border-radius: 0 var(--border-radius) var(--border-radius) 0;
        }

        .method-tab.active {
          background: var(--color-primary);
          color: white;
          border-color: var(--color-primary);
        }

        .method-help {
          font-size: 0.8rem;
          color: var(--text-secondary);
          margin-bottom: var(--spacing-sm);
        }

        .linkedin-csv-section,
        .linkedin-scrape-section {
          margin-bottom: var(--spacing-md);
        }

        .csv-input {
          width: 100%;
          padding: var(--spacing-sm);
          border: 2px dashed var(--border-color);
          border-radius: var(--border-radius);
          cursor: pointer;
        }

        .file-selected {
          font-size: 0.8rem;
          color: #10B981;
          margin-top: var(--spacing-xs);
        }

        .cookie-input {
          width: 100%;
          padding: var(--spacing-xs) var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-family: monospace;
          font-size: 0.85rem;
          margin-top: var(--spacing-xs);
        }

        .cookie-help {
          font-size: 0.7rem;
          color: var(--text-muted);
          margin-top: var(--spacing-xs);
        }

        .titles-filter {
          margin-bottom: var(--spacing-md);
        }

        .titles-filter label {
          display: block;
          font-size: 0.85rem;
          font-weight: 500;
        }

        .titles-filter input {
          width: 100%;
          padding: var(--spacing-xs) var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-size: 0.85rem;
          margin-top: var(--spacing-xs);
        }

        .linkedin-result {
          padding: var(--spacing-sm);
          border-radius: var(--border-radius);
          font-size: 0.85rem;
          margin-top: var(--spacing-md);
        }

        .linkedin-result.success {
          background: rgba(16, 185, 129, 0.1);
          border: 1px solid #10B981;
          color: #065F46;
        }

        .linkedin-result.info {
          background: rgba(59, 130, 246, 0.1);
          border: 1px solid #3B82F6;
          color: #1E40AF;
        }

        .linkedin-result.error {
          background: rgba(239, 68, 68, 0.1);
          border: 1px solid #EF4444;
          color: #991B1B;
        }

        .linkedin-result p {
          margin: 0 0 var(--spacing-xs);
        }

        .linkedin-result p:last-child {
          margin-bottom: 0;
        }

        .cookie-configured {
          padding: var(--spacing-sm);
          background: rgba(16, 185, 129, 0.1);
          border-radius: var(--border-radius);
          margin-bottom: var(--spacing-sm);
        }

        .status-badge {
          display: inline-block;
          font-size: 0.7rem;
          padding: 2px 8px;
          border-radius: 12px;
          font-weight: 600;
          margin-bottom: var(--spacing-xs);
        }

        .status-badge.success {
          background: #10B981;
          color: white;
        }

        .cookie-help code {
          background: var(--bg-tertiary);
          padding: 1px 4px;
          border-radius: 3px;
          font-size: 0.8em;
        }

        .cookie-help em {
          display: block;
          margin-top: var(--spacing-xs);
          color: var(--color-primary);
        }

        .pagination-controls {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: var(--spacing-sm);
          padding: var(--spacing-sm);
          border-top: 1px solid var(--border-color);
        }

        .pagination-info {
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .btn-sm {
          padding: 4px 8px;
          font-size: 0.75rem;
        }

        .btn-timeline {
          background: none;
          border: 1px solid var(--border-color);
          border-radius: 4px;
          color: var(--text-muted);
          cursor: pointer;
          font-size: 0.625rem;
          padding: 2px 6px;
          margin-left: auto;
        }

        .btn-timeline:hover {
          background: var(--bg-tertiary);
          color: var(--text-primary);
        }

        .rel-timeline-panel {
          margin-top: var(--spacing-md);
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
        }

        .rel-timeline-panel h5 {
          margin-bottom: var(--spacing-sm);
          font-size: 0.875rem;
        }

        .timeline-periods {
          list-style: none;
          margin: 0;
          padding: 0;
        }

        .timeline-period {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          padding: var(--spacing-xs) 0;
          border-bottom: 1px solid var(--border-color);
          font-size: 0.875rem;
        }

        .timeline-period:last-child {
          border-bottom: none;
        }

        .period-range {
          font-weight: 500;
          min-width: 200px;
        }

        .badge-current {
          font-size: 0.625rem;
          padding: 2px 6px;
          border-radius: 4px;
          background: #10B981;
          color: white;
        }

        .period-details {
          font-size: 0.75rem;
        }

        .funding-view,
        .changes-view {
          padding: var(--spacing-sm);
        }

        .funding-filter,
        .changes-filter {
          display: flex;
          gap: var(--spacing-md);
          align-items: flex-end;
          margin-bottom: var(--spacing-lg);
          flex-wrap: wrap;
        }

        .funding-filter label,
        .changes-filter label {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-xs);
          font-size: 0.875rem;
          font-weight: 500;
        }

        .funding-filter input,
        .changes-filter input {
          padding: var(--spacing-xs) var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-size: 0.875rem;
          width: 140px;
        }

        .funding-section {
          margin-bottom: var(--spacing-lg);
          padding-bottom: var(--spacing-lg);
          border-bottom: 1px solid var(--border-color);
        }

        .funding-section:last-child {
          border-bottom: none;
        }

        .funding-section h4 {
          margin-bottom: var(--spacing-sm);
        }

        .funding-table {
          width: 100%;
          border-collapse: collapse;
          font-size: 0.875rem;
        }

        .funding-table th,
        .funding-table td {
          padding: var(--spacing-sm);
          text-align: left;
          border-bottom: 1px solid var(--border-color);
        }

        .funding-table th {
          background: var(--bg-tertiary);
          font-weight: 600;
        }

        .funding-table a {
          color: var(--color-primary);
          text-decoration: none;
        }

        .funding-table a:hover {
          text-decoration: underline;
        }

        .funding-paths-list {
          list-style: none;
          margin: 0;
          padding: 0;
        }

        .funding-path-item {
          padding: var(--spacing-sm);
          border-bottom: 1px solid var(--border-color);
        }

        .funding-path-item:last-child {
          border-bottom: none;
        }

        .path-chain {
          display: flex;
          align-items: center;
          flex-wrap: wrap;
          gap: var(--spacing-xs);
        }

        .path-chain a {
          color: var(--color-primary);
          text-decoration: none;
        }

        .path-chain a:hover {
          text-decoration: underline;
        }

        .path-arrow {
          color: var(--text-muted);
          font-weight: 600;
        }

        .changes-content {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
        }

        .changes-section h4 {
          margin-bottom: var(--spacing-sm);
          font-weight: 600;
        }

        .changes-added { color: #10B981; }
        .changes-removed { color: #DC2626; }
        .changes-modified { color: #F59E0B; }

        .changes-list {
          list-style: none;
          margin: 0;
          padding: 0;
        }

        .change-item {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          padding: var(--spacing-xs) var(--spacing-sm);
          border-left: 3px solid transparent;
          font-size: 0.875rem;
        }

        .change-item.added { border-left-color: #10B981; }
        .change-item.removed { border-left-color: #DC2626; }
        .change-item.modified { border-left-color: #F59E0B; }

        .connector-section {
          margin-top: var(--spacing-md);
        }

        .connector-panel {
          margin-top: var(--spacing-sm);
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
        }

        .connector-desc {
          margin-bottom: var(--spacing-sm);
          font-size: 0.875rem;
        }

        .connector-search {
          position: relative;
          margin-bottom: var(--spacing-sm);
        }

        .connector-search input {
          width: 100%;
          padding: var(--spacing-xs) var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          font-size: 0.875rem;
        }

        .connector-dropdown {
          position: absolute;
          top: 100%;
          left: 0;
          right: 0;
          background: var(--bg-primary);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          box-shadow: var(--shadow-md);
          z-index: 100;
          max-height: 180px;
          overflow-y: auto;
        }

        .connector-result {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          width: 100%;
          padding: var(--spacing-sm);
          border: none;
          background: none;
          text-align: left;
          cursor: pointer;
          font-size: 0.875rem;
        }

        .connector-result:hover {
          background: var(--bg-tertiary);
        }

        .connector-chips {
          display: flex;
          flex-wrap: wrap;
          gap: var(--spacing-xs);
          margin-bottom: var(--spacing-sm);
        }

        .connector-chip {
          display: inline-flex;
          align-items: center;
          gap: var(--spacing-xs);
          padding: 4px 8px;
          background: var(--color-primary);
          color: white;
          border-radius: 16px;
          font-size: 0.75rem;
        }

        .connector-chip button {
          background: none;
          border: none;
          color: white;
          cursor: pointer;
          font-size: 12px;
          padding: 0 2px;
          opacity: 0.7;
        }

        .connector-chip button:hover {
          opacity: 1;
        }

        .connector-controls {
          margin-bottom: var(--spacing-sm);
        }

        .connector-controls label {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          font-size: 0.875rem;
          font-weight: 500;
        }

        .connector-controls input[type="range"] {
          flex: 1;
          max-width: 150px;
          accent-color: var(--color-primary);
        }

        .connector-results {
          margin-top: var(--spacing-md);
        }

        .connector-results h5 {
          margin-bottom: var(--spacing-sm);
          font-size: 0.875rem;
        }

        .connector-list {
          list-style: none;
          margin: 0;
          padding: 0;
        }

        .connector-item {
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: var(--spacing-xs) 0;
          border-bottom: 1px solid var(--border-color);
          font-size: 0.875rem;
        }

        .connector-item:last-child {
          border-bottom: none;
        }

        .connector-item a {
          color: var(--color-primary);
          text-decoration: none;
        }

        .connector-item a:hover {
          text-decoration: underline;
        }

        .connector-count {
          font-size: 0.75rem;
          color: var(--text-muted);
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
