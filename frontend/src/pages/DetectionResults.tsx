/**
 * Detection Results page for MITDS
 *
 * Run and view coordination detection analyses including:
 * - Temporal coordination analysis
 * - Funding cluster detection
 * - Infrastructure sharing analysis
 */

import { useState, useCallback } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import Timeline, { TimelineEvent, BurstPeriod } from '../components/graph/Timeline';
import {
  searchEntities,
  analyzeTemporalCoordination,
  calculateCompositeScore,
  detectFundingClusters,
  detectInfrastructureSharing,
  explainFinding,
  customInstance,
  type EntitySummary,
  type TemporalAnalysisResponse,
  type CompositeScoreResponse,
  type FundingClusterDetectionResponse,
  type InfrastructureSharingResponse,
  type FindingExplanation,
  type EntityType,
} from '@/api';

// API call for politically active funded entities (not yet in generated types)
const detectPoliticallyActiveFunded = (data: {
  jurisdiction?: string;
  min_funding?: number;
  fiscal_year?: number | null;
  include_lobbying?: boolean;
  include_political_ads?: boolean;
  limit?: number;
}) => customInstance<PoliticallyActiveFundedResponse>({
  url: '/detection/politically-active-funded',
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  data,
});

// Type definitions for detection result arrays (OpenAPI schema has incomplete types)
interface LeadLagPair {
  leader_entity_id?: string;
  follower_entity_id?: string;
  lag_minutes?: number;
  correlation?: number;
  is_significant?: boolean;
}

interface SynchronizedGroup {
  sync_score?: number;
  js_divergence?: number;
  overlap_ratio?: number;
  confidence?: number;
  entity_ids?: string[];
}

interface FundingCluster {
  cluster_id?: string;
  shared_funder?: { name?: string };
  members?: Array<{ id?: string; name?: string }>;
  total_funding?: number;
  score?: number;
}

interface InfraMatch {
  domain_a?: string;
  domain_b?: string;
  signals?: Array<{ signal_type?: string; value?: string; weight?: number; description?: string }>;
  total_score?: number;
  confidence?: number;
}

interface InfraProfile {
  domain?: string;
  error?: string;
  dns?: { nameservers?: string[] };
  whois?: { registrar?: string };
  ssl?: { issuer?: string };
  hosting?: Array<{ provider?: string; ip?: string }>;
}

interface PoliticallyActiveFundedResult {
  entity_id?: string;
  entity_name?: string;
  entity_type?: string;
  jurisdiction?: string;
  total_funding?: number;
  funder_count?: number;
  funders?: Array<{ id?: string; name?: string; jurisdiction?: string; amount?: number }>;
  has_political_ads?: boolean;
  political_ad_count?: number;
  political_ad_spend?: number;
  has_lobbying?: boolean;
  lobbying_count?: number;
}

interface PoliticallyActiveFundedResponse {
  results?: PoliticallyActiveFundedResult[];
  total_results?: number;
  explanation?: string;
}

export default function DetectionResults() {
  const [activeTab, setActiveTab] = useState<'temporal' | 'funding' | 'political' | 'infrastructure' | 'composite'>('temporal');

  // Temporal analysis form state
  const [selectedEntities, setSelectedEntities] = useState<EntitySummary[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [startDate, setStartDate] = useState(() => {
    const d = new Date();
    d.setMonth(d.getMonth() - 3);
    return d.toISOString().split('T')[0];
  });
  const [endDate, setEndDate] = useState(() => new Date().toISOString().split('T')[0]);
  const [excludeHardNegatives, setExcludeHardNegatives] = useState(true);
  const [analysisResult, setAnalysisResult] = useState<TemporalAnalysisResponse | null>(null);

  // Funding cluster form state
  const [fundingEntityType, setFundingEntityType] = useState<string>('');
  const [fundingFiscalYear, setFundingFiscalYear] = useState<number>(new Date().getFullYear() - 1);
  const [fundingCanadianOnly, setFundingCanadianOnly] = useState<boolean>(true);
  const [fundingMinShared, setFundingMinShared] = useState<number>(2);
  const [fundingResult, setFundingResult] = useState<FundingClusterDetectionResponse | null>(null);

  // Politically active funded form state
  const [politicalJurisdiction, setPoliticalJurisdiction] = useState<string>('CA');
  const [politicalMinFunding, setPoliticalMinFunding] = useState<number>(1000);
  const [politicalFiscalYear, setPoliticalFiscalYear] = useState<number | null>(null);
  const [politicalIncludeAds, setPoliticalIncludeAds] = useState<boolean>(true);
  const [politicalIncludeLobby, setPoliticalIncludeLobby] = useState<boolean>(true);
  const [politicalResult, setPoliticalResult] = useState<PoliticallyActiveFundedResponse | null>(null);

  // Infrastructure sharing form state
  const [infraDomains, setInfraDomains] = useState<string>('');
  const [infraMinScore, setInfraMinScore] = useState<number>(1.0);
  const [infraResult, setInfraResult] = useState<InfrastructureSharingResponse | null>(null);

  // Composite scoring form state
  const [compositeEntities, setCompositeEntities] = useState<EntitySummary[]>([]);
  const [compositeSearchQuery, setCompositeSearchQuery] = useState('');
  const [includeTemporal, setIncludeTemporal] = useState(true);
  const [includeFunding, setIncludeFunding] = useState(true);
  const [includeInfra, setIncludeInfra] = useState(true);
  const [compositeResult, setCompositeResult] = useState<CompositeScoreResponse | null>(null);
  const [explanation, setExplanation] = useState<FindingExplanation | null>(null);
  const [showExplanation, setShowExplanation] = useState(false);

  // Entity search
  const { data: searchResults, isLoading: isSearching } = useQuery({
    queryKey: ['entity-search', searchQuery],
    queryFn: () => searchEntities({ q: searchQuery, limit: 10 }),
    enabled: searchQuery.length >= 2,
  });

  // Temporal analysis mutation
  const temporalAnalysis = useMutation({
    mutationFn: (data: Parameters<typeof analyzeTemporalCoordination>[0]) => 
      analyzeTemporalCoordination(data),
    onSuccess: (data) => {
      setAnalysisResult(data);
    },
  });

  // Funding cluster mutation
  const fundingAnalysis = useMutation({
    mutationFn: (data: Parameters<typeof detectFundingClusters>[0]) => 
      detectFundingClusters(data),
    onSuccess: (data) => {
      setFundingResult(data);
    },
  });

  // Politically active funded mutation
  const politicalAnalysis = useMutation({
    mutationFn: (data: Parameters<typeof detectPoliticallyActiveFunded>[0]) =>
      detectPoliticallyActiveFunded(data),
    onSuccess: (data) => {
      setPoliticalResult(data);
    },
  });

  // Infrastructure sharing mutation
  const infraAnalysis = useMutation({
    mutationFn: (data: Parameters<typeof detectInfrastructureSharing>[0]) => 
      detectInfrastructureSharing(data),
    onSuccess: (data) => {
      setInfraResult(data);
    },
  });

  // Composite scoring mutation
  const compositeAnalysis = useMutation({
    mutationFn: (data: Parameters<typeof calculateCompositeScore>[0]) => 
      calculateCompositeScore(data),
    onSuccess: (data) => {
      setCompositeResult(data);
      setShowExplanation(false);
      setExplanation(null);
    },
  });

  // Composite entity search
  const { data: compositeSearchResults, isLoading: isCompositeSearching } = useQuery({
    queryKey: ['composite-entity-search', compositeSearchQuery],
    queryFn: () => searchEntities({ q: compositeSearchQuery, limit: 10 }),
    enabled: compositeSearchQuery.length >= 2,
  });

  const handleRunComposite = useCallback(() => {
    if (compositeEntities.length < 2) {
      alert('Please select at least 2 entities');
      return;
    }
    compositeAnalysis.mutate({
      entity_ids: compositeEntities.map((e) => e.id),
      include_temporal: includeTemporal,
      include_funding: includeFunding,
      include_infrastructure: includeInfra,
    });
  }, [compositeEntities, includeTemporal, includeFunding, includeInfra, compositeAnalysis]);

  const handleAddCompositeEntity = (entity: EntitySummary) => {
    if (!compositeEntities.find((e) => e.id === entity.id)) {
      setCompositeEntities([...compositeEntities, entity]);
    }
    setCompositeSearchQuery('');
  };

  const handleRemoveCompositeEntity = (entityId: string) => {
    setCompositeEntities(compositeEntities.filter((e) => e.id !== entityId));
  };

  const handleExplainFinding = useCallback(async (findingId: string) => {
    try {
      const result = await explainFinding(findingId);
      setExplanation(result);
      setShowExplanation(true);
    } catch (err) {
      console.error('Failed to load explanation:', err);
    }
  }, []);

  const handleRunFunding = useCallback(() => {
    fundingAnalysis.mutate({
      entity_type: (fundingEntityType || undefined) as EntityType | undefined,
      fiscal_year: fundingFiscalYear || undefined,
      jurisdiction: fundingCanadianOnly ? 'CA' : undefined,
      min_shared_funders: fundingMinShared,
      limit: 50,
    });
  }, [fundingEntityType, fundingFiscalYear, fundingCanadianOnly, fundingMinShared, fundingAnalysis]);

  const handleRunPolitical = useCallback(() => {
    politicalAnalysis.mutate({
      jurisdiction: politicalJurisdiction || undefined,
      min_funding: politicalMinFunding,
      fiscal_year: politicalFiscalYear || undefined,
      include_political_ads: politicalIncludeAds,
      include_lobbying: politicalIncludeLobby,
      limit: 100,
    });
  }, [politicalJurisdiction, politicalMinFunding, politicalFiscalYear, politicalIncludeAds, politicalIncludeLobby, politicalAnalysis]);

  const handleRunInfra = useCallback(() => {
    const domains = infraDomains.split(/[,\s]+/).filter(Boolean);
    if (domains.length < 2) {
      alert('Please enter at least 2 domains separated by commas');
      return;
    }
    infraAnalysis.mutate({
      domains,
      min_score: infraMinScore,
    });
  }, [infraDomains, infraMinScore, infraAnalysis]);

  const handleAddEntity = (entity: EntitySummary) => {
    if (!selectedEntities.find((e) => e.id === entity.id)) {
      setSelectedEntities([...selectedEntities, entity]);
    }
    setSearchQuery('');
  };

  const handleRemoveEntity = (entityId: string) => {
    setSelectedEntities(selectedEntities.filter((e) => e.id !== entityId));
  };

  const handleRunAnalysis = useCallback(() => {
    if (selectedEntities.length < 2) {
      alert('Please select at least 2 entities for analysis');
      return;
    }

    temporalAnalysis.mutate({
      entity_ids: selectedEntities.map((e) => e.id),
      start_date: new Date(startDate).toISOString(),
      end_date: new Date(endDate).toISOString(),
      exclude_hard_negatives: excludeHardNegatives,
      async_mode: false,
    });
  }, [selectedEntities, startDate, endDate, excludeHardNegatives, temporalAnalysis]);

  // Transform analysis results for timeline
  const timelineEvents: TimelineEvent[] = (analysisResult?.bursts ?? []).flatMap((burst) =>
    ((burst as { bursts?: Array<{ startTime: string; endTime: string; intensity?: number; eventCount: number }> }).bursts ?? []).map((b, i) => ({
      id: `${(burst as { entity_id?: string }).entity_id ?? 'unknown'}-burst-${i}`,
      entityId: (burst as { entity_id?: string }).entity_id ?? 'unknown',
      entityName: selectedEntities.find((e) => e.id === (burst as { entity_id?: string }).entity_id)?.name ?? (burst as { entity_id?: string }).entity_id ?? 'Unknown',
      timestamp: b.startTime,
      eventType: 'BURST' as const,
    }))
  );

  const timelineBursts: BurstPeriod[] = (analysisResult?.bursts ?? []).flatMap((burst) =>
    ((burst as { bursts?: Array<{ startTime: string; endTime: string; intensity?: number; eventCount: number }> }).bursts ?? []).map((b) => ({
      startTime: b.startTime,
      endTime: b.endTime,
      level: b.intensity ?? b.eventCount,
      eventCount: b.eventCount,
    }))
  );

  // Calculate score color
  const getScoreColor = (score: number) => {
    if (score >= 0.7) return '#DC2626'; // Red - high coordination
    if (score >= 0.5) return '#F59E0B'; // Amber - moderate
    if (score >= 0.3) return '#3B82F6'; // Blue - low
    return '#10B981'; // Green - minimal
  };

  return (
    <div className="detection-results">
      <header className="page-header">
        <h1>Detection Analysis</h1>
        <p>Detect coordination patterns in media influence networks</p>
      </header>

      {/* Detection Type Tabs */}
      <div className="tabs">
        <button
          className={`tab ${activeTab === 'temporal' ? 'active' : ''}`}
          onClick={() => setActiveTab('temporal')}
        >
          Temporal Coordination
        </button>
        <button
          className={`tab ${activeTab === 'funding' ? 'active' : ''}`}
          onClick={() => setActiveTab('funding')}
        >
          Funding Clusters
        </button>
        <button
          className={`tab ${activeTab === 'political' ? 'active' : ''}`}
          onClick={() => setActiveTab('political')}
        >
          Political Activity
        </button>
        <button
          className={`tab ${activeTab === 'infrastructure' ? 'active' : ''}`}
          onClick={() => setActiveTab('infrastructure')}
        >
          Infrastructure Sharing
        </button>
        <button
          className={`tab ${activeTab === 'composite' ? 'active' : ''}`}
          onClick={() => setActiveTab('composite')}
        >
          Composite Score
        </button>
      </div>

      {/* Temporal Coordination Tab */}
      {activeTab === 'temporal' && (
        <div className="detection-panel">
          <div className="card">
            <h2>Temporal Coordination Analysis</h2>
            <p className="text-muted mb-md">
              Detect statistically significant publication synchronization patterns
              between entities, excluding legitimate news cycles.
            </p>

            <form className="analysis-form" onSubmit={(e) => { e.preventDefault(); handleRunAnalysis(); }}>
              <div className="form-group">
                <label>Entities to Analyze</label>
                <div className="entity-search-container">
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    placeholder="Search for entities to add..."
                  />
                  {isSearching && <span className="search-loading">Searching...</span>}
                  {searchResults?.results && searchResults.results.length > 0 && searchQuery && (
                    <div className="search-dropdown">
                      {searchResults.results.map((entity) => (
                        <button
                          key={entity.id}
                          type="button"
                          className="search-result"
                          onClick={() => handleAddEntity(entity)}
                        >
                          <span className="entity-type-badge">{entity.entity_type}</span>
                          {entity.name}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                <div className="selected-entities">
                  {selectedEntities.map((entity) => (
                    <span key={entity.id} className="entity-chip">
                      {entity.name}
                      <button
                        type="button"
                        className="chip-remove"
                        onClick={() => handleRemoveEntity(entity.id)}
                      >
                        x
                      </button>
                    </span>
                  ))}
                </div>
                <small className="text-muted">
                  Select at least 2 entities for comparison ({selectedEntities.length} selected)
                </small>
              </div>

              <div className="form-row">
                <div className="form-group">
                  <label>Start Date</label>
                  <input
                    type="date"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                  />
                </div>
                <div className="form-group">
                  <label>End Date</label>
                  <input
                    type="date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                  />
                </div>
              </div>

              <div className="form-group">
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={excludeHardNegatives}
                    onChange={(e) => setExcludeHardNegatives(e.target.checked)}
                  />
                  Exclude hard negatives (breaking news, scheduled events)
                </label>
              </div>

              <button
                type="submit"
                className="btn btn-primary"
                disabled={selectedEntities.length < 2 || temporalAnalysis.isPending}
              >
                {temporalAnalysis.isPending ? 'Analyzing...' : 'Run Analysis'}
              </button>

              {temporalAnalysis.isError && (
                <div className="error-message">
                  Error: {(temporalAnalysis.error as Error).message}
                </div>
              )}
            </form>
          </div>

          {/* Results */}
          {analysisResult && (
            <div className="card mt-md">
              <h3>Results</h3>

              {/* Summary */}
              <div className="results-summary">
                <div className="summary-stat">
                  <div
                    className="stat-value coordination-score"
                    style={{ color: getScoreColor(analysisResult.coordination_score ?? 0) }}
                  >
                    {((analysisResult.coordination_score ?? 0) * 100).toFixed(1)}%
                  </div>
                  <div className="stat-label">Coordination Score</div>
                </div>
                <div className="summary-stat">
                  <div className="stat-value">{analysisResult.entity_count ?? 0}</div>
                  <div className="stat-label">Entities Analyzed</div>
                </div>
                <div className="summary-stat">
                  <div className="stat-value">{analysisResult.event_count ?? 0}</div>
                  <div className="stat-label">Events Processed</div>
                </div>
                <div className="summary-stat">
                  <div className="stat-value">{analysisResult.hard_negatives_filtered ?? 0}</div>
                  <div className="stat-label">Hard Negatives Filtered</div>
                </div>
              </div>

              {/* Coordination indicator */}
              <div className={`coordination-indicator ${analysisResult.is_coordinated ? 'coordinated' : 'not-coordinated'}`}>
                {analysisResult.is_coordinated ? (
                  <>
                    <span className="indicator-icon">!</span>
                    <span>Coordination Detected</span>
                  </>
                ) : (
                  <>
                    <span className="indicator-icon">OK</span>
                    <span>No Significant Coordination</span>
                  </>
                )}
              </div>

              <p className="explanation">{analysisResult.explanation}</p>

              {/* Timeline visualization */}
              {timelineEvents.length > 0 && (
                <div className="results-section">
                  <h4>Publication Timeline</h4>
                  <Timeline
                    events={timelineEvents}
                    bursts={timelineBursts}
                    height="300px"
                    startDate={analysisResult.time_range_start}
                    endDate={analysisResult.time_range_end}
                  />
                </div>
              )}

              {/* Lead-lag pairs */}
              {(analysisResult.lead_lag_pairs?.length ?? 0) > 0 && (
                <div className="results-section">
                  <h4>Lead-Lag Relationships</h4>
                  <table className="results-table">
                    <thead>
                      <tr>
                        <th>Leader</th>
                        <th>Follower</th>
                        <th>Lag (min)</th>
                        <th>Correlation</th>
                        <th>Significant</th>
                      </tr>
                    </thead>
                    <tbody>
                      {((analysisResult.lead_lag_pairs ?? []) as LeadLagPair[]).map((pair, i) => (
                        <tr key={i}>
                          <td>
                            {selectedEntities.find((e) => e.id === pair.leader_entity_id)?.name ??
                              pair.leader_entity_id ?? 'Unknown'}
                          </td>
                          <td>
                            {selectedEntities.find((e) => e.id === pair.follower_entity_id)?.name ??
                              pair.follower_entity_id ?? 'Unknown'}
                          </td>
                          <td>{pair.lag_minutes ?? 0}</td>
                          <td>{(pair.correlation ?? 0).toFixed(3)}</td>
                          <td>
                            <span className={`badge ${pair.is_significant ? 'badge-warning' : 'badge-muted'}`}>
                              {pair.is_significant ? 'Yes' : 'No'}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Synchronized groups */}
              {(analysisResult.synchronized_groups?.length ?? 0) > 0 && (
                <div className="results-section">
                  <h4>Synchronization Analysis</h4>
                  {((analysisResult.synchronized_groups ?? []) as SynchronizedGroup[]).map((group, i) => (
                    <div key={i} className="sync-group-card">
                      <div className="sync-score">
                        Sync Score: <strong>{((group.sync_score ?? 0) * 100).toFixed(1)}%</strong>
                      </div>
                      <div className="sync-details">
                        <span>JS Divergence: {(group.js_divergence ?? 0).toFixed(4)}</span>
                        <span>Overlap: {((group.overlap_ratio ?? 0) * 100).toFixed(1)}%</span>
                        <span>Confidence: {((group.confidence ?? 0) * 100).toFixed(1)}%</span>
                      </div>
                      <div className="sync-entities">
                        {(group.entity_ids ?? []).map((eid) => (
                          <span key={eid} className="entity-chip small">
                            {selectedEntities.find((e) => e.id === eid)?.name ?? eid}
                          </span>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {!analysisResult && !temporalAnalysis.isPending && (
            <div className="card mt-md">
              <h3>Results</h3>
              <div className="empty-state">
                <p>No analysis results yet.</p>
                <p className="text-muted">
                  Configure and run an analysis above to see results.
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Funding Clusters Tab */}
      {activeTab === 'funding' && (
        <div className="detection-panel">
          <div className="card">
            <h2>Funding Cluster Detection</h2>
            <p className="text-muted mb-md">
              Identify groups of entities that share common funders, indicating
              potential coordinated influence.
            </p>

            <form className="analysis-form" onSubmit={(e) => { e.preventDefault(); handleRunFunding(); }}>
              <div className="form-row">
                <div className="form-group">
                  <label>Entity Type</label>
                  <select
                    value={fundingEntityType}
                    onChange={(e) => setFundingEntityType(e.target.value)}
                  >
                    <option value="">All Types</option>
                    <option value="Organization">Organization</option>
                    <option value="Outlet">Outlet</option>
                    <option value="Person">Person</option>
                  </select>
                </div>
                <div className="form-group">
                  <label>Fiscal Year</label>
                  <input
                    type="number"
                    min="2000"
                    max="2030"
                    value={fundingFiscalYear}
                    onChange={(e) => setFundingFiscalYear(parseInt(e.target.value) || 0)}
                  />
                </div>
                <div className="form-group">
                  <label>Min Shared Funders</label>
                  <input
                    type="number"
                    min="1"
                    value={fundingMinShared}
                    onChange={(e) => setFundingMinShared(parseInt(e.target.value) || 2)}
                  />
                </div>
              </div>

              <div className="form-group">
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={fundingCanadianOnly}
                    onChange={(e) => setFundingCanadianOnly(e.target.checked)}
                  />
                  Canadian entities only (companies & non-profits)
                </label>
              </div>

              <button
                type="submit"
                className="btn btn-primary"
                disabled={fundingAnalysis.isPending}
              >
                {fundingAnalysis.isPending ? 'Detecting...' : 'Find Clusters'}
              </button>

              {fundingAnalysis.isError && (
                <div className="error-message">
                  Error: {(fundingAnalysis.error as Error).message}
                </div>
              )}
            </form>
          </div>

          {fundingResult && (
            <div className="card mt-md">
              <h3>Funding Clusters ({fundingResult.total_clusters ?? 0})</h3>
              <p className="explanation">{fundingResult.explanation ?? ''}</p>

              {(fundingResult.clusters?.length ?? 0) > 0 ? (
                <div className="results-section">
                  <table className="results-table">
                    <thead>
                      <tr>
                        <th>Shared Funder</th>
                        <th>Members</th>
                        <th>Total Funding</th>
                        <th>Score</th>
                      </tr>
                    </thead>
                    <tbody>
                      {((fundingResult.clusters ?? []) as FundingCluster[]).map((cluster, idx) => (
                        <tr key={cluster.cluster_id ?? idx}>
                          <td>{cluster.shared_funder?.name ?? 'Unknown'}</td>
                          <td>
                            <div className="selected-entities">
                              {(cluster.members ?? []).map((m, i) => (
                                <span key={m.id ?? i} className="entity-chip small">
                                  {m.name ?? 'Unknown'}
                                </span>
                              ))}
                            </div>
                          </td>
                          <td>${(cluster.total_funding ?? 0).toLocaleString()}</td>
                          <td>
                            <span
                              className="stat-value"
                              style={{ color: getScoreColor(cluster.score ?? 0), fontSize: '1rem' }}
                            >
                              {((cluster.score ?? 0) * 100).toFixed(1)}%
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="empty-state">
                  <p>No funding clusters found matching the criteria.</p>
                </div>
              )}
            </div>
          )}

          {!fundingResult && !fundingAnalysis.isPending && (
            <div className="card mt-md">
              <h3>Funding Clusters</h3>
              <div className="empty-state">
                <p>No results yet.</p>
                <p className="text-muted">
                  Configure and run a funding cluster search above.
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Politically Active Funded Tab */}
      {activeTab === 'political' && (
        <div className="detection-panel">
          <div className="card">
            <h2>Politically Active Funded Entities</h2>
            <p className="text-muted mb-md">
              Find organizations that receive funding AND engage in political activity
              (political ads, lobbying). Helps identify potential influence campaigns.
            </p>

            <form className="analysis-form" onSubmit={(e) => { e.preventDefault(); handleRunPolitical(); }}>
              <div className="form-row">
                <div className="form-group">
                  <label>Recipient Jurisdiction</label>
                  <select
                    value={politicalJurisdiction}
                    onChange={(e) => setPoliticalJurisdiction(e.target.value)}
                  >
                    <option value="CA">Canada</option>
                    <option value="US">United States</option>
                    <option value="">All</option>
                  </select>
                </div>
                <div className="form-group">
                  <label>Minimum Total Funding ($)</label>
                  <input
                    type="number"
                    min="0"
                    value={politicalMinFunding}
                    onChange={(e) => setPoliticalMinFunding(parseInt(e.target.value) || 0)}
                  />
                </div>
                <div className="form-group">
                  <label>Fiscal Year (optional)</label>
                  <input
                    type="number"
                    min="2000"
                    max="2030"
                    placeholder="Any year"
                    value={politicalFiscalYear ?? ''}
                    onChange={(e) => setPoliticalFiscalYear(e.target.value ? parseInt(e.target.value) : null)}
                  />
                </div>
              </div>

              <div className="form-group">
                <label>Include Activity Types</label>
                <div className="checkbox-row">
                  <label className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={politicalIncludeAds}
                      onChange={(e) => setPoliticalIncludeAds(e.target.checked)}
                    />
                    Political Ads (Meta)
                  </label>
                  <label className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={politicalIncludeLobby}
                      onChange={(e) => setPoliticalIncludeLobby(e.target.checked)}
                    />
                    Lobbying Activity
                  </label>
                </div>
              </div>

              <button
                type="submit"
                className="btn btn-primary"
                disabled={politicalAnalysis.isPending}
              >
                {politicalAnalysis.isPending ? 'Searching...' : 'Find Politically Active Entities'}
              </button>

              {politicalAnalysis.isError && (
                <div className="error-message">
                  Error: {(politicalAnalysis.error as Error).message}
                </div>
              )}
            </form>
          </div>

          {politicalResult && (
            <div className="card mt-md">
              <h3>Results ({politicalResult.total_results ?? 0} entities)</h3>
              <p className="explanation">{politicalResult.explanation ?? ''}</p>

              {(politicalResult.results?.length ?? 0) > 0 ? (
                <div className="results-section">
                  <table className="results-table">
                    <thead>
                      <tr>
                        <th>Entity</th>
                        <th>Jurisdiction</th>
                        <th>Total Funding</th>
                        <th>Funders</th>
                        <th>Political Ads</th>
                        <th>Ad Spend</th>
                        <th>Lobbying</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(politicalResult.results ?? []).map((result, idx) => (
                        <tr key={result.entity_id ?? idx}>
                          <td>
                            <strong>{result.entity_name ?? 'Unknown'}</strong>
                            <br />
                            <small className="text-muted">{result.entity_type ?? ''}</small>
                          </td>
                          <td>{result.jurisdiction ?? '-'}</td>
                          <td>${(result.total_funding ?? 0).toLocaleString()}</td>
                          <td>
                            <span className="badge badge-muted">{result.funder_count ?? 0}</span>
                            {(result.funders?.length ?? 0) > 0 && (
                              <div className="selected-entities" style={{ marginTop: '4px' }}>
                                {(result.funders ?? []).slice(0, 3).map((f, i) => (
                                  <span key={i} className="entity-chip small" title={`$${(f.amount ?? 0).toLocaleString()}`}>
                                    {f.name ?? 'Unknown'}
                                  </span>
                                ))}
                              </div>
                            )}
                          </td>
                          <td>
                            {result.has_political_ads ? (
                              <span className="badge badge-warning">{result.political_ad_count ?? 0} ads</span>
                            ) : (
                              <span className="text-muted">-</span>
                            )}
                          </td>
                          <td>
                            {result.has_political_ads ? (
                              <span>${(result.political_ad_spend ?? 0).toLocaleString()}</span>
                            ) : (
                              <span className="text-muted">-</span>
                            )}
                          </td>
                          <td>
                            {result.has_lobbying ? (
                              <span className="badge badge-warning">{result.lobbying_count ?? 0}</span>
                            ) : (
                              <span className="text-muted">-</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="empty-state">
                  <p>No politically active funded entities found matching the criteria.</p>
                </div>
              )}
            </div>
          )}

          {!politicalResult && !politicalAnalysis.isPending && (
            <div className="card mt-md">
              <h3>Politically Active Funded Entities</h3>
              <div className="empty-state">
                <p>No results yet.</p>
                <p className="text-muted">
                  Configure and run a search above to find entities that receive funding
                  and engage in political activity.
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Infrastructure Sharing Tab */}
      {activeTab === 'infrastructure' && (
        <div className="detection-panel">
          <div className="card">
            <h2>Infrastructure Sharing Analysis</h2>
            <p className="text-muted mb-md">
              Detect outlets sharing technical infrastructure (hosting, analytics,
              DNS, SSL) even when organizational links are hidden.
            </p>

            <form className="analysis-form" onSubmit={(e) => { e.preventDefault(); handleRunInfra(); }}>
              <div className="form-group">
                <label>Domains to Analyze</label>
                <input
                  type="text"
                  value={infraDomains}
                  onChange={(e) => setInfraDomains(e.target.value)}
                  placeholder="example1.com, example2.com, example3.com"
                />
                <small className="text-muted">
                  Enter at least 2 domains separated by commas
                </small>
              </div>

              <div className="form-row">
                <div className="form-group">
                  <label>Minimum Match Score</label>
                  <input
                    type="number"
                    min="0"
                    step="0.5"
                    value={infraMinScore}
                    onChange={(e) => setInfraMinScore(parseFloat(e.target.value) || 1.0)}
                  />
                </div>
              </div>

              <button
                type="submit"
                className="btn btn-primary"
                disabled={infraAnalysis.isPending}
              >
                {infraAnalysis.isPending ? 'Scanning...' : 'Analyze Infrastructure'}
              </button>

              {infraAnalysis.isError && (
                <div className="error-message">
                  Error: {(infraAnalysis.error as Error).message}
                </div>
              )}
            </form>
          </div>

          {infraResult && (
            <div className="card mt-md">
              <h3>Infrastructure Results</h3>
              <p className="explanation">{infraResult.explanation ?? ''}</p>

              {(infraResult.errors?.length ?? 0) > 0 && (
                <div className="error-message" style={{ marginTop: 'var(--spacing-sm)' }}>
                  {(infraResult.errors ?? []).map((err, i) => (
                    <div key={i}>{String(err)}</div>
                  ))}
                </div>
              )}

              <div className="results-summary">
                <div className="summary-stat">
                  <div className="stat-value">{infraResult.domains_scanned ?? 0}</div>
                  <div className="stat-label">Domains Scanned</div>
                </div>
                <div className="summary-stat">
                  <div className="stat-value">{infraResult.total_matches ?? 0}</div>
                  <div className="stat-label">Matches Found</div>
                </div>
              </div>

              {/* Pairwise Matches */}
              {(infraResult.matches?.length ?? 0) > 0 && (
                <div className="results-section">
                  <h4>Pairwise Matches</h4>
                  <table className="results-table">
                    <thead>
                      <tr>
                        <th>Domain A</th>
                        <th>Domain B</th>
                        <th>Signals</th>
                        <th>Score</th>
                        <th>Confidence</th>
                      </tr>
                    </thead>
                    <tbody>
                      {((infraResult.matches ?? []) as InfraMatch[]).map((match, i) => (
                        <tr key={i}>
                          <td>{match.domain_a ?? 'Unknown'}</td>
                          <td>{match.domain_b ?? 'Unknown'}</td>
                          <td>
                            <div className="selected-entities">
                              {(match.signals ?? []).map((sig, j) => (
                                <span
                                  key={j}
                                  className="entity-chip small"
                                  title={sig.description ?? ''}
                                  style={{
                                    background: (sig.weight ?? 0) >= 3 ? '#DC2626' : (sig.weight ?? 0) >= 2 ? '#F59E0B' : 'var(--color-primary)',
                                  }}
                                >
                                  {sig.signal_type ?? 'unknown'}: {sig.value ?? ''}
                                </span>
                              ))}
                            </div>
                          </td>
                          <td>{(match.total_score ?? 0).toFixed(1)}</td>
                          <td>{((match.confidence ?? 0) * 100).toFixed(0)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Domain Profiles */}
              {(infraResult.profiles?.length ?? 0) > 0 && (
                <div className="results-section">
                  <h4>Domain Profiles</h4>
                  {((infraResult.profiles ?? []) as InfraProfile[]).map((profile, i) => (
                    <div key={i} className="sync-group-card">
                      <div className="sync-score">
                        <strong>{profile.domain ?? 'Unknown'}</strong>
                        {profile.error && (
                          <span className="badge badge-warning" style={{ marginLeft: '8px' }}>
                            Error: {profile.error}
                          </span>
                        )}
                      </div>
                      <div className="sync-details">
                        {profile.dns && (
                          <span>NS: {profile.dns.nameservers?.join(', ') ?? 'N/A'}</span>
                        )}
                        {profile.whois?.registrar && (
                          <span>Registrar: {profile.whois.registrar}</span>
                        )}
                        {profile.ssl?.issuer && (
                          <span>SSL: {profile.ssl.issuer}</span>
                        )}
                        {profile.hosting && profile.hosting.length > 0 && (
                          <span>
                            Hosting: {profile.hosting.map((h) => h.provider ?? h.ip ?? 'Unknown').join(', ')}
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {(infraResult.matches?.length ?? 0) === 0 && (
                <div className="empty-state">
                  <p>No shared infrastructure detected above the minimum score.</p>
                </div>
              )}
            </div>
          )}

          {!infraResult && !infraAnalysis.isPending && (
            <div className="card mt-md">
              <h3>Shared Infrastructure</h3>
              <div className="empty-state">
                <p>No results yet.</p>
                <p className="text-muted">
                  Enter domains above to scan for shared infrastructure.
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Composite Score Tab */}
      {activeTab === 'composite' && (
        <div className="detection-panel">
          <div className="card">
            <h2>Composite Score Analysis</h2>
            <p className="text-muted mb-md">
              Calculate a multi-signal coordination score combining temporal,
              funding, and infrastructure signals with correlation-aware weighting.
            </p>

            <form className="analysis-form" onSubmit={(e) => { e.preventDefault(); handleRunComposite(); }}>
              <div className="form-group">
                <label>Entities to Analyze</label>
                <div className="entity-search-container">
                  <input
                    type="text"
                    value={compositeSearchQuery}
                    onChange={(e) => setCompositeSearchQuery(e.target.value)}
                    placeholder="Search for entities to add..."
                  />
                  {isCompositeSearching && <span className="search-loading">Searching...</span>}
                  {compositeSearchResults?.results && compositeSearchResults.results.length > 0 && compositeSearchQuery && (
                    <div className="search-dropdown">
                      {compositeSearchResults.results.map((entity) => (
                        <button
                          key={entity.id}
                          type="button"
                          className="search-result"
                          onClick={() => handleAddCompositeEntity(entity)}
                        >
                          <span className="entity-type-badge">{entity.entity_type}</span>
                          {entity.name}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                <div className="selected-entities">
                  {compositeEntities.map((entity) => (
                    <span key={entity.id} className="entity-chip">
                      {entity.name}
                      <button
                        type="button"
                        className="chip-remove"
                        onClick={() => handleRemoveCompositeEntity(entity.id)}
                      >
                        x
                      </button>
                    </span>
                  ))}
                </div>
                <small className="text-muted">
                  Select at least 2 entities ({compositeEntities.length} selected)
                </small>
              </div>

              <div className="form-group">
                <label>Signals to Include</label>
                <div className="checkbox-row">
                  <label className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={includeTemporal}
                      onChange={(e) => setIncludeTemporal(e.target.checked)}
                    />
                    Temporal coordination
                  </label>
                  <label className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={includeFunding}
                      onChange={(e) => setIncludeFunding(e.target.checked)}
                    />
                    Funding clusters
                  </label>
                  <label className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={includeInfra}
                      onChange={(e) => setIncludeInfra(e.target.checked)}
                    />
                    Infrastructure sharing
                  </label>
                </div>
              </div>

              <button
                type="submit"
                className="btn btn-primary"
                disabled={compositeEntities.length < 2 || compositeAnalysis.isPending}
              >
                {compositeAnalysis.isPending ? 'Calculating...' : 'Calculate Composite Score'}
              </button>

              {compositeAnalysis.isError && (
                <div className="error-message">
                  Error: {(compositeAnalysis.error as Error).message}
                </div>
              )}
            </form>
          </div>

          {/* Composite Results */}
          {compositeResult && (
            <div className="card mt-md">
              <h3>Composite Score Results</h3>

              {/* Main scores */}
              <div className="composite-scores">
                <div className="composite-main-score">
                  <div
                    className="stat-value coordination-score"
                    style={{ color: getScoreColor(compositeResult.overall_score ?? 0) }}
                  >
                    {((compositeResult.overall_score ?? 0) * 100).toFixed(1)}%
                  </div>
                  <div className="stat-label">Overall Score</div>
                </div>
                <div className="composite-main-score">
                  <div
                    className="stat-value coordination-score"
                    style={{ color: getScoreColor(compositeResult.adjusted_score ?? 0) }}
                  >
                    {((compositeResult.adjusted_score ?? 0) * 100).toFixed(1)}%
                  </div>
                  <div className="stat-label">Adjusted Score</div>
                </div>
              </div>

              {/* Flagged status */}
              <div className={`coordination-indicator ${compositeResult.flagged ? 'coordinated' : 'not-coordinated'}`}>
                {compositeResult.flagged ? (
                  <>
                    <span className="indicator-icon">!</span>
                    <span>Flagged: {compositeResult.flag_reason ?? 'Unknown reason'}</span>
                  </>
                ) : (
                  <>
                    <span className="indicator-icon">OK</span>
                    <span>Not Flagged</span>
                  </>
                )}
              </div>

              {/* Signal breakdown bar */}
              {compositeResult.signal_breakdown && (
                <div className="results-section">
                  <h4>Signal Breakdown</h4>
                  <div className="signal-breakdown">
                    {Object.entries(compositeResult.signal_breakdown).map(([signal, value]) => (
                      <div key={signal} className="signal-bar-row">
                        <span className="signal-label">{signal}</span>
                        <div className="signal-bar-track">
                          <div
                            className="signal-bar-fill"
                            style={{
                              width: `${(value as number) * 100}%`,
                              backgroundColor: getScoreColor(value as number),
                            }}
                          />
                        </div>
                        <span className="signal-value">{((value as number) * 100).toFixed(1)}%</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Category breakdown */}
              {compositeResult.category_breakdown && (
                <div className="results-section">
                  <h4>Category Breakdown</h4>
                  <table className="results-table">
                    <thead>
                      <tr>
                        <th>Category</th>
                        <th>Score</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(compositeResult.category_breakdown).map(([cat, score]) => (
                        <tr key={cat}>
                          <td>{cat}</td>
                          <td>
                            <span style={{ color: getScoreColor(score as number), fontWeight: 600 }}>
                              {((score as number) * 100).toFixed(1)}%
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {/* Confidence band */}
              {compositeResult.confidence_band && (
                <div className="results-section">
                  <h4>Confidence Band</h4>
                  <div className="confidence-band">
                    <div className="confidence-range">
                      <span className="confidence-lower">
                        {((compositeResult.confidence_band.lower ?? 0) * 100).toFixed(1)}%
                      </span>
                      <div className="confidence-bar-track">
                        <div
                          className="confidence-bar-range"
                          style={{
                            left: `${(compositeResult.confidence_band.lower ?? 0) * 100}%`,
                            width: `${((compositeResult.confidence_band.upper ?? 1) - (compositeResult.confidence_band.lower ?? 0)) * 100}%`,
                          }}
                        />
                        <div
                          className="confidence-bar-marker"
                          style={{ left: `${(compositeResult.overall_score ?? 0) * 100}%` }}
                        />
                      </div>
                      <span className="confidence-upper">
                        {((compositeResult.confidence_band.upper ?? 1) * 100).toFixed(1)}%
                      </span>
                    </div>
                  </div>
                </div>
              )}

              {/* Validation messages */}
              {compositeResult.validation_messages && compositeResult.validation_messages.length > 0 && (
                <div className="results-section">
                  <h4>Validation</h4>
                  <div className={`validation-status ${compositeResult.validation_passed ? 'passed' : 'failed'}`}>
                    {compositeResult.validation_passed ? 'Validation Passed' : 'Validation Issues'}
                  </div>
                  <ul className="validation-messages">
                    {compositeResult.validation_messages.map((msg, i) => (
                      <li key={i}>{msg}</li>
                    ))}
                  </ul>
                </div>
              )}

              {/* Explain button */}
              {compositeResult.finding_id && (
                <div className="results-section">
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => handleExplainFinding(compositeResult.finding_id!)}
                    disabled={showExplanation}
                  >
                    Explain Finding
                  </button>
                </div>
              )}

              {/* Explanation panel */}
              {showExplanation && explanation && (
                <div className="results-section explanation-panel">
                  <h4>Finding Explanation</h4>
                  <div className="explanation">{explanation.why_flagged}</div>

                  {explanation.evidence_summary && explanation.evidence_summary.length > 0 && (
                    <div style={{ marginTop: 'var(--spacing-md)' }}>
                      <h5>Evidence Summary</h5>
                      <ul className="evidence-list">
                        {explanation.evidence_summary.map((item, i) => (
                          <li key={i}>{typeof item === 'string' ? item : JSON.stringify(item)}</li>
                        ))}
                      </ul>
                    </div>
                  )}

                  {explanation.hard_negatives_checked && explanation.hard_negatives_checked.length > 0 && (
                    <p className="text-muted" style={{ marginTop: 'var(--spacing-sm)' }}>
                      Hard negatives checked: {explanation.hard_negatives_checked.length}
                    </p>
                  )}

                  {explanation.recommendations && explanation.recommendations.length > 0 && (
                    <div style={{ marginTop: 'var(--spacing-md)' }}>
                      <h5>Recommendations</h5>
                      <ul className="evidence-list">
                        {explanation.recommendations.map((rec, i) => (
                          <li key={i}>{rec}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {!compositeResult && !compositeAnalysis.isPending && (
            <div className="card mt-md">
              <h3>Composite Score</h3>
              <div className="empty-state">
                <p>No results yet.</p>
                <p className="text-muted">
                  Select entities and signal types above to calculate a composite coordination score.
                </p>
              </div>
            </div>
          )}
        </div>
      )}

      <style>{`
        .tabs {
          display: flex;
          gap: var(--spacing-xs);
          margin-bottom: var(--spacing-lg);
          border-bottom: 1px solid var(--border-color);
          padding-bottom: var(--spacing-xs);
        }

        .tab {
          padding: var(--spacing-sm) var(--spacing-md);
          border: none;
          background: none;
          cursor: pointer;
          font-size: 0.875rem;
          font-weight: 500;
          color: var(--text-secondary);
          border-radius: var(--border-radius) var(--border-radius) 0 0;
          transition: all 0.2s ease;
        }

        .tab:hover {
          background-color: var(--bg-tertiary);
          color: var(--text-primary);
        }

        .tab.active {
          background-color: var(--color-primary);
          color: white;
        }

        .detection-panel h2 {
          margin-bottom: var(--spacing-sm);
        }

        .analysis-form {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
          margin-top: var(--spacing-lg);
        }

        .form-group {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-xs);
        }

        .form-row {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
        }

        .entity-search-container {
          position: relative;
        }

        .search-dropdown {
          position: absolute;
          top: 100%;
          left: 0;
          right: 0;
          background: var(--bg-primary);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          box-shadow: var(--shadow-md);
          z-index: 100;
          max-height: 200px;
          overflow-y: auto;
        }

        .search-result {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          width: 100%;
          padding: var(--spacing-sm);
          border: none;
          background: none;
          text-align: left;
          cursor: pointer;
        }

        .search-result:hover {
          background: var(--bg-tertiary);
        }

        .entity-type-badge {
          font-size: 10px;
          padding: 2px 6px;
          border-radius: 4px;
          background: var(--bg-tertiary);
          color: var(--text-muted);
        }

        .selected-entities {
          display: flex;
          flex-wrap: wrap;
          gap: var(--spacing-xs);
          margin-top: var(--spacing-sm);
        }

        .entity-chip {
          display: inline-flex;
          align-items: center;
          gap: var(--spacing-xs);
          padding: 4px 8px;
          background: var(--color-primary);
          color: white;
          border-radius: 16px;
          font-size: 12px;
        }

        .entity-chip.small {
          padding: 2px 6px;
          font-size: 11px;
        }

        .chip-remove {
          background: none;
          border: none;
          color: white;
          cursor: pointer;
          font-size: 14px;
          line-height: 1;
          padding: 0 2px;
          opacity: 0.7;
        }

        .chip-remove:hover {
          opacity: 1;
        }

        .checkbox-label {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          cursor: pointer;
        }

        .checkbox-label input {
          width: auto;
        }

        .error-message {
          color: #DC2626;
          padding: var(--spacing-sm);
          background: rgba(220, 38, 38, 0.1);
          border-radius: var(--border-radius);
        }

        .results-summary {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
          gap: var(--spacing-md);
          margin: var(--spacing-lg) 0;
        }

        .summary-stat {
          text-align: center;
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
        }

        .stat-value {
          font-size: 1.5rem;
          font-weight: 700;
        }

        .stat-label {
          font-size: 0.75rem;
          color: var(--text-muted);
          margin-top: var(--spacing-xs);
        }

        .coordination-score {
          font-size: 2rem;
        }

        .coordination-indicator {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          padding: var(--spacing-md);
          border-radius: var(--border-radius);
          font-weight: 600;
        }

        .coordination-indicator.coordinated {
          background: rgba(220, 38, 38, 0.1);
          color: #DC2626;
        }

        .coordination-indicator.not-coordinated {
          background: rgba(16, 185, 129, 0.1);
          color: #10B981;
        }

        .indicator-icon {
          width: 24px;
          height: 24px;
          display: flex;
          align-items: center;
          justify-content: center;
          border-radius: 50%;
          font-size: 12px;
        }

        .coordinated .indicator-icon {
          background: #DC2626;
          color: white;
        }

        .not-coordinated .indicator-icon {
          background: #10B981;
          color: white;
        }

        .explanation {
          margin-top: var(--spacing-md);
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
          font-style: italic;
        }

        .results-section {
          margin-top: var(--spacing-lg);
          padding-top: var(--spacing-lg);
          border-top: 1px solid var(--border-color);
        }

        .results-section h4 {
          margin-bottom: var(--spacing-md);
        }

        .results-table {
          width: 100%;
          border-collapse: collapse;
          font-size: 0.875rem;
        }

        .results-table th,
        .results-table td {
          padding: var(--spacing-sm);
          text-align: left;
          border-bottom: 1px solid var(--border-color);
        }

        .results-table th {
          background: var(--bg-tertiary);
          font-weight: 600;
        }

        .badge {
          padding: 2px 8px;
          border-radius: 4px;
          font-size: 11px;
          font-weight: 500;
        }

        .badge-warning {
          background: rgba(245, 158, 11, 0.2);
          color: #D97706;
        }

        .badge-muted {
          background: var(--bg-tertiary);
          color: var(--text-muted);
        }

        .sync-group-card {
          padding: var(--spacing-md);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
          margin-bottom: var(--spacing-md);
        }

        .sync-score {
          font-size: 1.125rem;
          margin-bottom: var(--spacing-sm);
        }

        .sync-details {
          display: flex;
          gap: var(--spacing-md);
          font-size: 0.75rem;
          color: var(--text-muted);
          margin-bottom: var(--spacing-sm);
        }

        .sync-entities {
          display: flex;
          flex-wrap: wrap;
          gap: var(--spacing-xs);
        }

        .empty-state {
          text-align: center;
          padding: var(--spacing-xl);
        }

        .search-loading {
          position: absolute;
          right: 12px;
          top: 50%;
          transform: translateY(-50%);
          font-size: 12px;
          color: var(--text-muted);
        }

        .checkbox-row {
          display: flex;
          gap: var(--spacing-lg);
          flex-wrap: wrap;
        }

        .composite-scores {
          display: flex;
          gap: var(--spacing-xl);
          justify-content: center;
          margin: var(--spacing-lg) 0;
        }

        .composite-main-score {
          text-align: center;
          padding: var(--spacing-lg);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
          min-width: 160px;
        }

        .signal-breakdown {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
        }

        .signal-bar-row {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
        }

        .signal-label {
          width: 120px;
          font-size: 0.875rem;
          font-weight: 500;
          text-transform: capitalize;
        }

        .signal-bar-track {
          flex: 1;
          height: 20px;
          background: var(--bg-tertiary);
          border-radius: 10px;
          overflow: hidden;
        }

        .signal-bar-fill {
          height: 100%;
          border-radius: 10px;
          transition: width 0.3s ease;
        }

        .signal-value {
          width: 60px;
          text-align: right;
          font-size: 0.875rem;
          font-weight: 600;
        }

        .confidence-band {
          padding: var(--spacing-md) 0;
        }

        .confidence-range {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
        }

        .confidence-lower,
        .confidence-upper {
          font-size: 0.875rem;
          font-weight: 600;
          min-width: 50px;
        }

        .confidence-lower {
          text-align: right;
        }

        .confidence-bar-track {
          flex: 1;
          height: 24px;
          background: var(--bg-tertiary);
          border-radius: 12px;
          position: relative;
          overflow: hidden;
        }

        .confidence-bar-range {
          position: absolute;
          top: 0;
          height: 100%;
          background: rgba(59, 130, 246, 0.3);
          border-radius: 12px;
        }

        .confidence-bar-marker {
          position: absolute;
          top: 2px;
          width: 4px;
          height: 20px;
          background: var(--color-primary);
          border-radius: 2px;
          transform: translateX(-50%);
        }

        .validation-status {
          padding: var(--spacing-sm) var(--spacing-md);
          border-radius: var(--border-radius);
          font-weight: 600;
          margin-bottom: var(--spacing-sm);
        }

        .validation-status.passed {
          background: rgba(16, 185, 129, 0.1);
          color: #10B981;
        }

        .validation-status.failed {
          background: rgba(220, 38, 38, 0.1);
          color: #DC2626;
        }

        .validation-messages {
          list-style: none;
          padding: 0;
          margin: var(--spacing-sm) 0 0 0;
        }

        .validation-messages li {
          padding: var(--spacing-xs) 0;
          font-size: 0.875rem;
          color: var(--text-secondary);
          border-bottom: 1px solid var(--border-color);
        }

        .validation-messages li:last-child {
          border-bottom: none;
        }

        .explanation-panel {
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
          padding: var(--spacing-lg);
        }

        .explanation-panel h5 {
          margin-bottom: var(--spacing-sm);
          font-size: 0.875rem;
        }

        .evidence-list {
          list-style: disc;
          padding-left: var(--spacing-lg);
          margin: 0;
        }

        .evidence-list li {
          padding: var(--spacing-xs) 0;
          font-size: 0.875rem;
          color: var(--text-secondary);
        }
      `}</style>
    </div>
  );
}
