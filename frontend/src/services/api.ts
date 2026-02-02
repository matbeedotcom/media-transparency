/**
 * API client service for MITDS
 *
 * Provides typed API calls using axios and TanStack Query integration.
 */

import axios, { AxiosError, AxiosInstance } from 'axios';

// =========================
// Types
// =========================

export interface Entity {
  id: string;
  entity_type: string;
  name: string;
  aliases?: string[];
  created_at: string;
  updated_at?: string;
  confidence: number;
  properties?: Record<string, unknown>;
}

export interface Relationship {
  id: string;
  rel_type: string;
  source_entity: EntitySummary;
  target_entity: EntitySummary;
  valid_from?: string;
  valid_to?: string;
  confidence: number;
  properties?: Record<string, unknown>;
  evidence_count?: number;
}

export interface EntitySummary {
  id: string;
  entity_type: string;
  name: string;
}

export interface Evidence {
  id: string;
  evidence_type: string;
  source_url: string;
  archive_url?: string;
  retrieved_at: string;
}

export interface FundingCluster {
  id: string;
  outlets: EntitySummary[];
  shared_funders: EntitySummary[];
  total_shared_funding: number;
  coordination_score: number;
}

export interface IngestionStatus {
  source: string;
  status: 'healthy' | 'degraded' | 'failed' | 'stale' | 'disabled' | 'never_run' | 'running';
  last_run_id?: string;
  last_run_status?: string;
  last_successful_run?: string;
  next_scheduled_run?: string;
  records_processed: number;
  records_created: number;
  records_updated: number;
  last_error?: string;
}

export interface PaginatedResponse<T> {
  results: T[];
  total: number;
  limit: number;
  offset: number;
}

export interface AsyncJobResponse {
  job_id: string;
  status: 'queued' | 'running' | 'completed' | 'failed';
  status_url?: string;
}

export interface JobStatus {
  job_id: string;
  status: 'queued' | 'running' | 'completed' | 'failed';
  progress?: number;
  started_at?: string;
  completed_at?: string;
  result_url?: string;
  error?: string;
}

// =========================
// API Client
// =========================

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api/v1';

const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add auth token interceptor
apiClient.interceptors.request.use((config) => {
  const token = localStorage.getItem('auth_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Add error handling interceptor
apiClient.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response?.status === 401) {
      // Handle unauthorized - clear token
      // TODO: Add redirect to login page when auth is implemented
      localStorage.removeItem('auth_token');
    }
    return Promise.reject(error);
  }
);

// =========================
// API Functions
// =========================

// Entities
export const searchEntities = async (params: {
  q?: string;
  type?: string;
  jurisdiction?: string;
  limit?: number;
  offset?: number;
}): Promise<PaginatedResponse<Entity>> => {
  const response = await apiClient.get('/entities', { params });
  return response.data;
};

export const getEntity = async (id: string): Promise<Entity> => {
  const response = await apiClient.get(`/entities/${id}`);
  return response.data;
};

export const getEntityRelationships = async (
  id: string,
  params?: {
    rel_type?: string;
    direction?: 'inbound' | 'outbound' | 'both';
    as_of?: string;
  }
): Promise<{ relationships: Relationship[]; total: number }> => {
  const response = await apiClient.get(`/entities/${id}/relationships`, { params });
  return response.data;
};

export const getEntityEvidence = async (
  id: string
): Promise<{ evidence: Evidence[] }> => {
  const response = await apiClient.get(`/entities/${id}/evidence`);
  return response.data;
};

export interface BoardInterlock {
  director: EntitySummary;
  organizations: EntitySummary[];
}

export const getEntityBoardInterlocks = async (
  id: string
): Promise<{ interlocks: BoardInterlock[]; total: number }> => {
  const response = await apiClient.get(`/entities/${id}/board-interlocks`);
  return response.data;
};

// Path Result type
export interface PathResult {
  source: EntitySummary;
  target: EntitySummary;
  path_length: number;
  path_types: string[];
  nodes: EntitySummary[];
  relationships: Relationship[];
}

// Relationships
export const findPaths = async (params: {
  from_id: string;
  to_id: string;
  max_hops?: number;
  rel_types?: string;
}): Promise<{ paths: unknown[]; from_entity: EntitySummary; to_entity: EntitySummary }> => {
  const response = await apiClient.get('/relationships/path', { params });
  return response.data;
};

export const findAllPaths = async (
  fromId: string,
  toId: string,
  options?: {
    maxHops?: number;
    relTypes?: string;
    limit?: number;
  }
): Promise<{ paths: PathResult[]; paths_found: number }> => {
  const response = await apiClient.get('/relationships/paths/all', {
    params: {
      from_id: fromId,
      to_id: toId,
      max_hops: options?.maxHops,
      rel_types: options?.relTypes,
      limit: options?.limit,
    },
  });
  return response.data;
};

export const getGraphAtTime = async (
  entityId: string,
  asOf: string,
  options?: {
    depth?: number;
    relTypes?: string;
  }
): Promise<{
  entity: EntitySummary;
  as_of: string;
  relationships: Array<{
    id: string;
    rel_type: string;
    source: EntitySummary;
    target: EntitySummary;
    valid_from?: string;
    valid_to?: string;
    is_current: boolean;
  }>;
}> => {
  const response = await apiClient.get(`/relationships/graph-at-time/${entityId}`, {
    params: {
      as_of: asOf,
      depth: options?.depth,
      rel_types: options?.relTypes,
    },
  });
  return response.data;
};

export const getRelationshipChanges = async (
  entityId: string,
  fromDate: string,
  toDate: string,
  options?: { relTypes?: string }
): Promise<{
  entity_id: string;
  from_date: string;
  to_date: string;
  added_relationships: unknown[];
  removed_relationships: unknown[];
  modified_relationships: unknown[];
  summary: { total_added: number; total_removed: number; total_modified: number };
}> => {
  const response = await apiClient.get('/relationships/changes', {
    params: {
      entity_id: entityId,
      from_date: fromDate,
      to_date: toDate,
      rel_types: options?.relTypes,
    },
  });
  return response.data;
};

export const getFundingClusters = async (params?: {
  min_shared_funders?: number;
  min_funding_amount?: number;
  as_of?: string;
}): Promise<{ clusters: FundingCluster[] }> => {
  const response = await apiClient.get('/relationships/funding-clusters', { params });
  return response.data;
};

// Detection
export const analyzeTemporalCoordination = async (data: {
  entity_ids: string[];
  start_date: string;
  end_date: string;
  event_types?: string[];
  exclude_hard_negatives?: boolean;
  async_mode?: boolean;
}): Promise<TemporalAnalysisResponse> => {
  const response = await apiClient.post('/detection/temporal-coordination', data);
  return response.data;
};

export interface TemporalBurstPeriod {
  startTime: string;
  endTime: string;
  eventCount: number;
  intensity: number;
}

export interface TemporalBurstResult {
  entity_id: string;
  bursts: TemporalBurstPeriod[];
  total_events: number;
  burst_count: number;
  avg_events_per_day: number;
}

export interface LeadLagResult {
  leader_entity_id: string;
  follower_entity_id: string;
  lag_minutes: number;
  correlation: number;
  p_value: number;
  sample_size: number;
  is_significant: boolean;
}

export interface SyncGroupResult {
  entity_ids: string[];
  sync_score: number;
  js_divergence: number;
  overlap_ratio: number;
  time_window_hours: number;
  confidence: number;
}

export interface TemporalAnalysisResponse {
  analysis_id: string;
  status: string;
  analyzed_at: string;
  time_range_start: string;
  time_range_end: string;
  entity_count: number;
  event_count: number;
  coordination_score: number;
  confidence: number;
  is_coordinated: boolean;
  explanation: string;
  bursts: TemporalBurstResult[];
  lead_lag_pairs: LeadLagResult[];
  synchronized_groups: SyncGroupResult[];
  hard_negatives_filtered: number;
}

export interface CompositeScoreResponse {
  finding_id: string;
  overall_score: number;
  adjusted_score: number;
  signal_breakdown: Record<string, number>;
  category_breakdown: Record<string, number>;
  flagged: boolean;
  flag_reason: string | null;
  confidence_band: { lower: number; upper: number };
  entities_analyzed: number;
  explanation: string;
  validation_passed: boolean;
  validation_messages: string[];
}

export const calculateCompositeScore = async (data: {
  entity_ids: string[];
  weights?: Record<string, number>;
  include_temporal?: boolean;
  include_funding?: boolean;
  include_infrastructure?: boolean;
}): Promise<CompositeScoreResponse> => {
  const response = await apiClient.post('/detection/composite-score', data);
  return response.data;
};

// Funding Cluster Detection
export interface FundingClusterDetectionRequest {
  entity_type?: string;
  fiscal_year?: number;
  min_shared_funders?: number;
  limit?: number;
}

export interface FundingClusterDetectionResponse {
  clusters: Array<{
    cluster_id: string;
    shared_funder: EntitySummary;
    members: EntitySummary[];
    total_funding: number;
    funding_by_member: Record<string, number>;
    fiscal_years: number[];
    score: number;
    evidence_summary: string;
  }>;
  total_clusters: number;
  explanation: string;
}

export const detectFundingClusters = async (
  data: FundingClusterDetectionRequest
): Promise<FundingClusterDetectionResponse> => {
  const response = await apiClient.post('/detection/funding-clusters', data);
  return response.data;
};

// Infrastructure Sharing Detection
export interface InfrastructureSharingRequest {
  entity_ids?: string[];
  domains?: string[];
  min_score?: number;
}

export interface InfrastructureSharingResponse {
  profiles: Array<{
    domain: string;
    dns?: { nameservers: string[]; a_records: string[] };
    whois?: { registrar: string | null; registrant_org: string | null };
    hosting?: Array<{ ip: string; provider: string | null; asn: string | null }>;
    analytics?: { google_analytics: string[]; google_tag_manager: string[] };
    ssl?: { issuer: string | null; san_count: number };
    error?: string;
  }>;
  matches: Array<{
    domain_a: string;
    domain_b: string;
    signals: Array<{
      signal_type: string;
      value: string;
      weight: number;
      description: string;
    }>;
    total_score: number;
    confidence: number;
  }>;
  total_matches: number;
  domains_scanned: number;
  errors: string[];
  explanation: string;
}

export const detectInfrastructureSharing = async (
  data: InfrastructureSharingRequest
): Promise<InfrastructureSharingResponse> => {
  const response = await apiClient.post('/detection/infrastructure-sharing', data);
  return response.data;
};

// Reports
export const getReportTemplates = async (): Promise<unknown[]> => {
  const response = await apiClient.get('/reports/templates');
  return response.data;
};

export const generateReport = async (data: {
  template_id: string;
  entity_ids?: string[];
  time_range?: { start: string; end: string };
  options?: Record<string, unknown>;
}): Promise<AsyncJobResponse> => {
  const response = await apiClient.post('/reports', data);
  return response.data;
};

export const getReport = async (
  id: string,
  format: 'json' | 'html' | 'pdf' | 'markdown' = 'json'
): Promise<unknown> => {
  const response = await apiClient.get(`/reports/${id}`, { params: { format } });
  return response.data;
};

// Ingestion
export const getIngestionStatus = async (): Promise<{ sources: IngestionStatus[] }> => {
  const response = await apiClient.get('/ingestion/status');
  return response.data;
};

export interface CompanySearchResult {
  source: string;
  identifier: string;
  identifier_type: string;
  name: string;
  details: Record<string, unknown>;
}

export interface CompanySearchResponse {
  query: string;
  results: CompanySearchResult[];
  sources_searched: string[];
  sources_failed: string[];
  total: number;
}

export const searchIngestionSources = async (params: {
  q: string;
  sources?: string;
  limit?: number;
}): Promise<CompanySearchResponse> => {
  const response = await apiClient.get('/ingestion/search', { params });
  return response.data;
};

export const triggerIngestion = async (
  source: string,
  data?: {
    incremental?: boolean;
    date_range?: { start: string; end: string };
    start_year?: number;
    end_year?: number;
    limit?: number;
    target_entities?: string[];
  }
): Promise<AsyncJobResponse> => {
  const response = await apiClient.post(`/ingestion/${source}/trigger`, data);
  return response.data;
};

// Jobs
export const getJobStatus = async (jobId: string): Promise<JobStatus> => {
  const response = await apiClient.get(`/jobs/${jobId}`);
  return response.data;
};

// Health
export const healthCheck = async (): Promise<{ status: string }> => {
  const response = await apiClient.get('/health');
  return response.data;
};

// =========================
// Resolution
// =========================

export interface ResolutionCandidate {
  id: string;
  status: string;
  priority: string;
  source_entity_id: string;
  source_entity_name: string;
  source_entity_type: string;
  candidate_entity_id: string;
  candidate_entity_name: string;
  candidate_entity_type: string;
  match_strategy: string;
  match_confidence: number;
  match_details: Record<string, unknown>;
  created_at: string;
  assigned_to: string | null;
}

export interface ResolutionStats {
  total_pending: number;
  total_in_progress: number;
  total_completed: number;
  total_approved: number;
  total_rejected: number;
  total_merged: number;
  avg_confidence: number;
  by_priority: Record<string, number>;
  by_strategy: Record<string, number>;
}

export const getResolutionCandidates = async (params?: {
  status?: string;
  priority?: string;
  strategy?: string;
  limit?: number;
  offset?: number;
}): Promise<PaginatedResponse<ResolutionCandidate>> => {
  const response = await apiClient.get('/resolution/candidates', { params });
  return response.data;
};

export const mergeCandidate = async (id: string): Promise<unknown> => {
  const response = await apiClient.post(`/resolution/candidates/${id}/merge`);
  return response.data;
};

export const rejectCandidate = async (id: string): Promise<unknown> => {
  const response = await apiClient.post(`/resolution/candidates/${id}/reject`);
  return response.data;
};

export const triggerResolution = async (params?: {
  entity_type?: string;
  dry_run?: boolean;
}): Promise<{ status: string; entity_types: string[]; candidates_found: number; dry_run: boolean }> => {
  const response = await apiClient.post('/resolution/trigger', null, { params });
  return response.data;
};

export const getResolutionStats = async (): Promise<ResolutionStats> => {
  const response = await apiClient.get('/resolution/stats');
  return response.data;
};

// =========================
// Jobs (expanded)
// =========================

export interface JobStatusFull {
  job_id: string;
  job_type: string;
  status: string;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  progress: number | null;
  metadata: Record<string, unknown>;
  error: string | null;
}

export interface JobResult {
  job_id: string;
  job_type: string;
  status: string;
  created_at: string;
  completed_at: string | null;
  result: Record<string, unknown> | null;
  error: string | null;
}

export const listJobs = async (params?: {
  job_type?: string;
  status?: string;
  limit?: number;
  offset?: number;
}): Promise<{ jobs: JobStatusFull[]; total: number; limit: number; offset: number }> => {
  const response = await apiClient.get('/jobs/', { params });
  return response.data;
};

export const getJobResult = async (jobId: string): Promise<JobResult> => {
  const response = await apiClient.get(`/jobs/${jobId}/result`);
  return response.data;
};

export const cancelJob = async (jobId: string): Promise<JobStatusFull> => {
  const response = await apiClient.post(`/jobs/${jobId}/cancel`);
  return response.data;
};

// =========================
// Ingestion (expanded)
// =========================

export interface IngestionRun {
  run_id: string;
  source: string;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  records_processed: number;
  records_created: number;
  records_updated: number;
  duplicates_found: number;
  errors: Array<Record<string, unknown>>;
}

export const getIngestionRuns = async (params?: {
  source?: string;
  status?: string;
  limit?: number;
}): Promise<{ runs: IngestionRun[]; total: number }> => {
  const response = await apiClient.get('/ingestion/runs', { params });
  return response.data;
};

export const getIngestionRun = async (id: string): Promise<IngestionRun> => {
  const response = await apiClient.get(`/ingestion/runs/${id}`);
  return response.data;
};

export interface IngestionRunLogs {
  lines: string[];
  total_lines: number;
  is_live: boolean;
}

export const getIngestionRunLogs = async (
  runId: string,
  offset: number = 0,
): Promise<IngestionRunLogs> => {
  const response = await apiClient.get(`/ingestion/runs/${runId}/logs`, {
    params: { offset },
  });
  return response.data;
};

// =========================
// Detection (expanded)
// =========================

export interface FindingExplanation {
  finding_id: string;
  finding_type: string;
  entity_ids: string[];
  score: number;
  confidence: number;
  why_flagged: string;
  evidence_summary: Array<Record<string, unknown>>;
  hard_negatives_checked: Array<Record<string, unknown>>;
  recommendations: string[];
}

export const explainFinding = async (findingId: string): Promise<FindingExplanation> => {
  const response = await apiClient.get(`/detection/explain/${findingId}`);
  return response.data;
};

// =========================
// Relationships (expanded)
// =========================

export const getConnectingEntities = async (params: {
  entity_ids: string;
  max_hops?: number;
  rel_types?: string;
}): Promise<{ connecting_entities: Array<{ entity: EntitySummary; connections: number }>; total: number }> => {
  const response = await apiClient.get('/relationships/connecting-entities', { params });
  return response.data;
};

export const getRelationshipTimeline = async (params: {
  source_id: string;
  target_id: string;
  rel_type?: string;
}): Promise<{ timeline: Array<{ rel_type: string; valid_from: string | null; valid_to: string | null; is_current: boolean; properties: Record<string, unknown> }>; source: EntitySummary; target: EntitySummary }> => {
  const response = await apiClient.get('/relationships/timeline', { params });
  return response.data;
};

export const getFundingPaths = async (
  funderId: string,
  params?: { max_hops?: number; min_amount?: number; fiscal_year?: number }
): Promise<{ paths: Array<Record<string, unknown>>; total: number }> => {
  const response = await apiClient.get(`/relationships/funding-paths/${funderId}`, { params });
  return response.data;
};

export const getFundingRecipients = async (
  funderId: string,
  params?: { fiscal_year?: number; min_amount?: number; limit?: number }
): Promise<{ recipients: Array<{ entity: EntitySummary; amount: number; fiscal_year: number }>; total: number }> => {
  const response = await apiClient.get(`/relationships/funding-recipients/${funderId}`, { params });
  return response.data;
};

export const getFundingSources = async (
  recipientId: string,
  params?: { fiscal_year?: number; min_amount?: number; limit?: number }
): Promise<{ funders: Array<{ entity: EntitySummary; amount: number; fiscal_year: number }>; total: number }> => {
  const response = await apiClient.get(`/relationships/funding-sources/${recipientId}`, { params });
  return response.data;
};

export const getSharedFunders = async (params: {
  entity_ids: string;
  min_recipients?: number;
  fiscal_year?: number;
  limit?: number;
}): Promise<{ shared_funders: Array<{ funder: EntitySummary; recipients: EntitySummary[]; shared_count: number; total_funding: number; funding_concentration: number }>; total: number }> => {
  const response = await apiClient.get('/relationships/shared-funders', { params });
  return response.data;
};

export const analyzeDomainInfrastructure = async (domain: string): Promise<Record<string, unknown>> => {
  const response = await apiClient.post('/relationships/shared-infrastructure/analyze', null, {
    params: { domain },
  });
  return response.data;
};

// =========================
// Reports (expanded)
// =========================

export const getReportStatus = async (id: string): Promise<{ report_id: string; status: string; created_at: string; completed_at: string | null; error: string | null }> => {
  const response = await apiClient.get(`/reports/${id}/status`);
  return response.data;
};

// =========================
// Validation
// =========================

export const getValidationDashboard = async (): Promise<Record<string, unknown>> => {
  const response = await apiClient.get('/validation/dashboard');
  return response.data;
};

export const getGoldenDatasets = async (): Promise<Array<Record<string, unknown>>> => {
  const response = await apiClient.get('/validation/datasets');
  return response.data;
};

export const runValidation = async (data: {
  dataset_id: string;
  threshold?: number;
  include_synthetic?: boolean;
}): Promise<AsyncJobResponse> => {
  const response = await apiClient.post('/validation/run', data);
  return response.data;
};

// =========================
// Settings
// =========================

export type ConnectionStatusType = 'healthy' | 'unhealthy' | 'unknown';

export interface ConnectionInfo {
  name: string;
  status: ConnectionStatusType;
  host: string;
  port: number | null;
  latency_ms: number | null;
  error: string | null;
}

export interface DataSourceInfo {
  id: string;
  name: string;
  description: string;
  enabled: boolean;
  requires_api_key: boolean;
  has_api_key: boolean;
  api_key_env_var: string | null;
  feature_flag: string | null;
  last_successful_run: string | null;
  records_total: number;
}

export interface APIConfigInfo {
  environment: string;
  api_host: string;
  api_port: number;
  debug_mode: boolean;
  cors_origins: string[];
  log_level: string;
}

export interface SettingsResponse {
  api: APIConfigInfo;
  connections: ConnectionInfo[];
  data_sources: DataSourceInfo[];
}

export interface ConnectionsResponse {
  connections: ConnectionInfo[];
  all_healthy: boolean;
}

export interface DataSourcesResponse {
  sources: DataSourceInfo[];
  total_enabled: number;
  total_disabled: number;
}

export const getSettings = async (): Promise<SettingsResponse> => {
  const response = await apiClient.get('/settings');
  return response.data;
};

export const getConnectionsStatus = async (): Promise<ConnectionsResponse> => {
  const response = await apiClient.get('/settings/connections');
  return response.data;
};

export const getDataSources = async (): Promise<DataSourcesResponse> => {
  const response = await apiClient.get('/settings/sources');
  return response.data;
};

// =========================
// Meta OAuth
// =========================

export interface MetaAuthUrlResponse {
  auth_url: string;
  state: string;
}

export interface MetaAuthStatusResponse {
  connected: boolean;
  fb_user_id: string | null;
  fb_user_name: string | null;
  expires_at: string | null;
  days_until_expiry: number | null;
  expires_soon: boolean;
  scopes: string[] | null;
}

export interface MetaDisconnectResponse {
  success: boolean;
  message: string;
}

export interface MetaRefreshResponse {
  success: boolean;
  needs_reauth: boolean;
  message: string;
  days_until_expiry?: number;
}

export const getMetaAuthUrl = async (): Promise<MetaAuthUrlResponse> => {
  const response = await apiClient.get('/meta/auth/login');
  return response.data;
};

export const getMetaAuthStatus = async (): Promise<MetaAuthStatusResponse> => {
  const response = await apiClient.get('/meta/auth/status');
  return response.data;
};

export const disconnectMeta = async (): Promise<MetaDisconnectResponse> => {
  const response = await apiClient.delete('/meta/auth/disconnect');
  return response.data;
};

export const refreshMetaToken = async (): Promise<MetaRefreshResponse> => {
  const response = await apiClient.post('/meta/auth/refresh');
  return response.data;
};

// =========================
// Cases (Autonomous Case Intake)
// =========================

export interface CaseConfig {
  max_depth: number;
  max_entities: number;
  max_relationships: number;
  jurisdictions: string[];
  min_confidence: number;
  auto_merge_threshold: number;
  review_threshold: number;
  enable_llm_extraction: boolean;
}

export interface CaseStats {
  entity_count: number;
  relationship_count: number;
  evidence_count: number;
  pending_matches: number;
  leads_processed: number;
  leads_pending: number;
}

export interface CaseSummary {
  id: string;
  name: string;
  status: 'initializing' | 'processing' | 'paused' | 'completed' | 'failed';
  entry_point_type: 'meta_ad' | 'corporation' | 'url' | 'text';
  entity_count: number;
  created_at: string;
}

export interface CaseResponse {
  id: string;
  name: string;
  description: string | null;
  entry_point_type: 'meta_ad' | 'corporation' | 'url' | 'text';
  entry_point_value: string;
  status: 'initializing' | 'processing' | 'paused' | 'completed' | 'failed';
  config: CaseConfig;
  stats: CaseStats;
  research_session_id: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
}

export interface CreateCaseRequest {
  name: string;
  description?: string;
  entry_point_type: 'meta_ad' | 'corporation' | 'url' | 'text';
  entry_point_value: string;
  config?: Partial<CaseConfig>;
}

export interface CaseListResponse {
  items: CaseSummary[];
  total: number;
}

export interface RankedEntity {
  entity_id: string;
  name: string;
  entity_type: string;
  relevance_score: number;
  depth: number;
  key_relationships: string[];
  jurisdiction: string | null;
}

export interface RankedRelationship {
  source_entity_id: string;
  source_name: string;
  target_entity_id: string;
  target_name: string;
  relationship_type: string;
  significance_score: number;
  amount: number | null;
  evidence_ids: string[];
}

export interface CrossBorderFlag {
  us_entity_id: string;
  us_entity_name: string;
  ca_entity_id: string;
  ca_entity_name: string;
  relationship_type: string;
  amount: number | null;
  evidence_ids: string[];
}

export interface CaseReportSummary {
  entry_point: string;
  processing_time_seconds: number;
  entity_count: number;
  relationship_count: number;
  cross_border_count: number;
  has_unresolved_matches: boolean;
}

export interface CaseReport {
  id: string;
  case_id: string;
  generated_at: string;
  report_version: number;
  summary: CaseReportSummary;
  top_entities: RankedEntity[];
  top_relationships: RankedRelationship[];
  cross_border_flags: CrossBorderFlag[];
  unknowns: Array<{ entity_name: string; reason: string; attempted_sources: string[] }>;
  evidence_index: Array<{ evidence_id: string; source_type: string; source_url: string | null; retrieved_at: string }>;
}

export interface MatchSignals {
  name_similarity: number | null;
  identifier_match: { type: string; matched: boolean } | null;
  jurisdiction_match: boolean;
  address_overlap: { city: boolean; postal_fsa: boolean } | null;
  shared_directors: string[] | null;
}

export interface CaseEntitySummary {
  id: string;
  name: string;
  entity_type: string;
  jurisdiction: string | null;
  identifiers: Record<string, string>;
}

export interface EntityMatchResponse {
  id: string;
  source_entity: CaseEntitySummary;
  target_entity: CaseEntitySummary;
  confidence: number;
  match_signals: MatchSignals;
  status: 'pending' | 'approved' | 'rejected' | 'deferred';
  reviewed_by: string | null;
  reviewed_at: string | null;
  review_notes: string | null;
}

export interface MatchListResponse {
  items: EntityMatchResponse[];
  pending_count: number;
}

// Cases API
export const listCases = async (params?: {
  status?: string;
  created_by?: string;
  limit?: number;
  offset?: number;
}): Promise<CaseListResponse> => {
  const response = await apiClient.get('/cases', { params });
  return response.data;
};

export const createCase = async (data: CreateCaseRequest): Promise<CaseResponse> => {
  const response = await apiClient.post('/cases', data);
  return response.data;
};

export const getCase = async (id: string): Promise<CaseResponse> => {
  const response = await apiClient.get(`/cases/${id}`);
  return response.data;
};

export const deleteCase = async (id: string): Promise<void> => {
  await apiClient.delete(`/cases/${id}`);
};

export const startCase = async (id: string): Promise<CaseResponse> => {
  const response = await apiClient.post(`/cases/${id}/start`);
  return response.data;
};

export const pauseCase = async (id: string): Promise<CaseResponse> => {
  const response = await apiClient.post(`/cases/${id}/pause`);
  return response.data;
};

export const resumeCase = async (id: string): Promise<CaseResponse> => {
  const response = await apiClient.post(`/cases/${id}/resume`);
  return response.data;
};

export const getCaseReport = async (id: string, format: 'json' | 'markdown' = 'json'): Promise<CaseReport | string> => {
  const response = await apiClient.get(`/cases/${id}/report`, { params: { format } });
  return response.data;
};

export const generateCaseReport = async (id: string): Promise<CaseReport> => {
  const response = await apiClient.post(`/cases/${id}/report`);
  return response.data;
};

export const listCaseMatches = async (
  caseId: string,
  params?: { status?: string; limit?: number; offset?: number }
): Promise<MatchListResponse> => {
  const response = await apiClient.get(`/cases/${caseId}/matches`, { params });
  return response.data;
};

export const approveMatch = async (matchId: string, notes?: string): Promise<EntityMatchResponse> => {
  const response = await apiClient.post(`/cases/matches/${matchId}/approve`, { notes });
  return response.data;
};

export const rejectMatch = async (matchId: string, notes?: string): Promise<EntityMatchResponse> => {
  const response = await apiClient.post(`/cases/matches/${matchId}/reject`, { notes });
  return response.data;
};

export const deferMatch = async (matchId: string, notes?: string): Promise<EntityMatchResponse> => {
  const response = await apiClient.post(`/cases/matches/${matchId}/defer`, { notes });
  return response.data;
};

// =========================
// Autocomplete API
// =========================

export interface AutocompleteSuggestion {
  name: string;
  entity_type: string;
  source: string;
  id: string | null;
  jurisdiction: string | null;
}

export interface AutocompleteResponse {
  suggestions: AutocompleteSuggestion[];
  query: string;
  total: number;
}

export const autocompleteEntities = async (
  query: string,
  options?: { limit?: number; types?: string }
): Promise<AutocompleteResponse> => {
  if (query.length < 2) {
    return { suggestions: [], query, total: 0 };
  }
  const response = await apiClient.get('/ingestion/autocomplete', {
    params: { q: query, ...options },
  });
  return response.data;
};

// =========================
// Quick Ingest API
// =========================

export interface QuickIngestRequest {
  name: string;
  jurisdiction?: string;
  identifiers?: Record<string, string>;
  discover_executives?: boolean;
}

export interface QuickIngestResponse {
  found: boolean;
  entity_id: string | null;
  entity_name: string | null;
  source: string | null;
  sources_searched: string[];
  external_matches: Array<{
    name: string;
    source: string;
    identifiers: Record<string, string>;
    jurisdiction: string | null;
    status: string | null;
    address: string | null;
  }>;
  message: string;
  // LinkedIn executive discovery
  linkedin_available: boolean;
  linkedin_company_url: string | null;
  executives_hint: string | null;
}

export const quickIngestCorporation = async (
  request: QuickIngestRequest
): Promise<QuickIngestResponse> => {
  const response = await apiClient.post('/ingestion/quick-ingest', request);
  return response.data;
};

// LinkedIn Status & Ingestion
export interface LinkedInStatus {
  configured: boolean;
  message: string;
  methods_available: string[];
}

export const getLinkedInStatus = async (): Promise<LinkedInStatus> => {
  const response = await apiClient.get('/ingestion/linkedin/status');
  return response.data;
};

export interface LinkedInIngestionRequest {
  company_name?: string;
  company_url?: string;
  company_entity_id?: string;
  csv_data?: string;  // Base64-encoded CSV
  scrape?: boolean;
  session_cookie?: string;
  titles_filter?: string[];
  limit?: number;
}

export interface LinkedInIngestionResponse {
  run_id: string;
  status: string;
  message: string;
  profiles_found?: number;
  executives_found?: number;
}

export const ingestLinkedIn = async (
  request: LinkedInIngestionRequest
): Promise<LinkedInIngestionResponse> => {
  const response = await apiClient.post('/ingestion/linkedin', request);
  return response.data;
};

export default apiClient;
