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
      // Handle unauthorized - redirect to login or clear token
      localStorage.removeItem('auth_token');
      window.location.href = '/login';
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
  outlet_ids: string[];
  start_date: string;
  end_date: string;
  topic_filter?: string;
}): Promise<unknown> => {
  const response = await apiClient.post('/detection/temporal-coordination', data);
  return response.data;
};

export const calculateCompositeScore = async (data: {
  outlet_ids: string[];
  weights?: Record<string, number>;
}): Promise<unknown> => {
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

export const triggerIngestion = async (
  source: string,
  data?: { incremental?: boolean; date_range?: { start: string; end: string } }
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

export default apiClient;
