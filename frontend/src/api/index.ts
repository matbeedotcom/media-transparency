/**
 * API exports barrel file
 *
 * Re-exports generated hooks and types from Orval, plus the axios instance.
 */

// Axios instance for custom calls
export { axiosInstance, customInstance } from './axios-instance';

// =============================================================================
// Re-export all generated hooks and types for direct access
// =============================================================================

export * from './generated/cases/cases';
export * from './generated/detection/detection';
export * from './generated/entities/entities';
export * from './generated/health/health';
export * from './generated/ingestion/ingestion';
export * from './generated/jobs/jobs';
export * from './generated/meta-oauth/meta-oauth';
export * from './generated/relationships/relationships';
export * from './generated/reports/reports';
export * from './generated/research/research';
export * from './generated/resolution/resolution';
export * from './generated/settings/settings';
export * from './generated/validation/validation';

// Generated models/types
export * from './generated/models';

// =============================================================================
// Function Aliases - Map generated names to cleaner names used by components
// =============================================================================

// Resolution aliases (using clean names from generated code)
export {
  listCandidates as getResolutionCandidates,
  resolutionStats as getResolutionStats,
} from './generated/resolution/resolution';

// Reports aliases  
export {
  listReportTemplatesReportsTemplatesGet as getReportTemplates,
  generateReportReportsPost as generateReport,
  getReportReportsReportIdGet as getReport,
  getReportStatusReportsReportIdStatusGet as getReportStatus,
} from './generated/reports/reports';

// Validation aliases
export {
  getValidationDashboardValidationDashboardGet as getValidationDashboard,
  listGoldenDatasetsValidationDatasetsGet as getGoldenDatasets,
  runValidationValidationRunPost as runValidation,
} from './generated/validation/validation';

// Relationships aliases
export { findShortestPath as findPaths } from './generated/relationships/relationships';

// =============================================================================
// Type Aliases for Backward Compatibility
// =============================================================================

export type { EntityResponse as Entity } from './generated/models';
export type { RelationshipResponse as Relationship } from './generated/models';
export type { EvidenceResponse as Evidence } from './generated/models';
export type { BoardInterlockItem as BoardInterlock } from './generated/models';
export type { LinkedInStatusResponse as LinkedInStatus } from './generated/models';
export type { IngestionRunResponse as IngestionRun } from './generated/models';
export type { IngestionStatusResponseSourcesItem as IngestionStatus } from './generated/models';
export type { CaseReportResponse as CaseReport } from './generated/models';
export type { FundingClustersResponseClustersItem as FundingCluster } from './generated/models';
export type { AllPathsResponsePathsItem as PathResult } from './generated/models';
