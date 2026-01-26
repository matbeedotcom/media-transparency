/**
 * Report Generator page for MITDS
 *
 * Create, track, and export structural risk reports with API integration.
 */

import { useState, useEffect, useCallback } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';

// =========================
// Types
// =========================

interface ReportTemplate {
  id: string;
  name: string;
  description: string;
  sections: string[];
  required_data: string[];
  output_formats: string[];
}

interface EntitySearchResult {
  id: string;
  name: string;
  entity_type: string;
}

interface ReportRecord {
  id: string;
  report_type: string;
  status: 'pending' | 'generating' | 'completed' | 'failed';
  created_at: string;
  completed_at?: string;
  error?: string;
  format?: string;
  report?: ReportData;
  content?: string;
}

interface ReportData {
  metadata: {
    title: string;
    subtitle?: string;
    generated_by: string;
    generated_at: string;
    report_type: string;
  };
  executive_summary?: string;
  methodology?: string;
  sections: ReportSection[];
  limitations?: string[];
}

interface ReportSection {
  id: string;
  title: string;
  content: string;
  findings: ReportFinding[];
}

interface ReportFinding {
  id: string;
  title: string;
  description: string;
  severity: 'high' | 'medium' | 'low';
  confidence: number;
  why_flagged?: string;
  evidence_citations?: string[];
}

interface ReportRequest {
  report_type: string;
  entity_ids: string[];
  title?: string;
  date_range_start?: string;
  date_range_end?: string;
  options?: {
    include_confidence_bands?: boolean;
    include_methodology?: boolean;
    include_limitations?: boolean;
  };
}

// =========================
// API Functions
// =========================

async function fetchTemplates(): Promise<ReportTemplate[]> {
  const response = await fetch('/api/reports/templates');
  if (!response.ok) {
    throw new Error('Failed to fetch templates');
  }
  return response.json();
}

async function searchEntities(query: string): Promise<EntitySearchResult[]> {
  if (!query || query.length < 2) return [];
  const response = await fetch(`/api/entities?query=${encodeURIComponent(query)}&limit=10`);
  if (!response.ok) {
    throw new Error('Failed to search entities');
  }
  const data = await response.json();
  return data.entities || [];
}

async function generateReport(request: ReportRequest): Promise<{ report_id: string; status: string; status_url: string }> {
  const response = await fetch('/api/reports', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || 'Failed to generate report');
  }
  return response.json();
}

async function fetchReport(reportId: string, format: string = 'json'): Promise<ReportRecord> {
  const response = await fetch(`/api/reports/${reportId}?format=${format}`);
  if (!response.ok) {
    throw new Error('Failed to fetch report');
  }
  return response.json();
}

async function fetchReportStatus(reportId: string): Promise<{ id: string; status: string; error?: string }> {
  const response = await fetch(`/api/reports/${reportId}/status`);
  if (!response.ok) {
    throw new Error('Failed to fetch report status');
  }
  return response.json();
}

// =========================
// Component
// =========================

export default function ReportGenerator() {
  const queryClient = useQueryClient();

  // Template selection
  const [selectedTemplateId, setSelectedTemplateId] = useState<string>('');

  // Entity selection
  const [entitySearch, setEntitySearch] = useState('');
  const [selectedEntities, setSelectedEntities] = useState<EntitySearchResult[]>([]);
  const [searchResults, setSearchResults] = useState<EntitySearchResult[]>([]);

  // Form options
  const [reportTitle, setReportTitle] = useState('');
  const [dateRangeStart, setDateRangeStart] = useState('');
  const [dateRangeEnd, setDateRangeEnd] = useState('');
  const [includeConfidenceBands, setIncludeConfidenceBands] = useState(true);
  const [includeMethodology, setIncludeMethodology] = useState(true);
  const [includeLimitations, setIncludeLimitations] = useState(true);

  // Generated reports tracking
  const [generatedReports, setGeneratedReports] = useState<ReportRecord[]>([]);
  const [pollingReportIds, setPollingReportIds] = useState<Set<string>>(new Set());

  // View state
  const [viewingReport, setViewingReport] = useState<ReportRecord | null>(null);
  const [exportFormat, setExportFormat] = useState<'json' | 'markdown' | 'html' | 'pdf'>('json');

  // Fetch templates
  const { data: templates = [], isLoading: templatesLoading } = useQuery({
    queryKey: ['reportTemplates'],
    queryFn: fetchTemplates,
  });

  const selectedTemplate = templates.find((t) => t.id === selectedTemplateId);

  // Entity search with debounce
  useEffect(() => {
    if (entitySearch.length < 2) {
      setSearchResults([]);
      return;
    }

    const timer = setTimeout(async () => {
      try {
        const results = await searchEntities(entitySearch);
        // Filter out already selected entities
        const filtered = results.filter(
          (r) => !selectedEntities.some((e) => e.id === r.id)
        );
        setSearchResults(filtered);
      } catch (err) {
        console.error('Entity search failed:', err);
      }
    }, 300);

    return () => clearTimeout(timer);
  }, [entitySearch, selectedEntities]);

  // Poll for report completion
  useEffect(() => {
    if (pollingReportIds.size === 0) return;

    const interval = setInterval(async () => {
      for (const reportId of pollingReportIds) {
        try {
          const status = await fetchReportStatus(reportId);
          if (status.status === 'completed' || status.status === 'failed') {
            // Fetch full report
            const fullReport = await fetchReport(reportId);
            setGeneratedReports((prev) =>
              prev.map((r) => (r.id === reportId ? fullReport : r))
            );
            // Remove from polling
            setPollingReportIds((prev) => {
              const next = new Set(prev);
              next.delete(reportId);
              return next;
            });
          }
        } catch (err) {
          console.error(`Failed to poll report ${reportId}:`, err);
        }
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [pollingReportIds]);

  // Generate report mutation
  const generateMutation = useMutation({
    mutationFn: generateReport,
    onSuccess: (data) => {
      // Add to generated reports
      setGeneratedReports((prev) => [
        {
          id: data.report_id,
          report_type: selectedTemplateId,
          status: 'pending',
          created_at: new Date().toISOString(),
        },
        ...prev,
      ]);
      // Start polling
      setPollingReportIds((prev) => new Set(prev).add(data.report_id));
      // Reset form
      setSelectedEntities([]);
      setReportTitle('');
      setDateRangeStart('');
      setDateRangeEnd('');
    },
  });

  const handleAddEntity = useCallback((entity: EntitySearchResult) => {
    setSelectedEntities((prev) => [...prev, entity]);
    setSearchResults([]);
    setEntitySearch('');
  }, []);

  const handleRemoveEntity = useCallback((entityId: string) => {
    setSelectedEntities((prev) => prev.filter((e) => e.id !== entityId));
  }, []);

  const handleSubmit = useCallback(
    (e: React.FormEvent) => {
      e.preventDefault();
      if (!selectedTemplateId || selectedEntities.length === 0) return;

      const request: ReportRequest = {
        report_type: selectedTemplateId,
        entity_ids: selectedEntities.map((e) => e.id),
        title: reportTitle || undefined,
        date_range_start: dateRangeStart || undefined,
        date_range_end: dateRangeEnd || undefined,
        options: {
          include_confidence_bands: includeConfidenceBands,
          include_methodology: includeMethodology,
          include_limitations: includeLimitations,
        },
      };

      generateMutation.mutate(request);
    },
    [
      selectedTemplateId,
      selectedEntities,
      reportTitle,
      dateRangeStart,
      dateRangeEnd,
      includeConfidenceBands,
      includeMethodology,
      includeLimitations,
      generateMutation,
    ]
  );

  const handleViewReport = useCallback(async (reportId: string) => {
    try {
      const report = await fetchReport(reportId, 'json');
      setViewingReport(report);
    } catch (err) {
      console.error('Failed to load report:', err);
    }
  }, []);

  const handleExportReport = useCallback(async (reportId: string, format: 'json' | 'markdown' | 'html' | 'pdf') => {
    try {
      // PDF export uses browser print
      if (format === 'pdf') {
        const report = await fetchReport(reportId, 'html');
        const htmlContent = report.content || '';

        // Open print window with HTML content
        const printWindow = window.open('', '_blank');
        if (printWindow) {
          printWindow.document.write(htmlContent);
          printWindow.document.close();
          printWindow.focus();
          // Give time for content to render
          setTimeout(() => {
            printWindow.print();
          }, 500);
        }
        return;
      }

      const report = await fetchReport(reportId, format);

      let content: string;
      let mimeType: string;
      let extension: string;

      if (format === 'json') {
        content = JSON.stringify(report.report, null, 2);
        mimeType = 'application/json';
        extension = 'json';
      } else if (format === 'markdown') {
        content = report.content || '';
        mimeType = 'text/markdown';
        extension = 'md';
      } else {
        content = report.content || '';
        mimeType = 'text/html';
        extension = 'html';
      }

      // Trigger download
      const blob = new Blob([content], { type: mimeType });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `report-${reportId.slice(0, 8)}.${extension}`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error('Failed to export report:', err);
    }
  }, []);

  const getStatusColor = (status: string): string => {
    switch (status) {
      case 'completed':
        return 'var(--color-success)';
      case 'failed':
        return 'var(--color-danger)';
      case 'generating':
        return 'var(--color-warning)';
      default:
        return 'var(--text-muted)';
    }
  };

  const formatDate = (dateStr: string): string => {
    return new Date(dateStr).toLocaleString();
  };

  return (
    <div className="report-generator">
      <header className="page-header">
        <h1>Report Generator</h1>
        <p>Create comprehensive, evidence-linked reports with non-accusatory language</p>
      </header>

      {viewingReport ? (
        /* Report Viewer */
        <div className="report-viewer">
          <div className="viewer-header">
            <button type="button" className="btn btn-secondary" onClick={() => setViewingReport(null)}>
              &larr; Back to Generator
            </button>
            <div className="export-controls">
              <select
                value={exportFormat}
                onChange={(e) => setExportFormat(e.target.value as 'json' | 'markdown' | 'html' | 'pdf')}
                aria-label="Export format"
              >
                <option value="json">JSON</option>
                <option value="markdown">Markdown</option>
                <option value="html">HTML</option>
                <option value="pdf">PDF (Print)</option>
              </select>
              <button
                type="button"
                className="btn btn-primary"
                onClick={() => handleExportReport(viewingReport.id, exportFormat)}
              >
                Export
              </button>
            </div>
          </div>

          {viewingReport.report ? (
            <div className="report-content">
              <ReportView report={viewingReport.report} />
            </div>
          ) : (
            <div className="empty-state">
              <p>Report data not available.</p>
            </div>
          )}
        </div>
      ) : (
        /* Generator Form */
        <div className="generator-layout">
          {/* Template Selection */}
          <div className="templates-section">
            <h2>Select Template</h2>
            {templatesLoading ? (
              <div className="loading">Loading templates...</div>
            ) : (
              <div className="templates-grid">
                {templates.map((template) => (
                  <div
                    key={template.id}
                    className={`card template-card ${selectedTemplateId === template.id ? 'selected' : ''}`}
                    onClick={() => setSelectedTemplateId(template.id)}
                    onKeyDown={(e) => e.key === 'Enter' && setSelectedTemplateId(template.id)}
                    role="button"
                    tabIndex={0}
                  >
                    <h3>{template.name}</h3>
                    <p>{template.description}</p>
                    <div className="template-meta">
                      <span>{template.sections.length} sections</span>
                      <span>{template.output_formats.join(', ')}</span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Report Configuration */}
          <div className="config-section">
            <h2>Configure Report</h2>
            <div className="card">
              {selectedTemplate ? (
                <form className="config-form" onSubmit={handleSubmit}>
                  {/* Entity Selection */}
                  <div className="form-group">
                    <label htmlFor="entity-search">Entities to Include</label>
                    <input
                      id="entity-search"
                      type="text"
                      value={entitySearch}
                      onChange={(e) => setEntitySearch(e.target.value)}
                      placeholder="Search for entities..."
                      autoComplete="off"
                    />
                    {searchResults.length > 0 && (
                      <div className="search-results">
                        {searchResults.map((result) => (
                          <div
                            key={result.id}
                            className="search-result-item"
                            onClick={() => handleAddEntity(result)}
                            onKeyDown={(e) => e.key === 'Enter' && handleAddEntity(result)}
                            role="button"
                            tabIndex={0}
                          >
                            <span className="entity-name">{result.name}</span>
                            <span className="entity-type">{result.entity_type}</span>
                          </div>
                        ))}
                      </div>
                    )}
                    {selectedEntities.length > 0 && (
                      <div className="selected-entities">
                        {selectedEntities.map((entity) => (
                          <span key={entity.id} className="entity-tag">
                            {entity.name}
                            <button
                              type="button"
                              className="tag-remove"
                              onClick={() => handleRemoveEntity(entity.id)}
                              aria-label={`Remove ${entity.name}`}
                            >
                              &times;
                            </button>
                          </span>
                        ))}
                      </div>
                    )}
                    <small className="text-muted">
                      Select the entities to analyze in this report (min: 1)
                    </small>
                  </div>

                  {/* Date Range (for timeline reports) */}
                  {selectedTemplate.id === 'timeline_narrative' && (
                    <div className="form-row">
                      <div className="form-group">
                        <label htmlFor="date-start">Start Date</label>
                        <input
                          id="date-start"
                          type="date"
                          value={dateRangeStart}
                          onChange={(e) => setDateRangeStart(e.target.value)}
                        />
                      </div>
                      <div className="form-group">
                        <label htmlFor="date-end">End Date</label>
                        <input
                          id="date-end"
                          type="date"
                          value={dateRangeEnd}
                          onChange={(e) => setDateRangeEnd(e.target.value)}
                        />
                      </div>
                    </div>
                  )}

                  {/* Report Title */}
                  <div className="form-group">
                    <label htmlFor="report-title">Report Title (Optional)</label>
                    <input
                      id="report-title"
                      type="text"
                      value={reportTitle}
                      onChange={(e) => setReportTitle(e.target.value)}
                      placeholder="Auto-generated if empty"
                    />
                  </div>

                  {/* Options */}
                  <div className="form-group">
                    <label>
                      <input
                        type="checkbox"
                        checked={includeConfidenceBands}
                        onChange={(e) => setIncludeConfidenceBands(e.target.checked)}
                      />
                      Include confidence bands
                    </label>
                  </div>

                  <div className="form-group">
                    <label>
                      <input
                        type="checkbox"
                        checked={includeMethodology}
                        onChange={(e) => setIncludeMethodology(e.target.checked)}
                      />
                      Include methodology section
                    </label>
                  </div>

                  <div className="form-group">
                    <label>
                      <input
                        type="checkbox"
                        checked={includeLimitations}
                        onChange={(e) => setIncludeLimitations(e.target.checked)}
                      />
                      Include limitations
                    </label>
                  </div>

                  <div className="form-actions">
                    <button
                      type="submit"
                      className="btn btn-primary"
                      disabled={selectedEntities.length === 0 || generateMutation.isPending}
                    >
                      {generateMutation.isPending ? 'Generating...' : 'Generate Report'}
                    </button>
                    {generateMutation.isError && (
                      <span className="error-message">
                        {(generateMutation.error as Error).message}
                      </span>
                    )}
                  </div>
                </form>
              ) : (
                <div className="empty-state">
                  <p>Select a template to configure your report.</p>
                </div>
              )}
            </div>
          </div>

          {/* Generated Reports */}
          <div className="history-section">
            <h2>Generated Reports</h2>
            <div className="card">
              {generatedReports.length > 0 ? (
                <div className="reports-list">
                  {generatedReports.map((report) => (
                    <div key={report.id} className="report-item">
                      <div className="report-info">
                        <span className="report-type">{report.report_type}</span>
                        <span
                          className="report-status"
                          style={{ color: getStatusColor(report.status) }}
                        >
                          {report.status}
                        </span>
                      </div>
                      <div className="report-meta">
                        <span className="report-date">{formatDate(report.created_at)}</span>
                        {report.error && (
                          <span className="report-error">{report.error}</span>
                        )}
                      </div>
                      {report.status === 'completed' && (
                        <div className="report-actions">
                          <button
                            type="button"
                            className="btn btn-sm btn-secondary"
                            onClick={() => handleViewReport(report.id)}
                          >
                            View
                          </button>
                          <button
                            type="button"
                            className="btn btn-sm btn-secondary"
                            onClick={() => handleExportReport(report.id, 'markdown')}
                          >
                            Export MD
                          </button>
                          <button
                            type="button"
                            className="btn btn-sm btn-secondary"
                            onClick={() => handleExportReport(report.id, 'html')}
                          >
                            Export HTML
                          </button>
                          <button
                            type="button"
                            className="btn btn-sm btn-secondary"
                            onClick={() => handleExportReport(report.id, 'pdf')}
                          >
                            Print PDF
                          </button>
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="empty-state">
                  <p>No reports generated yet.</p>
                  <p className="text-muted">
                    Generated reports will appear here for download.
                  </p>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      <style>{`
        .report-generator {
          max-width: 1200px;
          margin: 0 auto;
        }

        .generator-layout {
          display: grid;
          gap: var(--spacing-lg);
        }

        .templates-section h2,
        .config-section h2,
        .history-section h2 {
          margin-bottom: var(--spacing-md);
        }

        .templates-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
          gap: var(--spacing-md);
        }

        .template-card {
          cursor: pointer;
          transition: all 0.2s ease;
        }

        .template-card:hover {
          border-color: var(--color-primary);
        }

        .template-card.selected {
          border-color: var(--color-primary);
          background-color: rgba(37, 99, 235, 0.05);
        }

        .template-card h3 {
          margin-bottom: var(--spacing-sm);
        }

        .template-meta {
          display: flex;
          gap: var(--spacing-md);
          margin-top: var(--spacing-sm);
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .config-form {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
        }

        .form-group {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-xs);
          position: relative;
        }

        .form-group label {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
        }

        .form-group input[type="checkbox"] {
          width: auto;
        }

        .form-row {
          display: grid;
          grid-template-columns: repeat(2, 1fr);
          gap: var(--spacing-md);
        }

        .search-results {
          position: absolute;
          top: 100%;
          left: 0;
          right: 0;
          background: var(--bg-primary);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
          z-index: 10;
          max-height: 200px;
          overflow-y: auto;
        }

        .search-result-item {
          padding: var(--spacing-sm);
          cursor: pointer;
          display: flex;
          justify-content: space-between;
          align-items: center;
        }

        .search-result-item:hover {
          background: var(--bg-secondary);
        }

        .entity-type {
          font-size: 0.75rem;
          color: var(--text-muted);
          text-transform: uppercase;
        }

        .selected-entities {
          display: flex;
          flex-wrap: wrap;
          gap: var(--spacing-xs);
          margin-top: var(--spacing-sm);
        }

        .entity-tag {
          display: inline-flex;
          align-items: center;
          gap: var(--spacing-xs);
          padding: 4px 8px;
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
          font-size: 0.875rem;
        }

        .tag-remove {
          background: none;
          border: none;
          color: var(--text-muted);
          cursor: pointer;
          padding: 0;
          font-size: 1rem;
          line-height: 1;
        }

        .tag-remove:hover {
          color: var(--color-danger);
        }

        .form-actions {
          display: flex;
          align-items: center;
          gap: var(--spacing-md);
          margin-top: var(--spacing-md);
        }

        .error-message {
          color: var(--color-danger);
          font-size: 0.875rem;
        }

        .reports-list {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-sm);
        }

        .report-item {
          padding: var(--spacing-sm);
          background: var(--bg-secondary);
          border-radius: var(--border-radius);
        }

        .report-info {
          display: flex;
          justify-content: space-between;
          align-items: center;
          margin-bottom: var(--spacing-xs);
        }

        .report-type {
          font-weight: 500;
          text-transform: capitalize;
        }

        .report-status {
          font-size: 0.875rem;
          font-weight: 500;
          text-transform: capitalize;
        }

        .report-meta {
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .report-error {
          color: var(--color-danger);
          display: block;
          margin-top: var(--spacing-xs);
        }

        .report-actions {
          display: flex;
          gap: var(--spacing-xs);
          margin-top: var(--spacing-sm);
        }

        .btn-sm {
          padding: 4px 8px;
          font-size: 0.75rem;
        }

        .empty-state {
          text-align: center;
          padding: var(--spacing-xl);
        }

        .loading {
          text-align: center;
          padding: var(--spacing-lg);
          color: var(--text-muted);
        }

        /* Report Viewer */
        .report-viewer {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-lg);
        }

        .viewer-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
        }

        .export-controls {
          display: flex;
          gap: var(--spacing-sm);
        }

        .export-controls select {
          padding: 8px 12px;
        }

        .report-content {
          background: var(--bg-secondary);
          border-radius: var(--border-radius);
          padding: var(--spacing-lg);
        }
      `}</style>
    </div>
  );
}

// =========================
// Report View Component
// =========================

interface ReportViewProps {
  report: ReportData;
}

function ReportView({ report }: ReportViewProps) {
  const { metadata, executive_summary, methodology, sections, limitations } = report;

  const getSeverityColor = (severity: string): string => {
    switch (severity) {
      case 'high':
        return 'var(--color-danger)';
      case 'medium':
        return 'var(--color-warning)';
      case 'low':
        return 'var(--color-success)';
      default:
        return 'var(--text-muted)';
    }
  };

  return (
    <div className="report-view">
      {/* Header */}
      <header className="report-header">
        <h1>{metadata.title}</h1>
        {metadata.subtitle && <p className="subtitle">{metadata.subtitle}</p>}
        <div className="report-metadata">
          <span>Generated by {metadata.generated_by}</span>
          <span>on {new Date(metadata.generated_at).toLocaleString()}</span>
        </div>
      </header>

      {/* Executive Summary */}
      {executive_summary && (
        <section className="report-section">
          <h2>Executive Summary</h2>
          <p>{executive_summary}</p>
        </section>
      )}

      {/* Methodology */}
      {methodology && (
        <section className="report-section">
          <h2>Methodology</h2>
          <p>{methodology}</p>
        </section>
      )}

      {/* Sections */}
      {sections.map((section) => (
        <section key={section.id} className="report-section">
          <h2>{section.title}</h2>
          <p>{section.content}</p>

          {section.findings.length > 0 && (
            <div className="findings-list">
              {section.findings.map((finding) => (
                <div
                  key={finding.id}
                  className="finding-card"
                  style={{ borderLeftColor: getSeverityColor(finding.severity) }}
                >
                  <div className="finding-header">
                    <h3>{finding.title}</h3>
                    <div className="finding-badges">
                      <span
                        className="severity-badge"
                        style={{ backgroundColor: getSeverityColor(finding.severity) }}
                      >
                        {finding.severity}
                      </span>
                      <span className="confidence-badge">
                        {(finding.confidence * 100).toFixed(0)}% confidence
                      </span>
                    </div>
                  </div>
                  <p className="finding-description">{finding.description}</p>
                  {finding.why_flagged && (
                    <div className="why-flagged">
                      <strong>Why flagged:</strong> {finding.why_flagged}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </section>
      ))}

      {/* Limitations */}
      {limitations && limitations.length > 0 && (
        <section className="report-section limitations">
          <h2>Limitations</h2>
          <ul>
            {limitations.map((limitation, idx) => (
              <li key={idx}>{limitation}</li>
            ))}
          </ul>
        </section>
      )}

      <style>{`
        .report-view {
          max-width: 800px;
          margin: 0 auto;
        }

        .report-header {
          margin-bottom: var(--spacing-xl);
          padding-bottom: var(--spacing-lg);
          border-bottom: 1px solid var(--border-color);
        }

        .report-header h1 {
          margin-bottom: var(--spacing-xs);
        }

        .report-header .subtitle {
          font-size: 1.125rem;
          color: var(--text-secondary);
          margin-bottom: var(--spacing-sm);
        }

        .report-metadata {
          font-size: 0.875rem;
          color: var(--text-muted);
          display: flex;
          gap: var(--spacing-sm);
        }

        .report-section {
          margin-bottom: var(--spacing-xl);
        }

        .report-section h2 {
          margin-bottom: var(--spacing-md);
          padding-bottom: var(--spacing-xs);
          border-bottom: 1px solid var(--border-color);
        }

        .findings-list {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-md);
          margin-top: var(--spacing-md);
        }

        .finding-card {
          padding: var(--spacing-md);
          background: var(--bg-primary);
          border-radius: var(--border-radius);
          border-left: 4px solid;
        }

        .finding-header {
          display: flex;
          justify-content: space-between;
          align-items: flex-start;
          margin-bottom: var(--spacing-sm);
        }

        .finding-header h3 {
          margin: 0;
          font-size: 1rem;
        }

        .finding-badges {
          display: flex;
          gap: var(--spacing-xs);
        }

        .severity-badge {
          padding: 2px 8px;
          border-radius: 4px;
          color: white;
          font-size: 0.75rem;
          font-weight: 500;
          text-transform: uppercase;
        }

        .confidence-badge {
          padding: 2px 8px;
          background: var(--bg-tertiary);
          border-radius: 4px;
          font-size: 0.75rem;
        }

        .finding-description {
          margin-bottom: var(--spacing-sm);
        }

        .why-flagged {
          padding: var(--spacing-sm);
          background: var(--bg-tertiary);
          border-radius: var(--border-radius);
          font-size: 0.875rem;
        }

        .limitations ul {
          margin: 0;
          padding-left: var(--spacing-lg);
        }

        .limitations li {
          margin-bottom: var(--spacing-xs);
          color: var(--text-secondary);
        }
      `}</style>
    </div>
  );
}
