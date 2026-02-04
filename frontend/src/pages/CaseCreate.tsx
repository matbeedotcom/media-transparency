/**
 * Case creation page for MITDS
 *
 * Allows researchers to create new cases from various entry points.
 */

import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMutation } from '@tanstack/react-query';
import { createCase, startCase, type CreateCaseRequest, type CreateCaseRequestEntryPointType } from '@/api';
import { EntryPointForm } from '../components/cases/EntryPointForm';

type UIEntryPointType = 'meta_ad' | 'corporation' | 'url' | 'text';

// Map UI types to API types
const entryPointTypeMap: Record<UIEntryPointType, CreateCaseRequestEntryPointType> = {
  meta_ad: 'META_AD',
  corporation: 'CORPORATION',
  url: 'URL',
  text: 'TEXT',
};

const entryPointLabels: Record<UIEntryPointType, { label: string; description: string; placeholder: string }> = {
  meta_ad: {
    label: 'Meta Ad Sponsor',
    description: 'Enter a Facebook/Instagram ad sponsor name or page ID',
    placeholder: 'e.g., Americans for Prosperity',
  },
  corporation: {
    label: 'Corporation Name',
    description: 'Search US and Canadian corporate registries',
    placeholder: 'e.g., Postmedia Network Canada Corp',
  },
  url: {
    label: 'URL / Webpage',
    description: 'Fetch and extract entities from a webpage',
    placeholder: 'https://example.org/about',
  },
  text: {
    label: 'Pasted Text',
    description: 'Extract entities from text content',
    placeholder: 'Paste text content here...',
  },
};

export default function CaseCreate() {
  const navigate = useNavigate();
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [entryPointType, setEntryPointType] = useState<UIEntryPointType>('meta_ad');
  const [entryPointValue, setEntryPointValue] = useState('');
  const [maxDepth, setMaxDepth] = useState(2);
  const [maxEntities, setMaxEntities] = useState(100);
  const [autoStart, setAutoStart] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const createMutation = useMutation({
    mutationFn: async (data: CreateCaseRequest) => {
      const caseData = await createCase(data);
      if (autoStart) {
        return startCase(caseData.id);
      }
      return caseData;
    },
    onSuccess: (data) => {
      navigate(`/cases/${data.id}`);
    },
    onError: (err: Error) => {
      setError(err.message || 'Failed to create case');
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!name.trim()) {
      setError('Case name is required');
      return;
    }
    if (!entryPointValue.trim()) {
      setError('Entry point value is required');
      return;
    }

    createMutation.mutate({
      name: name.trim(),
      description: description.trim() || undefined,
      entry_point_type: entryPointTypeMap[entryPointType],
      entry_point_value: entryPointValue.trim(),
      config: {
        max_depth: maxDepth,
        max_entities: maxEntities,
      },
    });
  };

  const typeInfo = entryPointLabels[entryPointType];

  return (
    <div className="case-create">
      <header className="page-header">
        <h1>Create New Case</h1>
        <p>Start an autonomous research investigation</p>
      </header>

      <form onSubmit={handleSubmit}>
        {error && (
          <div className="alert alert-danger">{error}</div>
        )}

        <div className="card">
          <h2>Case Details</h2>
          <div className="form-group">
            <label htmlFor="name">Case Name *</label>
            <input
              id="name"
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g., PAC Investigation"
              required
            />
          </div>
          <div className="form-group">
            <label htmlFor="description">Description</label>
            <textarea
              id="description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="Optional notes about this investigation"
              rows={3}
            />
          </div>
        </div>

        <div className="card">
          <h2>Entry Point</h2>
          <p className="text-muted">Choose how to start this investigation</p>

          <div className="entry-type-selector">
            {(Object.keys(entryPointLabels) as UIEntryPointType[]).map((type) => (
              <button
                key={type}
                type="button"
                className={`entry-type-btn ${entryPointType === type ? 'active' : ''}`}
                onClick={() => {
                  setEntryPointType(type);
                  setEntryPointValue('');
                }}
              >
                {entryPointLabels[type].label}
              </button>
            ))}
          </div>

          <EntryPointForm
            type={entryPointType}
            value={entryPointValue}
            onChange={setEntryPointValue}
            description={typeInfo.description}
            placeholder={typeInfo.placeholder}
          />
        </div>

        <div className="card">
          <h2>Configuration</h2>
          <div className="config-grid">
            <div className="form-group">
              <label htmlFor="maxDepth">Max Depth</label>
              <input
                id="maxDepth"
                type="number"
                min={1}
                max={5}
                value={maxDepth}
                onChange={(e) => setMaxDepth(parseInt(e.target.value, 10))}
              />
              <small>Maximum hops from entry point (1-5)</small>
            </div>
            <div className="form-group">
              <label htmlFor="maxEntities">Max Entities</label>
              <input
                id="maxEntities"
                type="number"
                min={10}
                max={1000}
                value={maxEntities}
                onChange={(e) => setMaxEntities(parseInt(e.target.value, 10))}
              />
              <small>Maximum entities to discover</small>
            </div>
          </div>
          <div className="form-group checkbox-group">
            <label>
              <input
                type="checkbox"
                checked={autoStart}
                onChange={(e) => setAutoStart(e.target.checked)}
              />
              Start processing immediately
            </label>
          </div>
        </div>

        <div className="form-actions">
          <button
            type="button"
            className="btn btn-secondary"
            onClick={() => navigate('/cases')}
          >
            Cancel
          </button>
          <button
            type="submit"
            className="btn btn-primary"
            disabled={createMutation.isPending}
          >
            {createMutation.isPending ? 'Creating...' : 'Create Case'}
          </button>
        </div>
      </form>

      <style>{`
        .page-header {
          margin-bottom: var(--spacing-lg);
        }

        .page-header p {
          color: var(--text-secondary);
        }

        .card {
          margin-bottom: var(--spacing-md);
        }

        .card h2 {
          margin-bottom: var(--spacing-md);
          font-size: 1.25rem;
        }

        .form-group {
          margin-bottom: var(--spacing-md);
        }

        .form-group label {
          display: block;
          margin-bottom: var(--spacing-xs);
          font-weight: 500;
        }

        .form-group input,
        .form-group textarea {
          width: 100%;
          padding: var(--spacing-sm);
          border: 1px solid var(--border-color);
          border-radius: var(--radius);
          font-size: 1rem;
        }

        .form-group small {
          display: block;
          margin-top: var(--spacing-xs);
          color: var(--text-secondary);
          font-size: 0.875rem;
        }

        .entry-type-selector {
          display: flex;
          gap: var(--spacing-sm);
          margin-bottom: var(--spacing-md);
          flex-wrap: wrap;
        }

        .entry-type-btn {
          padding: 8px 16px;
          border: 2px solid #d1d5db;
          border-radius: 6px;
          background: #ffffff;
          color: #374151;
          cursor: pointer;
          font-size: 0.875rem;
          font-weight: 500;
          transition: all 0.2s;
        }

        .entry-type-btn:hover {
          border-color: #2563eb;
          color: #2563eb;
        }

        .entry-type-btn.active {
          border-color: #2563eb;
          background: #2563eb;
          color: #ffffff;
        }

        .config-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
          gap: var(--spacing-md);
        }

        .checkbox-group {
          margin-top: var(--spacing-md);
        }

        .checkbox-group label {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
          cursor: pointer;
        }

        .form-actions {
          display: flex;
          gap: var(--spacing-md);
          justify-content: flex-end;
          margin-top: var(--spacing-lg);
        }

        .alert {
          padding: var(--spacing-md);
          border-radius: var(--radius);
          margin-bottom: var(--spacing-md);
        }

        .alert-danger {
          background: #fef2f2;
          color: #991b1b;
          border: 1px solid #fecaca;
        }

        .text-muted {
          color: var(--text-secondary);
        }
      `}</style>
    </div>
  );
}
