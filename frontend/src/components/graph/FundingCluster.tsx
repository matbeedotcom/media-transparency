/**
 * FundingCluster component for MITDS
 *
 * Visualizes funding clusters showing shared funder relationships.
 */

import { useEffect, useRef, useCallback } from 'react';
import cytoscape, { Core } from 'cytoscape';
import { type FundingCluster as FundingClusterType, type EntitySummary } from '../../services/api';

interface FundingClusterProps {
  cluster: FundingClusterType;
  onOutletClick?: (outlet: EntitySummary) => void;
  onFunderClick?: (funder: EntitySummary) => void;
  height?: string;
}

export default function FundingCluster({
  cluster,
  onOutletClick,
  onFunderClick,
  height = '400px',
}: FundingClusterProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);

  const buildElements = useCallback(() => {
    const elements: cytoscape.ElementDefinition[] = [];

    // Add funder nodes (center column)
    cluster.shared_funders.forEach((funder, index) => {
      elements.push({
        data: {
          id: funder.id,
          label: funder.name.length > 25 ? funder.name.slice(0, 25) + '...' : funder.name,
          fullLabel: funder.name,
          type: 'funder',
          entityType: funder.entity_type,
        },
        position: {
          x: 200,
          y: 50 + index * 80,
        },
      });
    });

    // Add outlet nodes (right side)
    cluster.outlets.forEach((outlet, index) => {
      elements.push({
        data: {
          id: outlet.id,
          label: outlet.name.length > 25 ? outlet.name.slice(0, 25) + '...' : outlet.name,
          fullLabel: outlet.name,
          type: 'outlet',
          entityType: outlet.entity_type,
        },
        position: {
          x: 400,
          y: 50 + index * 60,
        },
      });

      // Connect each outlet to all funders
      cluster.shared_funders.forEach((funder) => {
        elements.push({
          data: {
            id: `${funder.id}-${outlet.id}`,
            source: funder.id,
            target: outlet.id,
          },
        });
      });
    });

    return elements;
  }, [cluster]);

  useEffect(() => {
    if (!containerRef.current) return;

    const elements = buildElements();

    cyRef.current = cytoscape({
      container: containerRef.current,
      elements,
      style: [
        {
          selector: 'node[type="funder"]',
          style: {
            'background-color': '#D97706',
            label: 'data(label)',
            'text-valign': 'center',
            'text-halign': 'right',
            'text-margin-x': 10,
            'font-size': '11px',
            color: '#374151',
            width: 35,
            height: 35,
            shape: 'diamond',
            'border-width': 2,
            'border-color': '#ffffff',
          },
        },
        {
          selector: 'node[type="outlet"]',
          style: {
            'background-color': '#DC2626',
            label: 'data(label)',
            'text-valign': 'center',
            'text-halign': 'right',
            'text-margin-x': 10,
            'font-size': '11px',
            color: '#374151',
            width: 30,
            height: 30,
            shape: 'ellipse',
            'border-width': 2,
            'border-color': '#ffffff',
          },
        },
        {
          selector: 'node:selected',
          style: {
            'border-width': 4,
            'border-color': '#3B82F6',
          },
        },
        {
          selector: 'edge',
          style: {
            width: 1.5,
            'line-color': '#10B981',
            'target-arrow-color': '#10B981',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            opacity: 0.6,
          },
        },
        {
          selector: 'edge:selected',
          style: {
            width: 3,
            opacity: 1,
          },
        },
      ],
      layout: {
        name: 'preset',
      },
      userZoomingEnabled: true,
      userPanningEnabled: true,
      boxSelectionEnabled: false,
      minZoom: 0.5,
      maxZoom: 2,
    });

    // Fit to container
    cyRef.current.fit(undefined, 30);

    // Add event listeners
    cyRef.current.on('tap', 'node', (evt) => {
      const node = evt.target;
      const nodeType = node.data('type');
      const entity: EntitySummary = {
        id: node.id(),
        entity_type: node.data('entityType'),
        name: node.data('fullLabel'),
      };

      if (nodeType === 'outlet' && onOutletClick) {
        onOutletClick(entity);
      } else if (nodeType === 'funder' && onFunderClick) {
        onFunderClick(entity);
      }
    });

    return () => {
      if (cyRef.current) {
        cyRef.current.destroy();
        cyRef.current = null;
      }
    };
  }, [cluster, buildElements, onOutletClick, onFunderClick]);

  const formatCurrency = (amount: number): string => {
    if (amount >= 1_000_000) {
      return `$${(amount / 1_000_000).toFixed(1)}M`;
    } else if (amount >= 1_000) {
      return `$${(amount / 1_000).toFixed(0)}K`;
    }
    return `$${amount.toFixed(0)}`;
  };

  return (
    <div className="funding-cluster">
      <div className="cluster-header">
        <div className="cluster-stats">
          <div className="stat">
            <span className="stat-value">{cluster.outlets.length}</span>
            <span className="stat-label">Outlets</span>
          </div>
          <div className="stat">
            <span className="stat-value">{cluster.shared_funders.length}</span>
            <span className="stat-label">Shared Funders</span>
          </div>
          <div className="stat">
            <span className="stat-value">{formatCurrency(cluster.total_shared_funding)}</span>
            <span className="stat-label">Total Funding</span>
          </div>
          <div className="stat">
            <span className="stat-value">{(cluster.coordination_score * 100).toFixed(0)}%</span>
            <span className="stat-label">Coordination Score</span>
          </div>
        </div>
      </div>
      <div
        ref={containerRef}
        className="cluster-graph"
        style={{ height }}
      />
      <div className="cluster-legend">
        <span className="legend-item">
          <span className="legend-shape funder" />
          Funder
        </span>
        <span className="legend-item">
          <span className="legend-shape outlet" />
          Outlet
        </span>
        <span className="legend-item">
          <span className="legend-line" />
          Funding Relationship
        </span>
      </div>
      <style>{`
        .funding-cluster {
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          background: var(--bg-primary);
          overflow: hidden;
        }

        .cluster-header {
          padding: 12px 16px;
          background: var(--bg-tertiary);
          border-bottom: 1px solid var(--border-color);
        }

        .cluster-stats {
          display: flex;
          gap: 24px;
        }

        .stat {
          display: flex;
          flex-direction: column;
        }

        .stat-value {
          font-size: 1.25rem;
          font-weight: 700;
          color: var(--text-primary);
        }

        .stat-label {
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        .cluster-graph {
          background: var(--bg-secondary);
        }

        .cluster-legend {
          display: flex;
          gap: 16px;
          padding: 8px 16px;
          font-size: 11px;
          border-top: 1px solid var(--border-color);
          background: var(--bg-tertiary);
        }

        .legend-item {
          display: flex;
          align-items: center;
          gap: 6px;
          color: var(--text-muted);
        }

        .legend-shape {
          width: 14px;
          height: 14px;
        }

        .legend-shape.funder {
          background: #D97706;
          transform: rotate(45deg);
        }

        .legend-shape.outlet {
          background: #DC2626;
          border-radius: 50%;
        }

        .legend-line {
          width: 20px;
          height: 2px;
          background: #10B981;
        }
      `}</style>
    </div>
  );
}
