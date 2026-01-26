/**
 * EntityGraph component for MITDS
 *
 * Interactive graph visualization of entity relationships using Cytoscape.js
 */

import { useEffect, useRef, useCallback, useState } from 'react';
import cytoscape, { Core, NodeSingular, EdgeSingular } from 'cytoscape';
import { useQuery, useMutation } from '@tanstack/react-query';
import { getEntityRelationships, getGraphAtTime, findAllPaths, type EntitySummary, type Relationship, type PathResult } from '../../services/api';

// Node color mapping by entity type
const NODE_COLORS: Record<string, string> = {
  ORGANIZATION: '#4F46E5', // Indigo
  PERSON: '#059669', // Emerald
  OUTLET: '#DC2626', // Red
  SPONSOR: '#D97706', // Amber
  DEFAULT: '#6B7280', // Gray
};

// Edge color mapping by relationship type
const EDGE_COLORS: Record<string, string> = {
  FUNDED_BY: '#10B981', // Green
  DIRECTOR_OF: '#8B5CF6', // Purple
  EMPLOYED_BY: '#3B82F6', // Blue
  SHARED_INFRA: '#F59E0B', // Yellow
  DEFAULT: '#9CA3AF', // Gray
};

interface EntityGraphProps {
  entityId: string;
  onNodeClick?: (nodeId: string, entityType: string) => void;
  onEdgeClick?: (relationship: Relationship) => void;
  maxHops?: number;
  height?: string;
  enablePathFinding?: boolean;
  onPathFound?: (path: PathResult) => void;
  /** ISO date string for temporal graph queries - shows graph as it existed at this time */
  asOf?: string;
  /** Callback when temporal mode is active and date changes */
  onTemporalDateChange?: (date: string | undefined) => void;
}

interface GraphNode {
  id: string;
  label: string;
  type: string;
}

interface GraphEdge {
  id: string;
  source: string;
  target: string;
  type: string;
  properties?: Record<string, unknown>;
}

export default function EntityGraph({
  entityId,
  onNodeClick,
  onEdgeClick,
  maxHops = 2,
  height = '500px',
  enablePathFinding = false,
  onPathFound,
  asOf,
  onTemporalDateChange,
}: EntityGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [pathFindingMode, setPathFindingMode] = useState(false);
  const [pathSource, setPathSource] = useState<string | null>(null);
  const [pathTarget, setPathTarget] = useState<string | null>(null);
  const [foundPaths, setFoundPaths] = useState<PathResult[]>([]);
  const [selectedPathIndex, setSelectedPathIndex] = useState<number>(0);

  // Path finding mutation
  const pathMutation = useMutation({
    mutationFn: async ({ from, to }: { from: string; to: string }) => {
      const result = await findAllPaths(from, to, { maxHops: 5, limit: 5 });
      return result.paths || [];
    },
    onSuccess: (paths) => {
      setFoundPaths(paths);
      setSelectedPathIndex(0);
      if (paths.length > 0 && onPathFound) {
        onPathFound(paths[0]);
      }
    },
  });

  // Fetch relationships for the entity (or temporal graph if asOf is provided)
  const { data: relationshipData, isLoading } = useQuery({
    queryKey: ['entity-relationships', entityId, maxHops, asOf],
    queryFn: async () => {
      if (asOf) {
        // Use temporal query
        const temporalData = await getGraphAtTime(entityId, asOf, { depth: maxHops });
        // Transform temporal data to match relationship data structure
        return {
          entity_id: entityId,
          relationships: temporalData.relationships || [],
          total: temporalData.relationships?.length || 0,
        };
      }
      return getEntityRelationships(entityId, { direction: 'both' });
    },
    enabled: !!entityId,
  });

  // Transform relationship data into graph elements
  const transformToGraphElements = useCallback(
    (
      relationships: Relationship[],
      centerEntityId: string
    ): { nodes: GraphNode[]; edges: GraphEdge[] } => {
      const nodesMap = new Map<string, GraphNode>();
      const edges: GraphEdge[] = [];

      relationships.forEach((rel) => {
        // Add source node
        if (!nodesMap.has(rel.source_entity.id)) {
          nodesMap.set(rel.source_entity.id, {
            id: rel.source_entity.id,
            label: rel.source_entity.name,
            type: rel.source_entity.entity_type,
          });
        }

        // Add target node
        if (!nodesMap.has(rel.target_entity.id)) {
          nodesMap.set(rel.target_entity.id, {
            id: rel.target_entity.id,
            label: rel.target_entity.name,
            type: rel.target_entity.entity_type,
          });
        }

        // Add edge
        edges.push({
          id: rel.id,
          source: rel.source_entity.id,
          target: rel.target_entity.id,
          type: rel.rel_type,
          properties: rel.properties,
        });
      });

      return { nodes: Array.from(nodesMap.values()), edges };
    },
    []
  );

  // Initialize and update Cytoscape
  useEffect(() => {
    if (!containerRef.current || !relationshipData?.relationships) return;

    const { nodes, edges } = transformToGraphElements(
      relationshipData.relationships,
      entityId
    );

    // Create Cytoscape elements
    const elements = [
      ...nodes.map((node) => ({
        data: {
          id: node.id,
          label: node.label.length > 20 ? node.label.slice(0, 20) + '...' : node.label,
          fullLabel: node.label,
          type: node.type,
          isCenter: node.id === entityId,
        },
      })),
      ...edges.map((edge) => ({
        data: {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          type: edge.type,
          properties: edge.properties,
        },
      })),
    ];

    // Initialize Cytoscape if not already done
    if (!cyRef.current) {
      cyRef.current = cytoscape({
        container: containerRef.current,
        elements,
        style: [
          {
            selector: 'node',
            style: {
              'background-color': (ele: NodeSingular) =>
                NODE_COLORS[ele.data('type')] || NODE_COLORS.DEFAULT,
              label: 'data(label)',
              'text-valign': 'bottom',
              'text-halign': 'center',
              'font-size': '10px',
              'text-margin-y': 5,
              color: '#374151',
              width: 30,
              height: 30,
              'border-width': 2,
              'border-color': '#ffffff',
            },
          },
          {
            selector: 'node[?isCenter]',
            style: {
              width: 45,
              height: 45,
              'border-width': 3,
              'border-color': '#1F2937',
              'font-weight': 'bold',
            },
          },
          {
            selector: 'node:selected',
            style: {
              'border-width': 4,
              'border-color': '#F59E0B',
            },
          },
          {
            selector: 'edge',
            style: {
              width: 2,
              'line-color': (ele: EdgeSingular) =>
                EDGE_COLORS[ele.data('type')] || EDGE_COLORS.DEFAULT,
              'target-arrow-color': (ele: EdgeSingular) =>
                EDGE_COLORS[ele.data('type')] || EDGE_COLORS.DEFAULT,
              'target-arrow-shape': 'triangle',
              'curve-style': 'bezier',
              label: 'data(type)',
              'font-size': '8px',
              'text-rotation': 'autorotate',
              color: '#6B7280',
            },
          },
          {
            selector: 'edge:selected',
            style: {
              width: 4,
              'line-color': '#F59E0B',
              'target-arrow-color': '#F59E0B',
            },
          },
          {
            selector: 'node.path-source',
            style: {
              'border-width': 4,
              'border-color': '#10B981',
              'background-color': '#10B981',
            },
          },
          {
            selector: 'node.path-target',
            style: {
              'border-width': 4,
              'border-color': '#EF4444',
              'background-color': '#EF4444',
            },
          },
          {
            selector: 'node.path-highlight',
            style: {
              'border-width': 3,
              'border-color': '#8B5CF6',
            },
          },
          {
            selector: 'edge.path-highlight',
            style: {
              width: 4,
              'line-color': '#8B5CF6',
              'target-arrow-color': '#8B5CF6',
            },
          },
        ],
        layout: {
          name: 'cose',
          idealEdgeLength: 100,
          nodeOverlap: 20,
          refresh: 20,
          fit: true,
          padding: 30,
          randomize: false,
          componentSpacing: 100,
          nodeRepulsion: 400000,
          edgeElasticity: 100,
          nestingFactor: 5,
          gravity: 80,
          numIter: 1000,
          initialTemp: 200,
          coolingFactor: 0.95,
          minTemp: 1.0,
        },
        minZoom: 0.3,
        maxZoom: 3,
      });

      // Add event listeners
      cyRef.current.on('tap', 'node', (evt) => {
        const node = evt.target;
        const nodeId = node.id();
        setSelectedNode(nodeId);

        // Handle path finding mode
        if (pathFindingMode) {
          if (!pathSource) {
            setPathSource(nodeId);
            node.addClass('path-source');
          } else if (!pathTarget && nodeId !== pathSource) {
            setPathTarget(nodeId);
            node.addClass('path-target');
            // Trigger path finding
            pathMutation.mutate({ from: pathSource, to: nodeId });
          }
        } else if (onNodeClick) {
          onNodeClick(nodeId, node.data('type'));
        }
      });

      cyRef.current.on('tap', 'edge', (evt) => {
        const edge = evt.target;
        if (onEdgeClick) {
          const rel: Relationship = {
            id: edge.id(),
            rel_type: edge.data('type'),
            source_entity: {
              id: edge.source().id(),
              entity_type: edge.source().data('type'),
              name: edge.source().data('fullLabel'),
            },
            target_entity: {
              id: edge.target().id(),
              entity_type: edge.target().data('type'),
              name: edge.target().data('fullLabel'),
            },
            confidence: 1,
            properties: edge.data('properties'),
          };
          onEdgeClick(rel);
        }
      });
    } else {
      // Update existing graph with new data
      cyRef.current.elements().remove();
      cyRef.current.add(elements);
      cyRef.current.layout({
        name: 'cose',
        animate: true,
        animationDuration: 500,
      } as cytoscape.LayoutOptions).run();
    }

    return () => {
      if (cyRef.current) {
        cyRef.current.destroy();
        cyRef.current = null;
      }
    };
  }, [entityId, relationshipData, transformToGraphElements, onNodeClick, onEdgeClick]);

  // Highlight path in graph
  const highlightPath = useCallback((path: PathResult | null) => {
    if (!cyRef.current) return;

    // Clear previous highlights
    cyRef.current.elements().removeClass('path-highlight');

    if (!path) return;

    // Highlight nodes in path
    path.nodes.forEach((node) => {
      const cyNode = cyRef.current?.getElementById(node.id);
      if (cyNode) {
        cyNode.addClass('path-highlight');
      }
    });

    // Highlight edges in path
    path.relationships.forEach((rel) => {
      const cyEdge = cyRef.current?.getElementById(rel.id);
      if (cyEdge) {
        cyEdge.addClass('path-highlight');
      }
    });
  }, []);

  // Update path highlight when selection changes
  useEffect(() => {
    if (foundPaths.length > 0 && selectedPathIndex < foundPaths.length) {
      highlightPath(foundPaths[selectedPathIndex]);
    }
  }, [foundPaths, selectedPathIndex, highlightPath]);

  // Reset path finding
  const handleResetPathFinding = () => {
    setPathSource(null);
    setPathTarget(null);
    setFoundPaths([]);
    setSelectedPathIndex(0);
    if (cyRef.current) {
      cyRef.current.elements().removeClass('path-source path-target path-highlight');
    }
  };

  // Toggle path finding mode
  const handleTogglePathFinding = () => {
    if (pathFindingMode) {
      handleResetPathFinding();
    }
    setPathFindingMode(!pathFindingMode);
  };

  // Handle zoom controls
  const handleZoomIn = () => {
    if (cyRef.current) {
      cyRef.current.zoom(cyRef.current.zoom() * 1.2);
    }
  };

  const handleZoomOut = () => {
    if (cyRef.current) {
      cyRef.current.zoom(cyRef.current.zoom() / 1.2);
    }
  };

  const handleFit = () => {
    if (cyRef.current) {
      cyRef.current.fit(undefined, 30);
    }
  };

  const handleCenter = () => {
    if (cyRef.current && entityId) {
      const centerNode = cyRef.current.getElementById(entityId);
      if (centerNode.length) {
        cyRef.current.center(centerNode);
      }
    }
  };

  if (isLoading) {
    return (
      <div className="graph-loading" style={{ height }}>
        <div className="spinner" />
        <span>Loading graph...</span>
      </div>
    );
  }

  if (!relationshipData?.relationships?.length) {
    return (
      <div className="graph-empty" style={{ height }}>
        <p>No relationships found for this entity.</p>
      </div>
    );
  }

  return (
    <div className="entity-graph" style={{ height }}>
      <div className="graph-toolbar">
        <button onClick={handleZoomIn} title="Zoom In">+</button>
        <button onClick={handleZoomOut} title="Zoom Out">-</button>
        <button onClick={handleFit} title="Fit to Screen">Fit</button>
        <button onClick={handleCenter} title="Center on Entity">Center</button>
        {enablePathFinding && (
          <>
            <div className="toolbar-separator" />
            <button
              onClick={handleTogglePathFinding}
              className={pathFindingMode ? 'active' : ''}
              title={pathFindingMode ? 'Exit Path Mode' : 'Find Path'}
            >
              Path
            </button>
            {pathFindingMode && pathSource && (
              <button onClick={handleResetPathFinding} title="Reset Path">
                Reset
              </button>
            )}
          </>
        )}
      </div>

      {/* Path Finding Panel */}
      {pathFindingMode && (
        <div className="path-panel">
          <div className="path-instructions">
            {!pathSource && "Click a node to select the start point"}
            {pathSource && !pathTarget && "Click another node to find paths"}
            {pathMutation.isPending && "Finding paths..."}
          </div>
          {foundPaths.length > 0 && (
            <div className="path-results">
              <div className="path-count">
                Found {foundPaths.length} path{foundPaths.length > 1 ? 's' : ''}
              </div>
              <div className="path-selector">
                {foundPaths.map((path, index) => (
                  <button
                    key={index}
                    className={`path-option ${selectedPathIndex === index ? 'selected' : ''}`}
                    onClick={() => {
                      setSelectedPathIndex(index);
                      if (onPathFound) onPathFound(path);
                    }}
                  >
                    {index + 1}: {path.path_length} hop{path.path_length > 1 ? 's' : ''} via{' '}
                    {path.path_types.join(' → ')}
                  </button>
                ))}
              </div>
            </div>
          )}
          {pathMutation.isError && (
            <div className="path-error">
              No paths found between selected nodes
            </div>
          )}
        </div>
      )}
      <div
        ref={containerRef}
        className="graph-container"
        style={{ width: '100%', height: '100%' }}
      />
      <div className="graph-legend">
        <div className="legend-section">
          <span className="legend-title">Entities:</span>
          {Object.entries(NODE_COLORS).filter(([k]) => k !== 'DEFAULT').map(([type, color]) => (
            <span key={type} className="legend-item">
              <span className="legend-dot" style={{ backgroundColor: color }} />
              {type}
            </span>
          ))}
        </div>
        <div className="legend-section">
          <span className="legend-title">Relations:</span>
          {Object.entries(EDGE_COLORS).filter(([k]) => k !== 'DEFAULT').map(([type, color]) => (
            <span key={type} className="legend-item">
              <span className="legend-line" style={{ backgroundColor: color }} />
              {type.replace(/_/g, ' ')}
            </span>
          ))}
        </div>
      </div>
      <style>{`
        .entity-graph {
          position: relative;
          display: flex;
          flex-direction: column;
        }

        .graph-toolbar {
          position: absolute;
          top: 8px;
          right: 8px;
          z-index: 10;
          display: flex;
          gap: 4px;
        }

        .graph-toolbar button {
          width: 32px;
          height: 32px;
          border: 1px solid var(--border-color);
          border-radius: 4px;
          background: var(--bg-primary);
          cursor: pointer;
          font-size: 14px;
          display: flex;
          align-items: center;
          justify-content: center;
        }

        .graph-toolbar button:hover {
          background: var(--bg-tertiary);
        }

        .graph-container {
          flex: 1;
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          background: var(--bg-secondary);
        }

        .graph-legend {
          display: flex;
          flex-wrap: wrap;
          gap: 16px;
          padding: 8px;
          font-size: 11px;
          background: var(--bg-tertiary);
          border-radius: 0 0 var(--border-radius) var(--border-radius);
        }

        .legend-section {
          display: flex;
          align-items: center;
          gap: 8px;
        }

        .legend-title {
          font-weight: 600;
          color: var(--text-secondary);
        }

        .legend-item {
          display: flex;
          align-items: center;
          gap: 4px;
          color: var(--text-muted);
        }

        .legend-dot {
          width: 10px;
          height: 10px;
          border-radius: 50%;
        }

        .legend-line {
          width: 16px;
          height: 3px;
          border-radius: 2px;
        }

        .graph-loading,
        .graph-empty {
          display: flex;
          flex-direction: column;
          align-items: center;
          justify-content: center;
          gap: 8px;
          color: var(--text-muted);
        }

        .toolbar-separator {
          width: 1px;
          height: 24px;
          background: var(--border-color);
          margin: 0 4px;
        }

        .graph-toolbar button.active {
          background: var(--primary-color);
          color: white;
          border-color: var(--primary-color);
        }

        .path-panel {
          position: absolute;
          top: 48px;
          left: 8px;
          z-index: 10;
          background: var(--bg-primary);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          padding: 12px;
          max-width: 300px;
          box-shadow: var(--shadow-md);
        }

        .path-instructions {
          font-size: 12px;
          color: var(--text-secondary);
          margin-bottom: 8px;
        }

        .path-results {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }

        .path-count {
          font-size: 12px;
          font-weight: 600;
          color: var(--text-primary);
        }

        .path-selector {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }

        .path-option {
          padding: 8px;
          font-size: 11px;
          text-align: left;
          background: var(--bg-secondary);
          border: 1px solid var(--border-color);
          border-radius: 4px;
          cursor: pointer;
        }

        .path-option:hover {
          background: var(--bg-tertiary);
        }

        .path-option.selected {
          background: var(--primary-color);
          color: white;
          border-color: var(--primary-color);
        }

        .path-error {
          font-size: 12px;
          color: var(--error-color);
          margin-top: 8px;
        }
      `}</style>
    </div>
  );
}
