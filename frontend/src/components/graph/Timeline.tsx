/**
 * Timeline component for MITDS
 *
 * Interactive timeline visualization for temporal coordination analysis.
 * Shows publication/ad events over time with burst highlighting.
 */

import { useEffect, useRef, useMemo, useState } from 'react';

export interface TimelineEvent {
  id: string;
  entityId: string;
  entityName: string;
  timestamp: string;
  eventType: string;
  metadata?: Record<string, unknown>;
}

export interface BurstPeriod {
  startTime: string;
  endTime: string;
  level: number;
  eventCount: number;
}

export interface TimelineProps {
  events: TimelineEvent[];
  bursts?: BurstPeriod[];
  onEventClick?: (event: TimelineEvent) => void;
  onBurstClick?: (burst: BurstPeriod) => void;
  height?: string;
  startDate?: string;
  endDate?: string;
  showLegend?: boolean;
}

// Entity color palette
const ENTITY_COLORS = [
  '#4F46E5', // Indigo
  '#DC2626', // Red
  '#059669', // Emerald
  '#D97706', // Amber
  '#7C3AED', // Violet
  '#0891B2', // Cyan
  '#BE185D', // Pink
  '#65A30D', // Lime
];

export default function Timeline({
  events,
  bursts = [],
  onEventClick,
  onBurstClick,
  height = '300px',
  startDate,
  endDate,
  showLegend = true,
}: TimelineProps) {
  const svgRef = useRef<SVGSVGElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 300 });
  const [hoveredEvent, setHoveredEvent] = useState<TimelineEvent | null>(null);
  const [hoveredBurst, setHoveredBurst] = useState<BurstPeriod | null>(null);

  // Calculate dimensions on mount and resize
  useEffect(() => {
    const updateDimensions = () => {
      if (containerRef.current) {
        const rect = containerRef.current.getBoundingClientRect();
        setDimensions({
          width: rect.width || 800,
          height: parseInt(height) || 300,
        });
      }
    };

    updateDimensions();
    window.addEventListener('resize', updateDimensions);
    return () => window.removeEventListener('resize', updateDimensions);
  }, [height]);

  // Calculate timeline bounds and scales
  const { timeScale, entityScale, entityColorMap, timeRange } = useMemo(() => {
    if (events.length === 0) {
      const now = new Date();
      return {
        timeScale: () => 0,
        entityScale: () => 0,
        entityColorMap: new Map(),
        timeRange: { start: now, end: now },
      };
    }

    const timestamps = events.map((e) => new Date(e.timestamp).getTime());
    const minTime = startDate ? new Date(startDate).getTime() : Math.min(...timestamps);
    const maxTime = endDate ? new Date(endDate).getTime() : Math.max(...timestamps);

    const margin = { left: 60, right: 20, top: 40, bottom: 60 };
    const plotWidth = dimensions.width - margin.left - margin.right;
    const plotHeight = dimensions.height - margin.top - margin.bottom;

    // Time scale (x-axis)
    const timeScaleFn = (time: Date | string) => {
      const t = typeof time === 'string' ? new Date(time).getTime() : time.getTime();
      return margin.left + ((t - minTime) / (maxTime - minTime)) * plotWidth;
    };

    // Get unique entities
    const uniqueEntities = Array.from(new Set(events.map((e) => e.entityId)));
    const entityHeight = plotHeight / Math.max(uniqueEntities.length, 1);

    // Entity scale (y-axis) - one row per entity
    const entityScaleFn = (entityId: string) => {
      const index = uniqueEntities.indexOf(entityId);
      return margin.top + index * entityHeight + entityHeight / 2;
    };

    // Entity color mapping
    const colorMap = new Map<string, string>();
    uniqueEntities.forEach((entityId, index) => {
      colorMap.set(entityId, ENTITY_COLORS[index % ENTITY_COLORS.length]);
    });

    return {
      timeScale: timeScaleFn,
      entityScale: entityScaleFn,
      entityColorMap: colorMap,
      timeRange: { start: new Date(minTime), end: new Date(maxTime) },
    };
  }, [events, dimensions, startDate, endDate]);

  // Generate time axis ticks
  const timeTicks = useMemo(() => {
    const ticks: { date: Date; x: number }[] = [];
    const range = timeRange.end.getTime() - timeRange.start.getTime();
    const tickCount = Math.min(10, Math.floor(dimensions.width / 100));

    for (let i = 0; i <= tickCount; i++) {
      const date = new Date(timeRange.start.getTime() + (range * i) / tickCount);
      ticks.push({ date, x: timeScale(date) });
    }

    return ticks;
  }, [timeRange, dimensions, timeScale]);

  // Get unique entities for y-axis
  const uniqueEntities = useMemo(
    () => Array.from(new Set(events.map((e) => ({ id: e.entityId, name: e.entityName })))),
    [events]
  );

  const formatDate = (date: Date) => {
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
    });
  };

  // formatTime available for future use in tooltip display
  // const formatTime = (date: Date) => date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });

  if (events.length === 0) {
    return (
      <div className="timeline-empty" style={{ height }}>
        <p>No events to display</p>
      </div>
    );
  }

  return (
    <div className="timeline-container" ref={containerRef} style={{ height }}>
      <svg
        ref={svgRef}
        width={dimensions.width}
        height={dimensions.height}
        className="timeline-svg"
      >
        {/* Background grid */}
        <g className="timeline-grid">
          {timeTicks.map((tick, i) => (
            <line
              key={i}
              x1={tick.x}
              y1={40}
              x2={tick.x}
              y2={dimensions.height - 60}
              stroke="var(--border-color)"
              strokeDasharray="4,4"
              opacity={0.5}
            />
          ))}
        </g>

        {/* Burst highlights */}
        <g className="timeline-bursts">
          {bursts.map((burst, i) => {
            const x1 = timeScale(burst.startTime);
            const x2 = timeScale(burst.endTime);
            const width = Math.max(x2 - x1, 4);

            return (
              <rect
                key={i}
                x={x1}
                y={40}
                width={width}
                height={dimensions.height - 100}
                fill={`rgba(245, 158, 11, ${0.1 + burst.level * 0.1})`}
                stroke="#F59E0B"
                strokeWidth={burst.level}
                opacity={0.7}
                rx={4}
                className="burst-region"
                onClick={() => onBurstClick?.(burst)}
                onMouseEnter={() => setHoveredBurst(burst)}
                onMouseLeave={() => setHoveredBurst(null)}
                style={{ cursor: onBurstClick ? 'pointer' : 'default' }}
              />
            );
          })}
        </g>

        {/* Entity labels (y-axis) */}
        <g className="timeline-y-axis">
          {uniqueEntities.map((entity, i) => {
            const y = 40 + (i * (dimensions.height - 100)) / uniqueEntities.length +
                      (dimensions.height - 100) / (uniqueEntities.length * 2);
            const color = entityColorMap.get(entity.id) || '#6B7280';

            return (
              <g key={entity.id}>
                <rect
                  x={0}
                  y={y - 10}
                  width={56}
                  height={20}
                  fill={color}
                  rx={4}
                  opacity={0.1}
                />
                <text
                  x={54}
                  y={y}
                  textAnchor="end"
                  fontSize={10}
                  fill={color}
                  fontWeight={500}
                  dominantBaseline="middle"
                >
                  {entity.name.length > 12 ? entity.name.slice(0, 12) + '...' : entity.name}
                </text>
              </g>
            );
          })}
        </g>

        {/* Time axis (x-axis) */}
        <g className="timeline-x-axis">
          <line
            x1={60}
            y1={dimensions.height - 60}
            x2={dimensions.width - 20}
            y2={dimensions.height - 60}
            stroke="var(--border-color)"
          />
          {timeTicks.map((tick, i) => (
            <g key={i}>
              <line
                x1={tick.x}
                y1={dimensions.height - 60}
                x2={tick.x}
                y2={dimensions.height - 55}
                stroke="var(--text-secondary)"
              />
              <text
                x={tick.x}
                y={dimensions.height - 42}
                textAnchor="middle"
                fontSize={10}
                fill="var(--text-secondary)"
              >
                {formatDate(tick.date)}
              </text>
            </g>
          ))}
        </g>

        {/* Events */}
        <g className="timeline-events">
          {events.map((event) => {
            const x = timeScale(event.timestamp);
            const y = entityScale(event.entityId);
            const color = entityColorMap.get(event.entityId) || '#6B7280';

            return (
              <g key={event.id}>
                <circle
                  cx={x}
                  cy={y}
                  r={hoveredEvent?.id === event.id ? 8 : 5}
                  fill={color}
                  stroke="white"
                  strokeWidth={2}
                  className="timeline-event"
                  onClick={() => onEventClick?.(event)}
                  onMouseEnter={() => setHoveredEvent(event)}
                  onMouseLeave={() => setHoveredEvent(null)}
                  style={{ cursor: onEventClick ? 'pointer' : 'default' }}
                />
              </g>
            );
          })}
        </g>
      </svg>

      {/* Tooltip */}
      {hoveredEvent && (
        <div
          className="timeline-tooltip"
          style={{
            left: Math.min(timeScale(hoveredEvent.timestamp), dimensions.width - 200),
            top: entityScale(hoveredEvent.entityId) - 60,
          }}
        >
          <div className="tooltip-entity">{hoveredEvent.entityName}</div>
          <div className="tooltip-time">
            {new Date(hoveredEvent.timestamp).toLocaleString()}
          </div>
          <div className="tooltip-type">{hoveredEvent.eventType}</div>
        </div>
      )}

      {hoveredBurst && (
        <div
          className="timeline-tooltip burst-tooltip"
          style={{
            left: timeScale(hoveredBurst.startTime),
            top: 10,
          }}
        >
          <div className="tooltip-title">Burst Detected</div>
          <div className="tooltip-detail">Level: {hoveredBurst.level}</div>
          <div className="tooltip-detail">Events: {hoveredBurst.eventCount}</div>
          <div className="tooltip-time">
            {formatDate(new Date(hoveredBurst.startTime))} - {formatDate(new Date(hoveredBurst.endTime))}
          </div>
        </div>
      )}

      {/* Legend */}
      {showLegend && (
        <div className="timeline-legend">
          <div className="legend-section">
            <span className="legend-title">Entities:</span>
            {uniqueEntities.slice(0, 6).map((entity) => (
              <span key={entity.id} className="legend-item">
                <span
                  className="legend-dot"
                  style={{ backgroundColor: entityColorMap.get(entity.id) }}
                />
                {entity.name.length > 15 ? entity.name.slice(0, 15) + '...' : entity.name}
              </span>
            ))}
            {uniqueEntities.length > 6 && (
              <span className="legend-item text-muted">
                +{uniqueEntities.length - 6} more
              </span>
            )}
          </div>
          {bursts.length > 0 && (
            <div className="legend-section">
              <span className="legend-item">
                <span className="legend-burst" />
                Burst period
              </span>
            </div>
          )}
        </div>
      )}

      <style>{`
        .timeline-container {
          position: relative;
          overflow: hidden;
        }

        .timeline-svg {
          display: block;
        }

        .timeline-empty {
          display: flex;
          align-items: center;
          justify-content: center;
          color: var(--text-muted);
          background: var(--bg-secondary);
          border-radius: var(--border-radius);
        }

        .timeline-event {
          transition: r 0.15s ease;
        }

        .timeline-event:hover {
          filter: brightness(1.1);
        }

        .burst-region {
          transition: opacity 0.15s ease;
        }

        .burst-region:hover {
          opacity: 0.9;
        }

        .timeline-tooltip {
          position: absolute;
          background: var(--bg-primary);
          border: 1px solid var(--border-color);
          border-radius: var(--border-radius);
          padding: 8px 12px;
          font-size: 12px;
          box-shadow: var(--shadow-md);
          pointer-events: none;
          z-index: 100;
          max-width: 200px;
        }

        .tooltip-entity {
          font-weight: 600;
          margin-bottom: 4px;
        }

        .tooltip-time {
          color: var(--text-secondary);
          font-size: 11px;
        }

        .tooltip-type {
          color: var(--text-muted);
          font-size: 10px;
          text-transform: uppercase;
        }

        .tooltip-title {
          font-weight: 600;
          color: #F59E0B;
          margin-bottom: 4px;
        }

        .tooltip-detail {
          color: var(--text-secondary);
          font-size: 11px;
        }

        .burst-tooltip {
          border-color: #F59E0B;
        }

        .timeline-legend {
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
          flex-wrap: wrap;
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

        .legend-burst {
          width: 20px;
          height: 12px;
          background: rgba(245, 158, 11, 0.3);
          border: 2px solid #F59E0B;
          border-radius: 4px;
        }
      `}</style>
    </div>
  );
}
