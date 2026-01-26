/**
 * SourceLink component for MITDS
 *
 * Displays a source URL with archive fallback support.
 * Shows both original URL and archived version when available.
 */

import { useState } from 'react';

interface SourceLinkProps {
  url: string;
  archiveUrl?: string | null;
  title?: string;
  showArchiveBadge?: boolean;
  className?: string;
}

export default function SourceLink({
  url,
  archiveUrl,
  title,
  showArchiveBadge = true,
  className = '',
}: SourceLinkProps) {
  const [showOriginal, setShowOriginal] = useState(true);
  const [imgError, setImgError] = useState(false);

  // Extract domain from URL for display
  const getDomain = (urlString: string): string => {
    try {
      const parsed = new URL(urlString);
      return parsed.hostname.replace('www.', '');
    } catch {
      return urlString;
    }
  };

  // Detect if the archive URL is from a known archive service
  const getArchiveService = (archUrl: string): string | null => {
    if (archUrl.includes('archive.org') || archUrl.includes('web.archive.org')) {
      return 'Internet Archive';
    }
    if (archUrl.includes('archive.ph') || archUrl.includes('archive.is')) {
      return 'archive.today';
    }
    if (archUrl.includes('perma.cc')) {
      return 'Perma.cc';
    }
    return 'Archive';
  };

  const displayUrl = showOriginal ? url : (archiveUrl || url);
  const domain = getDomain(url);
  const archiveService = archiveUrl ? getArchiveService(archiveUrl) : null;

  return (
    <div className={`source-link ${className}`}>
      <div className="source-link-main">
        {!imgError && (
          <img
            src={`https://www.google.com/s2/favicons?domain=${domain}&sz=16`}
            alt=""
            className="source-favicon"
            onError={() => setImgError(true)}
          />
        )}
        <a
          href={displayUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="source-url"
          title={title || displayUrl}
        >
          {title || domain}
        </a>
        {showArchiveBadge && archiveUrl && (
          <span className="archive-badge" title={`Archived on ${archiveService}`}>
            Archived
          </span>
        )}
      </div>

      {archiveUrl && (
        <div className="source-link-actions">
          <button
            type="button"
            className={`link-toggle ${showOriginal ? 'active' : ''}`}
            onClick={() => setShowOriginal(true)}
            title="View original URL"
          >
            Original
          </button>
          <button
            type="button"
            className={`link-toggle ${!showOriginal ? 'active' : ''}`}
            onClick={() => setShowOriginal(false)}
            title={`View on ${archiveService}`}
          >
            {archiveService}
          </button>
        </div>
      )}

      <style>{`
        .source-link {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }

        .source-link-main {
          display: flex;
          align-items: center;
          gap: 8px;
        }

        .source-favicon {
          width: 16px;
          height: 16px;
          flex-shrink: 0;
        }

        .source-url {
          color: var(--color-primary);
          text-decoration: none;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          flex: 1;
        }

        .source-url:hover {
          text-decoration: underline;
        }

        .archive-badge {
          font-size: 0.625rem;
          padding: 2px 6px;
          border-radius: 4px;
          background-color: #059669;
          color: white;
          text-transform: uppercase;
          flex-shrink: 0;
        }

        .source-link-actions {
          display: flex;
          gap: 4px;
          margin-left: 24px;
        }

        .link-toggle {
          font-size: 0.75rem;
          padding: 2px 8px;
          border: 1px solid var(--border-color);
          border-radius: 4px;
          background: var(--bg-secondary);
          color: var(--text-muted);
          cursor: pointer;
          transition: all 0.15s ease;
        }

        .link-toggle:hover {
          background: var(--bg-tertiary);
          color: var(--text-primary);
        }

        .link-toggle.active {
          background: var(--color-primary);
          border-color: var(--color-primary);
          color: white;
        }
      `}</style>
    </div>
  );
}
