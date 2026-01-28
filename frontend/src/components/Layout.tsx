/**
 * Base layout component for MITDS
 *
 * Provides navigation header, sidebar, and main content area.
 */

import { Link, Outlet, useLocation } from 'react-router-dom';
import { clsx } from 'clsx';

interface NavItem {
  path: string;
  label: string;
  icon: string;
}

const navItems: NavItem[] = [
  { path: '/', label: 'Dashboard', icon: '📊' },
  { path: '/ingestion', label: 'Ingestion', icon: '📥' },
  { path: '/entities', label: 'Entity Explorer', icon: '🔍' },
  { path: '/detection', label: 'Detection', icon: '🎯' },
  { path: '/resolution', label: 'Resolution', icon: '🔗' },
  { path: '/reports', label: 'Reports', icon: '📄' },
  { path: '/validation', label: 'Validation', icon: '✅' },
  { path: '/settings', label: 'Settings', icon: '⚙️' },
];

export default function Layout() {
  const location = useLocation();

  return (
    <div className="layout">
      {/* Header */}
      <header className="header">
        <div className="header-brand">
          <span className="header-logo">🔬</span>
          <span className="header-title">MITDS</span>
        </div>
        <nav className="header-nav">
          {navItems.map((item) => (
            <Link
              key={item.path}
              to={item.path}
              className={clsx('nav-link', {
                'nav-link-active': location.pathname === item.path,
              })}
            >
              <span className="nav-icon">{item.icon}</span>
              <span className="nav-label">{item.label}</span>
            </Link>
          ))}
        </nav>
        <div className="header-actions">
          {/* Reserved for user menu / auth */}
        </div>
      </header>

      {/* Main Content */}
      <main className="main-content">
        <Outlet />
      </main>

      {/* Footer */}
      <footer className="footer">
        <p>Media Influence Topology & Detection System</p>
        <p className="text-muted">v1.0.0</p>
      </footer>

      <style>{`
        .layout {
          display: flex;
          flex-direction: column;
          min-height: 100vh;
        }

        .header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          padding: var(--spacing-md) var(--spacing-lg);
          background-color: var(--bg-primary);
          border-bottom: 1px solid var(--border-color);
          position: sticky;
          top: 0;
          z-index: 100;
        }

        .header-brand {
          display: flex;
          align-items: center;
          gap: var(--spacing-sm);
        }

        .header-logo {
          font-size: 1.5rem;
        }

        .header-title {
          font-size: 1.25rem;
          font-weight: 700;
          color: var(--text-primary);
        }

        .header-nav {
          display: flex;
          gap: var(--spacing-xs);
        }

        .nav-link {
          display: flex;
          align-items: center;
          gap: var(--spacing-xs);
          padding: var(--spacing-sm) var(--spacing-md);
          border-radius: var(--border-radius);
          color: var(--text-secondary);
          font-size: 0.875rem;
          font-weight: 500;
          text-decoration: none;
          transition: all 0.2s ease;
        }

        .nav-link:hover {
          background-color: var(--bg-tertiary);
          color: var(--text-primary);
          text-decoration: none;
        }

        .nav-link-active {
          background-color: var(--color-primary);
          color: white;
        }

        .nav-link-active:hover {
          background-color: var(--color-primary-dark);
          color: white;
        }

        .nav-icon {
          font-size: 1rem;
        }

        .header-actions {
          display: flex;
          gap: var(--spacing-sm);
        }

        .main-content {
          flex: 1;
          padding: var(--spacing-lg);
          max-width: 1400px;
          margin: 0 auto;
          width: 100%;
        }

        .footer {
          display: flex;
          justify-content: space-between;
          padding: var(--spacing-md) var(--spacing-lg);
          background-color: var(--bg-primary);
          border-top: 1px solid var(--border-color);
          font-size: 0.75rem;
          color: var(--text-muted);
        }

        @media (max-width: 768px) {
          .header {
            flex-direction: column;
            gap: var(--spacing-md);
          }

          .header-nav {
            flex-wrap: wrap;
            justify-content: center;
          }

          .nav-label {
            display: none;
          }
        }
      `}</style>
    </div>
  );
}
