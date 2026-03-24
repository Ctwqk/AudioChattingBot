import { NavLink } from 'react-router-dom';
import { useYouTubeAuth } from '../../hooks/useYouTubeAuth';

const links = [
  { to: '/editor', label: 'Editor', icon: '⬡' },
  { to: '/jobs', label: 'Jobs', icon: '▶' },
  { to: '/assets', label: 'Assets', icon: '📁' },
  { to: '/templates', label: 'Templates', icon: '📋' },
];

export default function Sidebar({
  collapsed,
  onToggle,
}: {
  collapsed: boolean;
  onToggle: () => void;
}) {
  const { authStatus, authLoading, authError, authInitialized, openYouTubeAuth } = useYouTubeAuth();
  const navWidth = collapsed ? 72 : 200;
  const checking = !authInitialized && !authStatus && !authError;

  return (
    <nav style={{
      width: navWidth,
      backgroundColor: '#1a1a2e',
      color: '#eee',
      display: 'flex',
      flexDirection: 'column',
      padding: '16px 0',
      transition: 'width 0.2s ease',
    }}>
      <div style={{ padding: collapsed ? '0 12px 18px' : '0 16px 24px' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: collapsed ? 'center' : 'space-between', gap: 8 }}>
          {!collapsed ? (
            <div style={{ fontSize: 18, fontWeight: 'bold' }}>
              VideoProcess
            </div>
          ) : null}
          <button
            type="button"
            onClick={onToggle}
            style={{
              width: 36,
              height: 36,
              border: '1px solid rgba(255,255,255,0.12)',
              borderRadius: 10,
              backgroundColor: 'rgba(255,255,255,0.04)',
              color: '#e2e8f0',
              cursor: 'pointer',
              fontSize: 16,
            }}
            title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            {collapsed ? '»' : '«'}
          </button>
        </div>
      </div>
      {links.map(link => (
        <NavLink
          key={link.to}
          to={link.to}
          style={({ isActive }) => ({
            display: 'flex',
            alignItems: 'center',
            justifyContent: collapsed ? 'center' : 'flex-start',
            gap: 8,
            padding: collapsed ? '12px 0' : '10px 16px',
            color: isActive ? '#fff' : '#aaa',
            backgroundColor: isActive ? '#16213e' : 'transparent',
            textDecoration: 'none',
            fontSize: 14,
          })}
          title={collapsed ? link.label : undefined}
        >
          <span>{link.icon}</span>
          {!collapsed ? <span>{link.label}</span> : null}
        </NavLink>
      ))}
      <div style={{ marginTop: 'auto', padding: collapsed ? '12px' : '16px', borderTop: '1px solid rgba(255,255,255,0.08)' }}>
        {collapsed ? (
          <div style={{ display: 'grid', gap: 8 }}>
            <div
              title={authStatus?.authenticated ? 'YouTube connected' : 'YouTube login required'}
              style={{
                width: 12,
                height: 12,
                borderRadius: 999,
                backgroundColor: authStatus?.authenticated ? '#86efac' : '#fbbf24',
                margin: '0 auto',
              }}
            />
            <button
              type="button"
              onClick={() => void openYouTubeAuth()}
              disabled={authLoading}
              title={authStatus?.authenticated ? 'Re-login YouTube' : 'Login YouTube'}
              style={{
                width: '100%',
                border: 'none',
                borderRadius: 10,
                padding: '10px 0',
                backgroundColor: '#2563eb',
                color: '#fff',
                cursor: authLoading ? 'default' : 'pointer',
                fontSize: 16,
                opacity: authLoading ? 0.7 : 1,
              }}
            >
              YT
            </button>
          </div>
        ) : (
          <>
            <div style={{ fontSize: 12, color: '#9aa4c7', marginBottom: 8 }}>
              YouTube
            </div>
            <div style={{ fontSize: 12, color: authStatus?.authenticated ? '#86efac' : '#fbbf24', marginBottom: 10 }}>
              {checking ? 'Checking...' : authStatus?.authenticated ? 'Connected' : 'Login required'}
            </div>
            <button
              type="button"
              onClick={() => void openYouTubeAuth()}
              disabled={authLoading}
              style={{
                width: '100%',
                border: 'none',
                borderRadius: 8,
                padding: '10px 12px',
                backgroundColor: '#2563eb',
                color: '#fff',
                cursor: authLoading ? 'default' : 'pointer',
                fontSize: 13,
                opacity: authLoading ? 0.7 : 1,
              }}
            >
              {authStatus?.authenticated ? 'Re-login YouTube' : 'Login YouTube'}
            </button>
            {authError ? (
              <div style={{ marginTop: 8, fontSize: 11, color: '#fca5a5' }}>
                {authError}
              </div>
            ) : null}
          </>
        )}
        {collapsed && authError ? (
          <div style={{ marginTop: 8, fontSize: 10, color: '#fca5a5', textAlign: 'center' }}>
            !
          </div>
        ) : null}
      </div>
    </nav>
  );
}
