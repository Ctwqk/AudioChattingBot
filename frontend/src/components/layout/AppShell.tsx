import { useEffect, useState } from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar from './Sidebar';
import { useYouTubeAuth } from '../../hooks/useYouTubeAuth';

export default function AppShell() {
  const {
    authStatus,
    authLoading,
    authError,
    authInitialized,
    openYouTubeAuth,
    logoutYouTubeAuth,
    refreshAuthStatus,
  } = useYouTubeAuth();

  const connected = authStatus?.authenticated;
  const checking = !authInitialized && !authStatus && !authError;
  const statusColor = connected ? '#16a34a' : '#d97706';
  const statusText = checking
    ? 'Checking YouTube login'
    : connected
      ? 'YouTube connected'
      : 'YouTube login required';
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => {
    if (typeof window === 'undefined') return false;
    return window.localStorage.getItem('vp_sidebar_collapsed') === 'true';
  });
  const [youtubePanelCollapsed, setYouTubePanelCollapsed] = useState(() => {
    if (typeof window === 'undefined') return true;
    const saved = window.localStorage.getItem('vp_youtube_panel_collapsed');
    return saved === null ? true : saved === 'true';
  });
  const quota = authStatus?.quota_estimate;
  const quotaText = quota
    ? `${quota.estimated_units_used} / ${quota.daily_limit} units today`
    : 'Quota estimate unavailable';
  const quotaRemaining = quota
    ? `${quota.estimated_units_remaining} units remaining`
    : null;

  useEffect(() => {
    window.localStorage.setItem('vp_sidebar_collapsed', String(sidebarCollapsed));
  }, [sidebarCollapsed]);

  useEffect(() => {
    window.localStorage.setItem('vp_youtube_panel_collapsed', String(youtubePanelCollapsed));
  }, [youtubePanelCollapsed]);

  useEffect(() => {
    if (authError) {
      setYouTubePanelCollapsed(false);
    }
  }, [authError]);

  return (
    <div style={{ display: 'flex', height: '100vh' }}>
      <Sidebar
        collapsed={sidebarCollapsed}
        onToggle={() => setSidebarCollapsed(prev => !prev)}
      />
      <main style={{ flex: 1, overflow: 'hidden', display: 'flex', flexDirection: 'column', position: 'relative' }}>
        <div style={{ flex: 1, overflow: 'hidden' }}>
          <Outlet />
        </div>
        <div style={{ position: 'absolute', top: 16, right: 16, zIndex: 20 }}>
          {youtubePanelCollapsed ? (
            <button
              type="button"
              onClick={() => setYouTubePanelCollapsed(false)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 10,
                border: '1px solid rgba(15, 23, 42, 0.08)',
                backgroundColor: 'rgba(255,255,255,0.92)',
                color: '#0f172a',
                borderRadius: 999,
                padding: '10px 14px',
                boxShadow: '0 14px 40px rgba(15,23,42,0.12)',
                backdropFilter: 'blur(12px)',
                cursor: 'pointer',
              }}
              title="Open YouTube status"
            >
              <span
                style={{
                  width: 10,
                  height: 10,
                  borderRadius: 999,
                  backgroundColor: statusColor,
                  boxShadow: `0 0 0 4px ${connected ? 'rgba(22,163,74,0.12)' : 'rgba(217,119,6,0.12)'}`,
                }}
              />
              <span style={{ fontSize: 13, fontWeight: 700 }}>
                {checking ? 'YouTube checking' : connected ? 'YouTube connected' : 'YouTube login'}
              </span>
            </button>
          ) : (
            <div
              style={{
                width: 320,
                borderRadius: 18,
                border: '1px solid rgba(148,163,184,0.18)',
                background: 'rgba(255,255,255,0.96)',
                boxShadow: '0 24px 60px rgba(15,23,42,0.18)',
                backdropFilter: 'blur(18px)',
                overflow: 'hidden',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '14px 16px 10px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <div
                    style={{
                      width: 12,
                      height: 12,
                      borderRadius: 999,
                      backgroundColor: statusColor,
                      boxShadow: `0 0 0 5px ${connected ? 'rgba(22,163,74,0.12)' : 'rgba(217,119,6,0.12)'}`,
                    }}
                  />
                  <div style={{ fontSize: 14, fontWeight: 700, color: '#0f172a' }}>{statusText}</div>
                </div>
                <button
                  type="button"
                  onClick={() => setYouTubePanelCollapsed(true)}
                  style={{
                    width: 30,
                    height: 30,
                    borderRadius: 999,
                    border: '1px solid rgba(148,163,184,0.22)',
                    backgroundColor: '#fff',
                    color: '#475569',
                    cursor: 'pointer',
                    fontSize: 14,
                  }}
                  title="Minimize"
                >
                  ×
                </button>
              </div>
              <div style={{ padding: '0 16px 14px' }}>
                <div style={{ fontSize: 12, color: '#64748b' }}>
                  {checking
                    ? 'Checking current token and auth service status.'
                    : connected
                    ? 'Uploads can run without reauthorizing.'
                    : 'Login is required before any youtube_upload node can succeed.'}
                </div>
                <div style={{ fontSize: 12, color: '#334155', marginTop: 8 }}>
                  {quotaText}
                  {quotaRemaining ? ` · ${quotaRemaining}` : ''}
                </div>
                <div style={{ fontSize: 11, color: '#64748b', marginTop: 4 }}>
                  {quota?.search_uses_official_quota === false
                    ? 'yt-dlp search does not consume official YouTube Data API quota.'
                    : 'Quota estimate is based on local tracking.'}
                </div>
                {authError ? (
                  <div style={{ fontSize: 12, color: '#b91c1c', marginTop: 8 }}>{authError}</div>
                ) : null}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap', marginTop: 12 }}>
                  <button
                    type="button"
                    onClick={() => void refreshAuthStatus()}
                    disabled={authLoading}
                    style={{
                      border: '1px solid #cbd5e1',
                      backgroundColor: '#fff',
                      color: '#334155',
                      borderRadius: 10,
                      padding: '8px 11px',
                      fontSize: 12,
                      cursor: authLoading ? 'default' : 'pointer',
                    }}
                  >
                    Refresh
                  </button>
                  {connected ? (
                    <button
                      type="button"
                      onClick={() => void logoutYouTubeAuth()}
                      disabled={authLoading}
                      style={{
                        border: '1px solid #fecaca',
                        backgroundColor: '#fff1f2',
                        color: '#b91c1c',
                        borderRadius: 10,
                        padding: '8px 11px',
                        fontSize: 12,
                        cursor: authLoading ? 'default' : 'pointer',
                      }}
                    >
                      Disconnect
                    </button>
                  ) : null}
                  <button
                    type="button"
                    onClick={() => void openYouTubeAuth()}
                    disabled={authLoading}
                    style={{
                      border: 'none',
                      backgroundColor: '#2563eb',
                      color: '#fff',
                      borderRadius: 10,
                      padding: '9px 12px',
                      fontSize: 12,
                      cursor: authLoading ? 'default' : 'pointer',
                      opacity: authLoading ? 0.7 : 1,
                    }}
                  >
                    {connected ? 'Re-login YouTube' : 'Login YouTube'}
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
