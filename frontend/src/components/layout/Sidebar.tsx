import { NavLink } from 'react-router-dom';

const links = [
  { to: '/editor', label: 'Editor', icon: '⬡' },
  { to: '/jobs', label: 'Jobs', icon: '▶' },
  { to: '/assets', label: 'Assets', icon: '📁' },
  { to: '/templates', label: 'Templates', icon: '📋' },
];

export default function Sidebar() {
  return (
    <nav style={{
      width: 200,
      backgroundColor: '#1a1a2e',
      color: '#eee',
      display: 'flex',
      flexDirection: 'column',
      padding: '16px 0',
    }}>
      <div style={{ padding: '0 16px 24px', fontSize: 18, fontWeight: 'bold' }}>
        VideoProcess
      </div>
      {links.map(link => (
        <NavLink
          key={link.to}
          to={link.to}
          style={({ isActive }) => ({
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            padding: '10px 16px',
            color: isActive ? '#fff' : '#aaa',
            backgroundColor: isActive ? '#16213e' : 'transparent',
            textDecoration: 'none',
            fontSize: 14,
          })}
        >
          <span>{link.icon}</span>
          <span>{link.label}</span>
        </NavLink>
      ))}
    </nav>
  );
}
