import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import apiClient from '../api/client';
import type { Job } from '../api/types';

const STATUS_COLORS: Record<string, string> = {
  PENDING: '#6b7280', VALIDATING: '#6b7280', PLANNING: '#6b7280',
  RUNNING: '#3b82f6', SUCCEEDED: '#22c55e', FAILED: '#ef4444',
  CANCELLED: '#f59e0b', PARTIALLY_FAILED: '#f97316',
};

export default function JobsPage() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchJobs = () => {
    apiClient.get('/jobs').then(res => {
      setJobs(res.data.items);
      setLoading(false);
    }).catch(() => setLoading(false));
  };

  useEffect(() => {
    fetchJobs();
    const interval = setInterval(fetchJobs, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div style={{ padding: 24, color: '#e2e8f0', overflowY: 'auto', height: '100%' }}>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 16 }}>Jobs</h1>

      {loading ? (
        <div style={{ color: '#94a3b8' }}>Loading...</div>
      ) : jobs.length === 0 ? (
        <div style={{ color: '#94a3b8' }}>No jobs yet. Create a pipeline and run it.</div>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #334155', color: '#94a3b8', textAlign: 'left' }}>
              <th style={{ padding: '8px 12px' }}>ID</th>
              <th style={{ padding: '8px 12px' }}>Status</th>
              <th style={{ padding: '8px 12px' }}>Submitted</th>
              <th style={{ padding: '8px 12px' }}>Duration</th>
              <th style={{ padding: '8px 12px' }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map(job => (
              <tr key={job.id} style={{ borderBottom: '1px solid #1e293b' }}>
                <td style={{ padding: '8px 12px' }}>
                  <Link to={`/jobs/${job.id}`} style={{ color: '#3b82f6', textDecoration: 'none' }}>
                    {job.id.slice(0, 8)}...
                  </Link>
                </td>
                <td style={{ padding: '8px 12px' }}>
                  <span style={{
                    color: STATUS_COLORS[job.status] || '#6b7280',
                    fontWeight: 600,
                  }}>
                    {job.status}
                  </span>
                </td>
                <td style={{ padding: '8px 12px', color: '#94a3b8' }}>
                  {new Date(job.submitted_at).toLocaleString()}
                </td>
                <td style={{ padding: '8px 12px', color: '#94a3b8' }}>
                  {job.completed_at && job.started_at
                    ? `${((new Date(job.completed_at).getTime() - new Date(job.started_at).getTime()) / 1000).toFixed(1)}s`
                    : job.status === 'RUNNING' ? 'running...' : '-'}
                </td>
                <td style={{ padding: '8px 12px' }}>
                  <Link to={`/jobs/${job.id}`}
                    style={{ color: '#3b82f6', textDecoration: 'none', marginRight: 12 }}>
                    View
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
