import { useEffect, useState } from 'react'
import { getPipelineStatus, type PipelineStats } from '../api'

export function PipelineStatus() {
  const [status, setStatus] = useState<PipelineStats | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      try {
        const data = await getPipelineStatus()
        setStatus(data)
      } catch (err) {
        console.error('Failed to load pipeline status:', err)
      } finally {
        setLoading(false)
      }
    }
    load()
    const interval = setInterval(load, 5000)
    return () => clearInterval(interval)
  }, [])

  if (loading) return <div className="loading">Loading pipeline status...</div>
  if (!status) return <div className="error">Failed to load pipeline status</div>

  return (
    <div className="pipeline">
      <h2>Ingestion Pipeline</h2>

      <div className="pipeline-overview">
        <div className="stat-card">
          <div className={`stat-value ${status.running ? 'text-green' : 'text-red'}`}>
            {status.running ? 'Running' : 'Stopped'}
          </div>
          <div className="stat-label">Status</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{status.ingested.toLocaleString()}</div>
          <div className="stat-label">Events Ingested</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{status.duplicates.toLocaleString()}</div>
          <div className="stat-label">Duplicates</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{status.errors.toLocaleString()}</div>
          <div className="stat-label">Errors</div>
        </div>
      </div>

      <h3>Connectors</h3>
      <div className="connectors-grid">
        {Object.entries(status.connectors || {}).map(([name, health]) => (
          <div key={name} className={`connector-card ${health.healthy ? 'healthy' : 'unhealthy'}`}>
            <div className="connector-header">
              <h4>{name}</h4>
              <span className={`status-dot ${health.healthy ? 'green' : 'red'}`} />
            </div>
            <div className="connector-stats">
              <div>Events: {health.events_total.toLocaleString()}</div>
              <div>Errors: {health.errors_total.toLocaleString()}</div>
              {health.last_event && (
                <div>Last event: {new Date(health.last_event).toLocaleTimeString()}</div>
              )}
            </div>
          </div>
        ))}
        {Object.keys(status.connectors || {}).length === 0 && (
          <div className="empty-state">No connectors configured.</div>
        )}
      </div>

      <div className="pipeline-meta">
        <p>Started: {status.started_at ? new Date(status.started_at).toLocaleString() : 'N/A'}</p>
      </div>
    </div>
  )
}
