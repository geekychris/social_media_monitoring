import { useEffect, useState } from 'react'
import { getStats, getPipelineStatus, searchEvents, type SocialEvent } from '../api'

export function Dashboard() {
  const [stats, setStats] = useState<Record<string, any>>({})
  const [pipeline, setPipeline] = useState<any>(null)
  const [recentEvents, setRecentEvents] = useState<SocialEvent[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      try {
        const [s, p, recent] = await Promise.all([
          getStats(),
          getPipelineStatus(),
          searchEvents({ sort_by: 'date', size: 5 }),
        ])
        setStats(s)
        setPipeline(p)
        setRecentEvents(recent.events || [])
      } catch (err) {
        console.error('Dashboard load failed:', err)
      } finally {
        setLoading(false)
      }
    }
    load()
    const interval = setInterval(load, 15000)
    return () => clearInterval(interval)
  }, [])

  if (loading) return <div className="loading">Loading dashboard...</div>

  const platforms = (stats.platforms || {}) as Record<string, number>
  const sentiments = (stats.sentiment || {}) as Record<string, number>

  return (
    <div className="dashboard">
      <div className="dashboard-grid">
        <div className="stat-card">
          <div className="stat-value">{(stats.total_events || 0).toLocaleString()}</div>
          <div className="stat-label">Total Events Indexed</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{pipeline?.ingested?.toLocaleString() || 0}</div>
          <div className="stat-label">Ingested This Session</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{pipeline?.duplicates?.toLocaleString() || 0}</div>
          <div className="stat-label">Duplicates Filtered</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{pipeline?.errors?.toLocaleString() || 0}</div>
          <div className="stat-label">Errors</div>
        </div>
      </div>

      <div className="dashboard-panels">
        <div className="panel">
          <h3>Platform Breakdown</h3>
          {Object.keys(platforms).length > 0 ? (
            <div className="bar-chart">
              {Object.entries(platforms)
                .sort(([, a], [, b]) => b - a)
                .map(([name, count]) => (
                  <div key={name} className="bar-row">
                    <span className="bar-label">{name}</span>
                    <div className="bar-track">
                      <div
                        className="bar-fill"
                        style={{
                          width: `${(count / Math.max(...Object.values(platforms))) * 100}%`,
                        }}
                      />
                    </div>
                    <span className="bar-value">{count.toLocaleString()}</span>
                  </div>
                ))}
            </div>
          ) : (
            <p className="empty-state">No data yet. Waiting for events...</p>
          )}
        </div>

        <div className="panel">
          <h3>Sentiment Distribution</h3>
          {Object.keys(sentiments).length > 0 ? (
            <div className="sentiment-grid">
              {Object.entries(sentiments).map(([name, count]) => (
                <div key={name} className={`sentiment-card sentiment-${name}`}>
                  <div className="sentiment-value">{count.toLocaleString()}</div>
                  <div className="sentiment-label">{name}</div>
                </div>
              ))}
            </div>
          ) : (
            <p className="empty-state">No sentiment data yet.</p>
          )}
        </div>
      </div>

      <div className="panel">
        <h3>Recent Events</h3>
        {recentEvents.length > 0 ? (
          <div className="recent-list">
            {recentEvents.map(event => (
              <div key={event.event_id} className="recent-item">
                <span className="recent-platform">{event.platform}</span>
                <span className="recent-author">@{event.author.handle}</span>
                <span className="recent-text">{event.content.text?.slice(0, 120)}...</span>
                <a href={event.source_url} target="_blank" rel="noopener noreferrer">link</a>
              </div>
            ))}
          </div>
        ) : (
          <p className="empty-state">No events ingested yet.</p>
        )}
      </div>
    </div>
  )
}
