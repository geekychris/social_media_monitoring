import { useEffect, useState } from 'react'
import { getMonitors, createMonitor, deleteMonitor, type Monitor } from '../api'

export function Monitors() {
  const [monitors, setMonitors] = useState<Monitor[]>([])
  const [loading, setLoading] = useState(true)
  const [showForm, setShowForm] = useState(false)
  const [name, setName] = useState('')
  const [query, setQuery] = useState('')
  const [platforms, setPlatforms] = useState('')

  useEffect(() => {
    loadMonitors()
  }, [])

  async function loadMonitors() {
    try {
      const data = await getMonitors()
      setMonitors(data)
    } catch (err) {
      console.error('Failed to load monitors:', err)
    } finally {
      setLoading(false)
    }
  }

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault()
    try {
      await createMonitor({
        name,
        query,
        platforms: platforms ? platforms.split(',').map(s => s.trim()) : [],
      })
      setName('')
      setQuery('')
      setPlatforms('')
      setShowForm(false)
      loadMonitors()
    } catch (err) {
      console.error('Failed to create monitor:', err)
    }
  }

  async function handleDelete(id: string) {
    try {
      await deleteMonitor(id)
      loadMonitors()
    } catch (err) {
      console.error('Failed to delete monitor:', err)
    }
  }

  if (loading) return <div className="loading">Loading monitors...</div>

  return (
    <div className="monitors">
      <div className="monitors-header">
        <h2>Saved Monitors</h2>
        <button className="btn-primary" onClick={() => setShowForm(!showForm)}>
          {showForm ? 'Cancel' : '+ New Monitor'}
        </button>
      </div>

      {showForm && (
        <form className="monitor-form" onSubmit={handleCreate}>
          <div className="form-field">
            <label>Name</label>
            <input
              type="text"
              value={name}
              onChange={e => setName(e.target.value)}
              placeholder="e.g. Brand Crisis Detection"
              required
            />
          </div>
          <div className="form-field">
            <label>Query (Boolean search)</label>
            <input
              type="text"
              value={query}
              onChange={e => setQuery(e.target.value)}
              placeholder='e.g. ("AcmeApp" OR "Acme") AND ("down" OR "broken")'
              required
            />
          </div>
          <div className="form-field">
            <label>Platforms (comma-separated, empty for all)</label>
            <input
              type="text"
              value={platforms}
              onChange={e => setPlatforms(e.target.value)}
              placeholder="e.g. bluesky, hackernews, reddit"
            />
          </div>
          <button type="submit" className="btn-primary">Create Monitor</button>
        </form>
      )}

      {monitors.length === 0 ? (
        <div className="empty-state">
          No monitors configured. Create one to start tracking mentions.
        </div>
      ) : (
        <div className="monitors-list">
          {monitors.map(m => (
            <div key={m.id} className="monitor-card">
              <div className="monitor-info">
                <h3>{m.name}</h3>
                <code className="monitor-query">{m.query}</code>
                {m.platforms.length > 0 && (
                  <div className="monitor-platforms">
                    {m.platforms.map(p => (
                      <span key={p} className="chip">{p}</span>
                    ))}
                  </div>
                )}
                <div className="monitor-meta">
                  Status: {m.active ? 'Active' : 'Paused'} | Created: {new Date(m.created_at).toLocaleDateString()}
                </div>
              </div>
              <button className="btn-danger" onClick={() => handleDelete(m.id)}>Delete</button>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
