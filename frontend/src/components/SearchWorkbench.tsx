import { useState, useCallback } from 'react'
import { searchEvents, type SocialEvent, type SearchParams } from '../api'
import { EventCard } from './EventCard'

const PLATFORMS = ['bluesky', 'hackernews', 'reddit', 'rss', 'mastodon']
const SENTIMENTS = ['positive', 'neutral', 'negative']

export function SearchWorkbench() {
  const [query, setQuery] = useState('')
  const [platforms, setPlatforms] = useState<string[]>([])
  const [sentiment, setSentiment] = useState('')
  const [sortBy, setSortBy] = useState('relevance')
  const [events, setEvents] = useState<SocialEvent[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [page, setPage] = useState(0)
  const pageSize = 20

  const doSearch = useCallback(async (pageNum = 0) => {
    setLoading(true)
    try {
      const params: SearchParams = {
        q: query,
        platforms: platforms.join(','),
        sentiment,
        sort_by: sortBy,
        from: pageNum * pageSize,
        size: pageSize,
      }
      const result = await searchEvents(params)
      setEvents(result.events || [])
      setTotal(result.total)
      setPage(pageNum)
    } catch (err) {
      console.error('Search failed:', err)
    } finally {
      setLoading(false)
    }
  }, [query, platforms, sentiment, sortBy])

  const togglePlatform = (p: string) => {
    setPlatforms(prev =>
      prev.includes(p) ? prev.filter(x => x !== p) : [...prev, p]
    )
  }

  return (
    <div className="search-workbench">
      <div className="search-bar">
        <input
          type="text"
          className="search-input"
          placeholder="Search posts... (e.g. login broken, outage, can't sign in)"
          value={query}
          onChange={e => setQuery(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && doSearch(0)}
        />
        <button className="search-btn" onClick={() => doSearch(0)} disabled={loading}>
          {loading ? 'Searching...' : 'Search'}
        </button>
      </div>

      <div className="search-filters">
        <div className="filter-group">
          <label>Platforms</label>
          <div className="filter-chips">
            {PLATFORMS.map(p => (
              <button
                key={p}
                className={`chip ${platforms.includes(p) ? 'active' : ''}`}
                onClick={() => togglePlatform(p)}
              >
                {p}
              </button>
            ))}
          </div>
        </div>
        <div className="filter-group">
          <label>Sentiment</label>
          <select value={sentiment} onChange={e => setSentiment(e.target.value)}>
            <option value="">All</option>
            {SENTIMENTS.map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
        <div className="filter-group">
          <label>Sort by</label>
          <select value={sortBy} onChange={e => setSortBy(e.target.value)}>
            <option value="relevance">Relevance</option>
            <option value="date">Date</option>
            <option value="engagement">Engagement</option>
          </select>
        </div>
      </div>

      <div className="search-results">
        {total > 0 && (
          <div className="results-count">{total.toLocaleString()} results found</div>
        )}
        {events.map(event => (
          <EventCard key={event.event_id} event={event} />
        ))}
        {events.length === 0 && !loading && (
          <div className="empty-state">
            Enter a search query to find social media posts across all connected platforms.
          </div>
        )}
        {total > pageSize && (
          <div className="pagination">
            <button disabled={page === 0} onClick={() => doSearch(page - 1)}>Previous</button>
            <span>Page {page + 1} of {Math.ceil(total / pageSize)}</span>
            <button disabled={(page + 1) * pageSize >= total} onClick={() => doSearch(page + 1)}>Next</button>
          </div>
        )}
      </div>
    </div>
  )
}
