import type { SocialEvent } from '../api'

const platformColors: Record<string, string> = {
  bluesky: '#0085ff',
  hackernews: '#ff6600',
  reddit: '#ff4500',
  rss: '#ee802f',
  mastodon: '#6364ff',
}

const sentimentColors: Record<string, string> = {
  positive: '#22c55e',
  negative: '#ef4444',
  neutral: '#6b7280',
}

function timeAgo(dateStr: string): string {
  const now = Date.now()
  const then = new Date(dateStr).getTime()
  const diff = Math.floor((now - then) / 1000)
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

export function EventCard({ event }: { event: SocialEvent }) {
  const platformColor = platformColors[event.platform] || '#888'
  const sentiment = event.enrichment?.sentiment
  const sentimentColor = sentiment ? sentimentColors[sentiment] : undefined

  return (
    <div className="event-card">
      <div className="event-meta">
        <span className="event-platform" style={{ color: platformColor }}>
          {event.platform}
        </span>
        <span className="event-type">{event.source_type}</span>
        {sentiment && (
          <span className="event-sentiment" style={{ color: sentimentColor }}>
            {sentiment}
          </span>
        )}
        <span className="event-time">{timeAgo(event.timestamps.published_at)}</span>
      </div>
      <div className="event-author">
        <strong>@{event.author.handle}</strong>
        {event.author.display_name && event.author.display_name !== event.author.handle && (
          <span className="author-name"> ({event.author.display_name})</span>
        )}
      </div>
      {event.content.title && (
        <div className="event-title">{event.content.title}</div>
      )}
      <div className="event-text">{event.content.text?.slice(0, 500)}</div>
      <div className="event-footer">
        <div className="event-engagement">
          {event.engagement.likes > 0 && <span>+{event.engagement.likes}</span>}
          {event.engagement.comments > 0 && <span>{event.engagement.comments} comments</span>}
          {event.engagement.shares > 0 && <span>{event.engagement.shares} shares</span>}
          {event.engagement.score > 0 && <span>{event.engagement.score} pts</span>}
        </div>
        <a href={event.source_url} target="_blank" rel="noopener noreferrer" className="event-link">
          View source
        </a>
      </div>
      {event.enrichment?.topics && event.enrichment.topics.length > 0 && (
        <div className="event-topics">
          {event.enrichment.topics.map(t => (
            <span key={t} className="topic-tag">{t}</span>
          ))}
        </div>
      )}
    </div>
  )
}
