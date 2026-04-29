const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8080';

export interface Author {
  id: string;
  handle: string;
  display_name: string;
  verified: boolean;
  follower_count: number;
}

export interface Content {
  text: string;
  title?: string;
  language: string;
  media?: { type: string; url: string; alt?: string }[];
  urls?: string[];
}

export interface Engagement {
  likes: number;
  shares: number;
  comments: number;
  views: number;
  score: number;
}

export interface SocialEvent {
  event_id: string;
  platform: string;
  source_type: string;
  source_url: string;
  author: Author;
  content: Content;
  engagement: Engagement;
  timestamps: {
    published_at: string;
    ingested_at: string;
  };
  enrichment?: {
    entities?: string[];
    sentiment?: string;
    intent?: string;
    topics?: string[];
  };
}

export interface SearchResult {
  total: number;
  events: SocialEvent[];
}

export interface Monitor {
  id: string;
  name: string;
  query: string;
  platforms: string[];
  active: boolean;
  created_at: string;
  updated_at: string;
}

export interface PipelineStats {
  running: boolean;
  started_at: string;
  ingested: number;
  duplicates: number;
  errors: number;
  connectors: Record<string, {
    healthy: boolean;
    last_event: string;
    events_total: number;
    errors_total: number;
  }>;
}

export interface SearchParams {
  q?: string;
  platforms?: string;
  sentiment?: string;
  start_date?: string;
  end_date?: string;
  sort_by?: string;
  from?: number;
  size?: number;
}

export async function searchEvents(params: SearchParams): Promise<SearchResult> {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== '') query.set(k, String(v));
  });
  const resp = await fetch(`${API_URL}/api/search?${query}`);
  return resp.json();
}

export async function getStats(): Promise<Record<string, any>> {
  const resp = await fetch(`${API_URL}/api/stats`);
  return resp.json();
}

export async function getPipelineStatus(): Promise<PipelineStats> {
  const resp = await fetch(`${API_URL}/api/pipeline/status`);
  return resp.json();
}

export async function getMonitors(): Promise<Monitor[]> {
  const resp = await fetch(`${API_URL}/api/monitors`);
  return resp.json();
}

export async function createMonitor(data: { name: string; query: string; platforms: string[] }): Promise<Monitor> {
  const resp = await fetch(`${API_URL}/api/monitors`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  return resp.json();
}

export async function deleteMonitor(id: string): Promise<void> {
  await fetch(`${API_URL}/api/monitors/${id}`, { method: 'DELETE' });
}
