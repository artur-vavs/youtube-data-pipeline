export interface ClientInfo {
  client_id: number;
  client_name: string;
  owner_handle: string | null;
}

export interface Channel {
  channel_id: string;
  client_id: number;
  title: string;
  handle: string;
  country: string;
  published_at: string | null;
  rank: number | null;
  is_top5: boolean;
  is_owner: boolean;
  subscriber_count: number | null;
  view_count: number | null;
  video_count: number | null;
  engagement_rate: number | null;
  videos_per_week: number | null;
  delta_subscribers: number | null;
  delta_views: number | null;
  rank_delta: number | null;
  movement: string | null;
  status_top5: string | null;
}

export interface SeriesPoint {
  ingested_at: string;
  subscriber_count: number | null;
  view_count: number | null;
  rank: number | null;
}

export interface Gold {
  generated_at: string;
  clients: ClientInfo[];
  channels: Channel[];
  series: {
    snapshots: string[];
    by_channel: Record<string, SeriesPoint[]>;
  };
}
