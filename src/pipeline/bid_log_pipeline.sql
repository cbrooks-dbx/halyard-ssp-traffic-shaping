-- Halyard Exchange bid-log pipeline (Lakeflow Spark Declarative Pipelines, serverless)
--
-- raw_landing/auctions/*.json.gz --Auto Loader--> bronze_auctions
--   -> silver_auctions   (typed, expectations drop bad rows)
--   -> silver_dsp_calls  (deduped replays, one row per outbound DSP call, joined to partner/publisher dims)
--   -> gold_segment_daily / gold_dsp_daily / gold_exchange_daily  (what ML, Genie and the app read)

-- ---------------------------------------------------------------- reference data
CREATE OR REFRESH MATERIALIZED VIEW dim_dsp_partners
COMMENT 'DSP partners Halyard sends bid requests to, with contracted QPS caps'
AS SELECT * FROM read_files('${source_path}/reference/dsp_partners.csv', format => 'csv', header => true, inferSchema => true);

CREATE OR REFRESH MATERIALIZED VIEW dim_publishers
COMMENT 'Publishers (supply) selling inventory through Halyard'
AS SELECT * FROM read_files('${source_path}/reference/publishers.csv', format => 'csv', header => true, inferSchema => true);

-- ---------------------------------------------------------------- bronze
CREATE OR REFRESH STREAMING TABLE bronze_auctions
COMMENT 'Raw auction logs exactly as emitted by the bidder fleet, incrementally ingested with Auto Loader'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path AS _source_file,
  current_timestamp() AS _ingested_at
FROM STREAM read_files(
  '${source_path}/auctions/',
  format => 'json',
  schema => 'auction_id STRING, event_ts STRING, publisher_id STRING, ad_format STRING, device_type STRING, os STRING,
             geo_country STRING, supply_path STRING, floor_cpm DOUBLE, user_id_present BOOLEAN,
             winning_dsp_id STRING, clearing_cpm DOUBLE,
             dsp_calls ARRAY<STRUCT<dsp_id: STRING, latency_ms: INT, timed_out: BOOLEAN, bid: BOOLEAN, bid_cpm: DOUBLE, won: BOOLEAN>>'
);

-- ---------------------------------------------------------------- silver
CREATE OR REFRESH STREAMING TABLE silver_auctions (
  CONSTRAINT valid_auction_id EXPECT (auction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_publisher  EXPECT (publisher_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_floor      EXPECT (floor_cpm >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp  EXPECT (event_ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_dsp_calls    EXPECT (size(dsp_calls) > 0) ON VIOLATION DROP ROW
)
COMMENT 'Typed, validated auctions. Rows failing expectations are dropped and counted in the pipeline event log'
TBLPROPERTIES ('quality' = 'silver')
AS SELECT
  auction_id,
  try_to_timestamp(event_ts) AS event_ts,
  publisher_id, ad_format, device_type, os, geo_country, supply_path,
  floor_cpm, user_id_present, winning_dsp_id, clearing_cpm, dsp_calls,
  _source_file, _ingested_at
FROM STREAM(bronze_auctions);

CREATE OR REFRESH MATERIALIZED VIEW silver_dsp_calls (
  CONSTRAINT known_dsp       EXPECT (dsp_name IS NOT NULL),
  CONSTRAINT known_publisher EXPECT (publisher_name IS NOT NULL)
)
COMMENT 'One row per outbound bid request (auction x DSP). Replayed auctions are deduplicated. This is the grain traffic shaping acts on'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH dedup AS (
  SELECT * FROM silver_auctions
  QUALIFY row_number() OVER (PARTITION BY auction_id ORDER BY _ingested_at) = 1
),
exploded AS (
  SELECT dedup.*, explode(dsp_calls) AS c FROM dedup
)
SELECT
  a.auction_id,
  a.event_ts,
  to_date(a.event_ts) AS event_date,
  hour(a.event_ts) AS event_hour,
  a.publisher_id, p.publisher_name, p.vertical, p.inventory_type, p.ads_txt_status,
  a.ad_format, a.device_type, a.os, a.geo_country, a.supply_path, a.floor_cpm, a.user_id_present,
  c.dsp_id, d.dsp_name,
  c.latency_ms, c.timed_out, c.bid, c.bid_cpm, c.won,
  CASE WHEN c.won THEN a.clearing_cpm END AS clearing_cpm,
  CASE WHEN c.won THEN a.clearing_cpm / 1000.0 ELSE 0.0 END AS gross_spend_usd,
  CASE WHEN c.won THEN a.clearing_cpm / 1000.0 * 0.15 ELSE 0.0 END AS net_revenue_usd
FROM exploded a
LEFT JOIN dim_dsp_partners d ON c.dsp_id = d.dsp_id
LEFT JOIN dim_publishers p ON a.publisher_id = p.publisher_id;

-- ---------------------------------------------------------------- gold
CREATE OR REFRESH MATERIALIZED VIEW gold_segment_daily
COMMENT 'Daily outbound call volume, bids, wins and revenue per traffic segment (DSP x publisher x format x device x country x supply path)'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  event_date, dsp_id, dsp_name, publisher_id, publisher_name, vertical,
  ad_format, device_type, geo_country, supply_path,
  count(*) AS outbound_calls,
  sum(CAST(bid AS INT)) AS bids,
  sum(CAST(won AS INT)) AS wins,
  sum(CAST(timed_out AS INT)) AS timeouts,
  round(avg(CAST(bid AS INT)), 5) AS bid_rate,
  round(sum(gross_spend_usd), 4) AS gross_spend_usd,
  round(sum(net_revenue_usd), 4) AS net_revenue_usd
FROM silver_dsp_calls
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW gold_dsp_daily
COMMENT 'Daily partner scorecard: how much traffic each DSP receives vs how often it bids, wins and pays'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  event_date, dsp_id, dsp_name,
  count(*) AS outbound_calls,
  sum(CAST(bid AS INT)) AS bids,
  sum(CAST(won AS INT)) AS wins,
  round(avg(CAST(bid AS INT)), 5) AS bid_rate,
  round(avg(CAST(timed_out AS INT)), 5) AS timeout_rate,
  round(percentile_approx(latency_ms, 0.95), 0) AS p95_latency_ms,
  round(sum(gross_spend_usd), 4) AS gross_spend_usd,
  round(sum(net_revenue_usd), 4) AS net_revenue_usd
FROM silver_dsp_calls
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW gold_exchange_daily
COMMENT 'Exchange-level daily KPIs: auctions, fan-out, bid rate, fill rate, revenue'
TBLPROPERTIES ('quality' = 'gold')
AS SELECT
  event_date,
  count(DISTINCT auction_id) AS auctions,
  count(*) AS outbound_calls,
  round(count(*) / count(DISTINCT auction_id), 2) AS calls_per_auction,
  round(avg(CAST(bid AS INT)), 5) AS bid_rate,
  round(count(DISTINCT CASE WHEN won THEN auction_id END) / count(DISTINCT auction_id), 4) AS fill_rate,
  round(sum(gross_spend_usd), 2) AS gross_spend_usd,
  round(sum(net_revenue_usd), 2) AS net_revenue_usd
FROM silver_dsp_calls
GROUP BY event_date;
