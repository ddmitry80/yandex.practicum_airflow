CREATE TABLE IF NOT EXISTS EXCHANGE_RATE.stg_pairs_exchange_rate (
    `pair` Nullable(String),
    `export_date` Nullable(String),
    `rate` Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY tuple()
    SETTINGS index_granularity = 8192