CREATE TABLE IF NOT EXISTS EXCHANGE_RATE.core_pairs_exchange_rate (
    `pair` String,
    `export_date` DateTime('Europe/Moscow'),
    `rate` Float64
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(export_date) 
    ORDER BY (export_date)
    SETTINGS index_granularity = 8192