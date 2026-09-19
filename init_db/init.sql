CREATE TABLE IF NOT EXISTS raw_stock_data (
    ticker VARCHAR(10),
    trade_timestamp TIMESTAMP,
    price NUMERIC(10, 2),
    volume INT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transformed_stock_data (
    ticker VARCHAR(10),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_price NUMERIC(10, 2),
    total_volume BIGINT,
    high_price NUMERIC(10, 2),
    low_price NUMERIC(10, 2),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);