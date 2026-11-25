CREATE TABLE IF NOT EXISTS daily_metrics (
    date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    dimension TEXT NOT NULL DEFAULT '',
    value DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (date, metric_name, dimension)
);
