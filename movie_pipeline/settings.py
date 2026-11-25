import os


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker-1:9092,broker-2:9092,broker-3:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "analytics")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://analytics:analytics@postgres:5432/analytics")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
BUCKET = "movie-analytics"
