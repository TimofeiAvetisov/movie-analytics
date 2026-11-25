"""Idempotent startup migrations for Kafka, Schema Registry and databases."""
import logging
from pathlib import Path

import boto3
import clickhouse_connect
import psycopg
import requests
from confluent_kafka.admin import AdminClient, NewTopic

from movie_pipeline import settings

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[1]


def main():
    admin = AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP})
    topics = admin.list_topics(timeout=20).topics
    if "movie-events" not in topics:
        future = admin.create_topics([NewTopic("movie-events", num_partitions=3, replication_factor=2,
                                               config={"min.insync.replicas": "1"})])["movie-events"]
        future.result(timeout=30)
        log.info("Created movie-events (3 partitions, replication factor 2)")
    else:
        topic = topics["movie-events"]
        if len(topic.partitions) < 3:
            raise RuntimeError("movie-events has fewer than 3 partitions")

    schema = (ROOT / "schema/movie-event-v1.avsc").read_text()
    url = f"{settings.SCHEMA_REGISTRY_URL}/subjects/movie-events-value/versions"
    response = requests.post(url, json={"schemaType": "AVRO", "schema": schema}, timeout=20)
    response.raise_for_status()
    latest = requests.get(f"{settings.SCHEMA_REGISTRY_URL}/subjects/movie-events-value/versions/latest", timeout=20)
    latest.raise_for_status()
    if latest.json()["version"] != 1 or latest.json()["id"] != response.json()["id"]:
        raise RuntimeError("Expected movie-events-value schema version 1")
    log.info("Registered movie-events-value schema version 1, id=%s", response.json()["id"])

    ch = clickhouse_connect.get_client(host=settings.CLICKHOUSE_HOST, username=settings.CLICKHOUSE_USER,
                                       password=settings.CLICKHOUSE_PASSWORD, database="default")
    for statement in (ROOT / "migrations/clickhouse.sql").read_text().split(";"):
        if statement.strip():
            ch.command(statement)
    log.info("ClickHouse migrations applied")

    with psycopg.connect(settings.POSTGRES_DSN) as conn:
        conn.execute((ROOT / "migrations/postgres.sql").read_text())
    log.info("PostgreSQL migrations applied")

    s3 = boto3.client("s3", endpoint_url=settings.S3_ENDPOINT)
    try:
        s3.head_bucket(Bucket=settings.BUCKET)
    except Exception:
        s3.create_bucket(Bucket=settings.BUCKET)
    log.info("S3 bucket ready")


if __name__ == "__main__":
    main()
