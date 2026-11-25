import io
import json
import logging
import os
import random
import struct
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from uuid import UUID, uuid4

import fastavro
import requests
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator, model_validator

from movie_pipeline import settings

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
VIEW_EVENTS = {"VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED"}


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    event_id: UUID
    user_id: str = Field(min_length=1)
    movie_id: str = Field(min_length=1)
    event_type: EventType
    timestamp: datetime
    device_type: DeviceType
    session_id: str = Field(min_length=1)
    progress_seconds: int | None = Field(default=None, ge=0)

    @field_validator("timestamp")
    @classmethod
    def utc_timestamp(cls, value):
        if value.tzinfo is None or value.utcoffset().total_seconds() != 0:
            raise ValueError("timestamp must include the UTC timezone")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_progress(self):
        if self.event_type.value in VIEW_EVENTS and self.progress_seconds is None:
            raise ValueError("progress_seconds is required for viewing events")
        if self.event_type.value not in VIEW_EVENTS and self.progress_seconds is not None:
            raise ValueError("progress_seconds is only allowed for viewing events")
        return self

    def avro_record(self):
        return {
            "event_id": str(self.event_id),
            "user_id": self.user_id,
            "movie_id": self.movie_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,
            "device_type": self.device_type.value,
            "session_id": self.session_id,
            "progress_seconds": self.progress_seconds,
        }


class MoviePublisher:
    def __init__(self):
        schema_text = (Path(__file__).resolve().parents[1] / "schema/movie-event-v1.avsc").read_text()
        self.schema = fastavro.parse_schema(json.loads(schema_text))
        response = requests.get(f"{settings.SCHEMA_REGISTRY_URL}/subjects/movie-events-value/versions/latest", timeout=10)
        response.raise_for_status()
        self.schema_id = response.json()["id"]
        self.producer = Producer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
        })

    def publish(self, event: MovieEvent):
        record = event.avro_record()
        fastavro.validation.validate(record, self.schema, raise_errors=True)
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, self.schema, record)
        payload = b"\x00" + struct.pack(">I", self.schema_id) + buffer.getvalue()

        for attempt in range(5):
            error = []

            def delivered(err, _message):
                if err is not None:
                    error.append(err)

            try:
                self.producer.produce("movie-events", key=event.user_id.encode(), value=payload,
                                      on_delivery=delivered)
                remaining = self.producer.flush(10)
                if remaining or error:
                    raise RuntimeError(f"Kafka delivery failed: {error or 'timeout'}")
                log.info("published event_id=%s event_type=%s timestamp=%s", event.event_id,
                         event.event_type.value, event.timestamp.isoformat())
                return
            except Exception:
                log.exception("Kafka publish attempt %s failed for event_id=%s", attempt + 1, event.event_id)
                if attempt == 4:
                    raise
                time.sleep(min(2 ** attempt, 8))


def generate_session(publisher: MoviePublisher):
    user_id = f"user-{random.randint(1, 100)}"
    movie_id = f"movie-{random.randint(1, 20)}"
    session_id = str(uuid4())
    device_type = random.choice(list(DeviceType))
    now = datetime.now(timezone.utc)
    for event_type, progress, timestamp in [
        (EventType.VIEW_STARTED, 0, now - timedelta(minutes=45)),
        (EventType.VIEW_PAUSED, 120, now - timedelta(minutes=43)),
        (EventType.VIEW_RESUMED, 130, now - timedelta(minutes=42)),
        (EventType.VIEW_FINISHED, random.randint(1800, 2700), now),
    ]:
        publisher.publish(MovieEvent(event_id=uuid4(), user_id=user_id, movie_id=movie_id,
                                     event_type=event_type, timestamp=timestamp, device_type=device_type,
                                     session_id=session_id, progress_seconds=progress))
    if random.random() < 0.5:
        publisher.publish(MovieEvent(event_id=uuid4(), user_id=user_id, movie_id=movie_id,
                                     event_type=EventType.LIKED, timestamp=now, device_type=device_type,
                                     session_id=session_id))
    if random.random() < 0.5:
        publisher.publish(MovieEvent(event_id=uuid4(), user_id=user_id, movie_id=movie_id,
                                     event_type=EventType.SEARCHED, timestamp=now, device_type=device_type,
                                     session_id=session_id))


def generator_loop(publisher, stop):
    interval = float(os.getenv("GENERATOR_INTERVAL_SECONDS", "5"))
    while not stop.is_set():
        try:
            generate_session(publisher)
        except Exception:
            log.exception("Event generation failed")
        stop.wait(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.publisher = MoviePublisher()
    stop = threading.Event()
    worker = None
    if os.getenv("GENERATE_EVENTS", "0") == "1":
        worker = threading.Thread(target=generator_loop, args=(app.state.publisher, stop), daemon=True)
        worker.start()
    yield
    stop.set()
    if worker:
        worker.join(timeout=2)


app = FastAPI(title="Movie event producer", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events", status_code=201)
def publish_event(event: MovieEvent):
    try:
        app.state.publisher.publish(event)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Kafka publication failed") from exc
    return {"event_id": str(event.event_id)}
