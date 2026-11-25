import time
from datetime import datetime, timezone
from uuid import uuid4

import clickhouse_connect
import requests

from movie_pipeline import settings


def test_event_reaches_clickhouse():
    event_id = str(uuid4())
    user_id = f"e2e-{uuid4()}"
    now = datetime.now(timezone.utc)
    payload = {
        "event_id": event_id, "user_id": user_id, "movie_id": "movie-e2e",
        "event_type": "VIEW_STARTED", "timestamp": now.isoformat(),
        "device_type": "DESKTOP", "session_id": str(uuid4()), "progress_seconds": 0,
    }
    response = requests.post("http://producer:8000/events", json=payload, timeout=20)
    assert response.status_code == 201, response.text
    assert response.json() == {"event_id": event_id}

    ch = clickhouse_connect.get_client(host=settings.CLICKHOUSE_HOST,
                                       username=settings.CLICKHOUSE_USER,
                                       password=settings.CLICKHOUSE_PASSWORD,
                                       database="analytics")
    deadline = time.monotonic() + 60
    rows = []
    while time.monotonic() < deadline:
        rows = ch.query(f"""
            SELECT toString(event_id), user_id, movie_id, event_type,
                   toUnixTimestamp64Milli(timestamp), device_type, session_id, progress_seconds
            FROM raw_events WHERE event_id = toUUID('{event_id}')
        """).result_rows
        if rows:
            break
        time.sleep(1)
    assert rows == [(
        event_id, user_id, "movie-e2e", "VIEW_STARTED", int(now.timestamp() * 1000),
        "DESKTOP", payload["session_id"], 0,
    )]
