import csv
import gzip
import json
import os
import random
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2


SEED = 42
random.seed(SEED)

EVENT_COUNT = 500_000

DB_HOST = os.getenv("DB_HOST", "marketplace-db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "marketplace")
DB_USER = os.getenv("DB_USER", "marketuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "marketpassword")

OUTPUT_DIR = Path("/data")

START_DATE = datetime(2026, 6, 1, 0, 0, 0)
END_DATE = datetime(2026, 9, 11, 23, 59, 59)


EVENT_TYPES = [
    "SEARCH_IMPRESSION",
    "LISTING_VIEW",
    "SAVE_VEHICLE",
    "EMAIL_LEAD",
    "PHONE_LEAD",
    "DEALER_WEBSITE_CLICK",
]

EVENT_WEIGHTS = [
    50,
    32,
    7,
    5,
    3,
    3,
]

DEVICES = [
    "MOBILE",
    "DESKTOP",
    "TABLET",
]

DEVICE_WEIGHTS = [
    65,
    30,
    5,
]


def connect():

    for attempt in range(30):

        try:
            return psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )

        except psycopg2.OperationalError:

            print(
                f"Marketplace DB not ready "
                f"(attempt {attempt + 1}/30)"
            )

            time.sleep(2)

    raise RuntimeError(
        "Could not connect to marketplace PostgreSQL"
    )


def load_listing_dealer_pairs(conn):

    print(
        "Reading listing/dealer relationships "
        "from marketplace database..."
    )

    cur = conn.cursor()

    cur.execute("""
        SELECT
            listing_id,
            dealer_id
        FROM listings;
    """)

    rows = cur.fetchall()

    cur.close()

    print(
        f"Loaded {len(rows):,} listing/dealer pairs."
    )

    return rows


def random_timestamp():

    total_seconds = int(
        (END_DATE - START_DATE).total_seconds()
    )

    random_seconds = random.randint(
        0,
        total_seconds
    )

    return START_DATE + timedelta(
        seconds=random_seconds
    )


def generate_events(listing_pairs):

    events_by_date = defaultdict(list)

    print(
        f"Generating {EVENT_COUNT:,} engagement events..."
    )

    for i in range(1, EVENT_COUNT + 1):

        listing_id, dealer_id = random.choice(
            listing_pairs
        )

        event_timestamp = random_timestamp()

        event_type = random.choices(
            EVENT_TYPES,
            weights=EVENT_WEIGHTS
        )[0]

        device = random.choices(
            DEVICES,
            weights=DEVICE_WEIGHTS
        )[0]

        session_number = random.randint(
            1,
            120_000
        )

        event = {
            "event_id": f"EV{i:09d}",
            "event_timestamp":
                event_timestamp.isoformat(),
            "listing_id": listing_id,
            "dealer_id": dealer_id,
            "session_id":
                f"SES{session_number:07d}",
            "event_type": event_type,
            "device": device,
        }

        event_date = (
            event_timestamp
            .date()
            .isoformat()
        )

        events_by_date[event_date].append(
            event
        )

        if i % 100_000 == 0:
            print(
                f"Generated {i:,} events..."
            )

    return events_by_date


def write_files(events_by_date):

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    total_written = 0

    print("Writing daily compressed JSON files...")

    for event_date, events in sorted(
        events_by_date.items()
    ):

        date_directory = (
            OUTPUT_DIR
            / f"event_date={event_date}"
        )

        date_directory.mkdir(
            parents=True,
            exist_ok=True
        )

        output_file = (
            date_directory
            / "events.jsonl.gz"
        )

        with gzip.open(
            output_file,
            "wt",
            encoding="utf-8"
        ) as file:

            for event in events:

                file.write(
                    json.dumps(event)
                    + "\n"
                )

        total_written += len(events)

    return total_written


def main():

    conn = connect()

    try:

        listing_pairs = (
            load_listing_dealer_pairs(conn)
        )

    finally:
        conn.close()

    if not listing_pairs:

        raise RuntimeError(
            "No listings found in marketplace database."
        )

    events_by_date = generate_events(
        listing_pairs
    )

    total_written = write_files(
        events_by_date
    )

    print()
    print("Engagement event generation complete.")
    print(
        f"Total events: {total_written:,}"
    )
    print(
        f"Daily partitions: "
        f"{len(events_by_date):,}"
    )


if __name__ == "__main__":
    main()