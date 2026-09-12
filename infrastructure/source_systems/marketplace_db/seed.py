import os
import random
import time
from datetime import date, datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker


SEED = 42
random.seed(SEED)
Faker.seed(SEED)

fake = Faker()

VEHICLE_COUNT = 100_000
LISTING_COUNT = 150_000
DEALER_COUNT = 5_000

DB_HOST = os.getenv("DB_HOST", "marketplace-db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "marketplace")
DB_USER = os.getenv("DB_USER", "marketuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "marketpassword")


MAKES_MODELS = {
    "BMW": ["1 Series", "3 Series", "5 Series", "X1", "X3", "X5"],
    "Mercedes-Benz": ["A-Class", "C-Class", "E-Class", "GLA", "GLC"],
    "Audi": ["A3", "A4", "A6", "Q3", "Q5"],
    "Volkswagen": ["Golf", "Passat", "Tiguan", "Polo", "T-Roc"],
    "Toyota": ["Corolla", "Yaris", "RAV4", "C-HR"],
    "Skoda": ["Octavia", "Superb", "Kodiaq", "Kamiq"],
    "Ford": ["Focus", "Kuga", "Puma", "Mustang"],
    "Tesla": ["Model 3", "Model Y", "Model S"],
    "Volvo": ["XC40", "XC60", "XC90", "V60"],
}

FUEL_TYPES = [
    "PETROL",
    "DIESEL",
    "HYBRID",
    "PLUG_IN_HYBRID",
    "ELECTRIC",
]

BODY_TYPES = [
    "SUV",
    "SEDAN",
    "HATCHBACK",
    "WAGON",
    "COUPE",
]

VISIBILITY_PACKAGES = [
    "STANDARD",
    "PREMIUM_PLACEMENT",
    "VISIBILITY_BOOST",
    "SOCIAL_BOOST",
]


def random_date(start, end):
    days = (end - start).days
    return start + timedelta(days=random.randint(0, days))


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
                f"Database not ready "
                f"(attempt {attempt + 1}/30)"
            )
            time.sleep(2)

    raise RuntimeError("Could not connect to PostgreSQL")


def create_schema(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vehicles (
            vehicle_id VARCHAR(20) PRIMARY KEY,
            make VARCHAR(50) NOT NULL,
            model VARCHAR(100) NOT NULL,
            model_year INTEGER NOT NULL,
            fuel_type VARCHAR(30),
            body_type VARCHAR(30),
            mileage_km INTEGER,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            listing_id VARCHAR(20) PRIMARY KEY,
            vehicle_id VARCHAR(20) NOT NULL,
            dealer_id VARCHAR(20) NOT NULL,
            listing_start_date DATE NOT NULL,
            listing_end_date DATE,
            asking_price_eur NUMERIC(12, 2),
            listing_status VARCHAR(20),
            visibility_package VARCHAR(40),
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (vehicle_id)
                REFERENCES vehicles(vehicle_id)
        );
    """)


def create_vehicles():
    rows = []

    makes = list(MAKES_MODELS.keys())

    for i in range(1, VEHICLE_COUNT + 1):
        make = random.choice(makes)
        model = random.choice(MAKES_MODELS[make])

        model_year = random.randint(2015, 2026)

        created = random_date(
            date(2024, 1, 1),
            date(2026, 9, 11)
        )

        updated = random_date(
            created,
            date(2026, 9, 11)
        )

        rows.append((
            f"V{i:07d}",
            make,
            model,
            model_year,
            random.choice(FUEL_TYPES),
            random.choice(BODY_TYPES),
            random.randint(0, 220_000),
            datetime.combine(created, datetime.min.time()),
            datetime.combine(updated, datetime.min.time()),
        ))

    return rows


def create_listings():
    rows = []

    for i in range(1, LISTING_COUNT + 1):

        vehicle_number = random.randint(
            1,
            VEHICLE_COUNT
        )

        dealer_number = random.randint(
            1,
            DEALER_COUNT
        )

        start_date = random_date(
            date(2025, 1, 1),
            date(2026, 9, 11)
        )

        status = random.choices(
            ["ACTIVE", "SOLD", "EXPIRED", "REMOVED"],
            weights=[45, 35, 15, 5]
        )[0]

        if status == "ACTIVE":
            end_date = None
        else:
            end_date = start_date + timedelta(
                days=random.randint(2, 180)
            )

        created_at = datetime.combine(
            start_date,
            datetime.min.time()
        )

        updated_at = datetime.combine(
            random_date(
                start_date,
                date(2026, 9, 11)
            ),
            datetime.min.time()
        )

        rows.append((
            f"L{i:07d}",
            f"V{vehicle_number:07d}",
            f"D{dealer_number:06d}",
            start_date,
            end_date,
            round(
                random.uniform(4_000, 95_000),
                2
            ),
            status,
            random.choices(
                VISIBILITY_PACKAGES,
                weights=[60, 20, 12, 8]
            )[0],
            created_at,
            updated_at,
        ))

    return rows


def main():

    print("Connecting to marketplace PostgreSQL...")

    conn = connect()

    try:
        cur = conn.cursor()

        print("Creating tables...")
        create_schema(cur)
        conn.commit()

        # Makes this seeding process repeatable.
        print("Clearing existing demo data...")
        cur.execute("TRUNCATE listings, vehicles;")
        conn.commit()

        print(
            f"Generating {VEHICLE_COUNT:,} vehicles..."
        )
        vehicles = create_vehicles()

        print("Loading vehicles into PostgreSQL...")

        execute_values(
            cur,
            """
            INSERT INTO vehicles (
                vehicle_id,
                make,
                model,
                model_year,
                fuel_type,
                body_type,
                mileage_km,
                created_at,
                updated_at
            )
            VALUES %s
            """,
            vehicles,
            page_size=5000,
        )

        conn.commit()

        print(
            f"Generating {LISTING_COUNT:,} listings..."
        )
        listings = create_listings()

        print("Loading listings into PostgreSQL...")

        execute_values(
            cur,
            """
            INSERT INTO listings (
                listing_id,
                vehicle_id,
                dealer_id,
                listing_start_date,
                listing_end_date,
                asking_price_eur,
                listing_status,
                visibility_package,
                created_at,
                updated_at
            )
            VALUES %s
            """,
            listings,
            page_size=5000,
        )

        conn.commit()

        print("Creating indexes...")

        cur.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_listings_dealer
            ON listings(dealer_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_listings_updated
            ON listings(updated_at);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_listings_vehicle
            ON listings(vehicle_id);
        """)

        conn.commit()

        cur.execute(
            "SELECT COUNT(*) FROM vehicles;"
        )

        vehicle_count = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM listings;"
        )

        listing_count = cur.fetchone()[0]

        print()
        print("Marketplace database ready.")
        print(
            f"Vehicles: {vehicle_count:,}"
        )
        print(
            f"Listings: {listing_count:,}"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()