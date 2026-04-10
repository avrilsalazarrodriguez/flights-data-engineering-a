import json
import sys

import boto3
import pandas as pd
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import Session

from postgres.models import Airline, Airport, Flight


DB_HOST = "itam-northwind-110276528929.cch6quk8gwt3.us-east-1.rds.amazonaws.com"
SECRET_ID = "itam/rds/northwind/credentials"
REGION = "us-east-1"


def get_db_creds():
    client = boto3.client("secretsmanager", region_name=REGION)
    secret = client.get_secret_value(SecretId=SECRET_ID)
    return json.loads(secret["SecretString"])


def get_engine():
    creds = get_db_creds()

    database_url = (
        f"postgresql+psycopg2://{creds['username']}:{creds['password']}"
        f"@{DB_HOST}:{creds['port']}/{creds['dbname']}"
    )

    return create_engine(database_url)


def load_airlines(session: Session):
    df = pd.read_csv("data/flights/airlines.csv")
    records = df.rename(
        columns={
            "IATA_CODE": "iata_code",
            "AIRLINE": "airline"
        }
    ).to_dict(orient="records")

    session.execute(insert(Airline), records)
    session.commit()
    print(f"Loaded {len(records)} rows into airlines")


def load_airports(session: Session):
    df = pd.read_csv("data/flights/airports.csv")
    records = df.rename(
        columns={
            "IATA_CODE": "iata_code",
            "AIRPORT": "airport",
            "CITY": "city",
            "STATE": "state"
        }
    ).to_dict(orient="records")

    session.execute(insert(Airport), records)
    session.commit()
    print(f"Loaded {len(records)} rows into airports")


def load_flights(session: Session):
    df = pd.read_csv("data/flights/flights.csv", nrows=500_000)

    keep_columns = [
        "YEAR",
        "MONTH",
        "DAY",
        "AIRLINE",
        "ORIGIN_AIRPORT",
        "DESTINATION_AIRPORT",
        "DEPARTURE_DELAY",
        "ARRIVAL_DELAY",
        "CANCELLED",
        "CANCELLATION_REASON",
        "DISTANCE",
        "AIR_SYSTEM_DELAY",
        "AIRLINE_DELAY",
        "WEATHER_DELAY",
        "LATE_AIRCRAFT_DELAY",
        "SECURITY_DELAY"
    ]

    df = df[keep_columns].rename(
        columns={
            "YEAR": "year",
            "MONTH": "month",
            "DAY": "day",
            "AIRLINE": "airline",
            "ORIGIN_AIRPORT": "origin_airport",
            "DESTINATION_AIRPORT": "destination_airport",
            "DEPARTURE_DELAY": "departure_delay",
            "ARRIVAL_DELAY": "arrival_delay",
            "CANCELLED": "cancelled",
            "CANCELLATION_REASON": "cancellation_reason",
            "DISTANCE": "distance",
            "AIR_SYSTEM_DELAY": "air_system_delay",
            "AIRLINE_DELAY": "airline_delay",
            "WEATHER_DELAY": "weather_delay",
            "LATE_AIRCRAFT_DELAY": "late_aircraft_delay",
            "SECURITY_DELAY": "security_delay"
        }
    )

    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")

    session.execute(insert(Flight), records)
    session.commit()
    print(f"Loaded {len(records)} rows into flights")


def main():
    try:
        engine = get_engine()

        with Session(engine) as session:
            load_airlines(session)
            load_airports(session)
            load_flights(session)

    except Exception as e:
        print(f"Error loading data: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()