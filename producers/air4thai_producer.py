"""
air4thai_producer.py — Pulls Air4Thai API and produces to Kafka.
Topic: air-quality-raw (3 partitions, key=station_id)
Run: python -m producers.air4thai_producer
"""

import logging
import requests
from producers.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    AIR4THAI_API_URL,
)
from producers.common.kafka_utils import (
    get_producer,
    produce_message,
    flush_producer,
)

logger = logging.getLogger(__name__)

TOPIC = "air-quality-raw"


def fetch_air4thai() -> list[dict]:
    """
    Fetch latest AQI readings from Air4Thai API.

    Returns:
        List of station dicts from the API response
    """
    response = requests.get(AIR4THAI_API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    stations = data.get("stations", [])
    logger.info(f"Fetched {len(stations)} stations from Air4Thai")
    return stations


def parse_station(station: dict) -> dict:
    """
    Flatten a station dict into a Bronze-ready record.
    Keeps raw values — no null handling here (that's Silver's job).

    Args:
        station: Raw station dict from Air4Thai API

    Returns:
        Flattened dict matching Bronze schema
    """
    aqi_last = station.get("AQILast", {})

    def safe_float(val: str) -> float | None:
        try:
            f = float(val)
            return None if f < 0 else f
        except (TypeError, ValueError):
            return None

    def safe_int(val: str) -> int | None:
        try:
            i = int(val)
            return None if i < 0 else i
        except (TypeError, ValueError):
            return None

    return {
        "station_id": station.get("stationID"),
        "station_name_en": station.get("nameEN"),
        "station_name_th": station.get("nameTH"),
        "area_en": station.get("areaEN"),
        "area_th": station.get("areaTH"),
        "station_type": station.get("stationType"),
        "lat": safe_float(station.get("lat")),
        "lon": safe_float(station.get("long")),
        "reading_date": aqi_last.get("date"),
        "reading_time": aqi_last.get("time"),
        "pm25_value": safe_float(aqi_last.get("PM25", {}).get("value")),
        "pm25_aqi": safe_int(aqi_last.get("PM25", {}).get("aqi")),
        "pm10_value": safe_float(aqi_last.get("PM10", {}).get("value")),
        "o3_value": safe_float(aqi_last.get("O3", {}).get("value")),
        "co_value": safe_float(aqi_last.get("CO", {}).get("value")),
        "no2_value": safe_float(aqi_last.get("NO2", {}).get("value")),
        "so2_value": safe_float(aqi_last.get("SO2", {}).get("value")),
        "overall_aqi": safe_int(aqi_last.get("AQI", {}).get("aqi")),
        "aqi_param": aqi_last.get("AQI", {}).get("param"),
    }


def run() -> None:
    """
    Main entry point — fetch API and produce all stations to Kafka.
    """
    logger.info("Starting Air4Thai producer")
    producer = get_producer(KAFKA_BOOTSTRAP_SERVERS)

    stations = fetch_air4thai()
    success_count = 0

    for station in stations:
        try:
            record = parse_station(station)
            station_id = record["station_id"]

            if not station_id:
                logger.warning("Skipping station with no ID")
                continue

            produce_message(
                producer=producer,
                topic=TOPIC,
                key=station_id,
                payload=record,
            )
            success_count += 1

        except Exception as e:
            logger.error(f"Failed to process station: {e}")
            continue

    flush_producer(producer)
    logger.info(f"Done — produced {success_count}/{len(stations)} stations")


if __name__ == "__main__":
    run()
