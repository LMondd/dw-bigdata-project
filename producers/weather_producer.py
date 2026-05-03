"""
weather_producer.py — Pulls OpenWeather API and produces to Kafka.
Topic: weather-raw (1 partition, key=city_id)
Run: python -m producers.weather_producer
"""

import logging
import requests
from producers.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    OPENWEATHER_API_KEY,
)
from producers.common.kafka_utils import (
    get_producer,
    produce_message,
    flush_producer,
)

logger = logging.getLogger(__name__)

TOPIC = "weather-raw"

# Thai cities to monitor
CITIES = [
    {"name": "Bangkok", "city_id": 1609350},
    {"name": "Chiang Mai", "city_id": 1153671},
    {"name": "Phuket", "city_id": 1151254},
    {"name": "Pattaya", "city_id": 1607981},
    {"name": "Chon Buri", "city_id": 1153673},
]

BASE_URL = "https://api.openweathermap.org/data/2.5/weather"


def fetch_weather(city_id: int) -> dict:
    """
    Fetch current weather for a city by ID.

    Args:
        city_id: OpenWeather city ID

    Returns:
        Raw API response dict
    """
    response = requests.get(
        BASE_URL,
        params={
            "id": city_id,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def parse_weather(data: dict) -> dict:
    """
    Flatten OpenWeather response into a Bronze-ready record.

    Args:
        data: Raw API response dict

    Returns:
        Flattened dict matching Bronze schema
    """
    weather = data.get("weather", [{}])[0]
    main = data.get("main", {})
    wind = data.get("wind", {})
    sys = data.get("sys", {})

    return {
        "city_id": data.get("id"),
        "city_name": data.get("name"),
        "country": sys.get("country"),
        "lat": data.get("coord", {}).get("lat"),
        "lon": data.get("coord", {}).get("lon"),
        "reading_ts": data.get("dt"),
        "temp_c": main.get("temp"),
        "feels_like_c": main.get("feels_like"),
        "pressure_hpa": main.get("pressure"),
        "humidity_pct": main.get("humidity"),
        "wind_speed_ms": wind.get("speed"),
        "wind_deg": wind.get("deg"),
        "visibility_m": data.get("visibility"),
        "weather_main": weather.get("main"),
        "weather_desc": weather.get("description"),
        "weather_icon": weather.get("icon"),
    }


def run() -> None:
    """
    Main entry point — fetch weather for all cities and produce to Kafka.
    """
    logger.info("Starting Weather producer")
    producer = get_producer(KAFKA_BOOTSTRAP_SERVERS)
    success_count = 0

    for city in CITIES:
        try:
            data = fetch_weather(city["city_id"])
            record = parse_weather(data)
            city_id = str(record["city_id"])

            produce_message(
                producer=producer,
                topic=TOPIC,
                key=city_id,
                payload=record,
            )
            logger.info(f"Produced weather for {city['name']}")
            success_count += 1

        except Exception as e:
            logger.error(f"Failed to fetch weather for {city['name']}: {e}")
            continue

    flush_producer(producer)
    logger.info(f"Done — produced {success_count}/{len(CITIES)} cities")


if __name__ == "__main__":
    run()
