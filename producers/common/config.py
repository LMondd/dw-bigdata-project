"""
config.py — Centralized config loaded from environment variables.
Never hardcode values — always read from .env or environment.
"""

import logging
import os
from dotenv import load_dotenv

load_dotenv()  # reads .env file if present

# Kafka
KAFKA_BOOTSTRAP_SERVERS: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

# S3
S3_BUCKET: str = os.environ["S3_BUCKET"]

# API Keys
AIR4THAI_API_URL: str = os.getenv(
    "AIR4THAI_API_URL",
    "http://air4thai.com/forweb/getAQI_JSON.php"
)
OPENWEATHER_API_KEY: str = os.environ["OPENWEATHER_API_KEY"]

# Logging — configure once here, all modules inherit this
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
