"""Shared configuration loaded from environment variables.

All producers and consumers should import from here rather than reading os.environ directly.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    kafka_bootstrap_servers: str
    kafka_client_id: str
    aws_region: str
    s3_bucket: str
    log_level: str

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            kafka_bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            kafka_client_id=os.environ.get("KAFKA_CLIENT_ID", "dw-bigdata"),
            aws_region=os.environ.get("AWS_REGION", "us-east-1"),
            s3_bucket=os.environ["S3_BUCKET"],
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
