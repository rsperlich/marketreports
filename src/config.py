"""
Shared configuration, logging setup, and factory functions.

All modules should import from here rather than duplicating
dotenv loading, logging setup, and driver/client construction.
"""

import logging
import os

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def get_neo4j_driver():
    """Create a Neo4j driver from environment variables."""
    from neo4j import GraphDatabase

    return GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=(
            os.getenv("NEO4J_USER", "neo4j"),
            os.getenv("NEO4J_PASSWORD", "password"),
        ),
    )


def get_openai_client():
    """Create an OpenAI client from environment variables."""
    from openai import OpenAI

    base_url = os.getenv("OPENAI_BASE_URL")
    return OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        base_url=base_url or None,
    )


def get_model(override: str | None = None) -> str:
    """Resolve the LLM model name: explicit override > env var > default."""
    if override:
        return override
    return os.getenv("OPENAI_MODEL_DEV", "gpt-4o-mini")
