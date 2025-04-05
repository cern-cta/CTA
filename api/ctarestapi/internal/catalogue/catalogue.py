from ast import Str
import logging
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.exc import SQLAlchemyError
from ctarestapi.internal.catalogue.drive_queries import DriveQueries
from pydantic import BaseModel
from typing import Literal


class CatalogueStatus(BaseModel):
    status: Literal["ok", "degraded", "unavailable"]
    schemaVersion: str
    backend: str
    host: str
    database: str


class Catalogue:

    drives: DriveQueries
    _engine: Engine

    def __init__(self, connection_string: str):
        self._engine = create_engine(connection_string)

    def get_status(self) -> CatalogueStatus:
        try:
            with self._engine.connect() as conn:
                query = text(
                    """
                    SELECT
                        CTA_CATALOGUE.SCHEMA_VERSION_MAJOR,
                        CTA_CATALOGUE.SCHEMA_VERSION_MINOR
                    FROM
                        CTA_CATALOGUE
                    LIMIT 1
                """
                )
                row = conn.execute(query).mappings().first()

                schema_version = f"{row['schema_version_major']}.{row['schema_version_minor']}"
                return CatalogueStatus(
                    status="ok",
                    schemaVersion=schema_version,
                    backend=self._engine.url.get_backend_name(),
                    host=self._engine.url.host or "unknown",
                    database=self._engine.url.database or "unknown",
                )

        except SQLAlchemyError as error:
            logging.warning(f"SQL Error: {error}")
            return CatalogueStatus(
                status="unavailable",
                schemaVersion="unknown",
                backend=self._engine.url.get_backend_name(),
                host=self._engine.url.host or "unknown",
                database=self._engine.url.database or "unknown",
            )

    def is_reachable(self) -> bool:
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError:
            return False
