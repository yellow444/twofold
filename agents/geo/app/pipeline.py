"""Geocoding pipeline for assigning flights to regions."""

from __future__ import annotations

from dataclasses import dataclass

from psycopg import Connection

from .repository import (
    FlightCandidate,
    FlightGeometry,
    FlightQualityIssueRecord,
    GeoRepository,
)

GEO_CHECK_NAME = "geo.unresolved_location"
DEFAULT_SEVERITY = "fail"


@dataclass(slots=True)
class GeocodeSummary:
    """Simple summary of the geocoding run."""

    dataset_version_id: int
    total_candidates: int
    resolved: int
    unresolved: int


class GeoPipeline:
    """Coordinate loading, geocoding, and persistence for flights."""

    def __init__(self, repository: GeoRepository | None = None) -> None:
        self.repository = repository or GeoRepository()

    def run_for_version(self, conn: Connection, version_name: str) -> GeocodeSummary:
        """Execute the pipeline for ``dataset_version.version_name``."""

        dataset_version_id = self.repository.fetch_dataset_version_id(conn, version_name)
        return self.run(conn, dataset_version_id)

    def run(self, conn: Connection, dataset_version_id: int) -> GeocodeSummary:
        """Execute the geocoding pipeline for ``dataset_version_id``."""

        candidates = self.repository.load_flight_candidates(conn, dataset_version_id)
        resolved_records: list[FlightGeometry] = []
        quality_issues: list[FlightQualityIssueRecord] = []

        for candidate in candidates:
            lat, lon = candidate.latitude, candidate.longitude
            if lat is None or lon is None:
                quality_issues.append(self._quality_issue(candidate, "missing_coordinates"))
                continue
            if lat == 0 or lon == 0:
                quality_issues.append(self._quality_issue(candidate, "zero_coordinates"))
                continue

            region_id = self.repository.resolve_region(conn, lat, lon)
            if region_id is None:
                quality_issues.append(self._quality_issue(candidate, "no_region_match"))
                continue

            resolved_records.append(
                FlightGeometry(
                    flight_uid=candidate.flight_uid,
                    region_id=region_id,
                    latitude=lat,
                    longitude=lon,
                    observed_at=candidate.observed_at,
                )
            )

        with conn.transaction():
            self.repository.clear_flights_geo(conn, dataset_version_id)
            self.repository.clear_flight_regions(conn, dataset_version_id)
            self.repository.insert_flight_geometries(conn, dataset_version_id, resolved_records)
            self.repository.update_flight_regions(conn, dataset_version_id, resolved_records)
            self.repository.replace_quality_issues_for_check(
                conn,
                dataset_version_id,
                GEO_CHECK_NAME,
                quality_issues,
            )

        return GeocodeSummary(
            dataset_version_id=dataset_version_id,
            total_candidates=len(candidates),
            resolved=len(resolved_records),
            unresolved=len(quality_issues),
        )

    def _quality_issue(self, candidate: FlightCandidate, reason: str) -> FlightQualityIssueRecord:
        """Build a structured quality issue payload."""

        details = {
            "reason": reason,
            "latitude": candidate.latitude,
            "longitude": candidate.longitude,
        }
        if candidate.payload_flight_id:
            details["flight_id"] = candidate.payload_flight_id
        return FlightQualityIssueRecord(
            flight_uid=candidate.flight_uid,
            severity=DEFAULT_SEVERITY,
            details=details,
        )


def geocode_dataset(conn: Connection, version_name: str, *, repository: GeoRepository | None = None) -> GeocodeSummary:
    """Convenience wrapper around :class:`GeoPipeline`."""

    pipeline = GeoPipeline(repository=repository)
    return pipeline.run_for_version(conn, version_name)
