"""Utilities for loading RF subject shapes into the database."""

from __future__ import annotations

import json
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import psycopg
import requests
from psycopg import sql
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon, mapping

TARGET_CRS = "EPSG:4326"


@dataclass
class RegionRecord:
    """Structured representation of a region ready for database insertion."""

    code: str
    name: str
    geometry: MultiPolygon


def load_subject_shapes(source: str) -> gpd.GeoDataFrame:
    """Load RF subject shapes from a local path or URL.

    The resulting GeoDataFrame is guaranteed to use the EPSG:4326 CRS and expose
    `code`, `name`, and `geometry` columns.
    """

    path = _materialize_source(source)
    gdf = _read_geodataframe(path)
    gdf = _normalise_columns(gdf)
    gdf = _ensure_crs(gdf, TARGET_CRS)
    gdf["geometry"] = gdf["geometry"].apply(_ensure_multipolygon)
    return gdf


def refresh_regions(conn: psycopg.Connection, source: str) -> int:
    """Refresh the `regions` table using shapes from ``source``.

    The operation runs inside a single transaction, truncating the table before inserting
    rows. Geometries are stored using ``ST_Multi(ST_GeomFromGeoJSON(...))`` ensuring a
    4326 SRID.
    """

    gdf = load_subject_shapes(source)
    records = [RegionRecord(row.code, row.name, row.geometry) for row in gdf.itertuples()]

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE regions RESTART IDENTITY")
            insert_stmt = sql.SQL(
                """
                INSERT INTO regions (code, name, boundary)
                VALUES (%s, %s, ST_SetSRID(ST_Multi(ST_GeomFromGeoJSON(%s)), 4326))
                ON CONFLICT (code)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    boundary = EXCLUDED.boundary
                """
            )

            for record in records:
                geojson = json.dumps(mapping(record.geometry))
                cur.execute(insert_stmt, (record.code, record.name, geojson))

    return len(records)


def _materialize_source(source: str) -> Path:
    """Download or resolve the source path for the shapefile."""

    if source.startswith("http://") or source.startswith("https://"):
        response = requests.get(source, timeout=60)
        response.raise_for_status()
        suffix = Path(source).suffix or ".zip"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(response.content)
            tmp.flush()
            return Path(tmp.name)

    return Path(source)


def _read_geodataframe(path: Path) -> gpd.GeoDataFrame:
    """Read a shapefile from ``path`` into a GeoDataFrame."""

    if path.suffix == ".zip":
        with tempfile.TemporaryDirectory() as tmpdir:
            with zipfile.ZipFile(path) as zf:
                zf.extractall(tmpdir)
            shapefiles = list(Path(tmpdir).glob("*.shp"))
            if not shapefiles:
                raise FileNotFoundError("No shapefile found in archive")
            return gpd.read_file(shapefiles[0], engine="pyogrio")

    if path.is_dir():
        shapefiles = list(path.glob("*.shp"))
        if not shapefiles:
            raise FileNotFoundError("No shapefile found in directory")
        return gpd.read_file(shapefiles[0], engine="pyogrio")

    return gpd.read_file(path, engine="pyogrio")


def _normalise_columns(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensure the GeoDataFrame exposes `code` and `name` columns."""

    columns = {col.lower(): col for col in gdf.columns}
    try:
        code_col = columns["code"]
        name_col = columns["name"]
    except KeyError as exc:  # pragma: no cover - guard clause
        raise ValueError("Shapefile must contain 'code' and 'name' columns") from exc

    gdf = gdf.rename(columns={code_col: "code", name_col: "name"})
    gdf = gdf.rename_geometry("geometry")
    return gdf[["code", "name", "geometry"]]


def _ensure_crs(gdf: gpd.GeoDataFrame, target: str) -> gpd.GeoDataFrame:
    """Reproject the GeoDataFrame to the target CRS if necessary."""

    if gdf.crs is None:
        gdf = gdf.set_crs(target)
        return gdf

    if str(gdf.crs).upper() in {target, "EPSG:4326"}:
        return gdf.to_crs(target)

    return gdf.to_crs(target)


def _ensure_multipolygon(geometry) -> MultiPolygon:
    """Convert any polygonal geometry into a MultiPolygon."""

    if geometry is None:
        raise ValueError("Geometry must not be None")

    if isinstance(geometry, MultiPolygon):
        return geometry

    if isinstance(geometry, Polygon):
        return MultiPolygon([geometry])

    if isinstance(geometry, GeometryCollection):
        polygons = [geom for geom in geometry.geoms if isinstance(geom, (Polygon, MultiPolygon))]
        if not polygons:
            raise ValueError("Geometry collection does not contain polygonal data")
        multi_geoms: list[Polygon] = []
        for geom in polygons:
            if isinstance(geom, Polygon):
                multi_geoms.append(geom)
            else:
                multi_geoms.extend(list(geom.geoms))
        return MultiPolygon(multi_geoms)

    if hasattr(geometry, "geoms"):
        polygons = [geom for geom in geometry.geoms if isinstance(geom, Polygon)]
        if polygons:
            return MultiPolygon(polygons)

    raise TypeError(f"Unsupported geometry type: {type(geometry)!r}")
