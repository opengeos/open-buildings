from enum import Enum
from typing import Dict
from pydantic import BaseModel

class Source(Enum):
    GOOGLE = 1
    OVERTURE = 2

class Format(Enum):
    SHAPEFILE = 1
    GEOJSON = 2
    GEOPACKAGE = 3
    FLATGEOBUF = 4
    PARQUET = 5


class SourceSettings(BaseModel):
    base_url: str
    hive_partitioning: bool

class SettingsSchema(BaseModel):
    sources: Dict[Source, SourceSettings]
    extensions: Dict[Format, str]

settings = SettingsSchema(
    sources={
        Source.GOOGLE: SourceSettings(
            base_url="s3://us-west-2.opendata.source.coop/google-research-open-buildings/geoparquet-by-country/*/*.parquet",
            hive_partitioning=True
        ),
        Source.OVERTURE: SourceSettings(
            base_url="s3://us-west-2.opendata.source.coop/cholmes/overture/geoparquet-country-quad-hive/*/*.parquet",
            hive_partitioning=True
        )
    },
    extensions={
        Format.SHAPEFILE: 'shp',
        Format.GEOJSON: 'json',
        Format.GEOPACKAGE: 'gpkg',
        Format.FLATGEOBUF: 'fgb',
        Format.PARQUET: 'parquet'
    }
)
