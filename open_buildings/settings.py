from enum import Enum
from typing import Dict
from pydantic import BaseModel

class Source(Enum):
    GOOGLE = 1
    OVERTURE = 2

class SourceSettings(BaseModel):
    base_url: str
    hive_partitioning: bool

class SettingsSchema(BaseModel):
    sources: Dict[Source, SourceSettings]
    extensions: Dict[str, str]

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
        'shapefile': '.shp',
        'geojson': '.json',
        'geopackage': '.gpkg',
        'flatgeobuf': '.fgb',
        'parquet': '.parquet'
    }
)
