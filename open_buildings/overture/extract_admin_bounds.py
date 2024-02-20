import pathlib

import duckdb
import geopandas as gpd
from shapely import wkb

con = duckdb.connect(database=":memory:", read_only=False)
con.execute("LOAD spatial;")

admin_level = 2

OVERTURE_DIR = pathlib.Path("~/data/src/overture/2024-02-15-alpha.0").expanduser()
OUT_DIR = pathlib.Path("~/data/prc/overture/2024-02-15").expanduser()
if not OUT_DIR.exists():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

# Adjusted query to perform a join and retrieve polygons
query = f"""
WITH admins_view AS (
    SELECT * FROM read_parquet('{str(OVERTURE_DIR)}/theme=admins/type=*/*')
)
SELECT
    admins.id,
    admins.isoCountryCodeAlpha2,
    admins.names,
    admins.isoSubCountryCode, 
    areas.areaGeometry as geometry 
FROM admins_view AS admins
INNER JOIN (
    SELECT 
        id as areaId, 
        localityId, 
        geometry AS areaGeometry
    FROM admins_view
) AS areas ON areas.localityId = admins.id
WHERE admins.adminLevel = {admin_level};
"""

# Execute the query and fetch the result
admins = con.execute(query).fetchdf()

# Convert the 'geometry' column from WKB to Shapely geometries and create a GeoDataFrame
admins = gpd.GeoDataFrame(
    admins,
    geometry=admins["geometry"].apply(lambda b: wkb.loads(bytes(b))),
    crs="EPSG:4326",
)

admins[f"admin_level_{admin_level}_name"] = admins.names.map(lambda r: r["primary"])
admins = admins.drop(columns=["names"])

outpath = OUT_DIR/ f"admin_bounds_level_{admin_level}.parquet"
admins.to_parquet(outpath)