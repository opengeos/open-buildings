import argparse
import pathlib

import duckdb
import geopandas as gpd
from shapely import wkb


def main(admin_level, overture_dir, out_dir):
    con = duckdb.connect(database=":memory:", read_only=False)
    con.execute("LOAD spatial;")

    OVERTURE_DIR = pathlib.Path(overture_dir).expanduser()
    OUT_DIR = pathlib.Path(out_dir).expanduser()
    if not OUT_DIR.exists():
        OUT_DIR.mkdir(parents=True, exist_ok=True)

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

    admins = con.execute(query).fetchdf()

    admins = gpd.GeoDataFrame(
        admins,
        geometry=admins["geometry"].apply(lambda b: wkb.loads(bytes(b))),
        crs="EPSG:4326",
    )

    admins[f"admin_level_{admin_level}_name"] = admins.names.map(lambda r: r["primary"])
    admins = admins.drop(columns=["names"])

    outpath = OUT_DIR / f"admin_bounds_level_{admin_level}.parquet"
    print(f"Writing admin boundaries level {admin_level} to: {outpath}")
    admins.to_parquet(outpath)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Overture admin level data.')
    parser.add_argument('admin_level', type=int, help='The admin level to process')
    parser.add_argument('-s', '--source', default='~/data/src/overture/2024-02-15-alpha.0', help='The source Overture directory')
    parser.add_argument('-o', '--output', default='~/data/prc/overture/2024-02-15', help='The output directory')

    args = parser.parse_args()

    main(args.admin_level, args.source, args.output)
