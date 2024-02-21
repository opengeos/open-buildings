import argparse
import pathlib

import duckdb
import geopandas as gpd
from shapely import wkb


def main(admin_level, overture_dir, out_dir):
    con = duckdb.connect(database=":memory:", read_only=False)
    con.execute("LOAD spatial;")

    out_dir = pathlib.Path(out_dir).expanduser()
    if not out_dir.exists():
        out_dir.mkdir(parents=True, exist_ok=True)

    # Common part of the query
    common_query = f"""
    WITH admins_view AS (
        SELECT * FROM read_parquet('{str(overture_dir)}/theme=admins/type=*/*')
    )
    SELECT
        admins.id,
        admins.isoCountryCodeAlpha2,
        admins.names,
    """

    # Conditional part of the query based on admin_level
    if admin_level == 2:
        admin_level_query = "admins.isoSubCountryCode,"
    else:
        admin_level_query = ""

    # Final part of the query
    final_query = f"""
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

    # Combine all parts to form the final query
    query = common_query + admin_level_query + final_query

    # Execute the query
    admins = con.execute(query).fetchdf()

    # Convert to GeoDataFrame and process geometry
    admins = gpd.GeoDataFrame(
        admins,
        geometry=admins["geometry"].apply(lambda b: wkb.loads(bytes(b))),
        crs="EPSG:4326",
    )

    # Process names and drop the original column
    admins["primary_name"] = admins.names.map(lambda r: r["primary"])
    admins = admins.drop(columns=["names"])

    # Write the output
    outpath = out_dir / f"admin_bounds_level_{admin_level}.parquet"
    print(f"Writing admin boundaries level {admin_level} to: {outpath}")
    admins.to_parquet(outpath)


if __name__ == "__main__":

    # NOTE: href or dir?
    # OVERTURE_HREF = str(pathlib.Path("~/data/src/overture/2024-02-15-alpha.0").expanduser())
    OVERTURE_HREF = "s3://overturemaps-us-west-2/release/2024-02-15-alpha.0"
    OUT_DIR = pathlib.Path("~/data/prc/overture/2024-02-15").expanduser()

    parser = argparse.ArgumentParser(description="Process Overture admin level data.")
    parser.add_argument("admin_level", type=int, help="The admin level to process")
    parser.add_argument(
        "-s",
        "--source",
        default=f"{OVERTURE_HREF}",
        help="The source Overture directory",
    )
    parser.add_argument(
        "-o", "--output", default=f"{OUT_DIR}", help="The output directory"
    )

    args = parser.parse_args()

    main(args.admin_level, args.source, args.output)
