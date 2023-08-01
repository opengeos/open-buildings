import os
import subprocess
import time
from datetime import datetime, timedelta
import json

import click
import glob
import duckdb
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import mapping
from openlocationcode import openlocationcode as olc

# Global variable, that runs GPQ (https://github.com/planetlabs/gpq) after DuckDB writes the Parquet file.
# This is necessary because DuckDB does not write the GeoParquet metadata (yet). Once DuckDB implements
# this feature can be removed. Setting it to false will give a sense of how fast DuckDB will be, but
# if you want to actually use the output GeoParquet files, set it to True.
RUN_GPQ_CONVERSION = True

# Global variable, that sets the compression type for the Parquet files. The two options that
# will work for both DuckDB and pandas are 'snappy' and 'gzip'. 'snappy' is the default. You can
# try out brotli with pandas, it seems to give the most compression. DuckDB additional supports
# zstd, but pandas does not. Note that GPQ conversion on DuckDB output likely keeps the same
# compression, but I have not tested this. GPQ conversion from Parquet does not yet support
# the other GPQ compression options.
PARQUET_COMPRESSION = 'snappy'

# Don't run the DuckDB GPKG conversion if set to true, as it takes a long time, likely due to a bug.
# It means longer runs and puts one big time on the graphs.
SKIP_DUCK_GPKG = True

@click.group()
def cli():
    pass


def define_output_paths(input_file_path, output_directory, format):
    output_file_name = os.path.basename(input_file_path)[:-3] + format
    output_file_path = os.path.join(output_directory, output_file_name)
    # TODO: the -3 doesn't work with .parquet, leads to a weird file name, but duck doesn't care.
    duckdb_file_path = output_file_path[:-3] + 'duckdb'
    return output_file_path, duckdb_file_path


def remove_existing_files(output_file_path, duckdb_file_path, overwrite):
    if overwrite:
        if os.path.exists(output_file_path):
            os.remove(output_file_path)
        if os.path.exists(duckdb_file_path):
            os.remove(duckdb_file_path)


def process_with_duckdb(
    input_file_path,
    duckdb_file_path,
    split_multipolygons,
    verbose,
    format,
    output_file_path,
):
    # new duckdb at input file path but with .duckdb
    conn = duckdb.connect(duckdb_file_path)
    c = conn.cursor()
    c.execute(f"install spatial;")
    c.execute(f"load spatial;")
    c.execute(
        f"create table buildings as (select * EXCLUDE (latitude, longitude) from '{input_file_path}');"
    )

    if verbose:
        c.execute("SELECT COUNT(*) FROM buildings")
        print(f"Original rows: {c.fetchone()[0]}")

    if split_multipolygons:
        # Fetch the multipolygons
        c.execute("SELECT * FROM buildings WHERE geometry LIKE 'MULTIPOLYGON%'")
        results = c.fetchall()
        columns = [desc[0] for desc in c.description]

        multipolygon_count = 0

        # Process each multipolygon
        for row in results:
            multipolygon_count += 1
            row_dict = dict(zip(columns, row))
            multipolygon = wkt.loads(row_dict['geometry'])

            if verbose:
                # Print the original MultiPolygon
                feature = {
                    "type": "Feature",
                    "properties": {
                        k: v for k, v in row_dict.items() if k != 'geometry'
                    },
                    "geometry": multipolygon.__geo_interface__,
                }
                print("Original MultiPolygon:")
                print(json.dumps(feature))

            for polygon in multipolygon.geoms:
                # Convert the polygon to a GeoSeries in order to project it
                polygon_projected = gpd.GeoSeries([polygon], crs="EPSG:4326").to_crs(
                    'EPSG:6933'
                )

                # Compute the new area (geopandas calculates area in square meters for projected CRS)
                new_area = polygon_projected.area.values[0]

                # Compute the centroid and encode it into a Plus Code
                centroid = polygon.centroid
                new_plus_code = olc.encode(centroid.y, centroid.x, codeLength=12)

                # Create new properties for the polygon
                properties = {k: v for k, v in row_dict.items() if k != 'geometry'}
                properties['area_in_meters'] = new_area
                properties['full_plus_code'] = new_plus_code

                if verbose:
                    # Print the new Polygon
                    feature = {
                        "type": "Feature",
                        "properties": properties,
                        "geometry": polygon.__geo_interface__,
                    }
                    print("Component Polygon:")
                    print(json.dumps(feature))

                # Insert new polygon into buildings table
                columns_str = ', '.join(
                    [f'"{k}"' for k in properties.keys()] + ['geometry']
                )
                values_str = ', '.join(
                    [
                        f"'{v}'" if isinstance(v, str) else str(v)
                        for v in properties.values()
                    ]
                    + [f"'{polygon.wkt}'"]
                )
                c.execute(
                    f"INSERT INTO buildings ({columns_str}) VALUES ({values_str})"
                )

        if verbose:
            print(f"Processed {multipolygon_count} multipolygons.")

        # Delete the original multipolygons
        c.execute("DELETE FROM buildings WHERE geometry LIKE 'MULTIPOLYGON%'")

    if verbose:
        c.execute("SELECT COUNT(*) FROM buildings")
        print(f"Output rows: {c.fetchone()[0]}")

        c.execute("SELECT COUNT(*) FROM buildings WHERE geometry LIKE 'MULTIPOLYGON%'")
        print(f"Output multipolygons: {c.fetchone()[0]}")

        c.execute("SELECT COUNT(*) FROM buildings WHERE geometry LIKE 'POLYGON%'")
        print(f"Output polygons: {c.fetchone()[0]}")

    if format == 'fgb':
        c.execute(
            f"COPY (SELECT * EXCLUDE geometry, ST_AsWKB(ST_GeomFromText(geometry)) AS geometry from buildings) \
                TO '{output_file_path}' WITH  (FORMAT GDAL, DRIVER 'FlatGeobuf');"
        )
    elif format == 'parquet':
        c.execute(
            f"COPY (SELECT * EXCLUDE geometry, ST_AsWKB(ST_GeomFromText(geometry)) AS geometry from buildings) \
                TO '{output_file_path}' WITH  (FORMAT PARQUET, COMPRESSION '{PARQUET_COMPRESSION}');"
        )
        if RUN_GPQ_CONVERSION:
            print(
                f"Running gpq convert on {output_file_path}. This takes extra time but ensures the output is valid GeoParquet."
            )
            base_name, ext = os.path.splitext(output_file_path)
            temp_output_file_path = base_name + '_temp' + ext

            # convert from parquet file with a geometry column named wkb to GeoParquet
            command = ['gpq', 'convert', output_file_path, temp_output_file_path]
            gpq_start_time = time.time()
            subprocess.run(command, check=True)
            os.rename(temp_output_file_path, output_file_path)
            gpq_end_time = time.time()
            gpq_elapsed_time = gpq_end_time - gpq_start_time
            print(f"Time taken to run gpq: {gpq_elapsed_time:.2f} seconds")
        else:
            print(
                f"Skipping gpq convert on {output_file_path}. This means the output will be WKB, but it will need to be converted to GeoParquet."
            )
    elif format == 'gpkg':
        if SKIP_DUCK_GPKG:
            print(
                f"Skipping duckdb-gpkg conversion on {output_file_path}, since SKIP_DUCK_GPKG is set to True. There is likely a bug, since it takes way longer and skews the graphs"
            )
        else:
            c.execute(
                f"COPY (SELECT * EXCLUDE geometry, ST_AsWKB(ST_GeomFromText(geometry)) AS geometry from buildings) \
                    TO '{output_file_path}' WITH  (FORMAT GDAL, DRIVER 'GPKG');"
            )
    elif format == 'shp':
        c.execute(
            f"COPY (SELECT * EXCLUDE geometry, ST_AsWKB(ST_GeomFromText(geometry)) AS geometry from buildings) \
                TO '{output_file_path}' WITH  (FORMAT GDAL, DRIVER 'ESRI Shapefile');"
        )

    conn.close()


def process_with_pandas(
    input_file_path, split_multipolygons, verbose, format, output_file_path
):
    df = pd.read_csv(input_file_path)
    df['geometry'] = df['geometry'].apply(wkt.loads)

    # Drop the 'latitude' and 'longitude' columns
    df = df.drop(['latitude', 'longitude'], axis=1)

    # Convert the DataFrame to a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.set_crs("EPSG:4326", inplace=True)

    # Create an empty GeoDataFrame for the output
    output_gdf = gpd.GeoDataFrame(columns=list(gdf.columns), crs=gdf.crs)

    if split_multipolygons:
        multipolygons = gdf[gdf.geometry.type == 'MultiPolygon']
        multipolygon_count = 0
        for i, row in multipolygons.iterrows():
            multipolygon_count += 1
            # Print the original MultiPolygon
            feature = {
                "type": "Feature",
                "properties": row.drop('geometry').to_dict(),
                "geometry": row.geometry.__geo_interface__,
            }
            if verbose:
                print("Original MultiPolygon:")
                print(json.dumps(feature))

            # Print each component Polygon
            for polygon in row.geometry.geoms:
                # Convert the polygon to a GeoSeries in order to project it
                polygon_projected = gpd.GeoSeries([polygon], crs=gdf.crs).to_crs(
                    'EPSG:6933'
                )

                # Compute the new area (geopandas calculates area in square meters for projected CRS)
                new_area = polygon_projected.area.values[0]

                # Compute the centroid and encode it into a Plus Code
                centroid = polygon.centroid
                new_plus_code = olc.encode(centroid.y, centroid.x, codeLength=12)

                # Create new properties for the polygon
                properties = row.drop('geometry').to_dict()
                properties['area_in_meters'] = new_area
                properties['full_plus_code'] = new_plus_code

                # Append to the output GeoDataFrame
                output_gdf = pd.concat(
                    [
                        output_gdf,
                        gpd.GeoDataFrame([properties], geometry=[polygon], crs=gdf.crs),
                    ],
                    ignore_index=True,
                )

                # Print the new Polygon
                feature = {
                    "type": "Feature",
                    "properties": properties,
                    "geometry": polygon.__geo_interface__,
                }
                if verbose:
                    print("Component Polygon:")
                    print(json.dumps(feature))

        print(f"Processed {multipolygon_count} multipolygons.")
        # Add the original Polygons to the output
        polygons = gdf[gdf.geometry.type == 'Polygon']
        output_gdf = pd.concat([output_gdf, polygons], ignore_index=True)
    else:
        output_gdf = gdf

    if verbose:
        # Print the number of original rows in the datafram, and the number of rows in the output
        print(f"Original rows: {len(gdf)}")
        print(f"Output rows: {len(output_gdf)}")
        # Print number of multipolygons and polygons for the output_gdf
        print(
            f"Output multipolygons: {len(output_gdf[output_gdf.geometry.type == 'MultiPolygon'])}"
        )
        print(
            f"Output polygons: {len(output_gdf[output_gdf.geometry.type == 'Polygon'])}"
        )
    # Write the output GeoDataFrame to a file
    if format == 'fgb':
        output_gdf.to_file(output_file_path, driver="FlatGeobuf")
    elif format == 'parquet':
        output_gdf.to_parquet(output_file_path, compression=PARQUET_COMPRESSION)
    elif format == 'gpkg':
        output_gdf.to_file(output_file_path, driver='GPKG')
    elif format == 'shp':
        output_gdf.to_file(output_file_path, driver='ESRI Shapefile')


def process_with_ogr2ogr(
    input_file_path, split_multipolygons, verbose, format, output_file_path
):
    # Define the SQL query to select specific columns
    table_name = os.path.splitext(os.path.basename(input_file_path))[0]

    if format == 'fgb':
        format_string = "FlatGeobuf"
    elif format == 'parquet':
        format_string = "Parquet"
    elif format == 'gpkg':
        format_string = "GPKG"
    elif format == 'shp':
        format_string = "ESRI Shapefile"

    fields_to_keep = ['confidence', 'area_in_meters', 'full_plus_code']

    # Define the ogr2ogr command
    cmd = [
        'ogr2ogr',
        '-f',
        format_string,
        '-select',
        ','.join(fields_to_keep),
        output_file_path,
        input_file_path,
        '-oo',
        'GEOM_POSSIBLE_NAMES=geometry',
        '-a_srs',
        'EPSG:4326',
    ]

    # If split_multipolygons is True, print a message and return.
    # But skip this if the output format is Shapefile, because shapefiles don't have a difference between polygons and multipolygons.
    if split_multipolygons and format != 'shp':
        print("OGR processing doesn't yet support multi polygons")
        return

    # print the ogr2ogr command that will be run
    if verbose:
        print("ogr2ogr command:")
        print(' '.join(cmd))

    # Run the command
    subprocess.run(cmd, check=True)

    if verbose:
        print(f"Converted {input_file_path} to {output_file_path} using ogr2ogr.")


def process_csv_file(
    input_file_path,
    output_directory,
    format,
    overwrite,
    process,
    split_multipolygons,
    verbose,
):
    output_file_path, duckdb_file_path = define_output_paths(
        input_file_path, output_directory, format
    )
    remove_existing_files(output_file_path, duckdb_file_path, overwrite)

    if os.path.exists(output_file_path):
        print(f'Skipping {input_file_path} as {output_file_path} already exists.')
        return
    else:
        print(
            f'Started converting {input_file_path} with {process} to {format} at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}...'
        )

    start_time = time.time()

    if process == 'duckdb':
        process_with_duckdb(
            input_file_path,
            duckdb_file_path,
            split_multipolygons,
            verbose,
            format,
            output_file_path,
        )
    elif process == 'pandas':
        process_with_pandas(
            input_file_path, split_multipolygons, verbose, format, output_file_path
        )
    elif process == 'ogr':
        process_with_ogr2ogr(
            input_file_path, split_multipolygons, verbose, format, output_file_path
        )

    execution_time = time.time() - start_time
    print(
        f'Finished processing {output_file_path} at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}. Execution time: {str(timedelta(seconds=execution_time))}'
    )


def process_geometries(
    input_path,
    output_directory,
    format,
    overwrite,
    process,
    split_multipolygons,
    verbose,
):
    # Check if the provided path is a directory or a file
    if os.path.isdir(input_path):
        # List all csv files in the directory
        csv_files = glob.glob(os.path.join(input_path, '*.csv'))

        # Sort files by size in ascending order
        csv_files.sort(key=lambda x: os.path.getsize(x))

        # Process each csv file
        for input_file_path in csv_files:
            process_csv_file(
                input_file_path,
                output_directory,
                format,
                overwrite,
                process,
                split_multipolygons,
                verbose,
            )
    elif os.path.isfile(input_path) and input_path.endswith('.csv'):
        # Process the single csv file
        process_csv_file(
            input_path,
            output_directory,
            format,
            overwrite,
            process,
            split_multipolygons,
            verbose,
        )
    else:
        raise ValueError(f"Invalid input path: {input_path}")


def process_benchmark(
    input_path, output_directory, processes, formats, split_multipolygons, verbose
):
    results = []
    for process in processes:
        for format in formats:
            start_time = time.time()
            process_geometries(
                input_path,
                output_directory,
                format,
                True,
                process,
                split_multipolygons,
                verbose,
            )
            execution_time = time.time() - start_time
            if process == 'duckdb' and format == 'gpkg' and SKIP_DUCK_GPKG:
                execution_time = 0
            results.append(
                {
                    'process': process,
                    'format': format,
                    #'execution_time': str(timedelta(seconds=execution_time)),
                    'execution_time': execution_time,
                }
            )
    return results

if __name__ == "__main__":
    cli()
