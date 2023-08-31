import json
import click
from math import tan, cos, log, pi
from shapely.geometry import shape
from typing import Tuple
import mercantile 
import duckdb
import time
import datetime
import os
import subprocess


def geojson_to_quadkey(data: dict) -> str:
    if 'bbox' in data:
        min_lon, min_lat, max_lon, max_lat = data['bbox']
    else:
        coords = data['geometry']['coordinates'][0]
        min_lon = min_lat = float('inf')
        max_lon = max_lat = float('-inf')
        
        for lon, lat in coords:
            min_lon = min(min_lon, lon)
            min_lat = min(min_lat, lat)
            max_lon = max(max_lon, lon)
            max_lat = max(max_lat, lat)

    for zoom in range(12, -1, -1):
        tiles = list(mercantile.tiles(min_lon, min_lat, max_lon, max_lat, zooms=zoom))
        if len(tiles) == 1:
            return mercantile.quadkey(tiles[0])

    return ''

def geojson_to_wkt(data: dict) -> str:
    geometry = shape(data['geometry'])
    return geometry.wkt

def quadkey_to_geojson(quadkey: str) -> dict:
    # Convert the quadkey to tile coordinates
    tile = mercantile.quadkey_to_tile(quadkey)
    
    # Get the bounding box of the tile
    bbox = mercantile.bounds(tile)
    
    # Construct a GeoJSON Polygon representation of the bounding box
    geojson = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [bbox.west, bbox.south],
                [bbox.east, bbox.south],
                [bbox.east, bbox.north],
                [bbox.west, bbox.north],
                [bbox.west, bbox.south]
            ]]
        }
    }
    
    return geojson

@click.group()
def cli():
    pass

@cli.command()
@click.argument('geojson_input', type=click.File('r'), required=False)
def quadkey(geojson_input):
    """Convert GeoJSON to quadkey."""
    if geojson_input:
        geojson_data = json.load(geojson_input)
    else:
        geojson_data = json.load(click.get_text_stream('stdin'))
    
    result = geojson_to_quadkey(geojson_data)
    click.echo(result)

@cli.command()
@click.argument('geojson_input', type=click.File('r'), required=False)
def WKT(geojson_input):
    """Convert GeoJSON to Well Known Text."""
    if geojson_input:
        geojson_data = json.load(geojson_input)
    else:
        geojson_data = json.load(click.get_text_stream('stdin'))
    
    result = geojson_to_wkt(geojson_data)
    click.echo(result)


@click.command()
@click.argument('geojson_input', type=click.File('r'), required=False)
@click.option('--only-quadkey', is_flag=True, help='Include only the quadkey in the WHERE clause.')
@click.option('--local', is_flag=True, help='Use local path for parquet files instead of the S3 URL.')
def sql(geojson_input, only_quadkey, local):
    """Generate an SQL query based on the input GeoJSON."""
    
    # Read the GeoJSON
    if geojson_input:
        geojson_data = json.load(geojson_input)
    else:
        geojson_data = json.load(click.get_text_stream('stdin'))

    quadkey = geojson_to_quadkey(geojson_data)
    wkt = geojson_to_wkt(geojson_data)

    # Adjust the path in read_parquet based on the --local flag
    path = '*.parquet' if local else 's3://us-west-2.opendata.source.coop/cholmes/overture/geoparquet-country-quad-2/*.parquet'
    base_sql = f"select * from read_parquet('{path}')"
    
    # Construct the WHERE clause based on the options
    where_clause = f"WHERE quadkey LIKE '{quadkey}%'"
    if not only_quadkey:
        where_clause += f" AND\nST_Within(ST_GeomFromWKB(geometry), ST_GeomFromText('{wkt}'))"

    sql_query = f"{base_sql},\n{where_clause}"
    full_sql_query = f"COPY ('{sql_query}' TO 'buildings.fgb' WITH (FORMAT GDAL, DRIVER 'FlatGeobuf')"
    click.echo(full_sql_query) 

@cli.command()
@click.argument('quadkey_input', type=str)
def quad2json(quadkey_input):
    """Convert quadkey to GeoJSON."""
    result = quadkey_to_geojson(quadkey_input)
    click.echo(json.dumps(result, indent=2))

@click.command(name="download")
@click.argument('geojson_input', type=click.File('r'), required=False)
@click.option('--only-quadkey', is_flag=True, help='Include only the quadkey in the WHERE clause.')
@click.option('--format', default=None, type=click.Choice(['shapefile', 'geojson', 'geopackage', 'flatgeobuf', 'parquet']), help='Output format for the SQL query. Defaults to the extension of the dst file.')
@click.option('--generate-sql', is_flag=True, default=False, help='Generate and print SQL without executing.')
@click.option('--dst', type=str, default="buildings.parquet", help='Destination file name (without extension) or full path for the output.')
@click.option('-s', '--silent', is_flag=True, default=False, help='Suppress all print outputs.')
@click.option('--time-report', is_flag=True, default=True, help='Report how long the operation took to run.')
@click.option('--overwrite', default=False, is_flag=True, help='Overwrite the destination file if it already exists.')
@click.option('--verbose', default=False, is_flag=True, help='Print detailed logs with timestamps.')
@click.option('--run-gpq', is_flag=True, default=True, help='Run gpq conversion to ensure the output is valid GeoParquet.')
@click.option('--data-path', type=str, default="s3://us-west-2.opendata.source.coop/cholmes/overture/geoparquet-country-quad-2/*.parquet", help='Path to the root of the buildings parquet data.')
@click.option('--hive-partitioning', is_flag=True, default=False, help='Use Hive partitioning when reading the parquet data.')
@click.option('--country_iso', type=str, default=None, help='Country ISO code to filter the data by.')

def download(geojson_input, only_quadkey, format, generate_sql, dst, silent, time_report, overwrite, verbose, run_gpq, data_path, hive_partitioning, country_iso):
    """Download buildings data based on the input GeoJSON."""

    def print_timestamped_message(message):
        if not silent:
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            click.echo(f"[{current_time}] {message}")

    start_time = time.time()
    if verbose:
        print_timestamped_message("Reading GeoJSON input...")

    # Read the GeoJSON
    if geojson_input:
        geojson_data = json.load(geojson_input)
    else:
        geojson_data = json.load(click.get_text_stream('stdin'))

    if verbose:
        print_timestamped_message("Converting GeoJSON to quadkey and WKT...")
    quadkey = geojson_to_quadkey(geojson_data)
    wkt = geojson_to_wkt(geojson_data)

    print_timestamped_message(f"Querying and downloading data with Quadkey: {quadkey}")
    hive_value = 1 if hive_partitioning else 0
    base_sql = f"select id, country_iso, ST_AsWKB(ST_GeomFromWKB(geometry)) AS geometry from read_parquet('{data_path}', hive_partitioning={hive_value})"
    where_clause = "WHERE "
    if country_iso:
        where_clause += f"country_iso = '{country_iso}' AND "
    where_clause += f"quadkey LIKE '{quadkey}%'"
    if not only_quadkey:
        where_clause += f" AND\nST_Within(ST_GeomFromWKB(geometry), ST_GeomFromText('{wkt}'))"

    output_extension = {
        'shapefile': '.shp',
        'geojson': '.geojson',
        'geopackage': '.gpkg',
        'flatgeobuf': '.fgb',
        'parquet': '.parquet'
    }

    if not format:
        for fmt, ext in output_extension.items():
            if dst.endswith(ext):
                format = fmt
                break
        else:  # The for-else structure means the else block runs if the loop completes normally, without a break.
            raise ValueError("Unknown file format. Please specify using --format option.")

    if not dst.endswith(output_extension[format]):
        dst += output_extension[format]

    create_clause = f"CREATE TABLE buildings AS ({base_sql},\n{where_clause});"
    if generate_sql or verbose:
        print_timestamped_message(create_clause)
    if not generate_sql:
        conn = duckdb.connect(database=':memory:')
        conn.execute("load spatial;")
        conn.execute(create_clause)

        count = conn.execute("SELECT COUNT(*) FROM buildings;").fetchone()[0]

        print_timestamped_message(f"Downloaded {count} features into DuckDB.")
    if overwrite and os.path.exists(dst):
        if verbose:
            print_timestamped_message(f"Deleting existing file at {dst}.")
        os.remove(dst)
    if not generate_sql:
        print_timestamped_message(f"Writing to {dst}...")

    if format == 'parquet':
        copy_statement = f"COPY buildings TO '{dst}' WITH (FORMAT Parquet);"
        if generate_sql or verbose:
            print_timestamped_message(copy_statement)
        if not generate_sql:
            conn.execute(f"COPY buildings TO '{dst}' WITH (FORMAT Parquet);")
            if run_gpq:
                print_timestamped_message(
                    f"Running gpq convert on {dst}. This takes extra time but ensures the output is valid GeoParquet."
                )
                base_name, ext = os.path.splitext(dst)
                temp_output_file_path = base_name + '_temp' + ext

                # convert from parquet file with a geometry column named wkb to GeoParquet
                command = ['gpq', 'convert', dst, temp_output_file_path]
                gpq_start_time = time.time()
                subprocess.run(command, check=True)
                os.rename(temp_output_file_path, dst)
                gpq_end_time = time.time()
                gpq_elapsed_time = gpq_end_time - gpq_start_time
                
                if verbose:
                    print_timestamped_message(f"Time taken to run gpq: {gpq_elapsed_time:.2f} seconds")
            else:
                print_timestamped_message(
                    f"Skipping gpq convert on {output_file_path}. This means the output Parquet will be WKB, but it will need to be converted to GeoParquet."
                )
    else:
        gdal_format = {
            'shapefile': 'ESRI Shapefile',
            'geojson': 'GeoJSON',
            'geopackage': 'GPKG',
            'flatgeobuf': 'FlatGeobuf'
        }
        conn.execute(f"COPY buildings TO '{dst}' WITH (FORMAT GDAL, DRIVER '{gdal_format[format]}');")
    end_time = time.time()

    if time_report:
        elapsed_time = end_time - start_time
        print_timestamped_message(f"Operation took {elapsed_time:.2f} seconds.")      


# Registering the commands with the main group
cli.add_command(quadkey)
cli.add_command(WKT)
cli.add_command(sql)
cli.add_command(quad2json)
cli.add_command(download)

if __name__ == '__main__':
    cli()

