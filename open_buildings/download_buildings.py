import json
import click
from math import tan, cos, log, pi
from shapely.geometry import shape
from typing import Dict, Any
import mercantile 
import duckdb
import time
from pathlib import Path
import datetime
import os
from typing import Literal, Optional
import pandas as pd
import geopandas as gpd
import subprocess
from shapely import wkb
import shutil

from open_buildings.settings import Source, Format, settings

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


def download(
        geojson_data: Dict[str, Any], 
        dst: Path | str = "buildings.json",
        source: Source | str = Source.OVERTURE,
        format: Optional[Format | str] = None, 
        country_iso: Optional[str] = None,
        *,
        generate_sql: bool = False, # whether to actually perform actions or just generate sql
        verbose: bool = False, # print detailed logs, use python logger instead!
        silent: bool = False, # no log output, use python logger instead!
        overwrite: bool = False # whether to overwirte existing output file
    ) -> None:
    """
    Extract buildings from online sources.

    Parameters
    ----------
    geojson_input : Dict[str, Any]
        GeoJSON dictionary
    dst : Path | str
        The path to write the output to. Can be either a file or a directory.
        If a directory is provided, a file "buildings.<ext>" will be created at that location.
    format : string, default "geojson"
        The output format, alternatively can be extracted from "dst". Explicitly naming the format can be useful if
        used in combination with a directory as "dst". If both file path and format param is provided, the format param takes
        precedence.
    country_iso : str, optional
        A two-letter ISO-3166 code for the country the AOI (geojson_input) is in. Not required but massively speeds up queries.
    generate_sql : bool, default False
        Whether to actually perform DuckDB queries or only generate the SQL.
    verbose : bool, default False
        Print more detailed log messages.
    silent : bool, default False
        Suppress log messages.
    overwrite : bool, default False
        Overwrite existing output files.
    """
    # type conversion
    if type(source) == str:
        try:
            source = Source(source.upper())
        except ValueError:
            raise ValueError(f"Source {source} is unknown. Please choose one of {' ,'.join([s.name.lower() for s in Source])}.") from e

    if type(format) == str:
        try:
            format = Format(format.upper())
        except ValueError:
            raise ValueError(f"Format {format} is unknown. Please choose one of {', '.join(f.name.lower() for f in Format)}.") from e

    if type(dst) == str:
        dst = Path(dst)

    # validate path and extension
    if os.path.isdir(dst):
        dst = dst.joinpath("buildings.json")

    if format and dst:
        # format takes precedence
        dst = dst.joinpath(f"{dst.stem}.{settings.extensions[format]}")

    if not format and dst:
        for fmt, ext in settings.extensions.items():
            if dst.name.endswith(ext):
                format = fmt
                break
        else:  # The for-else structure means the else block runs if the loop completes normally, without a break.
            raise ValueError(f"Can't identify file extension of {dst}. Please choose one of {', '.join([f.name.lower() for f in Format])}.")
    
    # utils (should be in separate utils file?)
    def print_timestamped_message(message):
        if not silent:
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            click.echo(f"[{current_time}] {message}")

    def print_elapsed_time(start_time):
        end_time = time.time()

        elapsed_time = end_time - start_time
        print_timestamped_message(f"Operation took {elapsed_time:.2f} seconds.")

    # main program
    start_time = time.time()
    if verbose:
        print_timestamped_message("Reading GeoJSON input...")

    if os.path.exists(dst) and not generate_sql:
        if overwrite:
            if verbose:
                print_timestamped_message(f"Deleting existing file at {dst}.")
            os.remove(dst)
        else:
            # Print message that the file already exists and cleanly exit the program
            print_timestamped_message(f"File at {dst} already exists. Use --overwrite to overwrite it.")
            return

    if verbose:
        print_timestamped_message("Converting GeoJSON to quadkey and WKT...")
    quadkey = geojson_to_quadkey(geojson_data)
    wkt = geojson_to_wkt(geojson_data)

    country_info = ""
    if country_iso is not None:
        country_info = f"in country {country_iso}"
    print_timestamped_message(f"Querying and downloading data for quadkey {quadkey} {country_info}...")
    if verbose:
        print_timestamped_message(f"WKT: {wkt}")
    if country_info != "":
        print_timestamped_message(f"Expect query times of at least 5-10 seconds")
    else:
        print_timestamped_message(f"Expect query times of at least 30 seconds - this can be lessened by using the --country-iso option")
   
    # download data into DuckDB
    hive_partitioning = settings.sources[source].hive_partitioning
    hive_value = 1 if hive_partitioning else 0
    select_values = "* EXCLUDE geometry"
    # if source is overture and the output is not parquet, then name the values to get
    # so we don't get the crazy structs that gis formats barf on
    if source == Source.OVERTURE and format != Format.PARQUET:
        select_values = "id, level, height, numfloors, class, country_iso, quadkey"
    base_sql = f"select {select_values}, ST_AsWKB(ST_GeomFromWKB(geometry)) AS geometry from read_parquet('{settings.sources[source].base_url}', hive_partitioning={hive_value})"
    where_clause = "WHERE "
    if country_iso:
        where_clause += f"country_iso = '{country_iso}' AND "
    where_clause += f"quadkey LIKE '{quadkey}%'"
    where_clause += f" AND\nST_Within(ST_GeomFromWKB(geometry), ST_GeomFromText('{wkt}'))"
                
    create_clause = f"CREATE TABLE buildings AS ({base_sql},\n{where_clause});"
    if generate_sql or verbose:
        print_timestamped_message(create_clause)

    if not generate_sql:
        conn = duckdb.connect(database=':memory:')

        spatial_extension_query = conn.execute("SELECT * FROM duckdb_extensions() WHERE installed IS TRUE AND extension_name = 'spatial';").fetchone()
        if spatial_extension_query is None:
            print_timestamped_message("Installing DuckDB spatial extension...")
            conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
        conn.execute(create_clause)

        count = conn.execute("SELECT COUNT(*) FROM buildings;").fetchone()[0]

        print_timestamped_message(f"Downloaded {count} features into DuckDB.")
        if count == 0:
            if country_iso is not None:
                print_timestamped_message(f"If you are sure that your GeoJSON should have buildings then check to be sure that {country_iso} is the right code.")
            if verbose:
                print_elapsed_time(start_time)
            return
    
    # export to dst
    if not generate_sql:
        print_timestamped_message(f"Writing to {dst}...")

    if format == Format.PARQUET:
        copy_statement = f"COPY buildings TO '{dst}' WITH (FORMAT Parquet);"
        if generate_sql or verbose:
            print_timestamped_message(copy_statement)
        if not generate_sql:
            conn.execute(f"COPY buildings TO '{dst}' WITH (FORMAT Parquet);")
            try:
                df = pd.read_parquet(dst)

                # Convert WKB geometry to geopandas geometry
                df['geometry'] = df['geometry'].apply(wkb.loads, hex=True)
                gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
                # Change output file the input_filename with .parquet replaced with _geo.parquet
                output_filename = dst.replace(".parquet", "_geo.parquet")
            
                gdf.to_parquet(output_filename)
                # delete the original file
                os.remove(dst)
                # Rename (move) the output file to the input filename
                shutil.move(output_filename, dst)
                if verbose:
                    print_timestamped_message(f"Finished processing {dst} at {time.ctime()}")
            except Exception as e:
                print(f"Error processing {dst} to geoparquet: {e}")
    else:
        gdal_format = {
            Format.SHAPEFILE: 'ESRI Shapefile',
            Format.GEOJSON: 'GeoJSON',
            Format.GEOPACKAGE: 'GPKG',
            Format.FLATGEOBUF: 'FlatGeobuf'
        }
        conn.execute(f"COPY buildings TO '{dst}' WITH (FORMAT GDAL, DRIVER '{gdal_format[format]}');")
          
    if verbose:
        print_elapsed_time(start_time)

# Registering the commands with the main group
cli.add_command(quadkey)
cli.add_command(WKT)
cli.add_command(sql)
cli.add_command(quad2json)
#cli.add_command(download)

if __name__ == '__main__':
    cli()