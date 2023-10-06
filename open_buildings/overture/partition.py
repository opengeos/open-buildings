"""
This script takes a DuckDB database with a buildings table and converts it to GeoParquet 
files partitioned on first country and then quadkey. The buildings table must have a
country_iso field and quadkey field, populated by overture-buildings-parquet-add-columns.py.
The main function is process_db(), and it will take as input a maximum number of rows per
file and a row group size for the Parquet files. It will then iterate through the countries
in the database and partition the buildings table into GeoParquet files for each country.
If the number of rows for a country is greater than the maximum number of rows per file,
it will partition the country into quadkeys and create GeoParquet files for each quadkey.
Those quadkeys will be further partitioned if necessary until the number of rows for a
quadkey is less than or equal to the maximum number of rows per file. 
"""

import duckdb
import datetime
import subprocess
import tempfile
import os
import click
import shutil
import geopandas as gpd
from shapely import wkb
import pandas as pd
import time

def current_time_str():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def print_verbose(msg, verbose):
    if verbose:
        print(f"[{current_time_str()}] {msg}")

def convert_gpq(input_filename, row_group_size, verbose):
    print_verbose(f"Starting conversion for {input_filename} using gpq (row_group_size ignored).", verbose)

    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    temp_file.close()  # Close the file so gpq can open it

    # Convert the Parquet file to a GeoParquet file using gpq
    gpq_cmd = ['gpq', 'convert', input_filename, temp_file.name]
    subprocess.run(gpq_cmd, check=True)

    print_verbose(f"Conversion for {input_filename} using gpq finished.", verbose)

    # Rename (move) the temp file to the final filename
    shutil.move(temp_file.name, input_filename)

    # Delete the initial temp file if it still exists
    #initial_temp_filename = f'{country_code}_temp.parquet'
    #if os.path.exists(initial_temp_filename):
    #    os.remove(initial_temp_filename)

def convert_pandas(input_filename, rg_size, verbose):
    # Placeholder function to be fleshed out
    print_verbose("Starting conversion using pandas.", verbose)
    try:
        df = pd.read_parquet(input_filename)

        # Convert WKB geometry to geopandas geometry
        df['geometry'] = df['geometry'].apply(wkb.loads, hex=True)
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
        # Change output file the input_filename with .parquet replaced with _geo.parquet
        output_filename = input_filename.replace(".parquet", "_geo.parquet")
    
        gdf.to_parquet(output_filename, row_group_size=rg_size)
        # delete the original file
        os.remove(input_filename)
        # Rename (move) the output file to the input filename
        shutil.move(output_filename, input_filename)
        print(f"Finished processing {input_filename} at {time.ctime()}")
    except Exception as e:
        print(f"Error processing {input_filename}: {e}")
    
# Note, this doesn't work, but I'm not sure why. May be that ogr doesn't really support
# compatible geospatial parquet, but it really looks like it should. Maybe there's something
# weird with the ones written out. 
def convert_ogr(input_filename, rg_size, verbose):
    output_filename = input_filename.replace(".parquet", "_geo.parquet")
    rg_cmd = f"ROW_GROUP_SIZE={rg_size}"
    cmd = [
        'ogr2ogr',
        '-f',
        'Parquet',
        output_filename,
        input_filename,
   #     '-oo',
   #     rg_cmd,
        '-oo',
        'GEOM_POSSIBLE_NAMES=geometry', ]

    # print the ogr2ogr command that will be run
    if verbose:
        print("ogr2ogr command:")
        print(' '.join(cmd))

    # Run the command
    subprocess.run(cmd, check=True)

    # delete the original file
    os.remove(input_filename)
    # Rename (move) the output file to the input filename
    shutil.move(output_filename, input_filename)
    print(f"Finished processing {input_filename} at {time.ctime()}")

    if verbose:
        print(f"Converted {input_filename} to {output_filename} using ogr2ogr.")

 

def fetch_quadkeys(conn, table_name, country_code, length, verbose, prev_qk=""):
    query = f"SELECT DISTINCT SUBSTR(quadkey, 1, {length}) FROM {table_name} WHERE country_iso = '{country_code}'"
    if prev_qk:
        query += f" AND SUBSTR(quadkey, 1, {len(prev_qk)}) = '{prev_qk}'"
    print_verbose(f'Executing: {query}', verbose)
    return conn.execute(query).fetchall()

def convert_to_geoparquet(parquet_path, geo_conversion, row_group_size, verbose):
    if geo_conversion == 'gpq':
        convert_gpq(parquet_path, row_group_size, verbose)
        print_verbose(f"File: {parquet_path} written with gpq", verbose)
    elif geo_conversion == 'pandas':
        convert_pandas(parquet_path, row_group_size, verbose)
        print_verbose(f"File: {parquet_path} written with pandas", verbose)
    elif geo_conversion == 'ogr':
        convert_ogr(parquet_path, row_group_size, verbose)
        print_verbose(f"File: {parquet_path} written with ogr", verbose)
    else:
        print_verbose(f"File: {parquet_path} written without converting to GeoParquet", verbose)

#TODO: go all the way into the quad to find the smallest quadkey that contains less than max_per_file rows
def process_quadkey_recursive(conn, table_name, country_code, output_folder, length, geo_conversion, row_group_size, verbose, max_per_file, current_qk=""):
    distinct_quadkeys = fetch_quadkeys(conn, table_name, country_code, length, verbose, current_qk)
    print_verbose(f"The list of quadkeys for country {country_code} and length {length} is {distinct_quadkeys}", verbose)
    #num_distinct_qk = len(distinct_quadkeys)
    for qk in distinct_quadkeys:
        qk_str = qk[0]
        qk_count_query = f"SELECT COUNT(*) FROM {table_name} WHERE country_iso = '{country_code}' AND SUBSTR(quadkey, 1, {length}) = '{qk_str}'"
        print_verbose(f'Executing: {qk_count_query}', verbose)
        qk_count = conn.execute(qk_count_query).fetchone()[0]
        print_verbose(f"Quadkey {qk_str} has {qk_count} rows", verbose)
        if qk_count > max_per_file:
            process_quadkey_recursive(conn, table_name, country_code, output_folder, length + 1, geo_conversion, row_group_size, verbose, max_per_file, qk_str)
        else:
            quad_output_filename = os.path.join(output_folder, f'{country_code}_{qk_str}.parquet')
            if os.path.exists(quad_output_filename):
                print_verbose(f"Output file {quad_output_filename} already exists, skipping...", verbose)
            else:
                copy_cmd = f"COPY (SELECT * FROM {table_name} WHERE country_iso = '{country_code}' AND SUBSTR(quadkey, 1, {length}) = '{qk_str}' ORDER BY quadkey) TO '{quad_output_filename}' WITH (FORMAT PARQUET);"
                print_verbose(f'Executing: {copy_cmd}', verbose)
                conn.execute(copy_cmd)
                convert_to_geoparquet(quad_output_filename, geo_conversion, row_group_size, verbose)


def process_db(duckdb_path, output_folder, geo_conversion, verbose, max_per_file, row_group_size, hive, table_name):
    # create output folder if it does not exist
    os.makedirs(output_folder, exist_ok=True)
    conn = duckdb.connect(duckdb_path)
    conn.execute('LOAD spatial;')
    cursor = conn.execute(f'SELECT DISTINCT country_iso FROM {table_name}')
    countries = cursor.fetchall()
    
    print_verbose(f'Found {len(countries)} unique countries', verbose)
    #countries.reverse()
    for country in countries:
        country_code = country[0]
        write_folder = output_folder
        if (hive):
            write_folder = os.path.join(output_folder, f'country_iso={country_code}')
            os.makedirs(write_folder, exist_ok=True)
        output_filename = os.path.join(write_folder, f'{country_code}.parquet')
        if os.path.exists(output_filename):
            print_verbose(f"Output file for country {country_code} already exists, skipping...", verbose)
            continue

        count_query = f"SELECT COUNT(*) FROM {table_name} WHERE country_iso = '{country_code}'"
        print_verbose(f'Executing: {count_query}', verbose)
        count = conn.execute(count_query).fetchone()[0]
        print_verbose(f"Country {country_code} has {count} rows", verbose)

        if count <= max_per_file:
            copy_cmd = f"COPY (SELECT * FROM {table_name} WHERE country_iso = '{country_code}' ORDER BY quadkey) TO '{output_filename}' WITH (FORMAT PARQUET);"
            print_verbose(f'Executing: {copy_cmd}', verbose)
            conn.execute(copy_cmd)
            convert_to_geoparquet(output_filename, geo_conversion, row_group_size, verbose)
        else:
            process_quadkey_recursive(conn, table_name, country_code, output_folder, 1, geo_conversion, row_group_size, verbose, max_per_file)

if __name__ == "__main__":
    process_db()