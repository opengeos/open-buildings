# This script is a slightly more generic version of the overture add_columns.py. There's
# some chance it could be completely generic, but I was just trying to work on google buildings
# so put it under there. The main difference is that it doesn't use the midpoint of the 
# bbox struct, since that's unique to overture. It just uses the centroid of the geometry.
# That could likely work just as well if not better for overture too, so we likely can just
# get rid of that.
# The other thing that would be nice to make it truly generic is to be able to supply the 
# table name, since this should work fine with other types of data. Could also just call it
# 'features' by default, the table name doesn't really matter in these processings. Should probably check
# to be sure it works with lines and points too. So this could use clean up, also just
# removing the 'midpoint' code. 

import os
import duckdb
import time
import tempfile
import subprocess
import glob
from duckdb.typing import *
import mercantile
from shapely import wkt
import shutil

def lat_lon_to_quadkey(wkt_point: VARCHAR, level: INTEGER) -> VARCHAR:

    geom = wkt.loads(wkt_point)

    # convert geom to tile
    tile = mercantile.tile(geom.x, geom.y, level)
    
    # Convert the tile to a quadkey
    quadKey = mercantile.quadkey(tile)
    return quadKey

def midpoint(minval: DOUBLE, maxval: DOUBLE) -> DOUBLE:
    return (minval + maxval) / 2.0

def add_quadkey(con):

    # Register Python UDFs
    con.create_function('lat_lon_to_quadkey', lat_lon_to_quadkey, [VARCHAR, INTEGER], VARCHAR)
    con.create_function('midpoint', midpoint, [DOUBLE, DOUBLE], DOUBLE)

    # Add a quadkey column to the table if it doesn't exist
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS quadkey VARCHAR")

    # Update the quadkey column
    con.execute("""
    UPDATE buildings 
    SET quadkey = lat_lon_to_quadkey(ST_Centroid(ST_GeomFromWKB(geometry)),  
        12
    );
    """)

def add_country_iso(con, country_parquet_path):
    # Load country parquet file into duckdb
    con.execute(f"CREATE TABLE countries AS SELECT * FROM read_parquet('{country_parquet_path}')")

    # Add a country_iso column to the buildings table
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS country_iso VARCHAR")
    
    # Update the country_iso column in the buildings table
    con.execute("""
    UPDATE buildings 
    SET country_iso = countries.isocountrycodealpha2 
    FROM countries 
    WHERE ST_Intersects(ST_GeomFromWKB(countries.geometry), ST_GeomFromWKB(buildings.geometry))
    """)

def process_parquet_file(input_parquet_path, output_folder, country_parquet_path, overwrite=False, add_quadkey_option=False, add_country_iso_option=False):
    # Ensure output_folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Get unique identifier from file name
    file_id = os.path.basename(input_parquet_path)
    
    # Define output paths
    output_db_path = os.path.join(output_folder, f'{file_id}.duckdb')
    output_parquet_path = os.path.join(output_folder, f'{file_id}')
    
    # Check if output files exist
    if (os.path.exists(output_db_path) or os.path.exists(output_parquet_path)) and not overwrite:
        print(f'Files with ID {file_id} already exist. Skipping...')
        return
    
    # Overwrite mode: remove existing files
    if overwrite:
        for file_path in [output_db_path, output_parquet_path]:
            if os.path.exists(file_path):
                os.remove(file_path)
    timestamp = time.time()
    print(f"Starting processing for file {input_parquet_path} at {time.ctime(timestamp)}")
    
    # Connect to DuckDB
    con = duckdb.connect(output_db_path)
    
    con.execute('LOAD spatial;')

    # Load parquet file into duckdb
    con.execute(f"CREATE TABLE buildings AS SELECT * FROM read_parquet('{input_parquet_path}')")
    
    if add_quadkey_option:
        add_quadkey(con)

    if add_country_iso_option:
        add_country_iso(con, country_parquet_path)

    # Write out to Parquet
    con.execute(f"COPY (SELECT * FROM buildings ORDER BY quadkey) TO '{output_parquet_path}' WITH (FORMAT Parquet)")
    
    if (False):
        print(f"Converting to geoparquet: {output_parquet_path}")
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        temp_file.close()  # Close the file so gpq can open it

        # Convert the Parquet file to a GeoParquet file using gpq
        gpq_cmd = ['gpq', 'convert', f'{output_parquet_path}', temp_file.name]
        subprocess.run(gpq_cmd, check=True)

        # Rename the temp file to the final filename
        shutil.move(temp_file.name, f'{output_parquet_path}')
        #os.rename(temp_file.name, f'{output_parquet_path}')

    print(f"Processing complete for file {input_parquet_path}")

    remove_duckdb = False

    # remove duckdb file
    if (remove_duckdb):
        os.remove(output_db_path)

def process_parquet_files(input_path, output_folder, country_parquet_path, overwrite=False, add_quadkey_option=False, add_country_iso_option=False):
    # If input_path is a directory, process all Parquet files in it
    if os.path.isdir(input_path):
        for file in glob.glob(os.path.join(input_path, "*.parquet")):
            process_parquet_file(file, output_folder, country_parquet_path, overwrite, add_quadkey_option, add_country_iso_option)
    else:
        process_parquet_file(input_path, output_folder, country_parquet_path, overwrite, add_quadkey_option, add_country_iso_option)

# Call the function
input_path = '/Users/cholmes/geodata/google-buildings-v3/geoparquet/'
output_folder = '/Users/cholmes/geodata/google-buildings-v3/geoparquet-columns'
country_parquet_path = '/Volumes/fastdata/overture/countries.parquet'
process_parquet_files(input_path, output_folder, country_parquet_path, overwrite=False, add_quadkey_option=True, add_country_iso_option=True)