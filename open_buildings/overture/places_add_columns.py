# This script is used to take an Overture Parquet file and add columns
# useful for partitioning - it can put in both a quadkey and the country
# ISO code. And then it will write out parquet and use gpq to convert the
# parquet to geoparquet.
#
# There is much more to do, my plan is to incorporate it into the open_buildings
# CLI and let people pick which of the columns they want to add. Also could
# be nice to add the ability to get the data downloaded - this just assumes
# you've already got it. Also need to add the command to create the 
# countries.parquet, but it's basically the one in https://github.com/OvertureMaps/data/blob/main/duckdb_queries/admins.sql
# but saved to parquet. You also could just use that command to pull it
# directly into your duckdb database, and change this code (perhaps we
# add an option to pull it remote if not present). This also would
# ideally work with any of the Overture data types, and let you choose
# your table names.
import os
import duckdb
import time
import tempfile
import subprocess
import glob
from duckdb.typing import *
import mercantile
import shutil

def lat_lon_to_quadkey(lat: DOUBLE, lon: DOUBLE, level: INTEGER) -> VARCHAR:
    # Convert latitude and longitude to tile using mercantile
    tile = mercantile.tile(lon, lat, level)
    
    # Convert the tile to a quadkey
    quadKey = mercantile.quadkey(tile)
    return quadKey

def add_quadkey(con):

    # Register Python UDFs
    con.create_function('lat_lon_to_quadkey', lat_lon_to_quadkey, [DOUBLE, DOUBLE, INTEGER], VARCHAR)

    # Add a quadkey column to the table if it doesn't exist
    con.execute("ALTER TABLE places ADD COLUMN IF NOT EXISTS quadkey VARCHAR")

    # Update the quadkey column
    # (no need to use midpoint as places is just points, so maxy and miny are the same)
    con.execute("""
    UPDATE places 
    SET quadkey = lat_lon_to_quadkey(
        bbox.maxy,  
        bbox.maxx, 
        12
    );
    """)

def add_country_iso(con, country_parquet_path):
    # Load country parquet file into duckdb
    con.execute(f"CREATE TABLE countries AS SELECT * FROM read_parquet('{country_parquet_path}')")

    # Add a country_iso column to the buildings table
    con.execute("ALTER TABLE places ADD COLUMN IF NOT EXISTS country_iso VARCHAR")
    
    # Update the country_iso column in the buildings table
    con.execute("""
    UPDATE places 
    SET country_iso = countries.isocountrycodealpha2 
    FROM countries 
    WHERE ST_Intersects(ST_GeomFromWKB(countries.geometry), ST_GeomFromWKB(places.geometry))
    """)

def process_parquet_file(input_parquet_path, output_folder, country_parquet_path, overwrite=False, add_quadkey_option=False, add_country_iso_option=False):
    # Ensure output_folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Get unique identifier from file name
    unique_id = os.path.basename(input_parquet_path).split('_')[-1]
    
    # Define output paths
    output_db_path = os.path.join(output_folder, f'{unique_id}.duckdb')
    output_parquet_path = os.path.join(output_folder, f'{unique_id}.parquet')
    
    # Check if output files exist
    if (os.path.exists(output_db_path) or os.path.exists(output_parquet_path)) and not overwrite:
        print(f'Files with ID {unique_id} already exist. Skipping...')
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
    con.execute(f"CREATE TABLE places AS SELECT * FROM read_parquet('{input_parquet_path}')")
    
    if add_quadkey_option:
        add_quadkey(con)

    if add_country_iso_option:
        add_country_iso(con, country_parquet_path)

    # Write out to Parquet
    con.execute(f"COPY (SELECT * FROM places ORDER BY quadkey) TO '{output_parquet_path}' WITH (FORMAT Parquet)")
    
    if (True):
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

def process_parquet_files(input_path, output_folder, country_parquet_path, overwrite=False, add_quadkey_option=False, add_country_iso_option=False):
    # If output_folder doesn't exist, create it
    os.makedirs(output_folder, exist_ok=True)
    # If input_path is a directory, process all Parquet files in it
    if os.path.isdir(input_path):
        for file in glob.glob(os.path.join(input_path, "*")):
            process_parquet_file(file, output_folder, country_parquet_path, overwrite, add_quadkey_option, add_country_iso_option)
    else:
        process_parquet_file(input_path, output_folder, country_parquet_path, overwrite, add_quadkey_option, add_country_iso_option)

# Call the function 
input_path = '/Volumes/fastdata/overture/s3-data/places/'
output_folder = '/Volumes/fastdata/overture/refined-places-geoparquet/'
country_parquet_path = '/Volumes/fastdata/overture/countries.parquet'
process_parquet_files(input_path, output_folder, country_parquet_path, overwrite=False, add_quadkey_option=True, add_country_iso_option=True)