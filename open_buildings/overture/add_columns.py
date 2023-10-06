# This script is used to take an Overture Parquet file and add columns
# useful for partitioning - it can put in both a quadkey and the country
# ISO code. And then it will write out parquet and use gpq to convert the
# parquet to geoparquet.


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

def midpoint(minval: DOUBLE, maxval: DOUBLE) -> DOUBLE:
    return (minval + maxval) / 2.0

def add_quadkey(con):

    # Register Python UDFs
    con.create_function('lat_lon_to_quadkey', lat_lon_to_quadkey, [DOUBLE, DOUBLE, INTEGER], VARCHAR)
    con.create_function('midpoint', midpoint, [DOUBLE, DOUBLE], DOUBLE)

    # Add a quadkey column to the table if it doesn't exist
    con.execute("ALTER TABLE buildings ADD COLUMN IF NOT EXISTS quadkey VARCHAR")

    # Update the quadkey column
    con.execute("""
    UPDATE buildings 
    SET quadkey = lat_lon_to_quadkey(
        midpoint(bbox.miny, bbox.maxy), 
        midpoint(bbox.minx, bbox.maxx), 
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

def process_parquet_file(input_parquet_path, output_folder, country_parquet_path, overwrite=False, add_quadkey_option=False, add_country_iso_option=False, verbose=False):
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
    con.execute(f"CREATE TABLE buildings AS SELECT * FROM read_parquet('{input_parquet_path}')")
    
    if add_quadkey_option:
        add_quadkey(con)

    if add_country_iso_option:
        add_country_iso(con, country_parquet_path)

    # Write out to Parquet
    con.execute(f"COPY (SELECT * FROM buildings ORDER BY quadkey) TO '{output_parquet_path}' WITH (FORMAT Parquet)")
    
    #TODO: turn this into an option to convert to geoparquet or not
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

def process_parquet_files(input_path, output_folder, country_parquet_path, overwrite=False, add_quadkey_option=False, add_country_iso_option=False, verbose=False):
    # If input_path is a directory, process all Parquet files in it
    if os.path.isdir(input_path):
        for file in glob.glob(os.path.join(input_path, "*")):
            process_parquet_file(file, output_folder, country_parquet_path, overwrite, add_quadkey_option, add_country_iso_option, verbose)
    else:
        process_parquet_file(input_path, output_folder, country_parquet_path, overwrite, add_quadkey_option, add_country_iso_option, verbose)

# Call the function - uncomment if you want to call this directly from python and put values in here.
#input_path = '/Volumes/fastdata/overture/s3-data/buildings/'
#output_folder = '/Volumes/fastdata/overture/refined-parquet/'
#country_parquet_path = '/Volumes/fastdata/overture/countries.parquet'
#process_parquet_files(input_path, output_folder, country_parquet_path, overwrite=False, add_quadkey_option=True, add_country_iso_option=True)