# This script takes a building table in DuckDB created from the Overture buildings
# with columns added for quadkey and country_iso, and then writes out one valid
# GeoParquet file for each country. 
# To use it you must have gpq installed (https://github.com/planetlabs/gpq).
# It can run on a duckdb processed on an individual file from Overture, but ideally
# it runs on the entire building set (created by combining all the individual files).
# Then each output file would have the complete overture data. 

# There's much more work to do on this - right now it just assumes any input duckdb
# has a buildings table with quadkey and country_iso columns created by the
# overture-buildings-parquet-add-columns.py script. Ideally it'd integrate as
# a command in the open_buildings CLI, and would run the proper column additions
# if it was needed to run. And ideally it could write out more than just countries,
# it'd have a variety of options to experiment with different partitioning schemes.

import duckdb
import datetime
import subprocess
import tempfile
import os

# Settings
duckdb_path = '/Users/cholmes/geodata/overture/buildings.duckdb'
table_name = 'buildings'
verbose = True

# Establish a connection to the DuckDB database
conn = duckdb.connect(duckdb_path)

# Load the spatial extension
conn.execute('LOAD spatial;')

# This one is more 'right' and will work for all countries, but is likely slow with big files
cursor = conn.execute('SELECT DISTINCT country_iso FROM buildings')
countries = cursor.fetchall()

# My initial run started with the US which is the biggest, so trying to mix up the order
countries.reverse()

for country in countries:
    country_code = country[0] # Extract the country code

    # Check if the output file already exists
    output_filename = f'{country_code}.parquet'
    if os.path.exists(output_filename):
        print(f'Output file for country {country_code} already exists, skipping...')
        continue

    # Build the COPY command
    copy_cmd = f"COPY (SELECT * FROM {table_name} WHERE country_iso = '{country_code}' ORDER BY quadkey) TO '{country_code}_temp.parquet' WITH (FORMAT PARQUET);"

    # Print the command if verbose is True
    if verbose:
        print(f'Executing: {copy_cmd}')

    # Execute the COPY command
    conn.execute(copy_cmd)

    # Print out each time a country is written, with the country code and the timestamp
    print(f'Country: {country_code} written at {datetime.datetime.now()}')

    # Create a temporary file
    temp_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    temp_file.close()  # Close the file so gpq can open it

    # Convert the Parquet file to a GeoParquet file using gpq
    gpq_cmd = ['gpq', 'convert', f'{country_code}_temp.parquet', temp_file.name]
    subprocess.run(gpq_cmd, check=True)

    # Rename the temp file to the final filename
    os.rename(temp_file.name, f'{country_code}.parquet')

    # Delete the initial temp file if it still exists
    initial_temp_filename = f'{country_code}_temp.parquet'
    if os.path.exists(initial_temp_filename):
        os.remove(initial_temp_filename)
