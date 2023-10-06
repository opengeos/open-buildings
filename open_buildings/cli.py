import sys
import os
import click
import pandas as pd
import matplotlib.pyplot as plt
from open_buildings.google.process import process_benchmark, process_geometries
from open_buildings.download_buildings import download as download_buildings
from open_buildings.overture.add_columns import process_parquet_files
from open_buildings.overture.partition import process_db
from datetime import datetime, timedelta
from tabulate import tabulate
import boto3  # Required for S3 operations

@click.group()
def main():
    """CLI for Open Buildings operations."""
    pass

@click.group()
def google():
    """Commands related to Google operations."""
    pass

@click.group()
def overture():
    """Commands related to Overture operations."""
    pass

main.add_command(google)
main.add_command(overture)

def handle_comma_separated(ctx, param, value):
    return value.split(',')

@main.command(name="get_buildings")
@click.argument('geojson_input', type=click.File('r'), required=False)
@click.argument('dst', type=str, default="buildings.json")
@click.option('--source', default="overture", type=click.Choice(['google', 'overture']), help='Dataset to query, defaults to Overture')
@click.option('--country_iso', type=str, default=None, help='A 2 character country ISO code to filter the data by.')
@click.option('-s', '--silent', is_flag=True, default=False, help='Suppress all print outputs.')
@click.option('--overwrite', default=False, is_flag=True, help='Overwrite the destination file if it already exists.')
@click.option('--verbose', default=False, is_flag=True, help='Print detailed logs with timestamps.')
def get_buildings(geojson_input, dst, source, country_iso, silent, overwrite, verbose):
    """Tool to extract buildings in common geospatial formats from large archives of GeoParquet data online. GeoJSON
    input can be provided as a file or piped in from stdin. If no GeoJSON input is provided, the tool will read from stdin.

    Right now the tool supports two sources of data: Google and Overture. The data comes from Cloud-Native Geospatial distributions
    on https://source.coop, that are partitioned by admin boundaries and use a quadkey for the spatial index. In time this tool will generalize
    to support any admin boundary partitioned GeoParquet data, but for now it is limited to the Google and Overture datasets.

    The default output is GeoJSON, in a file called buildings.json. Changing the suffix will change the output format - .shp for shapefile
    .gpkg for GeoPackage, .fgb for FlatGeobuf and .parquet for GeoParquet, and .json or .geojson for GeoJSON. If your query is
    all within one country it is strongly recommended to use country_iso to hint to the query engine which country to query, as this 
    will speed up the query significantly (5-10x). Expect query times of 5-10 seconds for small queries with country_iso and 30-60 seconds without country_iso.
    Large queries will take longer, as they have to download more data. 

    You can look up the country_iso for a country here: https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes/blob/master/all/all.csv
    If you get the country wrong you will get zero results. Currently you can only query one country, so if your query crosses country boundaries you should
    not use country_iso. In future versions of this tool we hope to eliminate the need to hint with the country_iso.
    """
    # map source of google and overture to values for data_path and hive
    data_path = None
    hive_partitioning = False
    # case insensitive matching
    if source.lower() == "google":
        data_path = "s3://us-west-2.opendata.source.coop/google-research-open-buildings/geoparquet-by-country/*/*.parquet"
        hive_partitioning = True
    elif source.lower() == "overture":
        data_path = "s3://us-west-2.opendata.source.coop/cholmes/overture/geoparquet-country-quad-hive/*/*.parquet"
        hive_partitioning = True
    else:
        raise ValueError('Invalid source')
    
    format = None # will be set by the extension of the dst file
    generate_sql = False
    download_buildings(geojson_input, format, generate_sql, dst, silent, overwrite, verbose, data_path, hive_partitioning, country_iso)

@google.command('benchmark')
@click.argument('input_path', type=click.Path(exists=True))
@click.argument('output_directory', type=click.Path(exists=True))
@click.option(
    '--processes',
    callback=handle_comma_separated,
    default='duckdb,pandas,ogr',
    help="The processing methods to use. One or more of duckdb, pandas or ogr, in a comma-separated list. Default is duckdb,pandas,ogr.",
)
@click.option(
    '--formats',
    callback=handle_comma_separated,
    default='fgb,parquet,shp,gpkg',
    help="The output formats to benchmark. One or more of fgb, parquet, shp or gpkg, in a comma-separated list. Default is fgb,parquet,shp,gpkg.",
)
@click.option(
    '--skip-split-multis',
    is_flag=True,
    help="Whether to keep multipolygons as they are without splitting into their component polygons.",
)
@click.option('--no-gpq', is_flag=True, help="Disable GPQ conversion. Timing will be faster, but not valid GeoParquet (until DuckDB adds support)")
@click.option(
    '--verbose', is_flag=True, help="Whether to print detailed processing information."
)
@click.option(
    '--output-format',
    callback=handle_comma_separated,
    default='ascii',
    help="The format of the output. Options: ascii, csv, json, chart.",
)
def benchmark(
    input_path,
    output_directory,
    processes,
    formats,
    skip_split_multis,
    no_gpq,
    verbose,
    output_format,
):
    """Runs the convert function on each of the supplied processes and formats, printing the timing of each as a table"""
    results = process_benchmark(
        input_path, output_directory, processes, formats, not skip_split_multis, verbose
    )

    df = pd.DataFrame(results)
    df = df.pivot(index='process', columns='format', values='execution_time')

    base_name = os.path.basename(input_path)
    file_name, file_ext = os.path.splitext(base_name)

    for format in output_format:
        if format == 'csv':
            df.to_csv(f"{output_directory}/{file_name}_benchmark.csv", index=False)
        elif format == 'json':
            df.to_json(f"{output_directory}/{file_name}_benchmark.json", orient='split', indent=4)
        elif format == 'chart':
            df.plot(kind='bar', rot=0)
            plt.title(f'Benchmark for file: {base_name}')
            plt.xlabel('Process')
            plt.ylabel('Execution Time (in seconds)')
            plt.tight_layout()
            plt.savefig(f"{output_directory}/{file_name}_benchmark.png")
            plt.clf()
        elif format == 'ascii':
            df_formatted = df.copy()
            for column in df_formatted.columns:
                df_formatted[column] = df_formatted[column].apply(lambda x: (datetime.min + timedelta(seconds=x)).strftime('%M:%S.%f')[:-3])

            print(f"\nTable for file: {base_name}")
            print(tabulate(df_formatted, headers="keys", tablefmt="fancy_grid"))
        else:
            raise ValueError('Invalid output format')

@google.command('convert')
@click.argument('input_path', type=click.Path(exists=True))
@click.argument('output_directory', type=click.Path(exists=True))
@click.option(
    '--format',
    type=click.Choice(['fgb', 'parquet', 'gpkg', 'shp']),
    default='fgb',
    help="The output format. The default is FlatGeobuf (fgb)",
)
@click.option(
    '--overwrite', is_flag=True, help="Whether to overwrite any existing output files."
)
@click.option(
    '--process',
    type=click.Choice(['duckdb', 'pandas', 'ogr']),
    default='pandas',
    help="The processing method to use. The default is pandas.",
)
@click.option(
    '--skip-split-multis',
    is_flag=True,
    help="Whether to keep multipolygons as they are without splitting into their component polygons.",
)
@click.option(
    '--verbose', is_flag=True, help="Whether to print detailed processing information."
)
def convert(
    input_path, output_directory, format, overwrite, process, skip_split_multis, verbose
):
    """Converts a CSV or a directory of CSV's to an alternate format. Input CSV's are assumed to be from Google's Open Buildings"""
    process_geometries(
        input_path,
        output_directory,
        format,
        overwrite,
        process,
        not skip_split_multis,
        verbose,
    )

@overture.command('add_columns')
@click.argument('input_folder', type=click.Path(exists=True))
@click.argument('output_folder', type=click.Path())
@click.argument('country_parquet_path', type=click.Path(exists=True))
@click.option('--overwrite', is_flag=True, help="Whether to overwrite any existing output files.")
@click.option('--no-quadkey', is_flag=True, help="Whether to add a quadkey column to the output.")
@click.option('--no-country-iso', is_flag=True, help="Whether to add a country_iso column to the output.")
@click.option('--verbose', is_flag=True, help="Whether to print detailed processing information.")
def add_columns(
    input_folder, output_folder, country_parquet_path, overwrite, no_quadkey, no_country_iso, verbose
):
    """Adds columns to the input Overture parquet files, using Overture country for admin boundaries, outputting GeoParquet ordered by quadkey the output folder"""
    add_quadkey = not no_quadkey
    add_country_iso = not no_country_iso
    """Adds columns to the input parquet files, outputting to the output folder"""
    process_parquet_files(
        input_folder, output_folder, country_parquet_path, overwrite, add_quadkey, add_country_iso, verbose
    )

@overture.command('download')
@click.argument('destination_folder', type=click.Path())
@click.option(
    '--theme',
    type=click.Choice(['buildings', 'admins', 'places', 'transportation']),
    default='buildings',
    help="Theme option for the files to download from S3. Default is buildings.",
)
def overture_download(destination_folder, theme):
    """Download building files from S3 (can change theme for other overture data)."""

    os.makedirs(destination_folder, exist_ok=True)

    s3 = boto3.client('s3')
    bucket = 'overturemaps-us-west-2'
    prefix = f"release/2023-07-26-alpha.0/theme={theme}/"
    
    objects = s3.list_objects(Bucket=bucket, Prefix=prefix)
    
    for obj in objects.get('Contents', []):
        print
        file_name = os.path.basename(obj['Key'])
        local_file_path = os.path.join(destination_folder, file_name)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] Downloading {file_name} to {destination_folder}")
        s3.download_file(bucket, obj['Key'], local_file_path)

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] Downloaded {file_name}")

@overture.command('partition')
@click.argument('duckdb-path', type=click.Path(exists=True))
@click.option('--output-folder', default=os.getcwd(), type=click.Path(), help='Folder to store the output files')
@click.option('--geo-conversion', default='gpq', type=click.Choice(['gpq', 'none', 'pandas', 'ogr'], case_sensitive=False))
@click.option('--verbose', is_flag=True, default=False, help='Print verbose output')
@click.option('--max-per-file', default=10000000, type=int, help='Maximum number of rows per file')
@click.option('--row-group-size', default=10000, type=int, help='Row group size for Parquet files')
@click.option('--hive', is_flag=True, default=False, help='Output files in Hive format (folder structure)')
@click.option('--table-name', default='buildings', type=str, help='Name of the table to process')
def partition(duckdb_path, output_folder, geo_conversion, verbose, max_per_file, row_group_size, hive, table_name):
    """Partition a DuckDB database of all overture data by country_iso"""
    process_db(duckdb_path, output_folder, geo_conversion, verbose, max_per_file, row_group_size, hive, table_name)


if __name__ == "__main__":
    sys.exit(main())
