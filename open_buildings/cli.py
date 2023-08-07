import sys
import os
import click
import pandas as pd
import matplotlib.pyplot as plt
from open_buildings import process_benchmark, process_geometries
from datetime import datetime, timedelta
from tabulate import tabulate


@click.group()
def main():
    """CLI to convert Google Open Building CSV files to alternate formats."""
    pass

def handle_comma_separated(ctx, param, value):
    return value.split(',')

@main.command('benchmark')
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
            # Format execution in the data frame from seconds into a string time with
            # minutes and seconds str(timedelta(seconds=execution_time))
            df_formatted = df.copy()
            for column in df_formatted.columns:
                df_formatted[column] = df_formatted[column].apply(lambda x: (datetime.min + timedelta(seconds=x)).strftime('%M:%S.%f')[:-3])

            print(f"\nTable for file: {base_name}")
            print(tabulate(df_formatted, headers="keys", tablefmt="fancy_grid"))

        else:
            raise ValueError('Invalid output format')

@main.command('convert')
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

if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
