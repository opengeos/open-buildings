"""CLI to convert Google Open Building CSV files to alternate formats."""
import sys
import click


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
    default='ascii',
    help="The format of the output. Options: ascii, csv, json.",
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

    if output_format == 'ascii':
        print(
            tabulate(df, headers="keys", tablefmt="fancy_grid")
        )  # or "grid" if you prefer
    elif output_format == 'csv':
        print(df.to_csv(index=False))
    elif output_format == 'json':
        print(df.to_json(orient='split', indent=4))
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
