"""Console script for open_buildings."""
import sys
import click


@click.group()
def main():
    """Console script for open_buildings."""
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
    help="The processing methods to use.",
)
@click.option(
    '--formats',
    callback=handle_comma_separated,
    default='fgb,parquet,shp,gpkg',
    help="The output formats.",
)
@click.option(
    '--skip-split-multis',
    is_flag=True,
    help="Whether to keep multipolygons as they are without splitting into their component polygons.",
)
@click.option('--no-gpq', is_flag=True, help="Disable GPQ conversion.")
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
    help="The output format.",
)
@click.option(
    '--overwrite', is_flag=True, help="Whether to overwrite existing output files."
)
@click.option(
    '--process',
    type=click.Choice(['duckdb', 'pandas', 'ogr']),
    default='pandas',
    help="The processing method to use.",
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
    process_geometries(
        input_path,
        output_directory,
        format,
        overwrite,
        process,
        not skip_split_multis,
        verbose,
    )

@main.command()
@click.argument('building_id')
def info(building_id):
    """Get information about a specific building."""
    click.echo(f"Getting information for building with ID: {building_id}")
    click.echo("More info...")
    # Add your logic to fetch building information here

if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
