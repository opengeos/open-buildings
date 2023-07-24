"""Console script for open_buildings."""
import sys
import click


@click.group()
def main():
    """Console script for open_buildings."""
    pass


@main.command()
@click.argument('name')
def convert(name):
    """Create a new building with the given name."""
    click.echo(f"Creating a new building: {name}")
    # Add your logic to create a new building here


@main.command()
@click.argument('building_id')
def info(building_id):
    """Get information about a specific building."""
    click.echo(f"Getting information for building with ID: {building_id}")
    click.echo("More info...")
    # Add your logic to fetch building information here


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
