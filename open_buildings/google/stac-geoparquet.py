# WARNING - Work in progress
# This isn't working yet, but it's close. The main issue is that the catalog
# and collections aren't getting formed right - I want them in the hive partitions, but
# pystac keeps trying to move them in the recommended STAC structure. Committing in case 
# its useful.
# Next approach may just be to form the items individually, as that part seems to be fine,
# and then place them in the catalog and collection manually (maybe pystac can help, but 
# may be easier to just use python to adjust the links)


import os
import pystac
from pystac import Catalog, Collection, Item, Asset, CatalogType
import geopandas as gpd
from datetime import datetime
import click
from shapely.geometry import box
from dateutil.parser import parse

def read_geoparquet_bounds(filepath):
    """
    Reads a Geoparquet file and returns its bounds and EPSG.
    """
    gdf = gpd.read_parquet(filepath)
    bounds = gdf.total_bounds.tolist()
    epsg = gdf.crs.to_epsg()  # Extract the EPSG code
    return bounds, epsg

def create_stac_item_for_geoparquet(filepath, collection, item_datetime):
    filename = os.path.basename(filepath)
    file_id, _ = os.path.splitext(filename)
    title = filename

    # Get the bounds and CRS
    bbox, epsg = read_geoparquet_bounds(filepath)
    
    # Use the bounds as the geometry too
    geometry = box(*bbox).__geo_interface__

    item = Item(id=file_id,
                geometry=geometry, 
                bbox=bbox,
                datetime=item_datetime,
                properties={'title': title, 'proj:epsg': epsg},
                collection=collection.id)

    pystac.extensions.projection.ProjectionExtension.add_to(item)
    item.add_asset(key="data", asset=Asset(href=filepath, media_type="application/parquet"))

    return item

@click.command()
@click.argument('directory', type=click.Path(exists=True))
@click.option('--collection-path', default='collection.json', help='Path to the collection.json file relative to the directory.')
@click.option('--item-datetime', default='2023-05-30T00:00:00Z', help='Datetime for the STAC items.')
@click.option('--catalog-type', type=click.Choice(['SELF_CONTAINED', 'ABSOLUTE_PUBLISHED'], case_sensitive=False), default='SELF_CONTAINED', help='Type of the catalog.')
@click.option('--root-path', default=None, help='Root path for the catalog. Relevant for ABSOLUTE_PUBLISHED catalog type.')
# ... [other necessary imports and functions]

def main(directory, collection_path, item_datetime, catalog_type, root_path):
    catalog_id = 'my-catalog'
    catalog_description = 'A catalog of geoparquet files.'
    item_datetime = parse(item_datetime)
    collection = Collection.from_file(collection_path)
    
    # Create the catalog first
    catalog = Catalog(id=catalog_id, description=catalog_description, catalog_type=CatalogType[catalog_type])
    
    items = []
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".parquet"):
                filepath = os.path.join(root, filename)
                item = create_stac_item_for_geoparquet(filepath, collection, item_datetime)
                
                # Save the item alongside the parquet file
                item_path = os.path.join(root, f"{item.id}.json")
                item.set_self_href(item_path)
                item.save_object()
                items.append(item)
    
     # Create and save the catalog
    catalog_path = os.path.join(directory, 'catalog.json')
    catalog.set_self_href(catalog_path)
    catalog.save_object()

    # Reload the catalog from file
    catalog = Catalog.from_file(catalog_path)

    # Add items to the catalog
    for item in items:
        catalog.add_item(item)
        item.add_link(pystac.Link("parent", os.path.relpath(catalog.get_self_href(), os.path.dirname(item.get_self_href()))))

    # Save the updated catalog
    catalog.save_object()

    # Load the collection and set its links
    collection_path_new = os.path.join(directory, "collection.json")
    collection.set_self_href(collection_path_new)
    collection.add_child(catalog)
    collection.save_object()

if __name__ == "__main__":
    main()
