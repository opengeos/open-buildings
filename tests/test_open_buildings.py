import pytest
from typing import Dict, Any
from pathlib import Path
import os
import json

from open_buildings.download_buildings import download, geojson_to_wkt, geojson_to_quadkey, quadkey_to_geojson
from open_buildings.settings import Source, Format, settings

###########################################################################
#                                                                         #
#   RUN TESTS with `python3 -m pytest . -n <number of parallel workers>`  #
#                                                                         #
###########################################################################


NUM_RERUNS = 2 # number of re-runs for integration tests

@pytest.fixture
def aoi() -> Dict[str, Any]:
    """ Sample AOI over Seychelles. """
    return {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "coordinates": [
          [
            [
              55.45280573412927,
              -4.6227964300457245
            ],
            [
              55.45280573412927,
              -4.623440862045413
            ],
            [
              55.453376761871795,
              -4.623440862045413
            ],
            [
              55.453376761871795,
              -4.6227964300457245
            ],
            [
              55.45280573412927,
              -4.6227964300457245
            ]
          ]
        ],
        "type": "Polygon"
      }
    }

def test_geojson_to_wkt(aoi: Dict[str, Any]):
    """ Tests the geojson_to_wkt() function. """
    assert geojson_to_wkt(aoi) == 'POLYGON ((55.45280573412927 -4.6227964300457245, 55.45280573412927 -4.623440862045413, 55.453376761871795 -4.623440862045413, 55.453376761871795 -4.6227964300457245, 55.45280573412927 -4.6227964300457245))'

def test_geojson_to_quadkey(aoi: Dict[str, Any]):
    """ Tests geojson_to_quadkey() using a pre-established true value. """
    assert geojson_to_quadkey(aoi) == '301001330310'

def test_quadkey_to_geojson():
    """ Tests quadkey_to_geojson() using a pre-established true value. """
    assert quadkey_to_geojson('031313131112') == {'type': 'Feature', 'geometry': {'type': 'Polygon', 'coordinates': [[[-0.17578125, 51.50874245880333], [-0.087890625, 51.50874245880333], [-0.087890625, 51.56341232867588], [-0.17578125, 51.56341232867588], [-0.17578125, 51.50874245880333]]]}}

@pytest.mark.integration
@pytest.mark.flaky(reruns=NUM_RERUNS)
@pytest.mark.parametrize("source", [s for s in Source])
def test_download(source: Source, aoi: Dict[str, Any], tmp_path: Path):
    """ Tests that the download function successfully downloads a GeoJSON file from all sources (parametrised test) into a temporary directory (teardown after test). """
    output_file = tmp_path.joinpath(f"output_{source.name}.json")
    download(aoi, source=source, dst=output_file, country_iso="SC")
    assert os.path.exists(output_file)
    assert os.path.getsize(output_file) != 0

@pytest.mark.integration
@pytest.mark.flaky(reruns=NUM_RERUNS)
def test_download_no_output(aoi: Dict[str, Any], tmp_path: Path):
    """ Test that no empty output file gets created if a query doesn't return anything (in this case because a wrong country_iso argument is given.) """
    output_file = tmp_path.joinpath("no_output.json")
    download(aoi, dst=output_file, country_iso="AI") # wrong country, aoi is in SC, not Anguilla
    assert not os.path.exists(output_file)

@pytest.mark.integration
@pytest.mark.flaky(reruns=NUM_RERUNS)
def test_download_directory(aoi: Dict[str, Any], tmp_path: Path):
    """ Test that, if a directory is passed, the output gets downloaded to a default file name in that directory. """
    download(aoi, dst=tmp_path, country_iso="SC")
    assert os.path.exists(tmp_path.joinpath("buildings.json"))
    assert os.path.getsize(tmp_path.joinpath("buildings.json")) != 0

@pytest.mark.integration
@pytest.mark.flaky(reruns=NUM_RERUNS)
@pytest.mark.parametrize("format", [f for f in Format if f != Format.SHAPEFILE]) # fails for shapefile!
def test_download_format(format: Format, aoi: Dict[str, Any], tmp_path: Path):
    """ Requests data in all file formats defined in the settings. Attempts to validate the output for each of those too. """
    output_file = tmp_path.joinpath(f"output.{settings.extensions[format]}")
    download(aoi, dst=output_file, country_iso="SC")
    assert os.path.exists(output_file)
    assert os.path.getsize(output_file) != 0

    # validate output
    match format:
        case Format.GEOJSON:
            with open(output_file, "r") as f:
                json.load(f)
        case Format.FLATGEOBUF:
            pass
        case Format.SHAPEFILE:
            pass
        case Format.PARQUET:
            pass
        case Format.GEOPACKAGE:
            pass
        case _:
            raise NotImplementedError(f"Test not implemented for {format} - please add.")

def test_download_unknown_format(aoi: Dict[str, Any]):
    """ Tests that an unknown format (.abc) raises an Exception. """
    with pytest.raises(ValueError):
        download(aoi, dst="buildings.abc")