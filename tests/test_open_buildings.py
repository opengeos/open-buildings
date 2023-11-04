import pytest
from typing import Dict, Any
from pathlib import Path
import os
import json
import re
import subprocess

from open_buildings.download_buildings import download, geojson_to_wkt, geojson_to_quadkey, quadkey_to_geojson, geocode
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

def test_geocode():
    """ Tests geocode() using a pre-established true value. """
    assert geocode('plymouth') == {"type": "Polygon", "coordinates": [[[-4.0196056, 50.3327426], [-4.0196056, 50.4441737], [-4.2055324, 50.4441737], [-4.2055324, 50.3327426], [-4.0196056, 50.3327426]]]}

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
def test_download_overwrite(aoi: Dict[str, Any], tmp_path: Path):
    """ Tests that, if the "overwrite" option is set to True, an existing file does indeed get overwritten. """
    output_path = tmp_path.joinpath("file_exists.json")
    with open(output_path, "w") as f:
        f.write("Foo bar")
    
    download(aoi, dst=output_path, country_iso="SC", overwrite=True)
    assert os.path.exists(output_path)
    with open(output_path, "r") as f:
        assert f.read() != "Foo bar" # verify that the file was updated

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
    if format == Format.GEOJSON:
        with open(output_file, "r") as f:
            json.load(f)
    elif format == Format.FLATGEOBUF:
        pass
    elif format == Format.SHAPEFILE:
        pass
    elif format == Format.PARQUET:
        pass
    elif format == Format.GEOPACKAGE:
        pass
    else:
        raise NotImplementedError(f"Test not implemented for {format} - please add.")

def test_download_unknown_format(aoi: Dict[str, Any]):
    """ Tests that an unknown format (.abc) raises an Exception. """
    with pytest.raises(ValueError):
        download(aoi, dst="buildings.abc")

@pytest.mark.integration
@pytest.mark.flaky(reruns=NUM_RERUNS)
def test_cli_get_buildings_from_file_to_directory(aoi: Dict[str, Any], tmp_path: Path):
    """ 
    Tests the CLI for get_buildings - provides the path to a GeoJSON file as input and a directory as output path. 
    Verifies that the output gets written to a default file name in the given directory.
    """
    # write aoi dict to geojson file in temporary directory
    input_path = tmp_path.joinpath("input.json")
    with open(input_path, "w") as f:
        json.dump(aoi, f)
    subprocess.run(["ob", "get_buildings", str(input_path), str(tmp_path), "--country_iso", "SC"])
    output_path = tmp_path.joinpath("buildings.json") # default file name
    assert os.path.exists(output_path)
    assert os.path.getsize(output_path) != 0
    

@pytest.mark.integration
@pytest.mark.flaky(reruns=NUM_RERUNS)
def test_cli_get_buildings_from_stdin_to_directory(aoi: Dict[str, Any], tmp_path: Path):
    """ 
    Tests the CLI for get_buildings - provides a GeoJSON string via stdin and a directory as output path. 
    Verifies that a log message with timestamp gets written to stdout. 
    """
    # we can't use pipes (e.g. f"echo {json.dumps(aoi)} | ...") in subprocess.run, instead we pass the json as stdin using the input/text arguments,
    process = subprocess.run([ "ob", "get_buildings", "-", str(tmp_path), "--country_iso", "SC"], input=json.dumps(aoi), text=True,check=True, capture_output=True)
    dt_regex = re.compile(r"^\[[0-9]{4}(-[0-9]{2}){2} ([0-9]{2}:){2}[0-9]{2}\] ") # match timestamp format e.g. "[2023-10-18 19:08:24]"
    assert dt_regex.search(process.stdout) # ensure that stdout contains at least one timestamped message
    output_path = tmp_path.joinpath("buildings.json") # default file name
    assert os.path.exists(output_path)
    assert os.path.getsize(output_path) != 0

@pytest.mark.integration
@pytest.mark.flaky(reruns=NUM_RERUNS)
def test_cli_get_buildings_from_stdin_to_file_silent(aoi: Dict[str, Any], tmp_path: Path):
    """ 
    Tests the CLI for get_buildings - provides a GeoJSON string via stdin and an exact filepath to write the output to. 
    Verifies that nothing gets written to stdout. 
    """
    output_path = tmp_path.joinpath("test123.json")
    # we can't use pipes (e.g. f"echo {json.dumps(aoi)} | ...") in subprocess.run, instead we pass the json as stdin using the input/text arguments,
    process = subprocess.run(["ob", "get_buildings", "-", str(output_path), "--silent", "--country_iso", "SC"], input=json.dumps(aoi), text=True, check=True, capture_output=True)
    assert process.stdout == "" # assert that nothing gets printed to stdout
    assert process.stderr == "" # assert that nothing gets printed to stdout
    assert os.path.exists(output_path)
    assert os.path.getsize(output_path) != 0


@pytest.mark.integration
@pytest.mark.flaky(reruns=NUM_RERUNS)
def test_cli_get_buildings_from_stdin_to_file_overwrite_false(aoi: Dict[str, Any], tmp_path: Path):
    """ 
    Tests the CLI for get_buildings - provides a GeoJSON string via stdin and an exact filepath to write the output to. 
    Verifies that, if the output file already exists, nothing happens and the user is notified of this. 
    """
    output_path = tmp_path.joinpath("file_exists.json")
    with open(output_path, "w") as f:
        f.write("Foo bar")
    # we can't use pipes (e.g. f"echo {json.dumps(aoi)} | ...") in subprocess.run, instead we pass the json as stdin using the input/text arguments,
    process = subprocess.run(["ob", "get_buildings", "-", str(output_path), "--country_iso", "SC"], input=json.dumps(aoi), text=True, check=True, capture_output=True)
    assert os.path.exists(output_path)
    with open(output_path, "r") as f:
        assert f.read() == "Foo bar" # verify that the file still has the same content as before
    assert "exists" in process.stdout # verify that the user has been warned about the existing file