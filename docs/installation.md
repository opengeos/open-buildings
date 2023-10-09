## Installation

To install open-buildings, run this command in your terminal:

```bash
pip install open-buildings
```

This is the preferred method to install open-buildings, as it will always install the most recent stable release.

If you don't have [pip](https://pip.pypa.io) installed, this [Python installation guide](http://docs.python-guide.org/en/latest/starting/installation/) can guide you through the process.

This should add a CLI that you can then use. If it's working then:

```bash
ob
```

Should print out a help message. You then should be able run the CLI (download [1.json](https://data.source.coop/cholmes/aois/1.json):


```bash
ob tools get_buildings 1.json my-buildings.geojson --country_iso RW
```

You can also stream the json in directly in one line:

```
curl https://data.source.coop/cholmes/aois/1.json | ob get_buildings - my-buildings.geojson --country_iso RW
```

## Install From sources

To install open-buildings from sources, run this command in your terminal:

```
pip install git+https://github.com/opengeos/open-buildings
```
