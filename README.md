# CSW to STAC Converter

This project facilitates the conversion of metadata records from a CSW (Catalogue Service for the Web) to STAC (SpatioTemporal Asset Catalog) format that is comptabile for the EDITO STAC. The converter fetches records from a CSW catalog, processes the metadata, supplements metadata, finds relevant assets, and finally adds them to a STAC catalog if a data product has been found. After the local STAC is made, it is transferred to an s3 bucket.  Then the STAC is ingested onto a resto instance, usually "https://api.dive.edito.eu/data/"

Qualifications for what a data product is can be found in utils.py get_mediatype().  Assets where the role returns as ['data'] are considered data products.  If the link works, this is considered a data product.

The csw catalog can be built from other CSW catalogs.  But is primarily for transforming the EMODnet Geonetwork Catalog in CSW format into a STAC catalog compatible for the EDITO STAC.  It does this from specific source catalogs from each of the thematic lots.  Specified in csw_catalog.py

## Installation

### Requirements

- Python 3.10
- Credentials for the 'emodnet' bucket

### Install dependencies

Install the required dependencies using pip:

```bash
pip install -r requirements.txt
```

### Scripts

csw_to_stac.py Main script that runs other scripts and keeps track of processed records

assets.py: Manages assets from each CSW record.  Based on media type, and if it qualifies as an asset viable for the EDITO STAC

add_metadata.py:  Supplements additional metadata to each CSW record if available from Geonetwork XML data.

stac.py:  Makes the STAC Catalog based on the processed_records metadata.  If a CSW record has the right qualifications it will be added into the local STAC.  Each record is added under a variable family catalog, and inside a collection.

stac_to_resto.py:  Ingests the local STAC onto a resto instance, where the EDITO STAC is hosted.  

utils.py: Various utility scripts


### Usage

First setup configuration.  Location of CSW catalog, title of CSW, title of STAC, local stac location, location for STAC on s3.  And if digesting on resto, which instance.

```json
{
    "csw_catalog_title": "emodnetgeonetwork",
    "csw_catalog_url": "https://emodnet.ec.europa.eu/geonetwork/srv/eng/csw",
    "stac_id": "emodnet_geonetwork",
    "STAC_title": "EMODnet Geonetwork",
    "stac_dir": "../data/stac/",
    "stac_s3": "geonetwork_stac",
    "resto_instance": "dive",
    "records_to_process": ["bf0bc42474b39b859495d4e64af4028aa2f452c1"]
}
```
You need credentials in 'data/creds/' to transfer your STAC to the s3 bucket (emods3.env) and ingest on resto (resto.env)

python csw_to_stac.py
