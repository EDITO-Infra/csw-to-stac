
import os
import logging
import re
import requests
import json
import math
from datetime import datetime, timezone
from pytz import utc
import pytz
from dateutil import parser
import boto3
import pandas as pd
from tqdm import tqdm
import xarray as xr
import dotenv


today = datetime.today().strftime('%Y-%m-%d')

logger = logging.getLogger('csw_to_stac')

dotenv_path = f'../data/creds/emods3.env'
if dotenv.load_dotenv(dotenv_path):
    logger.debug(f"Loaded .env file from {dotenv_path}")
else:
    logger.error(f"Failed to load .env file from {dotenv_path}")
access_key = os.getenv('EMOD_ACCESS_KEY')
secret_key = os.getenv('EMOD_SECRET_KEY')
bucket = 'emodnet'
host = 'https://s3.waw3-1.cloudferro.com'

class Utils:

    @staticmethod
    def get_logger(    
            LOG_FORMAT     = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s script:%(filename)s line:%(lineno)d',
            LOG_NAME       = '',
            LOG_DIRECTORY  = 'logs/',
            append_logs    = True):
        """
        Configures and returns a logger with handlers for info, error, and warning levels.

        :param LOG_FORMAT: Format string for log messages. Defaults to a custom format with time, level, and message.
        :type LOG_FORMAT: str, optional
        :param LOG_NAME: Name of the logger. Defaults to an empty string.
        :type LOG_NAME: str, optional
        :param LOG_DIRECTORY: Directory where log files will be saved. Defaults to '../data/logs/'.
        :type LOG_DIRECTORY: str, optional
        :param append_logs: If True, logs will be appended to existing files, otherwise overwritten. Defaults to False.
        :type append_logs: bool, optional
        :return: Configured logger instance.
        :rtype: logging.Logger
        """
        mode = 'a' if append_logs else 'w'
        
        os.makedirs(LOG_DIRECTORY, exist_ok=True)
        log_file_info = os.path.join(LOG_DIRECTORY, f"info_log_{today}.txt")
        log_file_error = os.path.join(LOG_DIRECTORY, f"error_log_{today}.txt")
        log_file_warning = os.path.join(LOG_DIRECTORY, f"warning_log_{today}.txt")

        log           = logging.getLogger(LOG_NAME)
        log_formatter = logging.Formatter(LOG_FORMAT)
        
        log_handler_info = logging.FileHandler(log_file_info, mode=mode)
        log_handler_info.setFormatter(log_formatter)
        log_handler_info.setLevel(logging.INFO)
        log.addHandler(log_handler_info)

        error_log_formatter = logging.Formatter('%(asctime)s - %(filename)s - %(message)s')
        log_handler_error = logging.FileHandler(log_file_error, mode=mode)
        log_handler_error.setFormatter(error_log_formatter)
        log_handler_error.setLevel(logging.ERROR)
        log.addHandler(log_handler_error)

        log_handler_warning = logging.FileHandler(log_file_warning, mode=mode)
        log_handler_warning.setFormatter(log_formatter)
        log_handler_warning.setLevel(logging.WARNING)
        log.addHandler(log_handler_warning)

        log.setLevel(logging.INFO)

        return log

   

    @staticmethod
    def custom_slugify(text, separator='_'):
        # Replace special characters with separator
        text = re.sub(r'[-\s]+', separator, text)
        # Replace dots followed by a digit with separator
        text = re.sub(r'\.(\d)', r'.\1', text)
        # Remove other non-alphanumeric characters
        text = re.sub(r'[^\w\s.-]', '', text)
        return text.lower()

    # add relevant asset types here
    @staticmethod
    def test_link(id, download_url):
        try:
            response = requests.get(download_url, timeout=10, stream=True)
            response.raise_for_status()
            if response.status_code == 200:
                return True
            else:
                logger.error(f"{id} {download_url} Failed {response.status_code}")
                return False
        except requests.RequestException as e:
            logger.error(f"{id} {download_url} Failed {e}")
            return None
    @staticmethod
    def make_stac_assets(links):
        """
        Create a dictionary of assets from the provided links, representing each asset as a JSON object.

        :param links: List of URLs representing asset links.
        :return: Dictionary of assets with asset types as keys and asset details as JSON objects.
        """
        assets = {}
        for link in links:
            asset_type, title, mediatype, role = Utils.get_media_type(link)
            if not asset_type or not mediatype:
                logger.warning(f'Failed to get asset type for link: {link}')
                continue
            asset = {
                "href": link,
                "title": title,
                "media_type": mediatype,
                "roles": role
            }
            assets[asset_type] = asset
        return assets
    @staticmethod
    def get_media_type(asset_url: str) -> tuple[str, str, str, list[str]]:
        """
        Get the media type of a STAC asset based on its URL or file extension.

        :param asset_url: The URL of the asset.
        :type asset_url: str
        :return: A tuple containing asset type, title, media type, and role.
        :rtype: tuple(str, str, str, list[str])
        :raises ValueError: If the media type is not supported.
        """
        logger.info(f"Getting media type for asset: {asset_url}")
        media_types = {
            '.zarr': ('zarr', 'Zarr', 'application/vnd+zarr', ['data']),
            '.zarr/': ('zarr', 'Zarr', 'application/vnd+zarr', ['data']),
            '.nc': ('netcdf', 'NetCDF', 'application/vnd+netcdf', ['data']),
            '.zip': ('zip', 'Zip', 'application/zip', ['data']),
            '.tif': ('geotiff', 'GeoTIFF', 'image/tiff; application=geotiff', ['data']),
            '.tiff': ('geotiff', 'GeoTIFF', 'image/tiff; application=geotiff', ['data']),
            '.parquet': ('parquet', 'Parquet', 'application/vnd+parquet', ['data']),
            '.parquet/': ('parquet', 'Parquet', 'application/vnd+parquet', ['data']),
            '.geoparquet': ('geoparquet', 'GeoParquet', 'application/vnd+parquet', ['data']),
            '.geoparquet/': ('geoparquet', 'GeoParquet', 'application/vnd+parquet', ['data']),
            '.csv': ('csv', 'CSV', 'text/csv', ['data']),
            '.json': ('json', 'JSON', 'application/json', ['metadata']),
            '.html': ('html', 'HTML', 'text/html', ['metadata']),
            '.png': ('png', 'PNG', 'image/png', ['thumbnail']),
            '.jpg': ('jpg', 'JPEG', 'image/jpeg', ['thumbnail']),
        }

        url_patterns = {
            'opendap': ('opendap', 'OPeNDAP ', 'application/opendap', ['data']),
            'wms': ('wms', 'WMS', 'OGC:WMS', ['thumbnail']),
            'wfs': ('wfs', 'WFS', 'OGC:WFS', ['data']),
            'wms?SERVICE=WMS&REQUEST=GetMap': ('wms', 'WMS', 'OGC:WMS', ['thumbnail']),
            'wfs?SERVICE=WFS&REQUEST=GetFeature': ('wfs', 'WFS', 'OGC:WFS', ['data']),
            "request=GetFeature&outputFormat=text%2Fcsv": ('wfscsv', 'CSV', 'text/csv', ['data']),
            'xml': ('xml', 'XML', 'application/xml', ['metadata']),
            r'csw\?request=': ('csw', 'CSW', 'application/csw', ['metadata']),
            'doi.org': ('doi', 'DOI', 'application/vnd+doi', ['data']),
            'doi:': ('doi', 'DOI', 'application/vnd+doi', ['data']),
            "eurobis.org/toolbox/en/download/": ('eurobistoolbox', 'Eurobis toolbox', 'application/html', ['metadata']),
            r'gbif.org/dataset/' : ('gbifdataset', 'GBIF Dataset', 'application/html', ['metadata']),
            r'ipt\.[a-zA-Z0-9-]+\.[a-zA-Z]+(?:/.*)?/resource\?r=': ('iptresource', 'IPT Resource', 'application/html', ['metadata']),
            
            r'ipt\.[a-zA-Z0-9-]+\.[a-zA-Z]+(?:/.*)?(?:/archive\.do\?)': ('iptdwca', 'Darwin Core Archive', 'application/zip', ['data']),
            "mda.vliz.be/directlink.php?": ('mdazip', 'Zip', 'application/zip', ['data']),
            "mda.vliz.be/mda/directlink.php?": ('mdazip', 'Zip', 'application/zip', ['data']),
        }

        # Check file extensions first
        for ext, (asset_type, title, mediatype, role) in media_types.items():
            if asset_url.endswith(ext):
                return asset_type, title, mediatype, role

        # Check URL patterns next
        for pattern, (asset_type, title, mediatype, role) in url_patterns.items():
            if re.search(pattern, asset_url):
                return asset_type, title, mediatype, role
            
        logger.warning(f"Valid media type not found for asset: {asset_url}")
        return None, None, None, None
        
    @staticmethod
    def convert_eurobis_toolbox_to_wfs(id, url):
        """
        Convert a Eurobis Toolbox URL to a WFS URL.

        :param url: The URL of the Eurobis Toolbox.
        :type url: str
        :return: The converted WFS URL.
        :rtype: str
        """
        logger.info(f"Converting Eurobis Toolbox URL to WFS: {url}")
        
        base_url = "https://geo.vliz.be/geoserver/Dataportal/wfs?service=wfs&version=1.1.0&typeName=eurobis-obisenv_basic&request=GetFeature&outputFormat=text%2Fcsv&viewParams=datasetid%3A"
        dataset_id = url.split('/')[-1]
        wfs_url = f"{base_url}{dataset_id}"
        logger.info(f"Converted Eurobis Toolbox URL to WFS: {wfs_url}, testing")
        if Utils.test_wfs(id, wfs_url, 'csv'):
            logger.info(f"Successfully converted Eurobis Toolbox URL to WFS: {wfs_url}")
        else:
            logger.error(f"Failed to convert Eurobis Toolbox URL to WFS: {url}")    
            return None
        return wfs_url

    @staticmethod
    def convert_ipt_to_dwca(id, ipt_link):
        # Convert /resource?r= to /archive.do?r= if applicable
        # Ensure the link starts with https
        if ipt_link.startswith("http://"):
            ipt_link = ipt_link.replace("http://", "https://")

        if "/resource?r=" in ipt_link:
            dwca_url = ipt_link.replace("/resource?r=", "/archive.do?r=")
            logger.info(f"Converted IPT link to DwC-A: {dwca_url}")
        elif "/archive.do" in ipt_link:
            dwca_url = ipt_link 
        else:
            raise ValueError("Invalid IPT link format.")

        # Test the DwC-A URL
        if Utils.test_link(id, dwca_url):
            logger.info(f"Successfully converted IPT link to DwC-A: {dwca_url}")
            return dwca_url
        else:
            logger.error(f"Failed to convert IPT link to DwC-A: {ipt_link}")
            return None
    @staticmethod
    def test_mda(id, mdalink):
        """
        Test an MDA direct link to check if it returns a valid response.

        :param id: The identifier of the MDA dataset.
        :type id: str
        :param mdalink: The direct link to the MDA dataset.
        :type mdalink: str
        :return: The direct link if it returns a valid response, otherwise None.
        :rtype: str
        """
        logger.info(f"Testing MDA direct link: {mdalink}")
        # check if https in link
        if 'https://' not in mdalink:
            mdalink = 'https://' + mdalink
        if '/mda/' in mdalink:
            mdalink = mdalink.replace('/mda/', '/')

        if Utils.test_link(id, mdalink):
            logger.info(f"Successfully tested MDA direct link: {mdalink}")
            return mdalink
        else:
            logger.error(f"Failed to test MDA direct link: {mdalink}")
            return None
    @staticmethod
    def test_wfs(id, dataset_url, expected_format="csv"):
        """
        Test a WFS dataset URL by checking if the content type matches the expected format.

        :param id: The identifier of the WFS dataset.
        :type id: str
        :param dataset_url: The URL of the WFS dataset.
        :type dataset_url: str
        :param expected_format: The expected format of the dataset (e.g., "csv", "json", "shp").
        :type expected_format: str
        """
        logger.info(f"Testing WFS dataset: {dataset_url} for expected format: {expected_format}")

        # Define MIME types for different expected formats
        expected_formats = {
            'csv': 'text/csv',
            'json': 'application/json',
            'shp': 'application/x-shapefile'
        }
        
        # Get the MIME type for the expected format
        expected_mime = expected_formats.get(expected_format.lower())
        if not expected_mime:
            logger.error(f"Unsupported format specified: {expected_format}")
            return False
        
        try:
            # Send a HEAD request to check the Content-Type header
            response = requests.head(dataset_url)
            response.raise_for_status()
            
            # Retrieve and check the Content-Type
            content_type = response.headers.get('Content-Type', '').lower()
            logger.info(f"Received Content-Type: {content_type}")

            # Check if the content type matches the expected MIME type
            if expected_mime in content_type:
                logger.info(f"{id}: Content-Type matches expected format ({expected_format}).")
                return True
            else:
                logger.warning(f"{id}: Unexpected Content-Type ({content_type}). Expected: {expected_mime}")
                return False

        except requests.RequestException as e:
            logger.error(f"Error testing WFS dataset {id} at {dataset_url}: {e}")
            return False
            
    @staticmethod
    def test_opendap(id, dataset_url):
        """
        Test an OpenDAP dataset URL by trying to open it with xarray.

        :param dataset_url: The URL of the OpenDAP dataset.
        :type dataset_url: str
        
        :type session: requests.Session
        """
        logger.info(f"Testing OpenDAP dataset: {dataset_url}")
        session = requests.Session()
        try:
            store = xr.backends.PydapDataStore.open(dataset_url, session=session)
            ds = xr.open_dataset(store)
            
            if ds:
                logger.info(f"Successfully opened the dataset: {dataset_url}")
                return dataset_url
        except Exception as e:
            logger.error(f"{id} Failed to open the opendap dataset: {e}")
            return None
    @staticmethod
    def test_wms_endpoint(layer):
        
        properties = layer.metadata.get('properties')
        base_url = properties['url']
        layers = properties['params']['LAYERS']
        version = properties['params']['VERSION']
        time = properties['params'].get('TIME')
        wms_extent = properties.get('extent', layer.metadata.get('geographic_extent', [-180, -90, 180, 90]))
        bbox = f"{wms_extent[0]},{wms_extent[1]},{wms_extent[2]},{wms_extent[3]}"
        wms_request_url = f"{base_url}?SERVICE=WMS&REQUEST=GetMap&LAYERS={layers}&VERSION={version}&CRS=CRS:84&BBOX={bbox}&WIDTH=800&HEIGHT=600&FORMAT=image/png"
        if time:
            wms_request_url += f"&time={time}"
        if 'GetLegendGraphic' in base_url:
            logger.info("GetLegendGraphic URL detected, skipping WMS request.")
            return
        try:
            response = requests.get(wms_request_url)
        except Exception as e:
            logger.info(f"Request to {wms_request_url} failed with error {e}.")
            return
        if response.status_code != 200:
            logger.info(f"Request to {wms_request_url} failed with status code {response.status_code}.")
            return 

        if response.headers['Content-Type'] != 'image/png':
            logger.info(f"Request to {wms_request_url} returned content of type {response.headers['Content-Type']}, expected 'image/png'.")
            return
        
        logger.info(f"Request to {wms_request_url} was successful and returned an image.")
        layer.metadata['assets'].append(wms_request_url)
        return

    @staticmethod
    def format_datetime_to_iso8601(datetime_str):
        """
        Convert a datetime string into a STAC-compliant ISO 8601 UTC string.

        :param datetime_str: A datetime string.
        :type datetime_str: str
        :return: A STAC-compliant ISO 8601 UTC string.
        :rtype: str
        :raises ValueError: If the datetime string cannot be parsed.
        """
        if len(datetime_str) == 4:
            datetime_str += '-01-01T00:00:00Z'
        elif len(datetime_str) == 7:
            datetime_str += '-01T00:00:00Z'
        if len(datetime_str) == 10:
            datetime_str += 'T00:00:00Z'
        elif len(datetime_str) == 16:
            datetime_str += ':00Z'
        elif len(datetime_str) == 19:
            datetime_str += 'Z'

        try:
            # Parse and reformat into ISO 8601 with UTC
            dt = parser.parse(datetime_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)  # Assume naive datetime as UTC
            else:
                dt = dt.astimezone(timezone.utc)  # Convert to UTC
            return dt.isoformat().replace('+00:00', 'Z')  # Explicit UTC format
        except ValueError as e:
            logger.error(f"Error parsing datetime string '{datetime_str}': {e}")
            return None  # Returning None if parsing fails
    
    @staticmethod
    def format_start_end_datetimes_to_iso8601(metadata):
        """
        Convert start and end datetime strings into STAC-compliant ISO 8601 UTC strings.

        :param metadata: A dictionary with 'start_datetime' and 'end_datetime' strings.
        :type metadata: dict
        :return: A dictionary with 'start_datetime' and 'end_datetime' in ISO 8601 format.
        :rtype: dict
        """
        metadata['start_datetime'] = Utils.format_datetime_to_iso8601(metadata.get('start_datetime'))
        metadata['end_datetime'] = Utils.format_datetime_to_iso8601(metadata.get('end_datetime'))

        return metadata
    @staticmethod
    def format_datetime(datetime_str):
        """
        Convert a datetime string into a Python datetime object in UTC format.

        :param datetime_str: A datetime string.
        :type datetime_str: str
        :return: A datetime object in UTC format.
        :rtype: datetime
        :raises ValueError: If the datetime string cannot be parsed.
        """
        if len(datetime_str) == 4:
            datetime_str += '-01-01T00:00:00Z'
        elif len(datetime_str) == 7:
            datetime_str += '-01T00:00:00Z'
        if len(datetime_str) == 10:
            datetime_str += 'T00:00:00Z'
        elif len(datetime_str) == 16:
            datetime_str += ':00Z'
        elif len(datetime_str) == 19:
            datetime_str += 'Z'
    
        try:
            # Parse the datetime string with dateutil to handle various formats
            dt = parser.parse(datetime_str)
            # Convert to UTC if it's not already
            dt = dt.astimezone(pytz.utc)
            return dt
        except ValueError as e:
            logger.error(f"Error converting datetime string: {e}")
            return datetime.today().replace(tzinfo=pytz.utc)
    @staticmethod
    def format_start_end_datetimes_stac(metadata):
        """
        Convert start and end datetime strings from a temporal extent into Python datetime objects.

        :param temporal_extent: A dictionary with 'start' and 'end' datetime strings.
        :type temporal_extent: dict
        :return: A tuple containing the start and end datetimes in UTC format.
        :rtype: tuple(datetime, datetime)
        :raises ValueError: If the datetime strings cannot be parsed.
        """
        metadata['start_datetime'] = Utils.format_datetime(metadata['start_datetime'])
        metadata['end_datetime'] = Utils.format_datetime(metadata['end_datetime'])

        return metadata

    def update_providers(metadata):
        
        provider_dict = {}  
        
        if 'creator' in metadata and metadata['creator']:
            provider_dict['name'] = metadata['creator']
            provider_dict['roles'] = 'creator'
        if 'publisher' in metadata and metadata['publisher']:
            provider_dict['name'] = metadata['publisher']
            provider_dict['roles'] = 'publisher'
        if 'distributor' in metadata and metadata['distributor']:
            provider_dict['name'] = metadata['distributor']
            provider_dict['roles'] = 'distributor'
        
        if not provider_dict:
            logger.warning('No provider found in csw metadata')
            provider = Utils.lookup_thematic_lot(metadata)
            provider_dict['name'] = provider
            provider_dict['roles'] = 'provider'

        metadata['provider'].append(provider_dict)
        return metadata
    
    @staticmethod
    def lookup_collection(metadata):
        collection_dict = {
            'seabed-habitats': 'Seabed Habitats',
            'seabedhabitats': 'Seabed Habitats',
            'emodnet-seabedhabitats': 'Seabed Habitats',
            "EMODnet Seabed Habitats": 'Seabed Habitats',
            'bathymetry': 'Bathymetry',
            'EMODnet Bathymetry': 'Bathymetry',
            'emodnet-bathymetry': 'Bathymetry',
            'geology': 'Geology',
            'EMODnet Geology': 'Geology',
            'emodnet-geology': 'Geology',
            'chemistry': 'Chemistry',
            'emodnet-chemistry': 'Chemistry',
            'EMODnet Chemistry': 'Chemistry',
            'gbif': 'Biology',
            'eurobis': 'Biology',
            'obis': 'Biology',
            'ipt': 'Biology',
            'mda.vliz': 'Biology',
            'emodnet-biology': 'Biology',
            'EMODnet Biology': 'Biology',
            'emodnet-physics': 'Physics',
            'EMODnet Physics': 'Physics',
            'physics': 'Physics',
            'humanactivities': 'Human Activities',
            'emodnet-humanactivities': 'Human Activities',
            'EMODnet Human Activities': 'Human Activities'
        }
        
        # check the provider metadata
        if 'provider' in metadata and metadata['provider']:
            for provider in metadata['provider']:
                if 'name' in provider:
                    for key, value in collection_dict.items():
                        if key in provider['name']:
                            logger.info(f"{key} found in provider {provider['name']}, collection {value} match")
                            return value
        # Check each asset individually
        if 'assets' in metadata and metadata['assets']:
            for asset in metadata['assets']:
                for key, value in collection_dict.items():
                    if key in asset:
                        logger.info(f"{key} found in asset {asset}, collection {value} match")
                        return value
        
        # Check the abstract
        if 'abstract' in metadata and metadata['abstract']:
            abstract = metadata['abstract']
            for key, value in collection_dict.items():
                if key in abstract:
                    logger.info(f"{key} found in abstract, collection {value} match")
                    return value
                
        if 'subjects' in metadata and metadata['subjects']:
            subjects = metadata['subjects']
            for key, value in collection_dict.items():
                if key in subjects:
                    logger.info(f" {key} found in subjects collection {value} match")
                    return value

        return 'EMODnet'
    

    @staticmethod
    def lookup_thematic_lot(metadata):
        geonetwork_uri = metadata['geonetwork_uri']
        request_url = f"https://emodnet.ec.europa.eu/geonetwork/srv/eng/q?_content_type=json&facet.q=sourceCatalog%2F{geonetwork_uri}&resultType=details"

        emodnet_provider_dict = {
            'seabed-habitats': 'EMODnet Seabed Habitats',
            'seabedhabitats': 'EMODnet Seabed Habitats',
            'emodnet-seabedhabitats': 'EMODnet Seabed Habitats',
            "EMODnet Seabed Habitats": 'EMODnet Seabed Habitats',
            'bathymetry': 'EMODnet Bathymetry',
            'EMODnet Bathymetry': 'EMODnet Bathymetry',
            'emodnet-bathymetry': 'EMODnet Bathymetry',
            'geology': 'EMODnet Geology',
            'EMODnet Geology': 'EMODnet Geology',
            'emodnet-geology': 'EMODnet Geology',
            'chemistry': 'EMODnet Chemistry',
            'emodnet-chemistry': 'EMODnet Chemistry',
            'EMODnet Chemistry': 'EMODnet Chemistry',
            'eurobis': 'EMODnet Biology',
            'obis': 'EMODnet Biology',
            'gbif': 'EMODnet Biology',
            'ipt': 'EMODnet Biology',
            'mda.vliz': 'EMODnet Biology',
            'emodnet-biology': 'EMODnet Biology',
            'EMODnet Biology': 'EMODnet Biology',
            'emodnet-physics': 'EMODnet Physics',
            'EMODnet Physics': 'EMODnet Physics',
            'physics': 'EMODnet Physics',
            'humanactivities': 'EMODnet Human Activities',
            'emodnet-humanactivities': 'EMODnet Human Activities',
            'EMODnet Human Activities': 'EMODnet Human Activities',
            
        }
        
        # Check each asset individually
        if 'assets' in metadata and metadata['assets']:
            for asset in metadata['assets']:
                for key, value in emodnet_provider_dict.items():
                    if key in asset:
                        logger.info(f"{key} found in asset {asset}, thematic lot {value} match")
                        return value
        
        # Check the abstract
        if 'abstract' in metadata and metadata['abstract']:
            abstract = metadata['abstract']
            for key, value in emodnet_provider_dict.items():
                if key in abstract:
                    logger.info(f"{key} found in abstract, thematic lot {value} match")
                    return value
                
        if 'subjects' in metadata and metadata['subjects']:
            subjects = metadata['subjects']
            for key, value in emodnet_provider_dict.items():
                if key in subjects:
                    logger.info(f" {key} found in subjects thematic lot {value} match")
                    return value
        logger.warning('No provider found in csw subjects, abstract or assets to link to thematic lot')
        return 'EMODnet'

    @staticmethod
    def lookup_variable_family(metadata):
        """
        Determine the provider, collection, and variable family from the metadata.
        
        :param metadata: A dictionary containing metadata.
        :return: A tuple (provider, collection, variable_family).
        """
        
        emodnet_metadata_dict = {
            'litter': {'variable_family': 'litter'},
            'oxygen': {'variable_family': 'o2'},
            'alkalinity': {'variable_family': 'ph'},
            'acidity': {'variable_family': 'ph'},
            'salinity': {'variable_family': 'Salinity'},
            'contaminants': {'variable_family': 'Contaminants'},
            'phosphate': {'variable_family': 'Nutrients'},
            'nitrate': {'variable_family': 'Nutrients'},
            'silicate': {'variable_family': 'Nutrients'},
            'currents': {'variable_family': 'Currents'},
            'temperature': {'variable_family': 'Temperature'},
            'waves': {'variable_family': 'Waves'},
            'elevation': {'variable_family': 'Elevation'},
            'seabed-habitats': {'variable_family': 'Seabed habitats'},
            'seabedhabitats': {'variable_family': 'Seabed habitats'},
            'emodnet-seabedhabitats': {'variable_family': 'Seabed habitats'},
            'EMODnet Seabed Habitats': {'variable_family': 'Seabed habitats'},
            'bathymetry': {'variable_family': 'Elevation'},
            'EMODnet Bathymetry': {'variable_family': 'Elevation'},
            'emodnet-bathymetry': {'variable_family': 'Elevation'},
            'geology': {'variable_family': 'Marine geology'},
            'EMODnet Geology': {'variable_family': 'Marine geology'},
            'emodnet-geology': {'variable_family': 'Marine geology'},
            'chemistry': {'variable_family': 'Chemistry'},
            'emodnet-chemistry': {'variable_family': 'Chemistry'},
            'EMODnet Chemistry': {'variable_family': 'Chemistry'},
            'eurobis': {'variable_family': 'Biodiversity'},
            'gbif': {'variable_family': 'Biodiversity'},
            'obis': {'variable_family': 'Biodiversity'},
            'ipt': {'variable_family': 'Biodiversity'},
            'mda.vliz': {'variable_family': 'Biodiversity'},
            'emodnet-biology': {'variable_family': 'Biodiversity'},
            'EMODnet Biology': {'variable_family': 'Biodiversity'},
            'emodnet-physics': {'variable_family': 'Physics'},
            'EMODnet Physics': {'variable_family': 'Physics'},
            'physics': {'variable_family': 'Physics'},
            'humanactivities': {'variable_family': 'Human marine activities'},
            'emodnet-humanactivities': {'variable_family': 'Human marine activities'},
            'EMODnet Human Activities': {'variable_family': 'Human marine activities'},
        }
        
        # Iterate through metadata fields for matching
        for field in ['assets', 'title', 'subjects']:
            if field in metadata and metadata[field]:
                values = metadata[field] if isinstance(metadata[field], list) else [metadata[field]]
                for value in values:
                    for key, attributes in emodnet_metadata_dict.items():
                        if key in str(value).lower():  # Case-insensitive matching
                            logger.info(f"Match found for {key} in {field}: {attributes}")
                            return attributes['variable_family']
        # or just use provider name
        if 'provider' in metadata and metadata['provider']:
            provider_list = metadata['provider']
            for provider in provider_list:
                if 'name' in provider:
                    for key, attributes in emodnet_metadata_dict.items():
                        if provider['name'] in key:
                            logger.info(f"Match found for {key} in provider: {attributes}")
                            return attributes['variable_family']
        # No match found, return default values
        logger.warning('match found in metadata')
        return 'EMODnet'


    def update_datetimes(metadata):
        logger.info('Updating datetimes')
        datetime_keys = ['created', 'date', 'issued', 'modified']
        
        for key in datetime_keys:
            if key in metadata and metadata[key]:
                if 'start_datetime' in metadata:
                    # check if earlier than current start_datetime
                    if metadata[key] < metadata['start_datetime']:
                        metadata['start_datetime'] = metadata[key]
                if 'end_datetime' in metadata:
                    # check if later than current end_datetime
                    if metadata[key] > metadata['end_datetime']:
                        metadata['end_datetime'] = metadata[key]
                if 'start_datetime' not in metadata:
                    metadata['start_datetime'] = metadata[key]
                if 'end_datetime' not in metadata:
                    metadata['end_datetime'] = metadata[key]
                
        if 'start_datetime' not in metadata:
            # use 1970-01-01 as default start datetime
            logger.warning('Start datetime not found, using 1970-01-01 as default')
            metadata['start_datetime'] = datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        if 'end_datetime' not in metadata or metadata['end_datetime'] == metadata['start_datetime']:
            # use 2100 as default end datetime
            logger.warning('End datetime not found, using 2100-01-01 as default')
            metadata['end_datetime'] = datetime(2100, 1, 1, 0, 0, 0, 0, tzinfo=utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        logger.info(f"Start datetime: {metadata['start_datetime']}, End datetime: {metadata['end_datetime']}")
        return metadata

    @staticmethod
    def finalize_boundaries(metadata):
        
        logger.info('Finalizing boundaries')
        
        def validate_and_convert(coord, min_val, max_val, coord_name):
            try:
                coord = float(coord)
                if coord < min_val or coord > max_val:
                    logger.warning(f'Invalid {coord_name} {coord}, setting to {min_val if coord < min_val else max_val}')
                    return min_val if coord < min_val else max_val
                return coord
            except (ValueError, TypeError):
                logger.warning(f'Invalid {coord_name} {coord}, setting to {min_val if "min" in coord_name else max_val}')
                return min_val if "min" in coord_name else max_val

        if 'bbox' in metadata and metadata['bbox']:
            logger.info('using CSW bbox')
            bbox = metadata['bbox']

            # handle the case of a list of coordinates
            if any(
                pd.isna(coord) or
                (isinstance(coord, float) and math.isnan(coord)) or
                (isinstance(coord, str) and coord.lower() == 'nan')
                for coord in bbox
            ):
                logger.warning(f'Invalid coordinates in bbox {bbox} setting to world')
                return [-180, -90, 180, 90]

            lon_min = validate_and_convert(bbox[0], -180, 180, 'longitude minimum')
            lat_min = validate_and_convert(bbox[1], -90, 90, 'latitude minimum')
            lon_max = validate_and_convert(bbox[2], -180, 180, 'longitude maximum')
            lat_max = validate_and_convert(bbox[3], -90, 90, 'latitude maximum')

            return [lon_min, lat_min, lon_max, lat_max]
        elif 'geographic_extent' in metadata and metadata['geographic_extent']:
            logger.info('using geonetwork extent')
            bbox = metadata['geographic_extent']
            if any(
                pd.isna(coord) or
                (isinstance(coord, float) and math.isnan(coord)) or
                (isinstance(coord, str) and coord.lower() == 'nan')
                for coord in bbox
            ):
                logger.warning(f'Invalid coordinates in bbox {bbox} setting to world')
                return [-180, -90, 180, 90]

            lon_min = validate_and_convert(bbox[0], -180, 180, 'longitude minimum')
            lat_min = validate_and_convert(bbox[1], -90, 90, 'latitude minimum')
            lon_max = validate_and_convert(bbox[2], -180, 180, 'longitude maximum')
            lat_max = validate_and_convert(bbox[3], -90, 90, 'latitude maximum')

            return [lon_min, lat_min, lon_max, lat_max]
        else:
            logger.warning('no boundaries found, using world coordinates')
            return [-180, -90, 180, 90]
   
    

    

class S3Utils:
    def __init__(self):
        self.credsfile = '../../data/emods3.env'
        import dotenv
        dotenv.load_dotenv(self.credsfile)
        self.access_key = os.getenv('EMOD_ACCESS_KEY')
        self.secret_key = os.getenv('EMOD_SECRET_KEY')
        self.bucket_name = 'emodnet'
        self.host = 'https://s3.waw3-1.cloudferro.com'
        self.s3_client = boto3.client('s3', aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key, endpoint_url=self.host)
    

    def list_s3_files(self, bucket_name, s3_loc):
        """
        List all files in a given S3 location.

        :param s3_client: The Boto3 S3 client.
        :type s3_client: boto3.client
        :param bucket_name: The name of the S3 bucket.
        :type bucket_name: str
        :param s3_loc: The S3 location (prefix) to list files.
        :type s3_loc: str
        :return: A dictionary mapping S3 file keys to their last modified timestamps.
        :rtype: dict
        """
        s3_files = {}
        paginator = self.s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_loc):
            for content in page.get('Contents', []):
                s3_files[content['Key']] = content['LastModified']
        return s3_files


    def sync_to_s3(self, path, s3_loc):
        """
        Synchronize a local directory or file with a remote S3 location.

        :param path: The local file or directory to sync.
        :type path: str
        :param s3_loc: The S3 location (prefix) to sync with.
        :type s3_loc: str
        :return: The S3 URL after successful sync, or None on failure.
        :rtype: str
        """

        s3_files = self.list_s3_files(self.bucket_name, s3_loc)

        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, path)
                    s3_key = f"{s3_loc}/{relative_path}"
                    if s3_key not in s3_files or os.path.getmtime(local_path) > s3_files[s3_key].timestamp():
                        try:
                            with tqdm(total=os.path.getsize(local_path), unit='B', unit_scale=True, desc=file, ncols=80) as pbar:
                                self.s3_client.upload_file(local_path, self.bucket_name, s3_key, Callback=lambda bytes_sent: pbar.update(bytes_sent))
                                logger.info(f"Successfully uploaded {local_path} to S3 as {s3_key}")
                        except Exception as e:
                            logger.error(f"Failed to upload {file} to S3: {str(e)}")
                            return None
        else:
            file_name = os.path.basename(path)
            s3_key = f"{s3_loc}/{file_name}"
            if s3_key not in s3_files or os.path.getmtime(path) > s3_files[s3_key].timestamp():
                total_size = os.path.getsize(path)
                with tqdm(total=total_size, unit='B', unit_scale=True, desc=path, ncols=80) as pbar:
                    try:
                        self.s3_client.upload_file(path, self.bucket_name, s3_key, Callback=lambda bytes_sent: pbar.update(bytes_sent))
                        logger.info(f"Successfully uploaded {path} to S3 as {s3_key}")
                    except Exception as e:
                        logger.error(f"Failed to upload {path} to S3: {str(e)}")
                        return None

        logger.info(f"Successfully transferred to S3: {s3_key}")
        asset_s3_url = f"{self.host}/{self.bucket_name}/{s3_loc}"
        return asset_s3_url


    def create_backup_stac_s3(self, current_stac_loc, backup_loc):
        
        today_date = datetime.now().strftime("%Y-%m-%d")
        stac_dir = f"{current_stac_loc}"
        backup_dir = f"{backup_loc}_{today_date}"
        if self.move_s3_objects(stac_dir, backup_dir):
            logger.info(f"-{stac_dir} backed up to {backup_dir}")
            return True
        logger.error(f"back up {stac_dir} to {backup_dir} failed")
        return False


    def move_s3_objects(self, source_loc, new_loc):
        """
        Move objects from one S3 location to another.

        :param source_loc: The source S3 location (prefix).
        :type source_loc: str
        :param new_loc: The destination S3 location (prefix).
        :type new_loc: str
        :return: True if the move was successful, False otherwise.
        :rtype: bool
        """
        
        try:
            # Ensure the prefix matches only the desired directory
            source_loc = source_loc.rstrip('/') + '/'
            new_loc = new_loc.rstrip('/') + '/'
        
            source_objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=source_loc).get('Contents', [])
            for obj in source_objects:
                source_key = obj['Key']
                new_key = source_key.replace(source_loc, new_loc, 1)
                # Copy each object to the backup location
                copy_source = {'Bucket': self.bucket_name, 'Key': source_key}
                self.s3_client.copy_object(Bucket=self.bucket_name, CopySource=copy_source, Key=new_key)
                logging.info(f"Successfully backed up {source_key} to {new_key}")
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=source_key)
                logger.info(f"Successfully deleted {source_key}")
            return True
        except Exception as e:
            logging.error(f"Failed to move objects from {source_loc} to {new_loc}: {str(e)}")
            return False
    

    def upload_to_s3(self, path, s3_loc):
        """
        Upload a local file or directory to an S3 bucket.

        :param path: The local file or directory path to upload.
        :type path: str
        :param s3_loc: The destination location in the S3 bucket.
        :type s3_loc: str
        :return: The S3 URL of the uploaded file or None if the upload fails.
        :rtype: str
        """
        

        file_name = os.path.basename(path)
        if os.path.isdir(path):
            total_size = os.path.getsize(path)
            for root, dirs, files in os.walk(path):
                for file in files:
                    local_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_path, path)
                    s3_key = f"{s3_loc}/{os.path.basename(path)}/{relative_path}"
                    try:
                        with tqdm(total=os.path.getsize(local_path), unit='B', unit_scale=True, desc=file_name, ncols=80) as pbar:
                            self.s3_client.upload_file(local_path, self.bucket_name, s3_key, Callback=lambda bytes_sent: pbar.update(bytes_sent))
                    except Exception as e:
                        logger.error(f"Failed to upload {file_name} to S3: {str(e)}")
                        return None
        else:
            
            s3_key = f"{s3_loc}/{file_name}"
            total_size = os.path.getsize(path)
            with tqdm(total=total_size, unit='B', unit_scale=True, desc=path, ncols=80) as pbar:
                try:
                    self.s3_client.upload_file(path, self.bucket_name, s3_key, Callback=lambda bytes_sent: pbar.update(bytes_sent))
                    logger.info(f"Successfully uploaded {path} to S3 as {s3_key}")
                except Exception as e:
                    logger.error(f"Failed to upload asset_url to S3: {str(e)}")
                    return None

        asset_s3_url = f"{self.host}/{self.bucket_name}/{s3_loc}/{file_name}"
        logger.info(f"Successfully transferred to S3: {s3_key}, link: {asset_s3_url}")
        return asset_s3_url
