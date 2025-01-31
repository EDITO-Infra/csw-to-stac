
import pandas as pd
import logging
import ast
import requests
from .stac import CSWSTACManager

from xml.etree import ElementTree as ET
from .utils import Utils
logger = logging.getLogger("csw_to_stac")

class AssetManager():
    def __init__(self):
        self.broken_links = []
        self.working_links = []

    
    def find_assets(self, csw_record):
        
        if 'links' in csw_record:
            record_links = csw_record['links']
            for link_item in record_links:
                # check if the link item is a dictionary and has a non-empty URL, look up STAC media type
                if isinstance(link_item, dict) and "url" in link_item and link_item["url"]:
                    link = link_item["url"]

                    # look up the asset type, title, mediatype, and role
                    asset_type, title, mediatype, role = Utils.get_media_type(link)
                    if not asset_type or not mediatype:
                        logger.warning(f"Failed to get asset type")
                        continue
     
                    # sort the asset, update the csw record
                    csw_record = self.sort_asset(csw_record, link_item, asset_type, mediatype, role) 
                    continue
                
                else:
                    logger.error(f"Link item has no URL: {link_item}")
                    continue
        else:
            logger.warning(f"No links found in record {csw_record['title']}")
        return csw_record

    def sort_asset(self, csw_record, link_item, asset_type, mediatype, role):
        record_id = csw_record['geonetwork_uri']
        if asset_type == 'wms':
            wms_url = self.test_wms(csw_record, link_item)
            if wms_url:
                csw_record['assets'].append(wms_url)
                csw_record['wms_asset'] = wms_url
                logger.info(f"WMS link {wms_url} added to assets")
            else:
                logger.warning(f"WMS link failed in record {csw_record['title']}")
        
        elif asset_type == 'wfs':
            logger.info(f'skipping wfs test')
            csw_record['assets'].append(link_item['url'])
            csw_record['wfs_asset'] = link_item['url']
            csw_record['data_assets'].append(link_item['url'])
           
        elif asset_type == 'iptresource':
            logger.info('converting to ipt resource to Darwin Core Archive')
            dwca_url = Utils.convert_ipt_to_dwca(record_id, link_item['url'])
            if dwca_url:
                csw_record['assets'].append(dwca_url)
                csw_record['dwca_asset'] = dwca_url
                csw_record['data_assets'].append(dwca_url)
                logger.info(f"Darwin Core Archive link {dwca_url} added to assets")
            else:
                logger.info(f"ipt link failed record {csw_record['title']} {csw_record['geonetwork_uri']}")
    
        elif asset_type == 'eurobistoolbox':
            logger.info(f'converting to WFS csv request')
            wfs_url = Utils.convert_eurobis_toolbox_to_wfs(record_id, link_item['url'])
            if wfs_url:
                csw_record['assets'].append(wfs_url)
                csw_record['wfs_asset'] = wfs_url
                csw_record['data_assets'].append(wfs_url)
                logger.info(f"WFS link {wfs_url} added to assets")
            else:
                logger.info(f"eurobis toolbox link failed record {csw_record['title']} {csw_record['geonetwork_uri']}")
 
        elif asset_type == 'opendap':
            logger.info(f"testing opendap link {link_item['url']}")
            
            opendap_url = Utils.test_opendap(record_id, link_item['url'])
            if opendap_url:
                logger.info(f"Link {link_item['url']} working")
                # add the link to the record assets, to be used in the STAC
                csw_record['assets'].append(opendap_url)
                csw_record['data_assets'].append(opendap_url)
        
        elif asset_type == 'mdazip':
            logger.info(f"testing mda link {link_item['url']}")
            # test the link
            mdalink = Utils.test_mda(record_id, link_item['url'])
            if mdalink:
                logger.info(f"Link {link_item['url']} working")
                # add the link to the record assets, to be used in the STAC
                csw_record['assets'].append(mdalink)
                csw_record['data_assets'].append(mdalink)

        elif role == ['data']:
            logger.info(f"testing data link {link_item['url']}")
            # test the link
            if Utils.test_link(record_id, link_item['url']):
                logger.info(f"Link {link_item['url']} working")

                # add the link to the record assets, to be used in the STAC
                csw_record['assets'].append(link_item['url'])
                csw_record['data_assets'].append(link_item['url'])
        
        elif role == ['thumbnail']:
            logger.info(f"testing image link {link_item['url']}")
            # test the link
            if Utils.test_link(record_id, link_item['url']):
                logger.info(f"Link {link_item['url']} working")

                # add the link to the record assets, to be used in the STAC
                csw_record['assets'].append(link_item['url'])
                csw_record['thumbnail_assets'].append(link_item['url'])
        else:
            logger.info(f"testing other link {link_item['url']} with mediatype {mediatype}")
            # test the link
            if Utils.test_link(record_id, link_item['url']):
                logger.info(f"Link {link_item['url']} working")

                # add the link to the record assets, to be used in the STAC
                csw_record['assets'].append(link_item['url'])
    
        return csw_record
    
    def get_wms_version_and_bbox(self, base_url):
        if base_url.endswith("?"):
            base_url = base_url[:-1]
        capabilities_url = f"{base_url}?SERVICE=WMS&REQUEST=GetCapabilities"
        try:
            response = requests.get(capabilities_url, timeout=20, stream=True)
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get WMS capabilities: {e}")
            return None, None
        
        if response.status_code == 200:
            try:
                tree = ET.fromstring(response.content)
            except Exception as e:
                logger.warning(f"failed to parse response to string with element tree {e}")
                return None, None
            version = tree.attrib.get('version')
            
            # Find the bounding box
            bbox = None
            for layer in tree.findall(".//{http://www.opengis.net/wms}Layer"):
                bbox_element = layer.find("{http://www.opengis.net/wms}BoundingBox")
                if bbox_element is not None:
                    minx = bbox_element.attrib.get('minx')
                    miny = bbox_element.attrib.get('miny')
                    maxx = bbox_element.attrib.get('maxx')
                    maxy = bbox_element.attrib.get('maxy')
                    bbox = f"{minx},{miny},{maxx},{maxy}"
                    break
            
            return version, bbox
        else:
            logger.info(f'response {response.status_code} Failed to get WMS capabilities')
            return None, None

    def construct_wms_request(self, base_url, layers, bbox, width, height, version="1.1.1", crs="EPSG:4326", styles="", format="image/png"):
        params = {
            "SERVICE": "WMS",
            "VERSION": version,
            "REQUEST": "GetMap",
            "LAYERS": layers,
            "STYLES": styles,
            "CRS": crs,
            "BBOX": bbox,
            "WIDTH": width,
            "HEIGHT": height,
            "FORMAT": format
        }
        if not base_url.endswith("?"):
            base_url += "?"
        request_url = base_url + "&".join([f"{key}={value}" for key, value in params.items()])
        return request_url

    def verify_wms_request(self, wms_request_url):
        try:
            response = requests.get(wms_request_url, timeout=20, stream=True)
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get WMS capabilities: {e}")
            return False
        
        if response.status_code != 200:
            logger.info(f"Request to {wms_request_url} failed with status code {response.status_code}.")
            return False

        if response.headers['Content-Type'] != 'image/png':
            logger.info(f"Request to {wms_request_url} returned content of type {response.headers['Content-Type']}, expected 'image/png'.")
            return False

        else:
            logger.info('WMS request successful.')
            return True

    def test_wms(self, csw_record, link_item):
        base_url = link_item["url"]
        layers = link_item['name']
        
        # Get WMS version and bounding box
        version, bbox = self.get_wms_version_and_bbox(base_url)
        if not version or not bbox:
            logger.error(f"Failed to get WMS version and bounding box for {base_url}")
            return None

        # Construct WMS request URL
        width = 800
        height = 600
        wms_request_url = self.construct_wms_request(base_url, layers, bbox, width, height, version=version)
        if self.verify_wms_request(wms_request_url):
            logger.info(f"WMS request verified: {wms_request_url}")
            return wms_request_url
        else:
            logger.error(f"Failed to verify WMS request {wms_request_url}")
            return None
