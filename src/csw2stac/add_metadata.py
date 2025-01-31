

import logging
import requests
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import xml.etree.ElementTree as ET
from io import StringIO
import xarray as xr
import pandas as pd
import pytz
from .utils import Utils

logger = logging.getLogger("csw_to_stac")

class MetadataUpdater():
    def __init__(self, metadata):
        self.metadata = metadata


    def supplement_metadata(self):
        self.look_up_thematic_lot()
        xml_root, name_spaces, xml_url, csw_url = self.get_xml_root_namespaces()

        if xml_root is None or name_spaces is None:
            return self.metadata
        self.xml_root = xml_root
        self.namespaces = name_spaces
        self.metadata['xml_asset'] = xml_url
        self.metadata['csw_asset'] = csw_url
        self.metadata['assets'].append(xml_url)
        self.metadata['assets'].append(csw_url)
        self.get_geonetwork_providers()
        self.get_rights_constraints()
        self.geographic_extent_from_xml()
        self.temporal_extent_from_xml()
        return self.metadata
    
    def get_xml_root_namespaces(self):
        if 'geonetwork_uri' not in self.metadata:
            logger.warning("Geonetwork URI not found in metadata.")
            return None, None, None, None
        geonetwork_uri = self.metadata['geonetwork_uri']
        xml_url = f'https://emodnet.ec.europa.eu/geonetwork/srv/api/records/{geonetwork_uri}/formatters/xml'
        csw_url =f'https://emodnet.ec.europa.eu/geonetwork/emodnet/eng/csw?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&id={geonetwork_uri}'
        try:
            xml_response = requests.get(xml_url)
        except Exception as e:
            logger.error(f"Failed to fetch XML from {xml_url}: {e}")
            return None, None, None, None
        if xml_response.status_code == 200:
            
            try:
                namespaces = dict([node for _, node in ET.iterparse(StringIO(xml_response.text), events=['start-ns'])])
                xml_root = ET.fromstring(xml_response.text)
                return xml_root, namespaces, xml_url, csw_url
            except Exception as e:
                logger.warning(f"Failed to get namespaces and xml root: {e} from {self.metadata['name']} {self.metadata['id']}")
                return None, None, None, None
        else:
            logger.warning(f"Failed to fetch XML from {xml_url}.")
            return None, None, None, None
       
    def geographic_extent_from_xml(self):
        
        geographical_element = self.xml_root.find(".//gmd:geographicElement/gmd:EX_GeographicBoundingBox", namespaces=self.namespaces)
            
        if geographical_element is not None:
            try:
                min_lon = float(geographical_element.find(".//gmd:westBoundLongitude/gco:Decimal", namespaces=self.namespaces).text)
                min_lat = float(geographical_element.find(".//gmd:southBoundLatitude/gco:Decimal", namespaces=self.namespaces).text)
                max_lon = float(geographical_element.find(".//gmd:eastBoundLongitude/gco:Decimal", namespaces=self.namespaces).text)
                max_lat = float(geographical_element.find(".//gmd:northBoundLatitude/gco:Decimal", namespaces=self.namespaces).text)

                self.metadata['geographic_extent'] = [min_lon, min_lat, max_lon, max_lat]
                return True
            except Exception as e:
                logger.error(f"bounds not available in geographic element: {e}")
                return False
        else:
            logger.warning("Geographic element not found in XML.")
            return False
    
    def temporal_extent_from_xml(self):

        if not self.metadata.get('temporal_extent'):
            self.metadata['temporal_extent'] = {}
        self.temporal_extent = self.metadata['temporal_extent']
        if not self.temporal_extent.get('start') and not self.temporal_extent.get('end'):
            logger.info('No temporal extent found in layer metadata, trying to read from geonetwork XML')
            self.try_read_temporal_element_from_xml()
            if not self.temporal_extent.get('start') or not self.temporal_extent.get('end'):
                logger.warning('Failed to read temporal extent from geonetwork XML')
                return
            else:
                self.fill_dates_and_format()
                return
    
    def try_read_temporal_element_from_xml(self):
        
        gml_prefixes = [prefix for prefix, uri in self.namespaces.items() if 'gml' in uri]

        for gml_prefix in gml_prefixes:
            gml_namespace = self.namespaces[gml_prefix]
            time_period_xpath = f".//{{{gml_namespace}}}TimePeriod"
            time_period = self.xml_root.find(time_period_xpath, namespaces=self.namespaces)
            logger.info(f"{gml_prefix} Time period found: {time_period}")
            if time_period is not None:
                logger.info('try to find xpath begin and end position')
                begin_pos = time_period.find("gml:beginPosition", namespaces=self.namespaces)
                end_pos = time_period.find("gml:endPosition", namespaces=self.namespaces)
                if begin_pos is not None:
                    logger.info(f"begin position found: {begin_pos.text}")
                    self.temporal_extent['start'] = begin_pos.text
                if end_pos is not None:
                    logger.info(f"end position found: {end_pos.text}")
                    self.temporal_extent['end'] = end_pos.text
        
                logger.info(f"temporal_extent from geonetwork_uri {self.temporal_extent}")
                
                if self.temporal_extent.get('end') is None and self.temporal_extent.get('start') is not None:
                    logger.warning(f"No end date found for geo {self.metadata['geonetwork_uri']},"
                                f" using today for end date")
                    self.temporal_extent['end'] = datetime.today().strftime("%Y-%m-%dT%H:%M:%S")
        return
    
    def fill_dates_and_format(self):
        logger.info('Filling dates and formatting')
        for key in ['start', 'end']:
            if self.temporal_extent.get(key):
                if len(self.temporal_extent[key]) == 4:
                    self.temporal_extent[key] += "-01-01T00:00:00"
                elif len(self.temporal_extent[key]) == 7:
                    self.temporal_extent[key] += "-01T00:00:00"
                elif len(self.temporal_extent[key]) == 10:
                    self.temporal_extent[key] += "T00:00:00"
                
                logger.info('formatting dates')
                formatted_value = self.format_datetimes(self.temporal_extent[key])
                if formatted_value:
                    self.temporal_extent[key] = formatted_value
                else:
                    logger.warning(f"Unable to format datetime {self.temporal_extent[key]} for {key}"
                                   f"set to None, will be set to today")
                    self.temporal_extent[key] = None


    def format_datetimes(self, value):
        # Remove milliseconds if present
        value = value.split('.')[0]
        
        try:
            # Try parsing the datetime string with timezone information
            logger.info(f"Trying to parse datetime with timezone: {value}")
            dt_obj = datetime.fromisoformat(value)
        except ValueError:
            try:
                # If parsing with timezone fails, assume the datetime is in local time and parse it
                logger.info(f"Trying to parse datetime without timezone: {value}")
                dt_obj = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                # Convert the datetime object to UTC
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
            except ValueError:

                logger.warning(f"Unable to parse and convert datetime {value}")
                return None
        
        # Convert the datetime object to UTC if it has timezone info
        logger.info(f"Converting datetime to UTC: {dt_obj}")
        dt_obj_utc = dt_obj.astimezone(timezone.utc)
        # Format the datetime string with 'Z' to indicate UTC
        logger.info(f"formatted datetime: {dt_obj_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}")
        return dt_obj_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    def look_up_thematic_lot(self):
        if 'thematic_lot' not in self.metadata or not self.metadata['thematic_lot']:

            #thematic_lot = Utils.lookup_thematic_lot(self.metadata)
            thematic_lot,collection,family = Utils.get_provider_collection_family()
            if thematic_lot:
                self.metadata['thematic_lot'] = thematic_lot
                logger.info(f"Thematic lot found: {thematic_lot}")
            else:
                logger.warning("No thematic lot found in metadata")
                self.metadata['thematic_lot'] = 'EMODnet_geonetwork'
        return
    
    def get_geonetwork_providers(self):
        
        if 'provider' in self.metadata and self.metadata['provider']:
            logger.info("Provider already set in metadata")
            return
        responsible_party = self.xml_root.find('.//gmd:CI_ResponsibleParty', namespaces=self.namespaces)
        if responsible_party is not None:
            organisation_name = responsible_party.find('.//gmd:organisationName/gco:CharacterString', namespaces=namespaces)

            if organisation_name and organisation_name.text is not None:
                self.metadata['provider'] = []
                provider_dict = {}
                provider_dict['name'] = organisation_name.text
                provider_dict['roles'] = 'provider'
                self.metadata['provider'].append(provider_dict)
                logger.info(f"Provider found: {organisation_name.text}")
            else:
                logger.warning("No provider found in metadata")
        else:
            logger.warning("No responsible party found in metadata")
        return
        # Data Rights and Constraints
    def get_rights_constraints(self):
        constraints_element = self.xml_root.find(
            ".//gmd:otherConstraints/gco:CharacterString", namespaces=self.namespaces)
        if constraints_element is not None:
            constraints = constraints_element.text
            self.metadata['data_rights_restrictions'] = constraints
            logger.info(f"Data rights constraints from geonetwork: {constraints}")
        # if nothing found in XML, set defaults
        
        return