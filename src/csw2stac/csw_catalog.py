from owslib.csw import CatalogueServiceWeb
from owslib.ows import BoundingBox, crs
import csv
import json
import logging
import os
import pandas as pd
import ast
import requests
from datetime import datetime
import time
from typing import Any, Dict, List
from xml.etree import ElementTree as ET

logger = logging.getLogger('csw_to_stac')

today = datetime.today().strftime('%Y-%m-%d') 

os.chdir(os.path.dirname(os.path.abspath(__file__)))


class CSWCatalogManager:
    def __init__(self, config, output_dir: str = "../../data"):
        self.config = config
        self.csw_title = config['csw_catalog_title']
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        self.json_output_path = os.path.join(self.output_dir, f'all_{self.csw_title}_records.json')
        self.csv_output_path = os.path.join(self.output_dir, f'all_{self.csw_title}_records.csv')

    def load_previous_records(self):
        """Load records from previous JSON file if it exists."""
        if os.path.exists(self.json_output_path):
            with open(self.json_output_path, 'r') as json_file:
                self.cswjson = json.load(json_file)
        return []
    
    def get_all_csw_records(self):
        # Check if all records already exist and can be processed
        if os.path.exists(self.json_output_path):
            logger.info(f"Records already fetched and saved at {self.json_output_path}")
            return
        # Fetch all records from CSW source
        records = self.collect_csw_records()
        if not records:
            logger.info("No records fetched.")
            return
        # Save records to JSON and CSV
        self.export_records(records, to_json=True, to_csv=True)
        self.cswjson = records
        print('All CSW records fetched and saved.')

    def collect_csw_records(self):
        """Collect records from CSW source."""
        if self.config.get('csw_catalog_title') == 'emodnetgeonetwork':
            records = self.fetch_records_from_geonetwork_source_catalogs()
            logger.info(f"Records fetched from predefined source catalogs.")
            return records
        
        elif self.config.get('csw_catalog_url'):
            records = self.fetch_records_from_csw_url(self.config['csw_catalog_url'])
            logger.info(f"Records fetched from CSW URL: {self.config['csw_catalog_url']}")
            return records
        else:
            logger.error("No valid CSW source provided.")
            return None


    def fetch_records_from_csw_url(self, csw_url: str) -> List[Dict[str, Any]]:
        """Fetch records directly from a given CSW URL."""
        csw = CatalogueServiceWeb(csw_url)
        total_records = csw.results['matches']

        # Define chunk size
        chunk_size = 100

        # Calculate number of iterations needed
        num_iterations = (total_records + chunk_size - 1) // chunk_size

        # Create a list to store all records
        all_records = {}
        records_list = []
        # Iterate through the records in chunks
        #for i in range(num_iterations):
        for i in range(num_iterations):
            logger.info(f"Fetching records {i * chunk_size + 1} to {min((i + 1) * chunk_size, total_records)}")
            start_position = i * chunk_size + 1
            end_position = min((i + 1) * chunk_size, total_records)
            
            csw.getrecords2(maxrecords=chunk_size, startposition=start_position, esn='full')
            records = csw.records
            
            for uuid in records.keys():
                cswrecord = csw.records[uuid]
                try:
                    record_info = self.get_info_csw_record(cswrecord)
                    
                except Exception as e:
                    logger.error(f"Error fetching record {uuid}: {e}")
                    continue
                all_records[uuid] = record_info
                records_list.append(record_info)
        
        return all_records

    def get_info_csw_record(self, record):
        baseInfo = {
                'geonetwork_uri': record.identifier,
                'title': record.title,
                'abstract': record.abstract,
                'bbox': self.make_serializable(record.bbox)['bbox'] if record.bbox else None,
                'crs' : self.make_serializable(record.bbox)['crs'] if record.bbox else None,
                'created': self.make_serializable(record.created),
                'creator': record.creator,
                'date': self.make_serializable(record.date),
                'format': record.format,
                'issued': self.make_serializable(record.issued),
                'language': record.language,
                'license': record.license,
                'modified': self.make_serializable(record.modified),
                'publisher': record.publisher,
                'references': record.references,
                'rights': record.rights,
                'source': record.source,
                'subjects': record.subjects,
                'type': record.type,
                'links': record.uris,
            }
        return baseInfo

    def fetch_records_from_geonetwork_source_catalogs(self) -> List[Dict[str, Any]]:
        """Fetch records from predefined source catalogs."""
        sourceCatalogs = {
                "EMODnet Bathymetry": "1754618d-b2ef-445e-9ab4-0f8854e51468",
                "EMODnet Biology": "d186c17b-a362-4348-aa58-163be5295306",
                "EMODnet Chemistry": "801cae4f-92fa-4738-8f51-9f5cf07af672",
                "EMODnet Human Activities": "661f317e-c9f1-4cbc-b271-7772a33f6a17",
                "EMODnet Physics": "c7fdf54e-36d6-47d5-8393-c6b921c720ed",
                "EMODnet Seabed Habitats": "9aa87211-9d31-4ffb-90d1-bc83ae610b94",
                "EMODnet Geology": "d3bfd960-59ff-4573-a35c-c63c22c0ba13"
            }
        all_records = {}
        for lot, catalog_id in sourceCatalogs.items():
            logger.info(f"Fetching records from thematic lot '{lot}' source catalog {catalog_id}")
            thematic_lot, thematic_uuids = self.retrieve_thematic_lot_uuids(catalog_id)

            if not thematic_uuids:
                logger.warning(f"No records fetched for thematic lot '{lot}'")
                continue
            logger.info(f"Parsing records for thematic lot '{thematic_lot}' length: {len(thematic_uuids)}")
            for uuid in thematic_uuids:
                
                csw_url = f'https://emodnet.ec.europa.eu/geonetwork/emodnet/eng/csw?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&id={uuid}'
                try:
                    response = requests.get(csw_url)
                    response.raise_for_status()
                    csw_xml = ET.fromstring(response.content)
                    record = self.csw_xml_to_record(csw_xml)
                    record['thematic_lot'] = thematic_lot
                    record['provider'] = [{"name": thematic_lot, "roles": "provider"}]
                    all_records[uuid] = record
                except Exception as e:
                    logger.error(f"Error fetching from catalog '{lot}': {e}")
            
            logger.info(f"Thematic lot: {thematic_lot} number of records: {len(thematic_uuids)}")

        return all_records

    @staticmethod
    def csw_xml_to_record(csw_root):
        baseInfo = {}
        baseInfo['subjects'] = []
        baseInfo['links'] = []
        for elem in csw_root.iter():
            if elem.tag.endswith('identifier'):
                baseInfo['geonetwork_uri'] = elem.text
            if elem.tag.endswith('title'):
                baseInfo['title'] = elem.text
            if elem.tag.endswith('abstract'):
                baseInfo['abstract'] = elem.text
            if 'BoundingBox' in elem.tag:
                crs = elem.attrib.get('crs', None)
                if crs:
                    crs = crs.split(':')[-1]
                    if crs.endswith(')'):
                        crs = crs.replace(')', '')
                    crs = f'EPSG:{crs}'
                baseInfo['crs'] = crs
            if 'LowerCorner' in elem.tag:
                try:
                    lon_min, lat_min = elem.text.split(' ')
                    baseInfo['bbox'] = [float(lon_min), float(lat_min)]
                except Exception as e:
                    logger.warning(f"Failed to parse LowerCorner: {e}")
            if 'UpperCorner' in elem.tag:
                try:
                    lon_max, lat_max = elem.text.split(' ')
                    baseInfo['bbox'].extend([float(lon_max), float(lat_max)])
                except Exception as e:
                    logger.warning(f"Failed to parse UpperCorner: {e}")
            if elem.tag.endswith('created'):
                baseInfo['created'] = elem.text
            if elem.tag.endswith('creator'):
                baseInfo['creator'] = elem.text
            if elem.tag.endswith('date'):
                baseInfo['date'] = elem.text
            if elem.tag.endswith('format'):
                baseInfo['format'] = elem.text
            if elem.tag.endswith('issued'):
                baseInfo['issued'] = elem.text
            if elem.tag.endswith('language'):
                baseInfo['language'] = elem.text
            if elem.tag.endswith('license'):
                baseInfo['license'] = elem.text
            if elem.tag.endswith('modified'):
                baseInfo['modified'] = elem.text
            if elem.tag.endswith('publisher'):
                baseInfo['publisher'] = elem.text
            if elem.tag.endswith('references'):
                baseInfo['references'] = elem.text
            if elem.tag.endswith('rights'):
                baseInfo['rights'] = elem.text
            if elem.tag.endswith('source'):
                baseInfo['source'] = elem.text
            if elem.tag.endswith('subject'):
                baseInfo['subjects'].append(elem.text)
            if elem.tag.endswith('type'):
                baseInfo['type'] = elem.text
            if elem.tag.endswith('URI'):
                link_item= {}
                link_item['url'] = elem.text
                link_item['name'] = elem.attrib.get('name', None)
                link_item['description'] = elem.attrib.get('description', None)
                link_item['protocol'] = elem.attrib.get('protocol', None)
                baseInfo['links'].append(link_item)

        return baseInfo

    def retrieve_thematic_lot_uuids(self, source_catalog):
        uuids = []
        thematic_lot = None
        page_size = 100
        from_record = 1
        total_records = page_size

        while from_record <= total_records:
            url = f"https://emodnet.ec.europa.eu/geonetwork/srv/eng/q?_content_type=json&facet.q=sourceCatalog%2F{source_catalog}&resultType=details&from={from_record}&to={from_record + page_size - 1}"
            response = requests.get(url)
            if response.status_code == 429:
                logger.warning("Too many requests. Waiting for 30 seconds.")
                time.sleep(30)
                continue
            response.raise_for_status()
            data = response.json()

            if from_record == 1:
                total_records = int(data.get("summary", {}).get("@count", 0))
                source_catalog_info = next(
                    (dim for dim in data.get("summary", {}).get("dimension", []) if dim.get("@name") == "sourceCatalog"), {}
                )
                thematic_lot = source_catalog_info.get("category", {}).get("@label", "Unknown")

            metadata = data.get("metadata", [])
            for item in metadata:
                if isinstance(item, dict):
                    uuids.append(item.get("uuid"))
                elif isinstance(item, list):
                    item = item[0]
                    uuids.append(item.get("uuid"))

            from_record += page_size

        return thematic_lot, uuids


    def parse_bbox(self, bbox_elem) -> Dict[str, Any]:
        """Parse bounding box information."""
        try:
            lower = bbox_elem.find(".//LowerCorner").text.split()
            upper = bbox_elem.find(".//UpperCorner").text.split()
            return {
                'bbox': [float(lower[0]), float(lower[1]), float(upper[0]), float(upper[1])],
                'crs': bbox_elem.attrib.get('crs', 'EPSG:4326')
            }
        except Exception as e:
            logger.error(f"Error parsing BoundingBox: {e}")
            return {}

    def load_records_from_json(self, json_output_path: str) -> List[Dict[str, Any]]:
        """Load records from JSON file."""
        with open(self.json_output_path, 'r') as json_file:
            records = json.load(json_file)
        return records
    
    def save_to_json(self, records: List[Dict[str, Any]]):
        """Save records to JSON."""
        
        with open(self.json_output_path, 'w') as json_file:
            json.dump(records, json_file, indent=4)
        logger.info(f"Records saved to JSON at {self.json_output_path}")

    def save_to_csv(self, records: List[Dict[str, Any]]):
        """Save records to CSV."""
        
        df = pd.DataFrame(records)
        df.to_csv(self.csv_output_path, index=False)
        logger.info(f"Records saved to CSV at {self.csv_output_path}")

    def export_records(self, records: List[Dict[str, Any]], to_json=True, to_csv=True):
        """Export records to JSON and/or CSV based on user preference."""
        if to_json:
            self.save_to_json(records)
        if to_csv:
            self.save_to_csv(records)


    def make_serializable(self, data: Any) -> Any:
        """Helper function to make data JSON serializable."""
        if data is None:
            return None
        if isinstance(data, datetime):
            return data.isoformat()
        if isinstance(data, BoundingBox):
            return {'bbox': (data.minx, data.miny, data.maxx, data.maxy), 'crs': data.crs.code}
        if isinstance(data, (list, tuple)):
            return [self.make_serializable(item) for item in data]
        if isinstance(data, dict):
            return {key: self.make_serializable(value) for key, value in data.items()}
        return data