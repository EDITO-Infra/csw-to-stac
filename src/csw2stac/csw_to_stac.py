from owslib.csw import CatalogueServiceWeb
from owslib.ows import BoundingBox, crs
import csv
import pandas as pd
from collections import Counter
import ast
import json
import logging
import os
from datetime import datetime, date
import re
import pystac
from typing import Any
from xml.etree import ElementTree as ET
from .csw_catalog import CSWCatalogManager

from .stac import CSWSTACManager
from .assets import AssetManager
from .add_metadata import MetadataUpdater
from .stac_to_resto import RestoStacManager
from .utils import Utils, S3Utils


os.chdir(os.path.dirname(os.path.abspath(__file__)))

today = datetime.today().strftime('%Y-%m-%d')

logger = Utils.get_logger(LOG_NAME='csw_to_stac')

class CSWSTAConverter:
    def __init__(self, pipeline_config):
        self.pipeline_config = pipeline_config
        self.stac_dir = pipeline_config['stac_dir']

        self.csw_catalog_title = pipeline_config['csw_catalog_title']

        self.cswcatalog = CSWCatalogManager(pipeline_config)
        self.cswstac = CSWSTACManager(pipeline_config)
        self.resto_stac_manager = RestoStacManager({'resto_instance': pipeline_config['resto_instance']})

    def process_records(self, all_csw_records=None):
        with open(self.cswcatalog.json_output_path, 'r') as file:
            all_csw_records = json.load(file)
        
        # Load the state of records already stored in STAC
        processedrecordsjson = f'../../data/all_{self.csw_catalog_title}_processed_records.json'
        if os.path.exists(processedrecordsjson):
            
            with open(processedrecordsjson, 'r') as file:
                self.processed_records = json.load(file)
            self.processed_records_df = pd.DataFrame.from_dict(self.processed_records, orient='index')
        
        else:
            self.processed_records = {}
            self.processed_records_df = pd.DataFrame()

        logger.info(f"\nProcessing {len(all_csw_records) - len(self.processed_records_df)} records")
        for id, metadata in all_csw_records.items():

            if 'records_to_process' in self.pipeline_config and self.pipeline_config['records_to_process'] and id not in self.pipeline_config['records_to_process']:
                logger.info(f"Skipping record with id {id}")
                continue
            if not self.processed_records_df.empty and id in self.processed_records_df['geonetwork_uri'].values:
                logger.info(f"Record with id {id} already processed and in STAC")
                continue

            logger.info(f"Processing record with id {id} \n")

            metadata['assets'] = []
            metadata['data_assets'] = []
            metadata['thumbnail_assets'] = []
            metadata['temporal_extent'] = {}

            logger.info(f"Checking for relevant assets in record {metadata['title']} with id {id}")
            assetmanager = AssetManager()
            metadata = assetmanager.find_assets(metadata)

            logger.info(f"Supplementing metadata for record {metadata['title']} with id {id}")
            geonetworkupdater = MetadataUpdater(metadata)
            metadata = geonetworkupdater.supplement_metadata()
            
            stac_ok = self.check_stac_ok(metadata)

            if not stac_ok:
                logger.info(f"Record id {metadata['geonetwork_uri']} not Ok for STAC")
                continue

            logger.info("adding to stac")
            stac_record = self.cswstac.add_to_stac(metadata)
            
            if stac_record is None:
                logger.error(f"Failed to add record with id {metadata['geonetwork_uri']} to STAC")
                self.update_progress(metadata, 'failed', 'failed_to_add')
                continue
            # Save a state of records stored in STAC
            self.processed_records.update({id: stac_record})

            self.update_progress(metadata, 'successful', 'Ok')

            failed_records = self.processed_records_df[self.processed_records_df['in_stac'] == 'failed']
            successful_records = self.processed_records_df[self.processed_records_df['in_stac'] == 'successful']
            logger.info(f'number failed {len(failed_records)} successful {len(successful_records)}')

        logger.info("All records processed.")
        failed_records = self.processed_records_df[self.processed_records_df['in_stac'] == 'failed']
        successful_records = self.processed_records_df[self.processed_records_df['in_stac'] == 'successful']
        logger.info(f"Records added to STAC catalog: {len(successful_records)}")
        logger.info(f"Records omitted from STAC catalog: {len(failed_records)}")

    
    def sync_stac_catalog_to_s3(self):
        stac_s3 = self.pipeline_config['stac_s3']
        S3Utils.sync_to_s3(self.stac_dir, stac_s3)
        logger.info("STAC catalog synced to S3")
    

    def update_progress(self, csw_record, status, reason):

        if status == 'failed' and reason == 'no_assets':
            csw_record['in_stac'] = 'failed'
            csw_record['reason'] = 'no_assets'
            self.processed_records.update({csw_record['geonetwork_uri']: csw_record})
            logger.info(f"Record id {csw_record['geonetwork_uri']} No assets, failed processing to STAC \n")

        elif status == 'failed' and reason == 'already_exists':
            csw_record['in_stac'] = 'failed'
            csw_record['reason'] = 'already_exists'
            self.processed_records.update({csw_record['geonetwork_uri']: csw_record})
            logger.info(f"Record id {csw_record['geonetwork_uri']} already exists in STAC \n")

        elif status == 'failed' and reason == 'no_data':
            csw_record['in_stac'] = 'failed'
            csw_record['reason'] = 'no_data'
            self.processed_records.update({csw_record['geonetwork_uri']: csw_record})
            logger.info(f"Record id {csw_record['geonetwork_uri']} No data assets, failed processing to STAC \n")

        elif status == 'successful' and reason == 'Ok':
            csw_record['in_stac'] = 'successful'
            csw_record['reason'] = 'Ok'
            self.processed_records.update({csw_record['geonetwork_uri']: csw_record})
            logger.info(f"Record id {csw_record['geonetwork_uri']} successfully processed to STAC \n")
        
        self.processed_records_df = pd.DataFrame.from_dict(self.processed_records, orient='index')
        self.processed_records_df.to_csv(f'../../data/all_{self.csw_catalog_title}_processed_records.csv', index=False) 
        with open(f'../../data/all_{self.csw_catalog_title}_processed_records.json', 'w') as file:
            json.dump(self.processed_records, file, indent=4, default=str)

    def check_stac_ok(self, metadata):

        if len(metadata['assets']) == 0:
            logger.error(f"No assets {metadata['geonetwork_uri']} {metadata['title']} not adding to STAC")
            self.update_progress(metadata, 'failed', 'no_assets')
            return False

        if len(metadata['data_assets']) == 0 and len(metadata['thumbnail_assets']) == 0:
            logger.warning(f"No data assets and no thumbnail assets {metadata['geonetwork_uri']} {metadata['title']}")
            self.update_progress(metadata, 'failed', 'no_data')
            return False

        if 'title' not in metadata or not metadata['title']:
            logger.error(f"No title {metadata['geonetwork_uri']} not adding to STAC")
            self.update_progress(metadata, 'failed', 'record_no_title')
            return False
        
        all_stac_items = self.cswstac.stac_catalog.get_all_items()
        all_stac_item_ids = [item.id for item in all_stac_items]
        if metadata['title'] in all_stac_item_ids:
            logger.warning(f"Record with id {metadata['geonetwork_uri']} already exists in STAC")
            self.update_progress(metadata, 'failed', 'already_exists')
            return False
        
        return True       


    def digest_in_resto(self):
        
        logger.info("Digesting STAC catalog in RESTO")
        
        self.resto_stac_manager.post_stac_data()
        # sync resto logs
        self.resto_stac_manager.backup_resto_logs()

        return

def csw2stac():

    pipeline_config = {
        "csw_catalog_title" : "emodnetgeonetwork",
        "csw_catalog_url" : "https://www.emodnet.eu/geonetwork/srv/eng/csw",
        "stac_id": "emodnet_geonetwork",
        "STAC_title" : "EMODnet Geonetwork",
        "stac_dir" : "../../data/stac",
        'stac_s3': 'geonetwork_stac',
        "resto_instance": "dive",
    }

    buildcsw = input("Do you want to build the CSW catalog? (y/n): ")
    if buildcsw == 'y':
        cswcatalog = CSWCatalogManager(pipeline_config)
        cswcatalog.get_all_csw_records()
    if buildcsw == 'n':
        cswcatalog = CSWCatalogManager(pipeline_config)
        cswcatalog.load_previous_records()
    
    removecp = input("Do you want to remove central portal entries from the CSW catalog? (y/n): ")
    if removecp == 'y':
        central_portal_url = "https://emodnet.ec.europa.eu/geoviewer/config.php"
        from .central_portal_layer_catalog import remove_central_portal_entries
        csw_json = cswcatalog.json_output_path
        remove_central_portal_entries(csw_json, central_portal_url)

    processrecords = input("Do you want to process the records into STAC? (y/n): ")

    if processrecords == 'y':
        csw_stac = CSWSTAConverter(pipeline_config)
        csw_stac.process_records()

    
    syncs3 = input(f"Do you want to sync the STAC {pipeline_config['stac_dir']} catalog to S3? (y/n): ")

    if syncs3 == 'y':
        csw_stac.sync_stac_catalog_to_s3()
    

    # optional: remove central portal entries from the geonetwork records
    # central_portal_url = "https://emodnet.ec.europa.eu/geoviewer/config.php"
    # from central_portal_layer_catalog import remove_central_portal_entries
    # csw_json = geonetwork_csw_stac.cswjson
    # remove_central_portal_entries(csw_json, central_portal_url)

    digestresto = input("Do you want to digest the STAC catalog in RESTO? (y/n): ")
    if digestresto == 'y':
        csw_stac.digest_in_resto()

