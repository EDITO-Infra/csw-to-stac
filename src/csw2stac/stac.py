import pystac
import requests
import ast
import pystac_client
import owslib
from shapely import Polygon

import json
from datetime import datetime
import os
import logging
from .utils import Utils

from shapely.geometry import mapping
os.chdir(os.path.dirname(os.path.abspath(__file__)))
logger = logging.getLogger("csw_to_stac")

class CSWSTACManager():
    def __init__(self, config):
        self.final_structure = {}
        self.layer_count = 0
        self.layers = []
        self.stac_root = f"{config['stac_dir']}/catalog.json"
        self.initialize_stac()
        self.stac_catalog = pystac.Catalog.from_file(self.stac_root)

    
    def initialize_stac(self):
        if not os.path.exists(self.stac_root):
            catalog = pystac.Catalog(id='emodnet_geonetwork', title="EMODnet Geonetwork Catalog", description='Catalog of records found on EMODnet Geonetwork')
            catalog.normalize_hrefs(root_href=self.stac_root)
            catalog.save(catalog_type='SELF_CONTAINED')
            logger.info(f'Initialized STAC catalog at {self.stac_root}')
        else:
            logger.info(f'STAC catalog already exists at {self.stac_root}')
            catalog = pystac.Catalog.from_file(self.stac_root)
        return
    
    def get_all_edito_collections(self):
        editocatalog = pystac_client.Client.open('https://catalog.staging.edito.eu/')
        collections = editocatalog.get_collections()
        self.edito_ids = [collection.id for collection in collections]

    def add_to_stac(self, metadata):
        #self.get_all_edito_collections()

        
        variable_family = Utils.lookup_variable_family(metadata)
        
        collection = Utils.lookup_collection(metadata)
        
        variable_family = self.add_or_use_variable_family(variable_family, metadata)

        collection = self.add_or_use_collection(variable_family, collection, metadata)
        
        updated_metadata = self.add_item(metadata, collection)
        if not updated_metadata:
            return None
        logger.info(f" {metadata['geonetwork_uri']} {metadata['title']} added successfully to STAC")
        return updated_metadata

    def add_or_use_variable_family(self, current_family, metadata):
        logger.info('updating provider info for variable family')
        current_family_id = Utils.custom_slugify(current_family)
        variable_family_ids = [catalog.id for catalog in self.stac_catalog.get_children()]
        
        if current_family_id not in variable_family_ids:
            logger.info(f'adding variable family {current_family} to STAC')
            new_family = pystac.Catalog(id=current_family_id,
                                        title=current_family,
                                        description=f'Variable Family {current_family}'
                                        )
            
            self.stac_catalog.add_child(new_family)
            self.stac_catalog.normalize_hrefs(self.stac_root)
            self.stac_catalog.save(catalog_type='SELF_CONTAINED')
            metadata['variable_family'] = current_family_id
            return new_family
        else:
            logger.info(f'Variable family  {current_family} already exists in the STAC catalog')
            metadata['variable_family'] = current_family_id
            current_family = self.stac_catalog.get_child(current_family_id)
            return current_family
        
    def add_or_use_collection(self, variable_family, current_collection, metadata):
    
        logger.info('updating provider info for collection')
        
        metadata = Utils.update_providers(metadata)
        unique_providers = {}
        for provider_dict in metadata['provider']:
            if provider_dict['name'] not in unique_providers:
                unique_providers[provider_dict['name']] = provider_dict['roles']
        
        current_collection_id = Utils.custom_slugify(current_collection)
        if any('EMODnet' in key for key in unique_providers.keys()):
            current_collection_id = f"emodnet-{current_collection_id}"
            collection_convention = 'EMODnet'
        collection_ids = [collection.id for collection in variable_family.get_collections()]
        if current_collection_id not in collection_ids:
            logger.info(f'adding collection {current_collection} to STAC')
            new_collection = pystac.Collection(id=current_collection_id,
                                                title=f"{current_collection} ({collection_convention} Convention)", 
                                               description=f'Collection of {current_collection} data',
                                               providers=[pystac.Provider(name=name, roles=roles) for name, roles in unique_providers.items()],
                                                license=metadata.get('license', 'CC-BY-4.0'),
                                                extent=pystac.Extent(
                                                    spatial=pystac.SpatialExtent([[-180.0, -90.0, 180.0, 90.0]]),
                                                    temporal=pystac.TemporalExtent([None, None])
                                                )
                                                  )
            metadata['variable_family'] = variable_family.id
            variable_family.add_child(new_collection)
            self.stac_catalog.normalize_hrefs(self.stac_root)
            self.stac_catalog.save(catalog_type='SELF_CONTAINED')
            return new_collection
        else:
            logger.info(f'Collection  {current_collection} already exists in the STAC catalog')
            current_collection = variable_family.get_child(current_collection_id)
            
            return current_collection

    def add_item(self, metadata, collection):
        """
        param: metadata: dict: metadata of the record
        param: collection: pystac.Collection: collection to which the item will be added

        Adds an item to the collection in the STAC catalog
        

        """
        def lookup_assets(links):
            assets = {}
            for link in links:
                
                asset_type, title, mediatype, role = Utils.get_media_type(link)
                if not asset_type or not mediatype:
                    logger.warning(f'Failed to get asset type')
                    continue
                asset = pystac.Asset(href=link, title=title, media_type=mediatype, roles=role)
                assets[asset_type] = asset
            return assets

        # look for existing items in collection
        items = collection.get_items()
        item_ids = [item.id for item in items]
        if 'title' not in metadata or not metadata['title']:
            logger.error(f"no title for {metadata['geonetwork_uri']} no stac item")
            return None

        new_item_id = Utils.custom_slugify(metadata['title'])
        
        if new_item_id in item_ids:
            logger.error(f'Item {new_item_id} already exists in the collection')
            return None

        bbox = Utils.finalize_boundaries(metadata)
        lon_min, lat_min, lon_max, lat_max = bbox
        item_geometry = Polygon([(lon_min, lat_min), (lon_max, lat_min), (lon_max, lat_max), (lon_min, lat_max), (lon_min, lat_min)])
        
        # times
        
        metadata = Utils.update_datetimes(metadata)
        metadata = Utils.format_start_end_datetimes_stac(metadata)
        
        new_item= pystac.Item(id=new_item_id, 
                                geometry=mapping(item_geometry),
                                bbox=[lon_min, lat_min, lon_max, lat_max],
                                start_datetime=metadata['start_datetime'],
                                end_datetime=metadata['end_datetime'], 
                                datetime=metadata.get('datetime', None),
                                
                                properties={
                                    "keywords": metadata['subjects'],
                                    "proj:epsg": metadata.get('crs', 4326), 
                                    "license": metadata.get('license', 'CC-BY-4.0'),
                                    "title": metadata['title'],
                                },
                                stac_extensions=["https://stac-extensions.github.io/projection/v1.1.0/schema.json"],
                                collection=collection.id)
        
        new_item.properties['provider'] = metadata['provider'][0]['name']

        if 'references' in metadata and metadata['references']:
            new_item.properties['references'] = metadata['references']
        new_item.assets = lookup_assets(metadata['assets'])

        if not new_item.assets:
            logger.info(f'Item {new_item_id} has no assets, no item will be added to the collection')
            return None

        collection.add_item(new_item)
        collection.extent = pystac.Extent.from_items(collection.get_all_items())
        logger.info(f'Added item {new_item_id} to collection {collection.id} in STAC')
        metadata['stac_id'] = new_item_id
        metadata['stac_collection'] = collection.id
        self.stac_catalog.normalize_hrefs(self.stac_root)
        self.stac_catalog.save(catalog_type='SELF_CONTAINED')

        return metadata

