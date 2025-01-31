import requests
import json
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

today = datetime.today().strftime('%Y-%m-%d')


def remove_central_portal_entries(csw_records_json, central_portal_url):
    """
    Remove central portal entries from all_geonetwork_records.json.
    
    :param csw_records_json: Path to the all_geonetwork_records.json file
    :type csw_records_json: str
    :param central_portal_url: URL of the central portal layer catalog
    :type central_portal_url: str
    """
    duplicate_records = []
    with open(csw_records_json, 'r') as file:
        all_csw_records = json.load(file)
    layer_catalog_manager = LayerCatalogManager()
    cp_layer_catalog = layer_catalog_manager.create_layer_catalog(central_portal_url, 'central_portal', f'central_portal_layer_catalog_{today}.json')    


    cp_layers = cp_layer_catalog.layers
    for cp_layer_metadata in cp_layers:
            # remove admin units, PACE and Ingestion from CP catalog
            if cp_layer_metadata['thematic_lot'] == "Administrative units" or cp_layer_metadata['thematic_lot'] == "EU-China EMOD-PACE project" or cp_layer_metadata['thematic_lot'] == 'EMODnet Ingestion':
                cp_layer_catalog.remove_layer(cp_layer_metadata['id'])
                logger.info(f"Removed layer {cp_layer_metadata['name']} {cp_layer_metadata['id']} {cp_layer_metadata['thematic_lot']} from layer catalog")

    for cp_layer_metadata in cp_layer_catalog.layers:
        cp_layer = Layer()
        cp_layer.create_layer_metadata(cp_layer_metadata)
        cp_layer.add_metadataSources()
        
        if 'geonetwork_uri' in cp_layer.metadata:
            record_geonetwork_uri = cp_layer.metadata['geonetwork_uri']

            if record_geonetwork_uri in all_csw_records:
                del all_csw_records[record_geonetwork_uri]
                duplicate_records.append(record_geonetwork_uri)
                logger.info(f"Removed duplicate record  {cp_layer_metadata['name']}  {record_geonetwork_uri} from all_csw_records")
    print(f'removed {len(duplicate_records)} duplicate records ')
    with open(csw_records_json, 'w') as file:
        json.dump(all_csw_records, file, indent=4)
    
    print('Removed central portal entries from all_geonetwork_records.json.')

class LayerCatalogManager:
    def __init__(self):
        """
        Initializes a LayerCatalogManager instance with an empty final_structure dictionary and an empty layers list.
        """
        self.final_structure = {}
        self.layer_count = 0
        self.layers = []

    def update_layer_themes(self, layer_collection_url, layer, thematic_lot_name, subtheme_name="", subsubtheme_name=""):
        """
        Updates the layer's themes in the final_structure dictionary and adds the layer to the layers list.
        
        :param layer_collection_url: URL of the layer collection
        :type layer_collection_url: str
        :param layer: Layer metadata
        :type layer: dict
        :param thematic_lot_name: Thematic lot name
        :type thematic_lot_name: str
        :param subtheme_name: Subtheme name
        :type subtheme_name: str
        :param subsubtheme_name: Subsubtheme name
        :type subsubtheme_name: str
        """
        if 'id' in layer:
            layer_id = layer['id']
            layer['thematic_lot'] = thematic_lot_name
            layer['subtheme'] = subtheme_name
            layer['subsubtheme'] = subsubtheme_name
            # ignore layers without subtheme or subsubtheme for central portal
            if layer_collection_url == "https://emodnet.ec.europa.eu/geoviewer/config.php":
                if subtheme_name == "":
                    logger.warning(f"Layer {layer_id} does not have a subtheme")
                if subsubtheme_name == "":
                    logger.warning(f"Layer {layer_id} does not have a subsubtheme")
            else:
                if subtheme_name == "":
                    logger.error(f"Layer {layer_id} does not have a subtheme, no variable family")
                if subsubtheme_name == "":
                    logger.error(f"Layer {layer_id} does not have a subsubtheme, no edito collection")
            self.final_structure[str(layer_id)] = layer
            self.layers.append(layer)  # Add layer to the layers list
            self.layer_count += 1

    def find_all_themes(self, children, thematic_lot_name, layer_collection_url, depth=1, subtheme_name="", subsubtheme_name=""):
        """
        Recursively finds all themes in the layer catalog and updates the layer themes in the final_structure dictionary.
        
        :param children: List of children themes
        :type children: list
        :param thematic_lot_name: Thematic lot name
        :type thematic_lot_name: str
        :param layer_collection_url: URL of the layer collection
        :type layer_collection_url: str
        :param depth: Depth of the theme in the layer catalog
        :type depth: int
        :param subtheme_name: Subtheme name
        :type subtheme_name: str
        :param subsubtheme_name: Subsubtheme name
        :type subsubtheme_name: str
        """
        for child in children:
            if depth == 1:
                current_subtheme_name = child.get('displayName', '')
                current_subsubtheme_name = ""
            elif depth == 2:
                current_subtheme_name = subtheme_name
                current_subsubtheme_name = child.get('displayName', '')
            elif depth == 3:
                current_subtheme_name = subtheme_name
                current_subsubtheme_name = subsubtheme_name
            if 'children' in child:
                self.find_all_themes(child['children'], thematic_lot_name, layer_collection_url, depth + 1, current_subtheme_name, current_subsubtheme_name)
            else:
                self.update_layer_themes(layer_collection_url, child, thematic_lot_name, current_subtheme_name, current_subsubtheme_name)

    def find_layer_catalog_themes(self, data, layer_collection_url):
        """
        Finds all themes in the layer catalog and updates the layer themes in the final_structure dictionary.
        
        :param data: Layer catalog data
        :type data: dict
        :param layer_collection_url: URL of the layer collection
        :type layer_collection_url: str
        """
        data = data['layerCatalog']
        for thematic_lot in data.get('children', []):
            thematic_lot_name = thematic_lot.get('name', '')
            if 'children' in thematic_lot:
                self.find_all_themes(thematic_lot['children'], thematic_lot_name, layer_collection_url)

    def save_final_structure_json(self, layer_catalog_outfile):
        with open(layer_catalog_outfile, 'w') as file:
            json.dump(self.final_structure, file, indent=4)

    def remove_layer(self, layer_id):
        layer_id_str = str(layer_id)
        if layer_id_str in self.final_structure:
            # Remove from final_structure
            del self.final_structure[layer_id_str]
            # Remove from layers list
            self.layers = [layer for layer in self.layers if layer['id'] != layer_id]
            # Decrement layer count
            self.layer_count -= 1
            logger.info(f"Layer {layer_id} removed successfully.")
        else:
            logger.warning(f"Layer {layer_id} not found.")
    
    def filter_layers_by_thematic_lot(self, layer_catalog, thematic_lot_name):
        self.layers = [layer for layer in layer_catalog.layers if layer.get('thematic_lot') == thematic_lot_name]
        self.final_structure = {str(layer['id']): layer for layer in self.layers}
        self.layer_count = len(self.layers)
        logger.info(f"Filtered layers by thematic lot: {thematic_lot_name}")
    

    def create_layer_catalog(self, layer_collection_url, layer_collection, layer_catalog_outfile):
        
        # Check if layer collection URL is a local file, using when you can't connect to dev endpoint
        if layer_collection_url.endswith('.json'):
            logger.info(f"Avoiding Dev endpoint, Reading layer catalog from local file: {layer_collection_url}")
            with open(layer_collection_url) as file:
                layer_catalog_data = json.load(file)

        # Check if layer collection URL is a dev endpoint, then save to a json file
        elif 'dev.' in layer_collection_url:
            logger.info(f"Fetching layer catalog from: {layer_collection_url}")
            try:
                layer_catalog_data = requests.get(layer_collection_url).json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch layer catalog from: {layer_collection_url}, VPN required")
                logger.error(e)
                
            with open(f"../unshared_data/layer_catalog_states/dev_{layer_collection}_{today}.json", 'w') as file:
                json.dump(layer_catalog_data, file, indent=4)

        # for standard case of fetching a layer catalog from a URL
        else:
            logger.info(f"Fetching layer catalog from: {layer_collection_url}")
            try:
                layer_catalog_data = requests.get(layer_collection_url).json()
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch layer catalog from: {layer_collection_url}")
                logger.error(e)


        self.find_layer_catalog_themes(layer_catalog_data, layer_collection_url)
        self.save_final_structure_json(layer_catalog_outfile)
        return self

class Layer:
    """
    Represents a layer in the Metagis catalog, handling its metadata and associated operations.
    """

    def __init__(self):
        """
        Initializes a Layer instance with an empty metadata dictionary and a reference to the Metagis catalog.

        Args:
            metagiscatalog: A reference to the MetagisEditoLayerCollection instance that this layer belongs to.
        """
        self.metadata = {}
        self.temp_assets = '../data/temp_assets'
        self.converted_arco = '../data/temp_assets/converted_arco'

    def create_layer_metadata(self, metadata):
        """
        Updates the layer's metadata from the layer catalog (Layer Collection) and initializes an empty list for assets.

        Args:
            metadata (dict): The new metadata for the layer.

        Returns:
            dict: The updated metadata dictionary for the layer.
        """
        self.metadata = metadata
        self.metadata['assets'] = []
        self.metadata['converted_arco_assets'] = []
        return self.metadata

    def add_metadataSources(self):
        logger.info(f"Adding metadata sources for {self.metadata['id']} {self.metadata['name']}")
        metadataSources = self.metadata['metadataSources']
        if isinstance(metadataSources, list):
            for source in metadataSources:
                self._find_metadata_sources(source)
        elif isinstance(metadataSources, dict):
            for source in metadataSources.values():
                self._find_metadata_sources(source)

    def _find_metadata_sources(self, source):
        
        logger.info(f"looking for download url in metadata sources {self.metadata['id']} {self.metadata['name']}")
        if source.get('metadata_type', '') == 'download_url':
            download_url = source['metadata_value']
            self.metadata['download_url'] = download_url
        logger.info(f"looking for geonetwork uri in metadata sources {self.metadata['id']} {self.metadata['name']}")
        if source.get('metadata_type', '') == 'geonetwork_uri':
            self.metadata['assets'] = []
            geonetwork_uri = source['metadata_value']
            self.metadata['geonetwork_uri'] = geonetwork_uri
            self.metadata['assets'].append(f"https://emodnet.ec.europa.eu/geonetwork/srv/api/records/{geonetwork_uri}/formatters/xml")
            self.metadata['assets'].append(f"https://emodnet.ec.europa.eu/geonetwork/emodnet/eng/csw?request=GetRecordById&service=CSW&version=2.0.2&elementSetName=full&id={geonetwork_uri}")
        logger.info('looking for native data product in metadata sources')
        if source.get('metadata_type', '') == 'edito_info':
            self.metadata['native_data_product'] = source['metadata_value']


