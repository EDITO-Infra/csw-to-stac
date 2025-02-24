import argparse
import requests
import json
import pystac
import os
import pandas as pd
from datetime import datetime
import logging
from .utils import Utils
import dotenv
os.chdir(os.path.dirname(os.path.abspath(__file__)))
today = datetime.now().strftime("%Y-%m-%d")
logger = logging.getLogger("csw_to_stac")

class RestoStacManager:
    def __init__(self, pipeline_config):
        self.resto_instance = pipeline_config.get('resto_instance', 'staging')
        self.pipeline_config = pipeline_config
        self.local_stac = '../data/stac'
        self.restologs = '../data/restologs'
        os.makedirs(self.restologs, exist_ok=True)
        self.token = None
        self.delete_log = {}
        self.myfeatures = {}
        self.posted_logs = {}

        self.load_credentials('../../data/creds/resto.env')
        self.get_initial_access_token()


    def load_credentials(self, credentials_path):
        """
        Load the RESTO credentials from the credentials file.  Update the class attributes with the credentials.
        """
        dotenv.load_dotenv(credentials_path)
        self.resto_user = os.getenv('RESTO_USERNAME')
        self.resto_password = os.getenv('RESTO_PASSWORD')
        logger.info(f"Loaded credentials for {self.resto_user}")

    def get_initial_access_token(self):
        """
        Get the initial access token from the RESTO API.  Update the class attribute with the initial access token.
        """
        url = f"https://auth.lab.{self.resto_instance}.edito.eu/auth/realms/datalab/protocol/openid-connect/token"
        response = requests.post(url, data={
            'Content-Type': 'application/x-www-form-urlencoded',
            'client_id': "edito",
            'username': self.resto_user,
            'password': self.resto_password,
            'grant_type': "password",
            'scope': "openid"
        })
        self.token = response.json()['access_token']
        logger.info(f"Initial token: {self.token}")

    def backup_resto_logs(self):

        logfiles = os.listdir(self.restologs)
        for csv in logfiles:
            if csv.endswith('.csv'):
                filepath = os.path.join(self.restologs, csv)
                s3_backup = self.pipeline_config['stac_s3']
                Utils.upload_to_s3(filepath, s3_backup)


    def post_stac_data(self):
        cat_posts = []
        vfc_posts = []
        coll_posts = []
        item_posts = []

        catalog = pystac.Catalog.from_file(f"{self.local_stac}/catalog.json")
        catalog_data = catalog.to_dict()

        logger.info(f"posting {catalog_data['id']} catalog")
        # here we post the root catalog
        cat_post_resp = self.post_catalog(catalog_data)
        cat_post_resp['catalog_id'] = catalog_data['id']
        cat_posts.append(cat_post_resp)
        
        vfc_count = 0
        for vfc in catalog.get_children():
            # if vfc_count >= 10:
            #     break
            #vfc_data = vfc.to_dict()

            # # here we post the variable family catalog to the variable family catalogs
            # vfc_resp = self.post_to_child_catalog(vfc_data, f"variable_families")
            # vfc_resp['catalog_id'] = vfc_data['id']
            # vfc_posts.append(vfc_resp)
            coll_count = 0
            for coll in vfc.get_children():
                # if coll_count >= 3:
                #     break
                
                coll_data = coll.to_dict()
                logger.info(f"posting collection {coll_data['id']} to /collections")
                coll_resp = self.post_collection(coll_data)
                coll_resp['collection_id'] = coll_data['id']
                coll_posts.append(coll_resp)
                # here we post collections to the relevant variable family catalog and the geonetwork catalog
                #coll_cat_resp1 = self.post_collection_to_child_catalog(coll_data, f"variable_families/{vfc_data['id']}")
                #coll_cat_resp1['collection_id'] = coll_data['id']
                #coll_posts.append(coll_cat_resp1)
                
                logger.info(f"posting collection {coll_data['id']} under emodnet_geonetwork catalog")
                coll_cat_resp2 = self.post_collection_to_child_catalog(coll_data, f"emodnet_geonetwork")
                coll_cat_resp2['collection_id'] = coll_data['id']
                coll_posts.append(coll_cat_resp2)

                item_count = 0
                for item in coll.get_all_items():
                    # if item_count >= 10:
                    #     break
                    item_data = item.to_dict()

                    logger.info(f"posting item {item_data['id']} to collection {item_data['collection']}")
                    item_resp = self.post_item(item_data)
                    if item_resp is None:
                        item_resp = {}
                        item_resp['error'] = "Failed to post item"
                        item_resp['variable_family_catalog'] = vfc.id
                        item_resp['item_id'] = item_data['id']
                        item_resp['collection_id'] = item_data['collection']
                        item_posts.append(item_resp)
                    
                    item_resp['variable_family_catalog'] = vfc.id
                    item_resp['collection_id'] = item_data['collection']
                    item_resp['item_id'] = item_data['id']
                    item_posts.append(item_resp)
                    item_count += 1
                coll_count += 1
            vfc_count += 1
        
        # Log and save response
        cat_post_df = pd.DataFrame(cat_posts).to_csv(f"{self.restologs}/posted_catalogs_{today}.csv", index=False)
        #vfc_post_df = pd.DataFrame(vfc_posts).to_csv(f"{self.restologs}/posted_variable_families_{today}.csv", index=False)
        coll_post_df = pd.DataFrame(coll_posts).to_csv(f"{self.restologs}/posted_collections_{today}.csv", index=False)
        item_post_df = pd.DataFrame(item_posts).to_csv(f"{self.restologs}/posted_items_{today}.csv", index=False)


    def post_catalog(self, catalog_data):
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/catalogs/"
        catalog_data['links'] = self.cleanup_links(catalog_data['links'], 'root', f'https://api.{self.resto_instance}.edito.eu/data/catalogs/')
        return self.post_data(post_url, catalog_data)

    def post_to_child_catalog(self, data, child_catalog):
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/catalogs/{child_catalog}/"
        data['links'] = []
        return self.post_data(post_url, data)

    def post_collection(self, collection_data):
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/collections/"
        collection_data['links'] = self.cleanup_links(collection_data['links'], 'parent', f"https://api.{self.resto_instance}.edito.eu/data/collections/")
        return self.post_data(post_url, collection_data)

    def post_collection_to_child_catalog(self, collection_data, child_catalog):
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/catalogs/{child_catalog}/"
        collection_data['links'] = self.cleanup_links(collection_data['links'], 'parent', f"https://api.{self.resto_instance}.edito.eu/data/catalogs/{child_catalog}")
        return self.post_data(post_url, collection_data)
    
    def post_item(self, item_data):
        post_url = f"https://api.{self.resto_instance}.edito.eu/data/collections/{item_data['collection']}/items/"
        item_data['links'] = self.cleanup_links(item_data['links'], 'parent', f"https://api.{self.resto_instance}.edito.eu/data/collections/{item_data['collection']}/items/")
        return self.post_data(post_url, item_data)

    def cleanup_links(self, links, rel_to_replace, new_href):
        new_links = []
        for link in links:
            if link['rel'] == rel_to_replace:
                link['href'] = new_href
                new_links.append(link)
        return new_links
        

    def post_data(self, url, data, update=True):
        try:
            response = requests.post(url, headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.token}'
            }, json=data)
            if response.status_code == 409 and update == True:
                return self.update_data(url, data)
            if response.status_code == 401:
                logger.info(f"Token expired, refreshing.")
                self.get_initial_access_token()
                return self.post_data(url, data, update)
            if update == False:
                logger.info(f"update set to false for {url} {data['id']}")
            return response.json()
        except Exception as e:
            logger.error(f"Failed to post data to {url}: {e}")
            return None

    def update_data(self, url, data):
        response = requests.put(f"{url}{data['id']}", headers={
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }, json=data)
        return response.json()


    def delete_single_feature(self, product_id, collection, resto_id):
        post_url = f"https://catalog.{self.resto_instance}.edito.eu/collections/{collection}/items/{resto_id}"
        try:
            response = requests.delete(
            post_url, headers={
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
            }
        )

            delete_response = json.loads(response.text)
            return delete_response
        except Exception as e:
            logger.error(f"Failed to delete product_id {product_id} resto id {resto_id} from collection {collection}: {e}")
        
        return response

    def get_local_stac_features(self):
        logger.info(f"Getting local STAC features from {self.local_stac}/catalog.json")
        collections = pystac.Catalog.from_file(f"{self.local_stac}/catalog.json").get_all_collections()
        self.local_features = []
        for collection in collections:
            for feature in collection.get_all_items():
                self.local_features.append(feature)
        logger.info(self.local_features)
        return self.local_features

    def delete_matching_features(self):
        response_list = []
        for resto_id in self.matched_features:
            product_id = self.matched_features[resto_id]['product_id']
            collection = self.matched_features[resto_id]['collection']
            delete_response = self.delete_single_feature(product_id, collection, resto_id)
            response_list.append(delete_response)
            logger.info(f"Deleted feature {product_id} from collection {collection} from resto")
        
        self.mydeletedfeatures = response_list
        delete_response_df = pd.concat([pd.DataFrame(response_list).reset_index(drop=True)], axis=1)
        delete_response_df.to_csv(f'{self.restologs}/deleted_resto_features.csv', index=False)


    def get_all_user_features(self):
        # Ensure tokens are available
        if not self.final_token:
            logger.info("Final token not available or expired, refreshing.")
            self.get_initial_access_token()
            self.get_final_token()

        owner_response = requests.get(f"https://api.{self.resto_instance}.edito.eu/data/user", headers={
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.final_token}'
        })

        if owner_response.status_code != 200:
            logger.error("Failed to retrieve user information.")
            return

        owner = owner_response.json().get('id')
        logger.info(f"Owner: {owner}")
        limit = 500
        start_index = 1
        date_str = datetime.now().strftime("%Y-%m-%d")
        csv_file = os.path.join(self.restologs, f"all_user_features_{date_str}.csv")

        # Make an initial request to get the total number of matches
        search_url = f"https://api.{self.resto_instance}.edito.eu/data/search?owner={owner}&limit={limit}&startIndex={start_index}"
        first_response = requests.get(search_url)
        if first_response.status_code != 200:
            logger.error(f"Failed to retrieve initial search results from {self.resto_instance}")
            return

        number_matched = first_response.json().get('numberMatched')
        logger.info(f"Total matches: {number_matched}")
        
        self.myfeatures = {}

        num_requests = (number_matched + limit - 1) // limit
        my_features_df = pd.DataFrame(columns=["resto_id", "product_id", "collection"])

        # Make the requests and process each batch
        for i in range(num_requests):
            start_index = i * limit + 1
            response = requests.get(f"{search_url}&startIndex={start_index}")
            if response.status_code == 200:
                features = response.json().get('features', [])
                
                logger.info('adding my features to resto stac manager')
                for feature in features:
                    self.myfeatures[feature.get('id')] = {
                        "product_id": feature.get('properties', {}).get('productIdentifier'),
                        "collection": feature.get('collection')
                    }
                logger.info(f"added my features to resto stac manager")

                logger.info('adding my features to csv')
                batch_df = pd.DataFrame([{
                    "resto_id": feature.get('id'),
                    "product_id": feature.get('properties', {}).get('productIdentifier'),
                    "collection": feature.get('collection')
                } for feature in features])
                # Append the batch DataFrame to the main DataFrame
                my_features_df = pd.concat([my_features_df, batch_df], ignore_index=True)
                logger.info(f"Processed items {start_index} to {start_index + limit - 1}")
            else:
                logger.error(f"Failed to retrieve data for start index {start_index}.")
    
        my_features_df.to_csv(csv_file, index=False)
        logger.info(f"Saved ids, featureids, and collections to {csv_file}")
        return my_features_df

    def match_features(self):
        matched_features = []
        resto_features = self.get_all_user_features()
        for feature in self.local_features:
            localid = feature.id
            user_features_ids = list(resto_features['product_id'])
            # Check if the feature is in the user features
            if localid in user_features_ids:
                resto_feature = resto_features[resto_features['product_id'] == localid]
                matched_features.append(resto_feature)
        self.matched_features = matched_features
        return matched_features

# pipeline_config = {
#     "resto_instance": "staging",
#     'stac_s3': 'geonetwork_test_stac'
# }
# resto_stac_manager = RestoStacManager(pipeline_config)
# resto_stac_manager.post_stac_data()
# resto_stac_manager.backup_resto_logs()