
import pandas as pd
import json
import os
import numpy as np
wkdir = os.getcwd()

# get the layers out of the processed records json that 'in_stac' is failed
# look at their 'links' from the csw, since not having a data product is the only reason to fail
processedrecordsjson = '../data/all_EMODnetGeoNetwork_processed_records.json'

with open(processedrecordsjson, 'r') as f:
    data = json.load(f)

failed_links_ids = []
# get all records
failed_ids_links = {}
for id, metadata in data.items():
    metadata['id'] = id
    # Extract the fields you need
    failed_links_ids.append(id)
    title = metadata.get('title', '')
    
    abstract = metadata.get('abstract', '')
    
    if metadata['in_stac'] == 'failed':

        links = metadata['links']
        non_asset_links = [link['url'] for link in links]

        failed_ids_links[id] = {
            'title': title,
            'abstract': abstract,
            'non_asset_links': non_asset_links,
        }

# Convert the failed links and metadata to a DataFrame
df = pd.DataFrame(failed_ids_links.items(), columns=['id', 'metadata'])

# Expand the metadata into separate columns
metadata_df = pd.json_normalize(df['metadata'])
df = pd.concat([df['id'], metadata_df], axis=1)

# Explode the failed_links column to separate rows
df = df.explode('non_asset_links')

# Eliminate repeating ids in the 'id' column
cols = df.columns.drop(['non_asset_links'])
df[cols] = df[cols].where(df[cols].apply(lambda x: x != x.shift()), '')

# Save to CSV
df.to_csv('non_asset_links.csv', index=False)

#total failed records

print(len(failed_links_ids))