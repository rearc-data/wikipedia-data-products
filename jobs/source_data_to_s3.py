# Install libraries
import os
import site
import importlib

from setuptools.command import easy_install
install_path = os.environ['GLUE_INSTALLATION']
easy_install.main( ["--install-dir", install_path, "pandas==1.1.5"] )
importlib.reload(site)


import os
import sys
from awsglue.utils import getResolvedOptions
import boto3
import re
import json
import logging
# from awsglue.context import GlueContext
# sc = SparkContext()
# glueContext = GlueContext(sc)
# logging = glueContext.get_logger()
# logging.info("info message")

import time
from datetime import datetime, timedelta
import pytz
from boto3.s3.transfer import TransferConfig
from io import BytesIO
import pandas as pd
import requests

# from s3_md5_compare import md5_compare

root = logging.getLogger()
root.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)
root.info("check")


logging.info('pandas version: {}'.format(pd.__version__))

logging.info(sys.argv)
# ['/tmp/glue-python-scripts-6ae9hoal/test.py', '--enable-metrics', 
# '--continuous-log-logGroup', 'fetch_data_from_source_to_s3', 
# '--enable-continuous-log-filter', 'false', 
# '--enable-continuous-cloudwatch-log', 'true', 
# '--job-bookmark-option', 'job-bookmark-disable', 
# '--scriptLocation', 's3://adx-glue-jobs/scripts/test.py', 
# '--job-language', 'python', 
# '--S3_BUCKET', 'adx-glue-jobs', 
# '--TempDir', 's3://adx-glue-jobs/temp']

## @params:
params = ['JOB_NAME', 'REGION', 'MANIFEST_S3_BUCKET', 'S3_BUCKET', 'S3_PREFIX', 'PRODUCT_NAME', 'PRODUCT_ID', 'DATASET_NAME', 'DATASET_ARN']
args = getResolvedOptions(sys.argv, params)
logging.info(args)

region = args['REGION']
manifest_s3_bucket = args['MANIFEST_S3_BUCKET']
s3_bucket = args['S3_BUCKET']
s3_prefix = args['S3_PREFIX'] #'wikipedia'
product_name = args['PRODUCT_NAME']
product_id = args['PRODUCT_ID']
dataset_name = args['DATASET_NAME']
dataset_arn = args['DATASET_ARN']

categories_filename = 'top-level-categories.txt'
subcats_filename = 'all-categories.csv'
index_filename = 'index.csv'
product_info_filename = 'product-info.jsonl'

timestamp = datetime.now(tz=pytz.timezone('US/Eastern')).strftime("%d%b%Y-%H%M%S")
manifest_filename = 'manifest-{}.json'.format(timestamp)

project_dir = 'wiki'
cats_dir = project_dir
pages_dir = os.path.join(cats_dir, 'pages')
max_depth = 1
    
categories_filepath = os.path.join(project_dir, categories_filename)
index_filepath = os.path.join(project_dir, index_filename)
subcats_filepath = os.path.join(project_dir, subcats_filename)
manifest_filepath = os.path.join(project_dir, manifest_filename)


if not os.path.exists(project_dir):
    logging.info('creating {} directory'.format(project_dir))
    os.makedirs(project_dir)

if not os.path.exists(cats_dir):
    logging.info('creating {} directory'.format(cats_dir))
    os.makedirs(cats_dir)

if not os.path.exists(pages_dir):
    logging.info('creating {} directory'.format(pages_dir))
    os.makedirs(pages_dir)
    
cat_to_slug = lambda x : re.sub(r'[^A-Za-z0-9]+', '-', x.lower())

flatten = lambda t: [item for sublist in t for item in sublist]
ordered_dedup = lambda t: list(dict.fromkeys(t))


config = TransferConfig(multipart_threshold=1024*25, max_concurrency=10,
                            multipart_chunksize=1024*25, use_threads=True)
s3 = boto3.client('s3')

# Get content of a page using title
# https://www.mediawiki.org/wiki/API:Get_the_contents_of_a_page
# api.php?action=query&prop=revisions&titles=Pet_door&rvslots=*&rvprop=content&formatversion=2
def save_wiki_content(s3_prefix, data_set_name, cmtitle, cmtype, target_filepath):
    s = requests.Session()

    URL = "https://en.wikipedia.org/w/api.php"
    i = 0
    cmcontinue = 'start'

    with open(target_filepath, 'w+') as writer:
#         while cmcontinue:

            params = {
                "action": "query",
                "prop": "revisions",
                "titles": cmtitle,
                "rvslots": "*",
                "rvprop": "content",
#                 "formatversion": "2",
                "format": "json"
#                 "cmcontinue": cmcontinue if cmcontinue != 'start' else None
            }

            success = False
            retries = 5
            i = 0
            while not success and i < retries:
                try:
                    i += 1
                    res = s.get(url=URL, params=params)
                    data = res.json()

                    pages = data["query"]["pages"]
                    success = True
                except Exception as e:
                    logging.info(e)
                    time.sleep(1)
                    if i >= retries:
                        sys.exit()
        
            
            for key, val in pages.items():
                writer.write(json.dumps(val) + '\n')
                
    new_s3_key = os.path.join(s3_prefix, data_set_name, 'dataset', target_filepath.split('/')[-1])
    s3.upload_file(target_filepath, s3_bucket, new_s3_key, Config=config)
#     logging.info('Uploaded {} to {}'.format(filename, new_s3_key))
    
    os.remove(target_filepath)
            

def get_page_files(target_dir, s3_prefix, data_set_name):
    """Download and save to file all pages under each category listed in cat_list"""
    
    filename_template = "pageid-{}.txt"
    
    session = requests.Session()

    api_url = "https://en.wikipedia.org/w/api.php"
    i = 0
    cmcontinue = 'start'
    
    pageid = None
    df = pd.read_csv(index_filepath, header=0, sep='|')
    logging.info(df.shape)
    logging.info(df.head())
    logging.info(df.columns)
    
    cmtype = 'page'
    for _, l in df.iterrows():
        # logging.info('______________')
        # logging.info(l)
        # logging.info('______________')
        pageid, cmtitle, cat, top_cat, cmtimestamp = list(l)
        page_filepath = os.path.join(target_dir, filename_template.format(pageid))
        save_wiki_content(s3_prefix, data_set_name, cmtitle, cmtype, page_filepath)


def fetch_pages(cats_dir, pages_dir, max_depth, s3_bucket, s3_prefix, data_set_name):
    """Fetch all pages from index file"""
    logging.info('download category index file')
    s3 = boto3.client('s3')
    index_file_s3_key = os.path.join(s3_prefix, data_set_name, 'dataset', index_filename)
    s3.download_file(s3_bucket, index_file_s3_key, index_filepath) 
    
    logging.info(index_file_s3_key)

    get_page_files(pages_dir, s3_prefix, data_set_name)
    

###############################################################            

logging.info('Get pages for all categories')
fetch_pages(cats_dir, pages_dir, max_depth, s3_bucket, s3_prefix, dataset_name)


