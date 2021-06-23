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

# logging = 

# sc = SparkContext()
# glueContext = GlueContext(sc)
# logger = glueContext.get_logger()
# logger.info("info message")

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

## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# print (args['JOB_NAME']+" START...")
print(sys.argv)
# ['/tmp/glue-python-scripts-6ae9hoal/test.py', '--enable-metrics', 
# '--continuous-log-logGroup', 'fetch_data_from_source_to_s3', 
# '--enable-continuous-log-filter', 'false', 
# '--enable-continuous-cloudwatch-log', 'true', 
# '--job-bookmark-option', 'job-bookmark-disable', 
# '--scriptLocation', 's3://adx-glue-jobs/scripts/test.py', 
# '--job-language', 'python', 
# '--S3_BUCKET', 'adx-glue-jobs', 
# '--TempDir', 's3://adx-glue-jobs/temp']

params = ['JOB_NAME', 'REGION', 'MANIFEST_S3_BUCKET', 'S3_BUCKET', 'S3_PREFIX', 'PRODUCT_NAME', 'PRODUCT_ID', 'DATASET_NAME', 'DATASET_ARN']
args = getResolvedOptions(sys.argv, params)
logging.info(args)

region = args['REGION']
manifest_s3_bucket = args['MANIFEST_S3_BUCKET']
s3_bucket = args['S3_BUCKET']
s3_prefix = args['S3_PREFIX']
product_name = args['PRODUCT_NAME']
product_id = args['PRODUCT_ID']
dataset_name = args['DATASET_NAME']
dataset_arn = args['DATASET_ARN']

categories_filename = 'top-level-categories.txt'
subcats_filename = 'all-categories.csv'
index_filename = 'index.csv'
product_info_filename = 'product-info.jsonl'

timestamp = datetime.now(tz=pytz.timezone('US/Eastern')).strftime("%d%b%Y-%H%M%S")
manifest_filename = 'manifest.json'  #'manifest-{}.json'.format(timestamp)

project_dir = 'wiki'
cats_dir = project_dir
pages_dir = os.path.join(cats_dir, 'pages')
max_depth = 3
    
categories_filepath = os.path.join(project_dir, categories_filename)
index_filepath = os.path.join(project_dir, index_filename)
subcats_filepath = os.path.join(project_dir, subcats_filename)
manifest_filepath = os.path.join(project_dir, manifest_filename)

global_titles = [
    'English-language films',
    'English-language television shows',
    'American films'
]

disney_domain_categories = [
    '20th Century Fox Television films',
    '20th Century Fox direct-to-video films',
    '20th Century Fox films',
    '20th Century Fox short films',
    '20th Century Studios films',
    'ABC Family original programming',
    'ABC Motion Pictures films',
    'American Broadcasting Company original programming',
    'Buena Vista Home Entertainment direct-to-video films',
    'Caravan Pictures films',
    'Disney Channel Original Movie films',
    'Disney Channel original programming',
    'Disney Television Animation films',
    'Disney XD original programming',
    'Disney direct-to-video animated films',
    'Disney direct-to-video films',
    'Disney documentary films',
    'Disney film remakes',
    'Disney+ original films',
    'Disney+ original programming',
    'DisneyToon Studios animated films',
    'Disneynature films',
    'Donald Duck television series',
    'Films produced by Walt Disney',
    'Fox Family Channel original programming',
    'Fox Searchlight Pictures films',
    'Fox Television Animation films',
    'Freeform (TV channel) original programming',
    'Hollywood Pictures films',
    'Hulu original programming',
    'Lucasfilm films',
    'Marvel Cinematic Universe films',
    'Marvel One-Shots',
    'Mickey Mouse short films',
    'National Geographic (American TV channel) original programming',
    'Pixar animated films',
    'Pixar short films',
    'SparkShorts',
    'Television series by 20th Century Fox Television',
    'Television series by ABC Signature Studios',
    'Television series by ABC Studios',
    'Television series by Disney',
    'Television series by Disney Television Animation',
    'Television series by Disney–ABC Domestic Television',
    'Television series by Fox Television Animation',
    "Television series by It's a Laugh Productions",
    'Television shows based on Marvel Comics',
    'Television shows based on works by Jack Kirby',
    'Television shows based on works by Stan Lee',
    'Touchstone Pictures films',
    'Walt Disney Animation Studios films',
    'Walt Disney Pictures films'
]

cats = global_titles + disney_domain_categories


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

def upload_top_level_cats_to_s3():
    
    s3 = boto3.client('s3')
    
    if not os.path.isfile(categories_filepath):
        logging.info('writing all categories to a file')
        with open(categories_filepath, 'w+') as writer:
            for cat in cats:
                writer.write(cat + '\n')
                categories_s3_key = os.path.join(s3_prefix, dataset_name, 'dataset', categories_filename)
                s3.upload_file(categories_filepath, s3_bucket, categories_s3_key)


def get_one_level_subcats(cat, slug):
    """Save one level of subcats of category <cat>"""
#     print(cat)
    
    api_url = "https://en.wikipedia.org/w/api.php"
    subcat_list = []
    
    session = requests.Session()
    
    # https://www.mediawiki.org/w/api.php?action=help&modules=main#main/datatype/timestamp
#     days_back = 2
#     # Local to ISO 8601 with TimeZone information (Python 3):
#     start = datetime.now()
#     end = start - timedelta(days=days_back)

    cmcontinue = 'start'
    while cmcontinue:
        
        prms = {
            "action": "query",
            "cmtitle": "Category:{}".format(cat),
            "cmlimit": "40",
            "list": "categorymembers",
            "format": "json",
            "cmtype": "subcat", # Default: page|subcat|file
            "cmprop": "title|type|timestamp", # ids|title|sortkey|sortkeyprefix|type|timestamp",
#             "cmsort": "timestamp",
#             "cmstart": end.astimezone().isoformat(), # starting timestamp
#             "cmend": start.astimezone().isoformat(), # ending timestamp
            "cmcontinue": cmcontinue if cmcontinue != 'start' else None
        }
        success = False
        retries = 5
        i = 0
        res = None
        while not success or i < retries:
            try:
                i += 1
                res = session.get(url=api_url, params=prms)
                success = True   
            except Exception as e:
                print(e.message())
                time.sleep(1)
                if i >= retries:
                    sys.exit()
            except Error as e:
                print(e.message())
                time.sleep(1)
                if i >= retries:
                    sys.exit()
            
        data = res.json()

        pages = data["query"]["categorymembers"]
#         print('cats: ' + json.dumps(pages))

        for page in pages:
#             print('cat: ' + json.dumps(page))
            if page['title']:
                subcat_list.append(page['title'])

        if "continue" in data:
            cmcontinue = data["continue"]["cmcontinue"]  
        else:
            break

    return subcat_list


def get_subcats(top_cat, slug, max_depth, cats_dir):
    """Save subcats of category <top_cat> to max_depth levels"""
    
    filename = os.path.join(cats_dir, "wiki-subcats-all-{}.csv".format(slug))
    filename_deduped = subcats_filepath
        
    subcat_list = [[top_cat]]
        
    with open(filename, 'w+') as writer:
        writer.write('category|level|top_category' + '\n')
        for d in range(max_depth+1):
            logging.info('depth ' + str(d))
            if len(subcat_list) > d:
                logging.info('cats in depth: ' + str(len(subcat_list[d])))
#                 logging.info('cats: ')
#                 logging.info(subcat_list[d])
                if len(subcat_list[d]) > 0:
                    for cat in subcat_list[d]:
                        writer.write(cat + '|' + str(d) + '|' + top_cat + '\n')
                    if d < max_depth:
                        subcat_list.append([])
                        for cat in subcat_list[d]:
                            title_subcats = get_one_level_subcats(cat, slug)
                            if title_subcats:
                                subcat_list[d+1].extend([item[9:].strip() for item in title_subcats])
                        
            else:
                break
            # break #####

    # dedup category file
    df = pd.read_csv(filename, header=0, sep='|')
    logging.info(df.shape)
    logging.info(df.head())

    df.drop_duplicates(ignore_index=True, inplace=True, subset=['category'])
    logging.info(df.shape)
    logging.info(df.head())
    
    df.to_csv(filename_deduped, mode='a', index=False, header=False, sep='|')
    
    os.remove(filename)
                
    return filename_deduped


def get_categorymembers(cats_filename, cats_dir):
    """
    Get a list of all pages under categories listed in cats_filename and save to a file
    ref: https://www.mediawiki.org/wiki/API:Categorymembers
    """
    
    tmp_index_filename = os.path.join(cats_dir, 'tmp-index.csv')
    session = requests.Session()
    api_url = "https://en.wikipedia.org/w/api.php"
    header = ["pageid", "title", "category", "top_category", "timestamp"]
    
    categories_df = pd.read_csv(cats_filename, header=0, sep='|')[['category', 'top_category']]
     
    with open(tmp_index_filename, 'w+') as writer:
        writer.write("|".join(header) + '\n')
        rnd, i = 0, 0
        for _, line in categories_df.iterrows():
            cat = str(line['category'].strip())
            top_cat = str(line['top_category'].strip())
            logging.info('category: ' + cat)
            cmcontinue = 'start'

            while cat and cmcontinue:

                params = {
                    "action": "query",
                    "cmtitle": "Category:{}".format(cat),
                    "cmlimit": "20",
                    "list": "categorymembers",
                    "format": "json",
                    "cmtype": "page", #Default: page|subcat|file
                    "cmprop": "ids|title|sortkey|sortkeyprefix|type|timestamp",
                    "cmcontinue": cmcontinue if cmcontinue != 'start' else None
                }

                success = False
                retries = 5
                i = 0
                while not success and i < retries:
                    try:
                        i += 1
                        res = session.get(url=api_url, params=params)
                        data = res.json()
                        pages = data["query"]["categorymembers"]
                        success = True
                    except Exception as e:
                        logging.info(e)
                        time.sleep(1)
                        if i >= retries:
                            sys.exit()

                for page in pages:
                    writer.write(
                        str(page['pageid']) + "|" +
                        str(page['title']) + "|" +
                        cat + "|" +
                        top_cat + "|" +
                        str(page['timestamp']) + '\n'
                    )
#                   print(page['title'])

                i += len(pages)
                rnd += 1
                if rnd % 100 == 0:
                    logging.info('processed ' + str(i))
            
                if "continue" in data:
                    cmcontinue = data["continue"]["cmcontinue"]  
                else:
                    break
                # break ###
                        
    logging.info('dedup temporary index file')
    df = pd.read_csv(tmp_index_filename, header=0, sep='|') #.reset_index()
    logging.info(df.shape)
    logging.info(df.head())

    df.drop_duplicates(ignore_index=True, inplace=True, subset=['pageid'])
    logging.info('After deduping the index file:')
    logging.info(df.shape)
    logging.info(df.head())
        
    deduped_index_filename = index_filepath #os.path.join(cats_dir, index_filename)
    df.to_csv(deduped_index_filename, header=False, index=False, mode='a+', sep='|')
    
    os.remove(tmp_index_filename)
    
    return deduped_index_filename


def get_index(cats_dir, categories, max_depth, s3_bucket, s3_prefix, data_set_name):
    """get all subcats up to max_depth"""
    
    if not os.path.exists(cats_dir):
        os.makedirs(cats_dir)
        
    logging.info("cates_dir: {}".format(cats_dir))
    
    with open(index_filepath, 'w+') as writer:
        header = ["pageid", "title", "category", "top_category", "timestamp"]
        writer.write("|".join(header) + '\n')
    
    with open(subcats_filepath, 'w+') as writer:
        header = ["category", "level", "top_category"]
        writer.write("|".join(header) + '\n')
    
    logging.info('satrting to loop through {} categories'.format(len(cats)))
    subcats_file = None
    for category in categories:
        cat_slug = cat_to_slug(category)
        logging.info(category)
        logging.info(cat_slug)
        subcats_file = get_subcats(category, cat_slug, max_depth, cats_dir)
        logging.info(subcats_file)

    index_file = get_categorymembers(subcats_file, cats_dir)
    logging.info('index file: {}'.format(index_file))
    
    logging.info('dedupe index file')
    df = pd.read_csv(index_filepath, header=0, sep='|') #.reset_index()
    logging.info(df.shape)
    logging.info(df.head())
    logging.info(df.columns)

    df.drop_duplicates(ignore_index=True, inplace=True, subset=['pageid'])
    logging.info('After deduping the index file:')
    logging.info(df.shape)
    logging.info(df.head())

    deduped_index_filename = index_filepath #os.path.join(cats_dir, 'test_'+index_filename) #
    df.to_csv(deduped_index_filename, index=False, sep='|')

    # upload product metafile
    s3 = boto3.client('s3')

    cats_file_s3_key = os.path.join(s3_prefix, data_set_name, 'dataset', subcats_file.split('/')[-1])
    s3.upload_file(subcats_file, s3_bucket, cats_file_s3_key)
    logging.info('uploaded ' + subcats_file + ' to ' + cats_file_s3_key)

    index_metafile_s3_key = os.path.join(s3_prefix, data_set_name, 'dataset', index_file.split('/')[-1])
    s3.upload_file(index_file, s3_bucket, index_metafile_s3_key) 
    logging.info('uploaded ' + index_file + ' to ' + index_metafile_s3_key)            
    

def upload_manifest_file():
    s3 = boto3.client('s3')
    pageids = list(pd.read_csv(index_filepath, header=0, sep='|')['pageid'])
    
#     page_s3_key = 's3://{}/'.format(os.path.join(s3_bucket, s3_prefix, dataset_name, 'dataset'))
    page_s3_key = os.path.join(s3_prefix, dataset_name, 'dataset')
    index_key = os.path.join(page_s3_key, index_filename)
    cats_key = os.path.join(page_s3_key, categories_filename)
    subcats_key = os.path.join(page_s3_key, subcats_filename)
    
    asset_list = [index_key, cats_key, subcats_key] + ['{}pageid-{}.txt'.format(page_s3_key, pid) for pid in pageids]
    manifest_data = {
        'product_name': product_name,
        'product_id': product_id,
        'dataset_name': dataset_name,
        'datset_arn': dataset_arn,
        'asset_list': asset_list
    }
    
    with open(manifest_filepath, 'w+') as writer:
        writer.write(json.dumps(manifest_data))
    
    manifest_s3_key = os.path.join(dataset_name, manifest_filename)
    s3.upload_file(manifest_filepath, manifest_s3_bucket, manifest_s3_key)
    logging.info('uploaded ' + manifest_filename + ' to ' + manifest_s3_key) 
    
###############################################################   
logging.info('Upload top level categories to S3')
upload_top_level_cats_to_s3()
            
logging.info('Get index for all categories')
get_index(cats_dir, cats, max_depth, s3_bucket, s3_prefix, dataset_name)

logging.info('Uploading manifest file to manifest S3 bucket')
upload_manifest_file()
