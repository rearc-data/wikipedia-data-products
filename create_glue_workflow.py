import os
import boto3
import logging

glue_scripts_s3_bucket='adx-glue-jobs'
job_dir = 'adx-glue-jobs'
dataset_name = 'wikipedia-film-categories-disney'

dataset_description_filename = 'dataset-description.md'
product_description_filename = 'product-description.md'

project_dir = os.getcwd() #'wiki'

dataset_description_filename = 'dataset-description.md'
dataset_description_filepath = os.path.join(project_dir, dataset_description_filename)

manifest_s3_bucket = 'rearc-adx-provider-manifests'
s3_bucket = 'rearc-data-provider'
s3_prefix = 'wikipedia'
product_name = 'WikiPedia Corpus - Films and TV Shows Categories'
product_id = 'blank'
dataset_name = 'wikipedia-film-categories-disney'
dataset_arn = 'arn:aws:dataexchange:us-east-1:796406704065:data-sets/706e060432f9936d142f34732ec04b7b'
dataset_id = ''
region = 'us-east-1'


s3 = boto3.client('s3')

def upload_job_file_to_s3(job_dir):
    logging.info('Uploading ')
    job_names = []
    for root,dirs,files in os.walk(job_dir):
        for f in files:
            if f.endswith('.py'):
                job_name = f.split('.')[0]
                job_names.append(job_name)
                job_filename = f
                job_filepath = os.path.join(root, f)
                job_s3_key = os.path.join('scripts', dataset_name, job_filename)
                s3.upload_file(job_filepath, glue_scripts_s3_bucket, job_s3_key)
    return job_names


def create_adx_dataset(dataset_description_filepath):
    logging.info('create a dataset on ADX')

    if not dataset_arn:
    #     logging.info('download dataset description file')
    #     s3 = boto3.client('s3')
    #     dataset_description_s3_key = os.path.join('scripts', dataset_name, dataset_description_filename)
    #     s3.download_file(glue_scripts_s3_bucket, dataset_description_s3_key, dataset_description_filepath) 
        dataexchange = boto3.client(
            service_name='dataexchange',
            region_name=region
        )
        with open(dataset_description_filepath, 'r') as reader:
            description = reader.read()
            try:
                response = dataexchange.create_data_set(
                    AssetType='S3_SNAPSHOT',
                    Description=description,
                    Name=product_name,
                    Tags={'Name': 'ADX'}
                )
                logging.info(response)
                dataset_arn = response['Arn']
                dataset_id = response['Id']
                logging.info('dataset_arn: {}'.format(dataset_arn))
                logging.info('dataset_id: {}'.format(dataset_id))
            except Exception as e:
                logging.error(e)
                
    return dataset_arn


def create_glue_jobs(job_dir, job_names):
    
    job_ids = []
    
    acct_number=boto3.client('sts').get_caller_identity().get('Account')
    scripts_bucket='adx-glue-jobs'

    # Create the AWS Glue Spark Jobs
    glue = boto3.client("glue", region_name='us-east-1')

    job_names = ['CREATE_INDEX_FILE', 'SOURCE_DATA_To_S3', 'CREATE_DATASET_REVISIONS']
    jns = ['{}_{}'.format(dataset_name.replace('-', '_'), jn.lower()) for jn in job_names]
    jobs_dir = 'adx-glue-jobs'

    for i, job_name in enumerate(job_names):
        job_filename = job_name.lower() + '.py'
        jn = '{}_{}'.format(dataset_name.replace('-', '_'), job_name.lower())
        print(jn)

        response=glue.create_job(
                             Name=jns[i],
                             Role=f"arn:aws:iam::{acct_number}:role/GlueJobsRole", #glue-labs-GlueServiceRole",
                             ExecutionProperty={'MaxConcurrentRuns': 1},
                             Command={'Name': 'pythonshell', #'glueetl',
                                      'ScriptLocation': f's3://{scripts_bucket}/scripts/{dataset_name}/{job_filename}',
                                      'PythonVersion': '3'},
                             DefaultArguments={
                                                 '--TempDir': f's3://{scripts_bucket}/temp',
                                                 '--enable-continuous-cloudwatch-log': 'true',
                                                 '--enable-continuous-log-filter': 'false',
                                                 '--continuous-log-logGroup': job_name.lower(),
                                                #'--enable-glue-datacatalog': '',
                                                 '--enable-metrics': '',
                                                #'--enable-spark-ui': 'true',
                                                #'--spark-event-logs-path': f's3://{bucket}/spark_glue_etl_logs/{job_name}',
                                                 '--job-bookmark-option': 'job-bookmark-disable',
                                                 '--job-language': 'python',
                                                 '--additional-python-modules': 'pandas==1.1.5,boto3==1.17.55',
                                                 '--extra-py-files': 's3://adx-glue-jobs/lib/awscli-1.19.61-py2.py3-none-any.whl,s3://adx-glue-jobs/lib/boto3-1.17.61-py2.py3-none-any.whl',
                                                 '--JOB_NAME': jns[i],
                                                 '--REGION': region,
                                                 '--MANIFEST_S3_BUCKET': manifest_s3_bucket,
                                                 '--S3_BUCKET': s3_bucket,
                                                 '--S3_PREFIX': s3_prefix,
                                                 '--PRODUCT_NAME': product_name,
                                                 '--PRODUCT_ID': product_id,
                                                 '--DATASET_NAME': dataset_name,
                                                 '--DATASET_ARN': dataset_arn
                                              },
                             MaxRetries=0,
                             Timeout=2880,
    #                          MaxCapacity=3.0,
                             GlueVersion='1.0',
                             Tags={'Name': 'ADX'}
                        )

        print(response)
        
        job_id = response['']
        job_ids.append(job_id)
    
    return job_ids


def create_glue_workflow(job_names, job_ids):

    glue = boto3.client("glue")

    workflow_name = 'ADX_Workflow_{}'.format(dataset_name.replace('-', '_'))

#     job_names = ['CREATE_INDEX_FILE', 'SOURCE_DATA_To_S3', 'CREATE_DATASET_REVISIONS']
    jns = ['{}_{}'.format(dataset_name.replace('-', '_'), jn.lower()) for jn in job_names]

    print(jns)

    # Create the AWS Glue Workflow
    response = glue.create_workflow(
        Name=workflow_name,
        Description='Publishing workflow for ADX product: {}'.format(product_name),
        MaxConcurrentRuns=1
    )
    print (response)

    # 1. Create index file 
    response = glue.create_trigger(
        Name='ADX_Trigger_{}'.format(jns[0]),
        WorkflowName=workflow_name,
        Type='SCHEDULED',
        Schedule='cron(00 18 ? * TUE *)',
        StartOnCreation=True,
        Actions=[
            {
                'JobName': jns[0],
                'Arguments': {'--job-bookmark-option': 'job-bookmark-disable'},
                'Timeout': 2880
            }
        ]
    )
    print (response)  

    # 2. Fetch pages from source to s3
    response = glue.create_trigger(
        Name='ADX_Trigger_{}'.format(jns[1]),
        WorkflowName=workflow_name,
        Type='CONDITIONAL',
        StartOnCreation=True,
        Actions=[
            {
                'JobName': jns[1],
                'Arguments': {'--job-bookmark-option': 'job-bookmark-disable'},
                'Timeout': 2880
            }
        ],
        Predicate= {
            'Logical': 'ANY',
            'Conditions': [{'LogicalOperator': 'EQUALS',
                          'JobName': jns[0],
                           'State': 'SUCCEEDED'}]
        }
    )
    print (response) 

    # 3. Add assets to dataset and create revisions
    response = glue.create_trigger(
        Name='ADX_Trigger_{}'.format(jns[2]),
        WorkflowName=workflow_name,
        Type='CONDITIONAL',
        StartOnCreation=True,
        Actions=[
            {
                'JobName': jns[2],
                'Arguments': {'--job-bookmark-option': 'job-bookmark-disable'},
                'Timeout': 2880
            }
        ],
        Predicate= {
            'Logical': 'ANY',
            'Conditions': [
                {'LogicalOperator': 'EQUALS',
                 'JobName': jns[1],
                 'State': 'SUCCEEDED'}
            ]
        }
    )
    print (response)  
    
    return workflow_name
    

def trigger_workflow(workflow_name):
    
    glue = boto3.client("glue")

    response = glue.start_workflow_run(
        Name=workflow_name
    )
    

############################

dataset_arn = create_adx_dataset(dataset_description_filepath)
        
job_names = upload_job_file_to_s3(job_dir)

job_ids = create_glue_jobs(job_dir, job_names)

workflow_name = create_glue_workflow(job_names, job_ids)

trigger_workflow(workflow_name)