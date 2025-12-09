import json
import boto3
import pandas as pd
import numpy as np
from io import StringIO, BytesIO
from datetime import datetime
import hashlib
import uuid

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

JOBS_TABLE = 'JobsTable'
SCHEMA_TABLE = 'SchemaTable'
DATALAKE_BUCKET = 'data-lake-bucket-processed'

def lambda_handler(event, context):
    try:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        parts = key.split('/')
        user_id = parts[1] if len(parts) > 1 else 'anonymous'
        job_id = str(uuid.uuid4())

        update_job_status(job_id, user_id, 'PROCESSING', {
            'source_bucket': bucket,
            'source_key': key,
            'started_at': datetime.now().isoformat()
        })

        df = read_csv_from_s3(bucket, key)
        original_rows = len(df)
        print(f"Original rows: {original_rows}")

        # Thêm transform
        df = transform_data(df)

        schema_id = detect_and_register_schema(df, user_id, key)
        print(f"Schema ID: {schema_id}")

        output_path = save_to_datalake(df, user_id, schema_id, job_id)
        print(output_path)

        # Update COMPLETED với output_path
        update_job_status(job_id, user_id, 'COMPLETED', {
            'source_bucket': bucket,
            'source_key': key,
            'output_path': output_path,
            'ended_at': datetime.now().isoformat()
        })

    except Exception as e:
        print(f"Error: {str(e)}")
        if 'job_id' in locals():
            update_job_status(job_id, user_id, 'FAILED', {'error': str(e)})


def update_job_status(job_id, user_id, status, metadata):
    """Update job status in DynamoDB"""
    jobs_table = dynamodb.Table(JOBS_TABLE)

    item = {
        'jobId': job_id,
        'userId': user_id,
        'status': status,
        'timestamp': int(datetime.now().timestamp()),
        'metadata': metadata
    }

    jobs_table.put_item(Item=item)
    print(f"Job {job_id} status updated: {status}")

def read_csv_from_s3(bucket, key):
    """Read CSV from S3 and return DataFrame"""
    try:
        respone = s3.get_object(Bucket=bucket, Key=key)
        csv_content = respone['Body'].read().decode('utf-8')

        for delimiter in [',', ', ', ':', '\t', '|']:
            try:
                df = pd.read_csv(StringIO(csv_content), delimiter=delimiter)
                if len(df.columns) > 1:
                    return df
            except:
                continue
        
        raise ValueError(f"Could not parse CSV with any common delimiter")
    except Exception as e:
        raise Exception(f"Error reading CSV: {str(e)}")
    
def detect_and_register_schema(df, user_id, filename):
    """ 
    Detect schema and register in DynamoDB
    Returns schema_id for tracking
    """
    # Extract schema information
    schema_info = {
        'columns': list(df.columns),
        'dtypes': {col:str(dtype) for col, dtype in df.dtypes.items()},
    }

    schema_fingerprint = hashlib.md5(
        json.dumps(sorted(df.columns.tolist())).encode()
    ).hexdigest()

    schema_id = f"schema_{schema_fingerprint}"

    # Save to DynamoDB
    schema_table = dynamodb.Table(SCHEMA_TABLE)

    try:
        respone = schema_table.get_item(Key={'schemaId': schema_id})
        if 'Item' in respone:
            print(f"Schema {schema_id} already exists")
        else:
            # Register new schema
            schema_table.put_item(Item={
                'schemaId': schema_id,
                'userId': user_id,
                'schemaInfo': schema_info,
                'filename': filename,
            })
            print(f"New schema registered: {schema_id}")
    except Exception as e:
        print(f"Error registering schema: {e}")
    
    return schema_id
    
def transform_data(df):
    """
    Apply transformations to clean the data
    """
    df_clean = df.copy()
    
    # 1. Remove completely empty rows
    df_clean = df_clean.dropna(how='all')
    
    # 2. Remove duplicate rows
    df_clean = df_clean.drop_duplicates()
    
    # 3. Handle missing values by column type
    for column in df_clean.columns:
        dtype = df_clean[column].dtype
        
        if pd.api.types.is_numeric_dtype(dtype):
            # Fill numeric columns with median
            if df_clean[column].isnull().any():
                median_val = df_clean[column].median()
                df_clean[column].fillna(median_val, inplace=True)
        
        elif pd.api.types.is_object_dtype(dtype):
            # Fill string columns with 'Unknown'
            df_clean[column].fillna('Unknown', inplace=True)
        
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            # Fill datetime columns with mode or drop
            if df_clean[column].isnull().any():
                df_clean[column].fillna(method='ffill', inplace=True)
    
    # 4. Remove outliers for numeric columns (optional)
    for column in df_clean.select_dtypes(include=[np.number]).columns:
        Q1 = df_clean[column].quantile(0.25)
        Q3 = df_clean[column].quantile(0.75)
        IQR = Q3 - Q1
        
        # Keep data within 1.5*IQR
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        df_clean = df_clean[
            (df_clean[column] >= lower_bound) & 
            (df_clean[column] <= upper_bound)
        ]
    
    # 5. Strip whitespace from string columns
    for column in df_clean.select_dtypes(include=['object']).columns:
        df_clean[column] = df_clean[column].str.strip()
    
    # 6. Add metadata columns
    df_clean['_processed_at'] = datetime.now().isoformat()
    df_clean['_source_file'] = 'uploaded'
    
    return df_clean

def save_to_datalake(df, user_id, schema_id, job_id):
    """
    Save DataFrame to Data Lake as Parquet
    Organized by user and schema for easy querying
    """
    # Create partition path: processed/{userId}/{schemaId}/{date}/{jobId}.parquet
    date_partition = datetime.now().strftime('%Y-%m-%d')
    
    output_key = f"processed/user={user_id}/schema={schema_id}/date={date_partition}/{job_id}.parquet"
    
    # Convert to Parquet in memory
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine='fastparquet', compression='snappy', index=False)
    parquet_buffer.seek(0)
    
    # Upload to S3
    s3.put_object(
        Bucket=DATALAKE_BUCKET,
        Key=output_key,
        Body=parquet_buffer.getvalue(),
        ContentType='application/octet-stream'
    )
    
    return f"s3://{DATALAKE_BUCKET}/{output_key}"

