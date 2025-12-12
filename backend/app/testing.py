from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from schemas import Job
import uuid
import boto3
from datetime import timedelta
from pydantic import BaseModel
import boto3
from datetime import datetime

app = FastAPI(title="ETL SaaS API")

import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
JOBS_TABLE = 'JobsTable'

from jose import jwt, JWTError
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, options={"verify_signature": False})  # Demo only, add verification in production
        return payload
    except JWTError:
        raise HTTPException(401, "Invalid token")

def create_job(job_id: str, user_id: str, filename: str, key: str):
    table = dynamodb.Table(JOBS_TABLE)
    table.put_item(Item={
        'jobId': job_id,
        'userId': user_id,
        'status': 'QUEUED',
        'filename': filename,
        's3_key': key,
        'created_at': datetime.now().isoformat()
    })

def get_jobs_by_user(user_id: str):
    table = dynamodb.Table(JOBS_TABLE)
    response = table.scan(FilterExpression='userId = :uid', ExpressionAttributeValues={':uid': user_id})
    return response.get('Items', [])

def get_job(job_id: str):
    table = dynamodb.Table(JOBS_TABLE)
    response = table.get_item(Key={'jobId': job_id})
    return response.get('Item', {})

class Job(BaseModel):
    jobId: str
    status: str
    filename: str

dynamodb = boto3.resource('dynamodb')
JOBS_TABLE = 'JobsTable'

def create_job(job_id: str, user_id: str, filename: str, key: str):
    table = dynamodb.Table(JOBS_TABLE)
    table.put_item(Item={
        'jobId': job_id,
        'userId': user_id,
        'status': 'QUEUED',
        'filename': filename,
        's3_key': key,
        'created_at': datetime.now().isoformat()
    })

def get_jobs_by_user(user_id: str):
    table = dynamodb.Table(JOBS_TABLE)
    response = table.scan(FilterExpression='userId = :uid', ExpressionAttributeValues={':uid': user_id})
    return response.get('Items', [])

def get_job(job_id: str):
    table = dynamodb.Table(JOBS_TABLE)
    response = table.get_item(Key={'jobId': job_id})
    return response.get('Item', {})

# Thêm CORS để front-end trên S3 gọi được API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Hoặc cụ thể URL S3 của bạn
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

s3 = boto3.client('s3')
UPLOAD_BUCKET = 'source-bucket-oabga'  # Set in EC2 env if needed
DATALAKE_BUCKET = 'data-lake-bucket-processed'

@app.post("/api/upload")
async def upload_csv(file: UploadFile = File(...), user=Depends(get_current_user)):
    user_id = user.get('sub', 'anonymous')
    if not file.filename.endswith('.csv'):
        raise HTTPException(400, "Only CSV allowed")
    
    job_id = str(uuid.uuid4())
    key = f"uploads/{user_id}/{job_id}_{file.filename}"
    
    s3.put_object(Bucket=UPLOAD_BUCKET, Key=key, Body=await file.read())
    
    create_job(job_id, user_id, file.filename, key)
    
    return {"job_id": job_id, "status": "QUEUED"}

@app.get("/api/jobs")
def list_jobs(user=Depends(get_current_user)):
    return get_jobs_by_user(user.get('sub'))

@app.get("/api/jobs/{job_id}")
def get_job_detail(job_id: str, user=Depends(get_current_user)):
    job = get_job(job_id)
    if job.get('userId') != user.get('sub'):
        raise HTTPException(403)
    return job

@app.get("/api/jobs/{job_id}/download")
def download_parquet(job_id: str, user=Depends(get_current_user)):
    job = get_job(job_id)
    if job.get('userId') != user.get('sub'):
        raise HTTPException(403)
    metadata = job.get('metadata', {})
    output_path = metadata.get('output_path')
    if not output_path:
        raise HTTPException(404, "No Parquet")
    
    parts = output_path.replace('s3://', '').split('/', 1)
    bucket = parts[0]
    key = parts[1]
    
    url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=3600)
    return {"download_url": url}
