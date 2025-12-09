from pydantic import BaseModel

class Job(BaseModel):
    jobId: str
    status: str
    filename: str