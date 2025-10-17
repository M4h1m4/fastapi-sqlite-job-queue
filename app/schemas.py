from __future__ import annotations
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel

class CreateJobResponse(BaseModel):
    job_id: UUID
    status: str
 
class JobStatusResponse(BaseModel):
    job_id : UUID
    status: str
    created_at: datetime
    updated_at: datetime
    processing_by: Optional[str] = None
    lease_until: Optional[datetime] = None
    
class JobResultResponse(BaseModel):
    job_id : UUID
    status: str 
    characters: int
