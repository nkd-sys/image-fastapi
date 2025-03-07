from fastapi import FastAPI, UploadFile, File, BackgroundTasks
from pydantic import BaseModel
import csv
import uuid
import os
import shutil
import requests
from PIL import Image
from io import BytesIO
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import sessionmaker, declarative_base
import celery
from celery import Celery

app = FastAPI()

# Configure Celery
celery_app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

# Database setup
DATABASE_URL = "sqlite:///./image_processing.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ImageProcessingRequest(Base):
    __tablename__ = "image_requests"
    id = Column(String, primary_key=True, index=True)
    status = Column(String, default="pending")
    csv_file_path = Column(String, nullable=False)
    output_csv_path = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/upload/")
def upload_csv(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    request_id = str(uuid.uuid4())
    file_path = f"./uploads/{request_id}.csv"
    os.makedirs("./uploads", exist_ok=True)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    db = next(get_db())
    db_request = ImageProcessingRequest(id=request_id, csv_file_path=file_path)
    db.add(db_request)
    db.commit()
    
    background_tasks.add_task(process_images, request_id, file_path)
    return {"request_id": request_id, "status": "Processing started ctrl"}

@celery_app.task
def process_images(request_id: str, file_path: str):
    db = SessionLocal()
    db_request = db.query(ImageProcessingRequest).filter_by(id=request_id).first()
    output_csv_path = f"./uploads/{request_id}_output.csv"
    
    with open(file_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        headers = next(reader)
        
        output_rows = [headers + ["Output Image Urls ind"]]
        for row in reader:
            serial, product_name, image_urls = row
            image_urls_list = image_urls.split(",")
            output_urls = []
            
            for image_url in image_urls_list:
                processed_url = compress_image(image_url, request_id)
                output_urls.append(processed_url)
            
            output_rows.append([serial, product_name, image_urls, ",".join(output_urls)])
    
    with open(output_csv_path, "w", newline="", encoding="utf-8") as csv_out:
        writer = csv.writer(csv_out)
        writer.writerows(output_rows)
    
    db_request.status = "completed"
    db_request.output_csv_path = output_csv_path
    db.commit()
    db.close()
    trigger_webhook(request_id, output_csv_path)

def compress_image(image_url: str, request_id: str):
    response = requests.get(image_url, stream=True)
    image = Image.open(BytesIO(response.content))
    output_path = f"./processed/{request_id}_{os.path.basename(image_url)}"
    os.makedirs("./processed", exist_ok=True)
    image.save(output_path, quality=50)
    return output_path

@app.get("/status/{request_id}")
def get_status(request_id: str):
    db = next(get_db())
    db_request = db.query(ImageProcessingRequest).filter_by(id=request_id).first()
    if db_request:
        return {"request_id": request_id, "status": db_request.status, "output_csv": db_request.output_csv_path}
    else:
        return {"error": "Invalid ID"}

def trigger_webhook(request_id: str, output_csv: str):
    webhook_url = "https://your-webhook-endpoint.com/callback"
    requests.post(webhook_url, json={"request_id": request_id, "output_csv": output_csv})
