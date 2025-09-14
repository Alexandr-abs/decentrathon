from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime

from models import get_db, create_tables
from database_service import DatabaseService
from data_processor import process_all_data
from config import OPENAI_API_KEY

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Astana Taxi Analytics API",
    description="API for processed taxi and GPS data with LLM insights",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variable to track processing status
processing_status = {
    "is_processing": False,
    "progress": 0,
    "status": "idle",
    "last_updated": None
}

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    create_tables()
    logger.info("Database tables created successfully")

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Astana Taxi Analytics API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "openai_configured": bool(OPENAI_API_KEY and OPENAI_API_KEY != "your_openai_api_key_here")
    }

@app.post("/process-data")
async def process_data(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Start data processing in background"""
    global processing_status
    
    if processing_status["is_processing"]:
        raise HTTPException(status_code=400, detail="Data processing already in progress")
    
    if not OPENAI_API_KEY or OPENAI_API_KEY == "your_openai_api_key_here":
        raise HTTPException(status_code=400, detail="OpenAI API key not configured")
    
    # Start background processing
    background_tasks.add_task(process_data_background, db)
    
    return {
        "message": "Data processing started",
        "status": "processing"
    }

async def process_data_background(db: Session):
    """Background task for data processing"""
    global processing_status
    
    try:
        processing_status.update({
            "is_processing": True,
            "progress": 0,
            "status": "loading_data",
            "last_updated": datetime.utcnow().isoformat()
        })
        
        logger.info("Starting data processing...")
        
        # Process data
        gps_data, taxi_data = process_all_data()
        
        processing_status.update({
            "progress": 50,
            "status": "saving_to_database",
            "last_updated": datetime.utcnow().isoformat()
        })
        
        # Save to database
        db_service = DatabaseService(db)
        
        gps_saved = db_service.save_gps_data(gps_data)
        taxi_saved = db_service.save_taxi_data(taxi_data)
        
        # Calculate and save metrics
        metrics = db_service.calculate_aggregate_metrics()
        db_service.save_analytics_metrics(metrics)
        
        processing_status.update({
            "is_processing": False,
            "progress": 100,
            "status": "completed",
            "last_updated": datetime.utcnow().isoformat(),
            "gps_saved": gps_saved,
            "taxi_saved": taxi_saved
        })
        
        logger.info(f"Data processing completed. GPS: {gps_saved}, Taxi: {taxi_saved}")
        
    except Exception as e:
        processing_status.update({
            "is_processing": False,
            "progress": 0,
            "status": "error",
            "error": str(e),
            "last_updated": datetime.utcnow().isoformat()
        })
        logger.error(f"Data processing failed: {e}")

@app.get("/processing-status")
async def get_processing_status():
    """Get current processing status"""
    return processing_status

@app.get("/gps-data")
async def get_gps_data(
    limit: int = 1000,
    area: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get processed GPS data"""
    db_service = DatabaseService(db)
    return db_service.get_gps_data(limit=limit, area=area)

@app.get("/taxi-data")
async def get_taxi_data(
    limit: int = 1000,
    trip_category: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get processed taxi data"""
    db_service = DatabaseService(db)
    return db_service.get_taxi_data(limit=limit, trip_category=trip_category)

@app.get("/heatmap-data")
async def get_heatmap_data(
    heatmap_type: str = "density",  # density, speed, altitude
    db: Session = Depends(get_db)
):
    """Get heatmap data for visualization"""
    db_service = DatabaseService(db)
    
    if heatmap_type == "speed":
        return db_service.get_speed_heatmap_data()
    elif heatmap_type == "altitude":
        return db_service.get_altitude_heatmap_data()
    else:
        return db_service.get_heatmap_data()

@app.get("/analytics-metrics")
async def get_analytics_metrics(db: Session = Depends(get_db)):
    """Get calculated analytics metrics"""
    db_service = DatabaseService(db)
    return db_service.get_analytics_metrics()

@app.get("/dashboard-data")
async def get_dashboard_data(db: Session = Depends(get_db)):
    """Get all data needed for dashboard"""
    db_service = DatabaseService(db)
    
    # Get metrics
    metrics = db_service.calculate_aggregate_metrics()
    
    # Get heatmap data
    heatmap_data = db_service.get_heatmap_data()
    speed_data = db_service.get_speed_heatmap_data()
    altitude_data = db_service.get_altitude_heatmap_data()
    
    # Get sample data for insights
    gps_sample = db_service.get_gps_data(limit=100)
    taxi_sample = db_service.get_taxi_data(limit=100)
    
    return {
        "metrics": metrics,
        "heatmap_data": {
            "density": heatmap_data,
            "speed": speed_data,
            "altitude": altitude_data
        },
        "sample_data": {
            "gps": gps_sample,
            "taxi": taxi_sample
        }
    }

@app.get("/area-analysis")
async def get_area_analysis(db: Session = Depends(get_db)):
    """Get area-based analysis"""
    db_service = DatabaseService(db)
    
    # Get GPS data by area
    areas = ["North", "Center", "South"]
    area_data = {}
    
    for area in areas:
        area_data[area] = db_service.get_gps_data(limit=500, area=area)
    
    return area_data

@app.get("/trip-analysis")
async def get_trip_analysis(db: Session = Depends(get_db)):
    """Get trip-based analysis"""
    db_service = DatabaseService(db)
    
    # Get taxi data by category
    categories = ["Short", "Medium", "Long"]
    category_data = {}
    
    for category in categories:
        category_data[category] = db_service.get_taxi_data(limit=200, trip_category=category)
    
    return category_data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
