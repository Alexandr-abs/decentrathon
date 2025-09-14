from sqlalchemy import Column, Integer, Float, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from config import DATABASE_URL
from datetime import datetime

Base = declarative_base()

class ProcessedGPSData(Base):
    __tablename__ = "processed_gps_data"
    
    id = Column(Integer, primary_key=True, index=True)
    original_id = Column(String, index=True)
    lat = Column(Float, index=True)
    lng = Column(Float, index=True)
    alt = Column(Float)
    spd = Column(Float)
    azm = Column(Float)
    
    # Processed fields
    area_classification = Column(String)  # North, Center, South, etc.
    activity_level = Column(String)  # High, Medium, Low
    road_type = Column(String)  # Highway, Street, Residential
    processed_at = Column(DateTime, default=datetime.utcnow)
    
    # LLM insights
    insights = Column(Text)  # JSON string with LLM insights

class ProcessedTaxiData(Base):
    __tablename__ = "processed_taxi_data"
    
    id = Column(Integer, primary_key=True, index=True)
    trip_duration_sec = Column(Integer)
    trip_duration_min = Column(Float)
    distance_traveled_km = Column(Float)
    kph = Column(Float)
    wait_time_cost = Column(Float)
    distance_cost = Column(Float)
    total_fare_new = Column(Float)
    num_of_passengers = Column(Integer)
    surge_applied = Column(Boolean)
    
    # Processed fields
    trip_category = Column(String)  # Short, Medium, Long
    price_category = Column(String)  # Low, Medium, High, Premium
    time_of_day = Column(String)  # Morning, Afternoon, Evening, Night
    efficiency_score = Column(Float)  # Calculated efficiency metric
    processed_at = Column(DateTime, default=datetime.utcnow)
    
    # LLM insights
    insights = Column(Text)  # JSON string with LLM insights

class AnalyticsMetrics(Base):
    __tablename__ = "analytics_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    metric_name = Column(String, unique=True, index=True)
    metric_value = Column(Float)
    metric_type = Column(String)  # GPS, TAXI, CALCULATED
    description = Column(Text)
    calculated_at = Column(DateTime, default=datetime.utcnow)

# Database setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def create_tables():
    Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
