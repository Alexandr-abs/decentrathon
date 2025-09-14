from sqlalchemy.orm import Session
from models import ProcessedGPSData, ProcessedTaxiData, AnalyticsMetrics
from typing import List, Dict, Any
import json
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DatabaseService:
    def __init__(self, db: Session):
        self.db = db
    
    def save_gps_data(self, gps_data: List[Dict[str, Any]]) -> int:
        """Save processed GPS data to database"""
        saved_count = 0
        
        for data in gps_data:
            try:
                gps_record = ProcessedGPSData(
                    original_id=data.get('original_id'),
                    lat=data.get('lat'),
                    lng=data.get('lng'),
                    alt=data.get('alt'),
                    spd=data.get('spd'),
                    azm=data.get('azm'),
                    area_classification=data.get('area_classification'),
                    activity_level=data.get('activity_level'),
                    road_type=data.get('road_type'),
                    insights=json.dumps(data.get('insights', {}))
                )
                
                self.db.add(gps_record)
                saved_count += 1
                
            except Exception as e:
                logger.error(f"Error saving GPS data: {e}")
                continue
        
        self.db.commit()
        return saved_count
    
    def save_taxi_data(self, taxi_data: List[Dict[str, Any]]) -> int:
        """Save processed taxi data to database"""
        saved_count = 0
        
        for data in taxi_data:
            try:
                taxi_record = ProcessedTaxiData(
                    trip_duration_sec=data.get('trip_duration_sec'),
                    trip_duration_min=data.get('trip_duration_min'),
                    distance_traveled_km=data.get('distance_traveled_km'),
                    kph=data.get('kph'),
                    wait_time_cost=data.get('wait_time_cost'),
                    distance_cost=data.get('distance_cost'),
                    total_fare_new=data.get('total_fare_new'),
                    num_of_passengers=data.get('num_of_passengers'),
                    surge_applied=data.get('surge_applied'),
                    trip_category=data.get('trip_category'),
                    price_category=data.get('price_category'),
                    efficiency_score=data.get('efficiency_score'),
                    insights=json.dumps(data.get('insights', {}))
                )
                
                self.db.add(taxi_record)
                saved_count += 1
                
            except Exception as e:
                logger.error(f"Error saving taxi data: {e}")
                continue
        
        self.db.commit()
        return saved_count
    
    def save_analytics_metrics(self, metrics: Dict[str, Any]) -> int:
        """Save calculated analytics metrics"""
        saved_count = 0
        
        for metric_name, metric_data in metrics.items():
            try:
                metric_record = AnalyticsMetrics(
                    metric_name=metric_name,
                    metric_value=metric_data.get('value'),
                    metric_type=metric_data.get('type', 'CALCULATED'),
                    description=metric_data.get('description', ''),
                    calculated_at=datetime.utcnow()
                )
                
                self.db.add(metric_record)
                saved_count += 1
                
            except Exception as e:
                logger.error(f"Error saving metric {metric_name}: {e}")
                continue
        
        self.db.commit()
        return saved_count
    
    def get_gps_data(self, limit: int = 1000, area: str = None) -> List[Dict[str, Any]]:
        """Get GPS data with optional filtering"""
        query = self.db.query(ProcessedGPSData)
        
        if area:
            query = query.filter(ProcessedGPSData.area_classification == area)
        
        records = query.limit(limit).all()
        
        return [{
            'id': record.id,
            'lat': record.lat,
            'lng': record.lng,
            'alt': record.alt,
            'spd': record.spd,
            'azm': record.azm,
            'area_classification': record.area_classification,
            'activity_level': record.activity_level,
            'road_type': record.road_type,
            'insights': json.loads(record.insights) if record.insights else {}
        } for record in records]
    
    def get_taxi_data(self, limit: int = 1000, trip_category: str = None) -> List[Dict[str, Any]]:
        """Get taxi data with optional filtering"""
        query = self.db.query(ProcessedTaxiData)
        
        if trip_category:
            query = query.filter(ProcessedTaxiData.trip_category == trip_category)
        
        records = query.limit(limit).all()
        
        return [{
            'id': record.id,
            'trip_duration_min': record.trip_duration_min,
            'distance_traveled_km': record.distance_traveled_km,
            'kph': record.kph,
            'total_fare_new': record.total_fare_new,
            'num_of_passengers': record.num_of_passengers,
            'surge_applied': record.surge_applied,
            'trip_category': record.trip_category,
            'price_category': record.price_category,
            'efficiency_score': record.efficiency_score,
            'insights': json.loads(record.insights) if record.insights else {}
        } for record in records]
    
    def get_analytics_metrics(self) -> Dict[str, Any]:
        """Get all analytics metrics"""
        records = self.db.query(AnalyticsMetrics).all()
        
        return {
            record.metric_name: {
                'value': record.metric_value,
                'type': record.metric_type,
                'description': record.description,
                'calculated_at': record.calculated_at.isoformat()
            }
            for record in records
        }
    
    def get_heatmap_data(self) -> List[List[float]]:
        """Get GPS data formatted for heatmap"""
        records = self.db.query(ProcessedGPSData).all()
        
        return [[record.lat, record.lng, record.spd] for record in records]
    
    def get_speed_heatmap_data(self) -> List[List[float]]:
        """Get speed-based heatmap data"""
        records = self.db.query(ProcessedGPSData).filter(ProcessedGPSData.spd > 0).all()
        
        return [[record.lat, record.lng, record.spd] for record in records]
    
    def get_altitude_heatmap_data(self) -> List[List[float]]:
        """Get altitude-based heatmap data"""
        records = self.db.query(ProcessedGPSData).filter(ProcessedGPSData.alt > 0).all()
        
        return [[record.lat, record.lng, record.alt] for record in records]
    
    def calculate_aggregate_metrics(self) -> Dict[str, Any]:
        """Calculate aggregate metrics from processed data"""
        # GPS metrics
        gps_count = self.db.query(ProcessedGPSData).count()
        avg_speed = self.db.query(ProcessedGPSData).filter(ProcessedGPSData.spd > 0).with_entities(
            ProcessedGPSData.spd
        ).all()
        avg_speed = sum([r[0] for r in avg_speed]) / len(avg_speed) if avg_speed else 0
        
        # Taxi metrics
        taxi_count = self.db.query(ProcessedTaxiData).count()
        avg_fare = self.db.query(ProcessedTaxiData).with_entities(
            ProcessedTaxiData.total_fare_new
        ).all()
        avg_fare = sum([r[0] for r in avg_fare]) / len(avg_fare) if avg_fare else 0
        
        avg_duration = self.db.query(ProcessedTaxiData).with_entities(
            ProcessedTaxiData.trip_duration_min
        ).all()
        avg_duration = sum([r[0] for r in avg_duration]) / len(avg_duration) if avg_duration else 0
        
        avg_distance = self.db.query(ProcessedTaxiData).with_entities(
            ProcessedTaxiData.distance_traveled_km
        ).all()
        avg_distance = sum([r[0] for r in avg_distance]) / len(avg_distance) if avg_distance else 0
        
        surge_trips = self.db.query(ProcessedTaxiData).filter(ProcessedTaxiData.surge_applied == True).count()
        surge_percentage = (surge_trips / taxi_count * 100) if taxi_count > 0 else 0
        
        return {
            'gps_points_count': gps_count,
            'avg_speed_mps': avg_speed,
            'avg_speed_kmh': avg_speed * 3.6,
            'taxi_trips_count': taxi_count,
            'avg_fare_usd': avg_fare,
            'avg_fare_tenge': avg_fare * 541,
            'avg_trip_duration_min': avg_duration,
            'avg_distance_km': avg_distance,
            'surge_percentage': surge_percentage,
            'price_per_km_tenge': (avg_fare / avg_distance * 541) if avg_distance > 0 else 0
        }
