#!/usr/bin/env python3
"""Упрощенная инициализация базы данных без OpenAI"""

import os
import sys
import logging
import pandas as pd
from sqlalchemy.orm import Session
from models import create_tables, SessionLocal
from database_service import DatabaseService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_requirements():
    """Проверка требований"""
    if not os.path.exists("../data/geo_locations_astana_hackathon.csv"):
        logger.error("GPS data file not found: ../data/geo_locations_astana_hackathon.csv")
        return False
    
    if not os.path.exists("../data/Taxi_Set.csv"):
        logger.error("Taxi data file not found: ../data/Taxi_Set.csv")
        return False
    
    return True

def initialize_database():
    """Создание таблиц базы данных"""
    logger.info("Creating database tables...")
    create_tables()
    logger.info("Database tables created successfully")

def load_data_simple():
    """Загрузка данных без обработки OpenAI"""
    logger.info("Loading data files...")
    
    try:
        gps_file = "../data/geo_locations_astana_hackathon.csv"
        gps_data = pd.read_csv(gps_file)
        logger.info(f"Loaded {len(gps_data)} GPS records")
        
        taxi_file = "../data/Taxi_Set.csv"
        taxi_data = pd.read_csv(taxi_file)
        logger.info(f"Loaded {len(taxi_data)} taxi records")
        
        return gps_data, taxi_data
        
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def save_data_to_database(gps_data, taxi_data):
    """Сохранение данных в базу"""
    logger.info("Saving data to database...")
    
    db = SessionLocal()
    try:
        db_service = DatabaseService(db)
        
        logger.info("Saving GPS data...")
        gps_dict = gps_data.to_dict('records')
        gps_saved = db_service.save_gps_data(gps_dict)
        logger.info(f"Saved {gps_saved} GPS records")
        
        logger.info("Saving taxi data...")
        taxi_dict = taxi_data.to_dict('records')
        taxi_saved = db_service.save_taxi_data(taxi_dict)
        logger.info(f"Saved {taxi_saved} taxi records")
        
        logger.info("Creating basic analytics metrics...")
        metrics = {
            'total_gps_points': len(gps_data),
            'total_taxi_trips': len(taxi_data),
            'avg_trip_duration': taxi_data['trip_duration'].mean() if 'trip_duration' in taxi_data.columns else 0,
            'avg_trip_distance': taxi_data['trip_distance'].mean() if 'trip_distance' in taxi_data.columns else 0,
        }
        
        db_service.save_analytics_metrics(metrics)
        logger.info("Basic analytics metrics saved")
        
        logger.info("Data processing completed successfully!")
        logger.info(f"Summary: {gps_saved} GPS points, {taxi_saved} taxi trips processed")
        
    finally:
        db.close()

def main():
    """Главная функция"""
    logger.info("Starting Simple Database Initialization")
    
    if not check_requirements():
        logger.error("Requirements check failed")
        sys.exit(1)
    
    try:
        initialize_database()
        gps_data, taxi_data = load_data_simple()
        save_data_to_database(gps_data, taxi_data)
        logger.info("Simple database initialization completed successfully!")
        logger.info("You can now start the API server with: python main.py")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()