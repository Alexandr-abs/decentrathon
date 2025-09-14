import pandas as pd
import numpy as np
import json
from typing import List, Dict, Any
from langchain_community.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain_core.output_parsers import BaseOutputParser
from config import OPENAI_API_KEY, BATCH_SIZE
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataInsightsParser(BaseOutputParser):
    """Парсер для результатов LLM"""
    
    def parse(self, text: str) -> Dict[str, Any]:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {
                "insights": text,
                "confidence": 0.7,
                "processed_at": pd.Timestamp.now().isoformat()
            }

class TaxiDataProcessor:
    def __init__(self):
        self.llm = OpenAI(
            openai_api_key=OPENAI_API_KEY,
            temperature=0.3,
            max_tokens=500
        )
        self.parser = DataInsightsParser()
        
        self.gps_prompt = PromptTemplate(
            input_variables=["lat", "lng", "alt", "spd", "azm", "context"],
            template="""
            Analyze this GPS data point and provide insights:
            Latitude: {lat}
            Longitude: {lng}
            Altitude: {alt}
            Speed: {spd} m/s
            Azimuth: {azm}
            Context: {context}
            
            Please provide:
            1. Area classification (North/Center/South based on latitude)
            2. Activity level (High/Medium/Low based on speed and context)
            3. Road type prediction (Highway/Street/Residential)
            4. Any interesting patterns or insights
            
            Return as JSON with keys: area_classification, activity_level, road_type, insights
            """
        )
        
        self.taxi_prompt = PromptTemplate(
            input_variables=["duration", "distance", "speed", "fare", "passengers", "surge", "context"],
            template="""
            Analyze this taxi trip data and provide insights:
            Duration: {duration} minutes
            Distance: {distance} km
            Speed: {speed} km/h
            Fare: {fare} USD
            Passengers: {passengers}
            Surge pricing: {surge}
            Context: {context}
            
            Please provide:
            1. Trip category (Short/Medium/Long based on duration and distance)
            2. Price category (Low/Medium/High/Premium based on fare per km)
            3. Time efficiency score (0-1 based on speed and duration)
            4. Any interesting patterns or insights
            
            Return as JSON with keys: trip_category, price_category, efficiency_score, insights
            """
        )
    
    def process_gps_batch(self, gps_data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Обработка GPS данных через LLM"""
        results = []
        
        for idx, row in gps_data.iterrows():
            try:
                context = f"GPS point {idx} of batch, speed: {row['spd']:.2f} m/s"
                gps_chain = LLMChain(llm=self.llm, prompt=self.gps_prompt, output_parser=self.parser)
                
                result = gps_chain.run(
                    lat=row['lat'],
                    lng=row['lng'],
                    alt=row['alt'],
                    spd=row['spd'],
                    azm=row['azm'],
                    context=context
                )
                
                result['original_id'] = str(row.get('randomized_id', idx))
                result['lat'] = row['lat']
                result['lng'] = row['lng']
                result['alt'] = row['alt']
                result['spd'] = row['spd']
                result['azm'] = row['azm']
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"Error processing GPS data point {idx}: {e}")
                results.append({
                    'original_id': str(row.get('randomized_id', idx)),
                    'lat': row['lat'],
                    'lng': row['lng'],
                    'alt': row['alt'],
                    'spd': row['spd'],
                    'azm': row['azm'],
                    'area_classification': self._classify_area(row['lat']),
                    'activity_level': self._classify_activity(row['spd']),
                    'road_type': 'Unknown',
                    'insights': f"Error processing: {str(e)}"
                })
        
        return results
    
    def process_taxi_batch(self, taxi_data: pd.DataFrame) -> List[Dict[str, Any]]:
        """Обработка данных такси через LLM"""
        results = []
        
        for idx, row in taxi_data.iterrows():
            try:
                context = f"Taxi trip {idx} of batch, {row['num_of_passengers']} passengers"
                taxi_chain = LLMChain(llm=self.llm, prompt=self.taxi_prompt, output_parser=self.parser)
                
                result = taxi_chain.run(
                    duration=row['trip_duration_min'],
                    distance=row['distance_traveled_Km'],
                    speed=row['KPH'],
                    fare=row['total_fare_new'],
                    passengers=row['num_of_passengers'],
                    surge=row['surge_applied'],
                    context=context
                )
                
                result['trip_duration_sec'] = row['trip_duration_sec']
                result['trip_duration_min'] = row['trip_duration_min']
                result['distance_traveled_km'] = row['distance_traveled_Km']
                result['kph'] = row['KPH']
                result['wait_time_cost'] = row['wait_time_cost']
                result['distance_cost'] = row['distance_cost']
                result['total_fare_new'] = row['total_fare_new']
                result['num_of_passengers'] = row['num_of_passengers']
                result['surge_applied'] = row['surge_applied']
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"Error processing taxi data point {idx}: {e}")
                results.append({
                    'trip_duration_sec': row['trip_duration_sec'],
                    'trip_duration_min': row['trip_duration_min'],
                    'distance_traveled_km': row['distance_traveled_Km'],
                    'kph': row['KPH'],
                    'wait_time_cost': row['wait_time_cost'],
                    'distance_cost': row['distance_cost'],
                    'total_fare_new': row['total_fare_new'],
                    'num_of_passengers': row['num_of_passengers'],
                    'surge_applied': row['surge_applied'],
                    'trip_category': self._classify_trip_duration(row['trip_duration_min']),
                    'price_category': self._classify_price(row['total_fare_new'], row['distance_traveled_Km']),
                    'efficiency_score': self._calculate_efficiency(row['KPH'], row['trip_duration_min']),
                    'insights': f"Error processing: {str(e)}"
                })
        
        return results
    
    def _classify_area(self, lat: float) -> str:
        """Классификация района по широте"""
        if lat > 51.12:
            return "North"
        elif lat < 51.08:
            return "South"
        else:
            return "Center"
    
    def _classify_activity(self, speed: float) -> str:
        """Классификация активности по скорости"""
        if speed > 10:
            return "High"
        elif speed > 3:
            return "Medium"
        else:
            return "Low"
    
    def _classify_trip_duration(self, duration: float) -> str:
        """Классификация длительности поездки"""
        if duration < 10:
            return "Short"
        elif duration < 30:
            return "Medium"
        else:
            return "Long"
    
    def _classify_price(self, fare: float, distance: float) -> str:
        """Классификация цены по стоимости за км"""
        if distance > 0:
            price_per_km = fare / distance
            if price_per_km < 2:
                return "Low"
            elif price_per_km < 5:
                return "Medium"
            elif price_per_km < 10:
                return "High"
            else:
                return "Premium"
        return "Unknown"
    
    def _calculate_efficiency(self, speed: float, duration: float) -> float:
        """Расчет эффективности поездки"""
        speed_score = min(speed / 60, 1.0)
        duration_score = max(0, 1 - (duration - 15) / 30)
        return (speed_score + duration_score) / 2

def process_all_data():
    """Основная функция обработки данных"""
    processor = TaxiDataProcessor()
    
    logger.info("Loading GPS data...")
    gps_df = pd.read_csv("../data/geo_locations_astana_hackathon.csv")
    
    logger.info("Loading taxi data...")
    taxi_df = pd.read_csv("../data/Taxi_Set.csv")
    
    logger.info("Processing GPS data...")
    gps_results = []
    for i in range(0, len(gps_df), BATCH_SIZE):
        batch = gps_df.iloc[i:i+BATCH_SIZE]
        batch_results = processor.process_gps_batch(batch)
        gps_results.extend(batch_results)
        logger.info(f"Processed GPS batch {i//BATCH_SIZE + 1}/{(len(gps_df)-1)//BATCH_SIZE + 1}")
    
    logger.info("Processing taxi data...")
    taxi_results = []
    for i in range(0, len(taxi_df), BATCH_SIZE):
        batch = taxi_df.iloc[i:i+BATCH_SIZE]
        batch_results = processor.process_taxi_batch(batch)
        taxi_results.extend(batch_results)
        logger.info(f"Processed taxi batch {i//BATCH_SIZE + 1}/{(len(taxi_df)-1)//BATCH_SIZE + 1}")
    
    return gps_results, taxi_results

if __name__ == "__main__":
    gps_data, taxi_data = process_all_data()
    print(f"Processed {len(gps_data)} GPS points and {len(taxi_data)} taxi trips")