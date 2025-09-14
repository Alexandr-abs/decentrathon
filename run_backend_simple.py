#!/usr/bin/env python3
"""Упрощенный запуск бэкенда системы аналитики такси"""

import os
import sys
import subprocess
import time
import webbrowser
from pathlib import Path

def check_requirements():
    """Проверка требований"""
    if sys.version_info < (3, 8):
        print("❌ Требуется Python 3.8+")
        return False
    
    data_files = ["data/geo_locations_astana_hackathon.csv", "data/Taxi_Set.csv"]
    for file in data_files:
        if not os.path.exists(file):
            print(f"❌ Файл данных не найден: {file}")
            return False
    
    print("✅ Все требования выполнены")
    return True

def install_dependencies():
    """Установка зависимостей"""
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                      check=True, capture_output=True)
        print("✅ Зависимости установлены")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка установки зависимостей: {e}")
        return False

def initialize_database():
    """Упрощенная инициализация базы данных"""
    try:
        os.chdir("backend")
        result = subprocess.run([sys.executable, "simple_init_database.py"], 
                              capture_output=True, text=True, timeout=120)
        os.chdir("..")
        
        if result.returncode == 0:
            print("✅ База данных инициализирована")
            return True
        else:
            print(f"❌ Ошибка инициализации БД: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print("❌ Таймаут инициализации БД")
        return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def start_api_server():
    """Запуск API сервера"""
    try:
        os.chdir("backend")
        print("🌐 API сервер запускается на http://localhost:8000")
        print("📖 Документация API: http://localhost:8000/docs")
        print("🛑 Для остановки нажмите Ctrl+C")
        subprocess.run([sys.executable, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"])
    except KeyboardInterrupt:
        print("\n🛑 Сервер остановлен")
    except Exception as e:
        print(f"❌ Ошибка запуска сервера: {e}")
    finally:
        os.chdir("..")

def main():
    """Главная функция"""
    print("🚕 ASTANA TAXI ANALYTICS - SIMPLE BACKEND")
    
    if not check_requirements():
        input("Нажмите Enter для выхода...")
        return
    
    if not install_dependencies():
        input("Нажмите Enter для выхода...")
        return
    
    if not initialize_database():
        print("⚠️ Продолжаем без инициализации БД...")
    
    start_api_server()

if __name__ == "__main__":
    main()