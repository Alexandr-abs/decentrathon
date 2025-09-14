#!/usr/bin/env python3
"""Запуск бэкенда системы аналитики такси"""

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
    
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key == "your_openai_api_key_here":
        print("❌ OpenAI API ключ не настроен!")
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
    """Инициализация базы данных"""
    try:
        os.chdir("backend")
        result = subprocess.run([sys.executable, "init_database.py"], 
                              capture_output=True, text=True, timeout=300)
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
        process = subprocess.Popen([sys.executable, "main.py"], 
                                 stdout=subprocess.PIPE, 
                                 stderr=subprocess.PIPE)
        os.chdir("..")
        
        time.sleep(5)
        
        if process.poll() is None:
            print("✅ API сервер запущен на http://localhost:8000")
            return process
        else:
            print("❌ Ошибка запуска API сервера")
            return None
    except Exception as e:
        print(f"❌ Ошибка запуска сервера: {e}")
        return None

def generate_frontend():
    """Генерация фронтенда"""
    try:
        os.chdir("backend")
        result = subprocess.run([sys.executable, "frontend_with_api.py"], 
                              capture_output=True, text=True, timeout=60)
        os.chdir("..")
        
        if result.returncode == 0:
            print("✅ Фронтенд сгенерирован")
            return True
        else:
            print(f"❌ Ошибка генерации фронтенда: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print("❌ Таймаут генерации фронтенда")
        return False
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

def open_browser():
    """Открытие браузера"""
    html_file = Path("html/fullsite.html")
    if html_file.exists():
        webbrowser.open(f"file://{html_file.absolute()}")
    else:
        print("❌ HTML файл не найден")

def main():
    """Главная функция"""
    print("🚕 ASTANA TAXI ANALYTICS - BACKEND LAUNCHER")
    
    if not check_requirements():
        input("Нажмите Enter для выхода...")
        return
    
    if not install_dependencies():
        input("Нажмите Enter для выхода...")
        return
    
    if not initialize_database():
        input("Нажмите Enter для выхода...")
        return
    
    api_process = start_api_server()
    if not api_process:
        input("Нажмите Enter для выхода...")
        return
    
    try:
        if not generate_frontend():
            print("⚠️ Ошибка генерации фронтенда, но API работает")
        
        open_browser()
        
        print("\n🎉 СИСТЕМА ЗАПУЩЕНА УСПЕШНО!")
        print("🌐 Дашборд: html/fullsite.html")
        print("🌐 API: http://localhost:8000")
        print("🌐 API Docs: http://localhost:8000/docs")
        print("🛑 Для остановки нажмите Ctrl+C")
        
        try:
            api_process.wait()
        except KeyboardInterrupt:
            print("\n🛑 Остановка системы...")
            api_process.terminate()
            api_process.wait()
            print("✅ Система остановлена")
    
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        if api_process:
            api_process.terminate()
        input("Нажмите Enter для выхода...")

if __name__ == "__main__":
    main()