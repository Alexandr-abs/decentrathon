# 🚀 Быстрый старт

## Запуск системы

### Способ 1: Простой запуск (рекомендуется)
```cmd
start_simple_backend.bat
```

### Способ 2: Полный запуск с OpenAI
```cmd
start_backend.bat
```

### Способ 3: Через Python
```cmd
python run_backend_simple.py
```

## Что получите

- **API сервер:** http://localhost:8000
- **Документация API:** http://localhost:8000/docs
- **Дашборд:** откроется автоматически в браузере

## Структура проекта

```
📁 data/                    # Данные (CSV файлы)
📁 backend/                 # Backend API
├── main.py                # FastAPI приложение
├── models.py              # Модели базы данных
├── database_service.py    # Работа с БД
├── data_processor.py      # Обработка данных
├── config.py              # Конфигурация
└── simple_init_database.py # Инициализация БД

📁 html/                    # Веб-интерфейс
├── fullsite.html          # Полный дашборд
├── dashboard.html         # Основной дашборд
└── heatmap.html           # Тепловая карта

📄 requirements.txt         # Зависимости Python
📄 run_backend_simple.py    # Простой запуск
📄 run_backend.py          # Полный запуск с OpenAI
📄 start_simple_backend.bat # Windows: простой запуск
📄 start_backend.bat       # Windows: полный запуск
```

## Требования

- Python 3.8+
- Файлы данных в папке `data/`
- Для полного функционала: OpenAI API ключ

## Проблемы?

1. **Ошибка зависимостей:** `pip install -r requirements.txt`
2. **Нет данных:** Проверьте папку `data/`
3. **API не работает:** Перезапустите скрипт
