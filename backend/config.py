import os
from dotenv import load_dotenv

load_dotenv()

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "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")

# Database Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./taxi_analytics.db")

# Data Processing Configuration
BATCH_SIZE = 1000  # Process data in batches
MAX_RETRIES = 3
TIMEOUT = 30
