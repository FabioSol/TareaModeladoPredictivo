
# src/config/constants.py
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PARSED_DATA_DIR = os.path.join(DATA_DIR, "parsed")
CLEANED_DATA_DIR = os.path.join(DATA_DIR, "cleaned")

NYC_DATA_API = {
    "base_url": "https://data.cityofnewyork.us/resource/h9gi-nx95",
    "formats": {
        "json": "https://data.cityofnewyork.us/resource/h9gi-nx95.json",
        "csv": "https://data.cityofnewyork.us/resource/h9gi-nx95.csv"
    },
    "batch_size": 50000,
    "default_format": "json"
}