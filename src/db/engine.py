from sqlalchemy import create_engine
from pathlib import Path
import os

def getEngine(database_name: str):
	
	DB_DIR = Path(__file__).parents[2] / "data"
	os.makedirs(DB_DIR, exist_ok=True)
	DB_PATH = DB_DIR / database_name

	engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)

	return engine 