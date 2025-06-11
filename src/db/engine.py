from sqlalchemy import create_engine
from pathlib import Path

def get_engine(database_name: str):
	
	DB_PATH = Path(__file__).parents[1].parent / "data" / database_name

	engine = create_engine(f"sqlite:///{DB_PATH}", echo=True)

	return engine