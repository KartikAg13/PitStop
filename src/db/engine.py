from sqlalchemy import create_engine, Engine
from pathlib import Path
import os

def getEngine(database_name: str) -> Engine:
	
	db_dir: Path = Path(__file__).parents[2] / "data"
	os.makedirs(db_dir, exist_ok=True)
	db_path = db_dir / database_name

	engine: Engine = create_engine(url=f"sqlite:///{db_path}", echo=False)

	return engine