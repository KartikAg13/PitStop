import pandas as pd

from src.db.engine import getEngine
from src.db.tables import TABLE_NAME 

def getData() -> pd.DataFrame:

	engine = getEngine(TABLE_NAME + ".db")
	connection = engine.connect()

	data = pd.read_sql("SELECT * FROM qualifying", connection)
	print(data.head())
	return data