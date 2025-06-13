import pandas as pd

from src.db.engine import getEngine
from src.db.tables import TABLE_NAME 

def getData() -> pd.DataFrame:

	engine = getEngine(TABLE_NAME + ".db")
	connection = engine.connect()

	data = pd.read_sql("SELECT * FROM qualifying", connection)
	print(data.head())
	print(data.info())
	print(data.describe())
	return data

def removeNullQ1(data: pd.DataFrame) -> pd.DataFrame:
	data = data.dropna(subset=['q1'])
	return data