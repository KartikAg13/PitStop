import pandas as pd

from src.db.engine import getEngine
from src.db.tables import TABLE_NAME 

def printUniqueValues(data: pd.DataFrame) -> None:
	for column in data.columns:
		try:
			unique_values = data[column].unique()
			print(f"Column Name: {column}")
			print(f"Data Type: {data[column].dtype}")
			print(f"Unique Values: {len(unique_values)}")
			print(unique_values)
		except Exception as e:
			print(f"Error in printUniqueValues: {e}")

def getData() -> pd.DataFrame:

	engine = getEngine(TABLE_NAME + ".db")
	connection = engine.connect()

	data = pd.read_sql("SELECT * FROM qualifying", connection)
	print(data.head())
	print(data.info())
	print(data.describe())

	printUniqueValues(data)
	return data

def removeNullQ1(data: pd.DataFrame) -> pd.DataFrame:
	data = data.dropna(subset=['q1'])
	return data