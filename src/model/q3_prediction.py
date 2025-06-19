import pandas as pd

from src.db.engine import getEngine
from src.db.tables import TABLE_NAME 

def convertToExcel(data: pd.DataFrame) -> None:
	data.to_excel("qualifying.xlsx", float_format="%.4f", engine="openpyxl")

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
	print(data.info())

	return data

def removeNullQ1(data: pd.DataFrame) -> pd.DataFrame:
	
	# remove the null values in q1
	data = data.dropna(subset=['q1'])

	# remove the null values in q1 weather
	data = data[~(data['q1'].notna() & (
				data['air_temp_q1'].isna() | 
				data['track_temp_q1'].isna() | 
				data['humidity_q1'].isna() | 
				data['pressure_q1'].isna() | 
				data['wind_speed_q1'].isna() | 
				data['wind_direction_q1'].isna() | 
				data['rain_flag_q1'].isna()
			))]
	return data

def removeNullQ2(data: pd.DataFrame) -> pd.DataFrame:

		# remove the null values in q2 weather
	data = data[~(data['q2'].notna() & (
				data['air_temp_q2'].isna() | 
				data['track_temp_q2'].isna() | 
				data['humidity_q2'].isna() | 
				data['pressure_q2'].isna() | 
				data['wind_speed_q2'].isna() | 
				data['wind_direction_q2'].isna() | 
				data['rain_flag_q2'].isna()
			))]
	
	return data

def removeNullQ3(data: pd.DataFrame) -> pd.DataFrame:

	# remove the null values in q3 weather
	data = data[~(data['q3'].notna() & (
				data['air_temp_q3'].isna() | 
				data['track_temp_q3'].isna() | 
				data['humidity_q3'].isna() | 
				data['pressure_q3'].isna() | 
				data['wind_speed_q3'].isna() | 
				data['wind_direction_q3'].isna() | 
				data['rain_flag_q3'].isna()
			))]
	
	return data	

def preprocessData() -> pd.DataFrame:
	
	data = getData()

	data = removeNullQ1(data)
	data = removeNullQ2(data)
	data = removeNullQ3(data)
	data.info()
	
	return data