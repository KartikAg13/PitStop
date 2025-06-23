import pandas as pd

from src.data.load_data import getData

def removeNullQ1(data: pd.DataFrame) -> pd.DataFrame:
	
	# Remove the null values in q1
	data = data.dropna(subset=['q1'])

	# Remove the null values in q1 weather
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

	# Remove the null values in q2 weather
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

	# Remove the null values in q3 weather
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
	
	return data