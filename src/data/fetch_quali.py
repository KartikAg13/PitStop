import pandas as pd

from fastf1 import get_session, get_event, Cache

from src.db.engine import getEngine
from src.db.tables import getQualifyingTable, TABLE_NAME

def parseTimeDelta(time_delta) -> float | None:
	if time_delta is None or (isinstance(time_delta, float) and pd.isna(time_delta)):
		return None
	elif isinstance(time_delta, float):
		return time_delta
	try:
		return time_delta.total_seconds()
	except:
		try:
			return float(time_delta)
		except  Exception as e:
			print(f"Error in parseTimeDelta: {e}")
			return None
		
# def getWeather(laps: pd.DataFrame, weather: pd.DataFrame, target_second: float, tolerance: float = 31.0):
# 	if laps is None or laps.empty or weather is None or weather.empty:
# 		return (None, ) * 7
	
# 	# remove the NaT valued rows, removes the In laps and Out laps, keeps Flying Laps and Slow Laps
# 	laps_copy = laps[laps['LapTime'].notna()].copy()
# 	# convert time to seconds for comparison
# 	laps_copy['LapTime_seconds'] = laps_copy['LapTime'].dt.total_seconds()
	
# 	# get the row index of closest time to q1/q2/q3 time (laptime in laps dataframe with minimum difference to laptime in results dataframe)
# 	row = (laps_copy['LapTime_seconds'] - target_second).abs().idxmin(skipna=True)

# 	time = laps_copy.loc[row, 'Time']
# 	# convert the time located to seconds
# 	time = time.total_seconds() if isinstance(time, pd.Timedelta) else time

# 	weather_copy = weather.copy()
# 	weather_copy['Time_seconds'] = weather_copy['Time'].dt.total_seconds()
# 	weather_copy['Time_seconds'] = pd.to_numeric(weather_copy['Time_seconds'], errors='coerce')
# 	time = pd.to_numeric(time, errors='coerce')
# 	# search for the time in the weather dataframe to get the weather
# 	index = (weather_copy['Time_seconds'] - time).abs().idxmin(skipna=True)
# 	data = weather_copy.loc[index]

# 	return data["AirTemp"], data["TrackTemp"], data["Humidity"], data["Pressure"], data["WindSpeed"], data["WindDirection"], data["Rainfall"]

def getWeather(laps: pd.DataFrame, weather: pd.DataFrame, target_second: float, tolerance: float = 31.0):
    
	# Check if laps or weather data is empty or target_second is NaN
	if laps is None or laps.empty or weather is None or weather.empty or pd.isna(target_second):
		return (None, ) * 7
	
	# Filter valid laps
	laps_copy = laps.dropna(subset=['LapTime']).copy()
	if laps_copy.empty:
		return (None, ) * 7

	# Convert lap times to seconds
	laps_copy['LapTime_seconds'] = laps_copy['LapTime'].dt.total_seconds()

	# Find closest lap to target time
	time_diffs = (laps_copy['LapTime_seconds'] - target_second).abs()
	if time_diffs.min() > 0.1:
		return (None, ) * 7
	
	row = time_diffs.idxmin()
	lap_time = laps_copy.loc[row, 'Time']

	# Convert to seconds if needed
	if isinstance(lap_time, pd.Timedelta):
		lap_seconds = lap_time.total_seconds()
	else:
		lap_seconds = lap_time
	
	# Find closest weather data
	weather_copy = weather.copy()
	weather_copy['Time_seconds'] = weather_copy['Time'].dt.total_seconds()
	weather_copy['Time_seconds'] = pd.to_numeric(weather_copy['Time_seconds'], errors='coerce')
	time = pd.to_numeric(lap_seconds, errors='coerce')
	weather_diffs = (weather_copy['Time_seconds'] - time).abs()

	if weather_diffs.empty or weather_diffs.min() > tolerance:  # 5 min tolerance
		return (None, ) * 7
	
	weather_idx = weather_diffs.idxmin()
	data = weather.loc[weather_idx]

	wind_dir = data.get("WindDirection")
	if isinstance(wind_dir, bytes):
		try:
			wind_dir = int.from_bytes(wind_dir, 'little')
		except:
			wind_dir = None
	elif not isinstance(wind_dir, (int, float)) or pd.isna(wind_dir):
		wind_dir = None

	return (
		data.get("AirTemp"), 
		data.get("TrackTemp"), 
		data.get("Humidity"), 
		data.get("Pressure"), 
		data.get("WindSpeed"), 
		wind_dir, 
		data.get("Rainfall")
	)

def fetchQualifyingData(start_year: int, end_year: int):
	
	Cache.enable_cache("cache/")
	qualifying = getQualifyingTable()
	engine = getEngine(TABLE_NAME + ".db")

	for year in range(start_year, end_year + 1):
		
		for weekend in range(1, 25):
			try:
				event = get_event(year, weekend)
				event_name = event['EventName']
				session = get_session(year, weekend, 'Q')
				session.load(weather=True)
				results = session.results
				laps = session.laps
				weather = session.weather_data

				with engine.begin() as connection:
					for row in results.itertuples():
						number = int(getattr(row, "DriverNumber"))
						name = str(getattr(row, "Abbreviation"))
						company = str(getattr(row, "TeamName"))

						q1_time: float | None = parseTimeDelta(getattr(row, "Q1", None))
						q2_time: float | None = parseTimeDelta(getattr(row, "Q2", None))
						q3_time: float | None = parseTimeDelta(getattr(row, "Q3", None))

						# filter the laps for the driver
						laps_driver = laps[laps['DriverNumber'] == str(number)]
						if isinstance(weather, pd.DataFrame):
							at1, tt1, hu1, pr1, ws1, wd1, rf1 = getWeather(laps_driver, weather, q1_time) if q1_time is not None else (None, ) * 7
							at2, tt2, hu2, pr2, ws2, wd2, rf2 = getWeather(laps_driver, weather, q2_time) if q2_time is not None else (None, ) * 7
							at3, tt3, hu3, pr3, ws3, wd3, rf3 = getWeather(laps_driver, weather, q3_time) if q3_time is not None else (None, ) * 7

						insert_statement = qualifying.insert().values(
							season = year, round_number = weekend,
							round_name = event_name, driver_number = number,
							driver_name = name, team = company,
							air_temp_q1 = at1, track_temp_q1 = tt1,
							humidity_q1 = hu1, pressure_q1 = pr1,
							wind_speed_q1 = ws1, wind_direction_q1 = wd1,
							rain_flag_q1 = rf1, q1 = q1_time,
							air_temp_q2 = at2, track_temp_q2 = tt2,
							humidity_q2 = hu2, pressure_q2 = pr2,
							wind_speed_q2 = ws2, wind_direction_q2 = wd2,
							rain_flag_q2 = rf2, q2 = q2_time,
							air_temp_q3 = at3, track_temp_q3 = tt3,
							humidity_q3 = hu3, pressure_q3 = pr3,
							wind_speed_q3 = ws3, wind_direction_q3 = wd3,
							rain_flag_q3 = rf3, q3 = q3_time
						)

						try:
							connection.execute(insert_statement)
						except Exception as e:
							print(f"Error storing data in database in fetchQualifyingData: {e}")

			except Exception as e:
				print(f"Error in fetchQualifyingData(): {e}")
