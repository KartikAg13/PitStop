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
		
def getWeather(laps: pd.DataFrame, weather: pd.DataFrame, target_second: float, tolerance: float = 31.0):
	if laps is None or laps.empty or weather is None or weather.empty:
		return (None, ) * 7
	
	laps_copy = laps[laps['LapTime'].notna()].copy()		# remove the NaT valued rows, removes the In laps and Out laps, keeps Flying Laps and Slow Laps
	laps_copy['LapTime_seconds'] = laps_copy['LapTime'].dt.total_seconds()		# convert time to seconds for comparison
	row = (laps_copy['LapTime_seconds'] - target_second).abs().idxmin()		# get the row index of closest time to q1/q2/q3 time (laptime in laps dataframe with minimum difference to laptime in results dataframe)

	time = laps_copy.loc[row, 'Time']
	time = time.total_seconds() if isinstance(time, pd.Timedelta) else time		# convert the time located to seconds

	weather_copy = weather.copy()
	weather_copy['Time_seconds'] = weather_copy['Time_seconds'].dt.total_seconds()
	weather_copy['Time_seconds'] = pd.to_numeric(weather_copy['Time_seconds'], errors='coerce')
	time = pd.to_numeric(time, errors='coerce')
	index = (weather_copy['Time_seconds'] - time).abs().idxmin()	# search for the time in the weather dataframe to get the weather
	data = weather_copy.loc[index]

	return data["AirTemp"], data["TrackTemp"], data["Humidity"], data["Pressure"], data["WindSpeed"], data["Rainfall"], data["WindDirection"]

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

			except Exception as e:
				print(f"Error in fetchQualifyingData(): {e}")

def trying():
	Cache.enable_cache('cache/')
	session = get_session(2023, 20, 'Q')
	event = get_event(2023, 20)
	session.load(weather=True)
	lap = session.laps
	data = session.results
	weather = session.weather_data
	print(data.info())
	print("Weather Data:")
	print(weather)
	print("Lap Data:")
	print(lap)
	print("Lap Data Info:")
	print(lap.info())
	print(event['EventName'])
