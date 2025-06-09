import pandas as pd
import fastf1

def fetch_weekend_laps(year: int, gp: str, session_type) -> pd.DataFrame:
	fastf1.Cache.enable_cache('cache/')
	session = fastf1.get_session(year, gp, session_type)
	session.load()

	laps: pd.DataFrame = session.laps
	return laps

dataframe = fetch_weekend_laps(2022, 'Bahrain', 'R')
print(dataframe.head())