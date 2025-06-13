import pandas as pd

from fastf1 import get_session, Cache

from src.db.engine import getEngine
from src.db.tables import getQualifyingTable, TABLE_NAME

def parseTimeDelta(time_delta) -> float | None:
	if pd.isna(time_delta) or time_delta is None:
		return None
	try:
		return time_delta.total_seconds()
	except Exception as e:
		print(f"Error in parseTimeDelta(): {e}")
		return None

def fetchQualifyingData(start_year: int, end_year: int):
	
	Cache.enable_cache("cache/")
	qualifying = getQualifyingTable()
	engine = getEngine(TABLE_NAME + ".db")

	for year in range(start_year, end_year + 1):
		
		for weekend in range(1, 25):
			try:
				session = get_session(year, weekend, 'Q')
				session.load()
				dataframe = session.results
				
				with engine.begin() as connection:
					for row in dataframe.itertuples(index=False):
						driver_name = getattr(row, "Abbreviation")
						q1_time = parseTimeDelta(getattr(row, "Q1"))
						q2_time = parseTimeDelta(getattr(row, "Q2"))
						q3_time = parseTimeDelta(getattr(row, "Q3"))
						company = getattr(row, "TeamName")

						insert_statement = qualifying.insert().values( 
							season = year, round = weekend,
							driver = driver_name,
							q1 = q1_time, q2 = q2_time, q3 = q3_time,
							team = company
						)
						connection.execute(insert_statement)
					
					print(f"Round {weekend} for year {year} successfully stored")

			except Exception as e:
				print(f"Error in fetchQualifyingData(): {e}")