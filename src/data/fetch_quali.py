import pandas as pd
import os

from fastf1 import get_session, Cache
from sqlalchemy import insert

from src.db.engine import get_engine
from src.db.tables import get_qualifying_table

def fetch_qualifying_data(start_year: int, end_year: int):
	
	Cache.enable_cache("cache/")
	qualifying = get_qualifying_table()

	for year in range(start_year, end_year + 1):
		
		for round in range(1, 25):
			try:
				session = get_session(year, round, 'Q')
				session.load()
				dataframe = session.results
				

			except Exception as e:
				print(f"Round {round} for year {year} was not found")