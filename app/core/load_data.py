import pandas as pd

from fastf1 import Cache,  get_event, get_session
from fastf1.events import Event, Session

from sqlalchemy import Engine, Table, Insert, Connection, text, TextClause

from app.core.db.engine import getEngine
from app.core.db.tables import getQualifyingTable, QUALIFYING_TABLE
from .export import convertToExcel

DB_NAME: str = "f1_data.db"


def getQualifyingTimes(row):
	
	q1: pd.Timedelta | None = getattr(row, "Q1")
	
	if q1 is None:
		return (None, ) * 3
	else:
		q1_time: float = q1.total_seconds()
		q2: pd.Timedelta | None = getattr(row, "Q2")
		
		if q2 is None:
			return q1_time, None, None
		else:
			q2_time: float = q2.total_seconds()
			q3: pd.Timedelta | None = getattr(row, "Q3")

			if q3 is None:
				return q1_time, q2_time, None
			else:
				q3_time: float = q3.total_seconds()
				return q1_time, q2_time, q3_time




def fetchQualifyingData(start_year: int, end_year: int):

	engine: Engine = getEngine(database_name=DB_NAME)
	qualifying: Table = getQualifyingTable(engine=engine)

	Cache.enable_cache(cache_dir="cache/")

	for year in range(start_year, end_year + 1):

		for weekend in range(1, 25):

			try:
				event: Event = get_event(year=year, gp=weekend)
				event_name: str = event["EventName"]

				session: Session = get_session(year=year, gp=weekend, identifier='Q')
				session.load()
				results: pd.DataFrame = session.results

				with engine.begin() as connection:
					
					for row in results.itertuples():
						
						number: int = int(getattr(row, "DriverNumber"))
						name: str = str(getattr(row, "Abbreviation"))
						company: str = str(getattr(row, "TeamName"))

						q1_time, q2_time, q3_time = getQualifyingTimes(row=row)

						insert_statement: Insert = qualifying.insert().values(
							season = year, round_number = weekend,
							round_name = event_name, driver_number = number,
							driver_name = name, team = company,
							q1 = q1_time, q2 = q2_time, q3 = q3_time
						)

						connection.execute(statement=insert_statement)

			except Exception as e:
				print(f"Error in fetchQualifyingData: {e}")

	convertToExcel(table_name=QUALIFYING_TABLE, file_location=f"data/{QUALIFYING_TABLE}.xlsx")


def getData(table_name: str) -> pd.DataFrame:

	engine: Engine = getEngine(database_name=DB_NAME)
	connection: Connection = engine.connect()

	data = pd.read_sql(sql=f"SELECT * FROM {table_name}", con=connection)

	connection.close()
	return data

def deleteYear(year: int, table_name: str) -> None:
    engine: Engine = getEngine(database_name=DB_NAME)
    connection: Connection = engine.connect()

    delete_statement: TextClause = text(text=f"DELETE FROM {table_name} WHERE season = {year}")
    connection.execute(statement=delete_statement)

    connection.close()
    print(f"Deleted data for year {year} from table {table_name}.")
