from sqlalchemy import MetaData, Table, Column, Integer, String, Float, Boolean

from .engine import getEngine

TABLE_NAME: str = "qualifying"

def getQualifyingTable():

	metadata = MetaData()

	qualifying = Table(
		TABLE_NAME, metadata,
		Column("id", Integer, primary_key=True, autoincrement=True),
		Column("season", Integer, nullable=False),
		Column("round_number", Integer, nullable=False),
		Column("round_name", String, nullable=False),
		Column("driver_number", Integer, nullable=False),
		Column("driver_name", String, nullable=False),
		Column("team", String, nullable=False),

		Column("air_temp_q1", Float),
    	Column("track_temp_q1", Float),
    	Column("humidity_q1", Float),
    	Column("pressure_q1", Float),
    	Column("wind_speed_q1", Float),
		Column("wind_direction_q1", Integer),
    	Column("rain_flag_q1", Boolean),
		Column("q1", Float),
		
		Column("air_temp_q2", Float),
    	Column("track_temp_q2", Float),
    	Column("humidity_q2", Float),
    	Column("pressure_q2", Float),
    	Column("wind_speed_q2", Float),
		Column("wind_direction_q2", Integer),
    	Column("rain_flag_q2", Boolean),
		Column("q2", Float),
		
		Column("air_temp_q3", Float),
    	Column("track_temp_q3", Float),
    	Column("humidity_q3", Float),
    	Column("pressure_q3", Float),
    	Column("wind_speed_q3", Float),
		Column("wind_direction_q3", Integer),
    	Column("rain_flag_q3", Boolean),
		Column("q3", Float),
	)

	engine = getEngine(TABLE_NAME + ".db")
	metadata.create_all(engine)

	return qualifying