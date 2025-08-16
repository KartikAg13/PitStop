from sqlalchemy import  Engine, MetaData, Table, Column, Integer, String, Float

QUALIFYING_TABLE: str = "qualifying"
RACE_TABLE: str = "race"

def getQualifyingTable(engine: Engine) -> Table:

	metadata = MetaData()

	qualifying = Table(
		
		QUALIFYING_TABLE, metadata,
		
		Column("id", Integer, primary_key=True, autoincrement=True),
		Column("season", Integer, nullable=False),
		Column("round_number", Integer, nullable=False),
		Column("round_name", String, nullable=False),
		Column("driver_number", Integer, nullable=False),
		Column("driver_name", String, nullable=False),
		Column("team", String, nullable=False),

		Column("q1", Float),
		Column("q2", Float),
		Column("q3", Float),
	)

	metadata.create_all(bind=engine)

	return qualifying

def getRaceTable(engine: Engine) -> Table:

	metadata = MetaData()

	race = Table(
		RACE_TABLE, metadata,
		Column("id", Integer, primary_key=True, autoincrement=True),
		Column("round_number", Integer, nullable=False)
	)

	metadata.create_all(bind=engine)

	return race