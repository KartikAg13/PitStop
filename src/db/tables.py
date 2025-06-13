from sqlalchemy import MetaData, Table, Column, Integer, String, Float

from .engine import getEngine

TABLE_NAME: str = "qualifying"

def getQualifyingTable():

	metadata = MetaData()

	qualifying = Table(
		TABLE_NAME, metadata,
		Column("id", Integer, primary_key=True, autoincrement=True),
		Column("season", Integer, nullable=False),
		Column("round", Integer, nullable=False),
		Column("driver", String, nullable=False),
		Column("q1", Float),
		Column("q2", Float),
		Column("q3", Float),
		Column("team", String)
	)

	engine = getEngine(TABLE_NAME + ".db")
	metadata.create_all(engine)

	return qualifying