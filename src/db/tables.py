from sqlalchemy import MetaData, Table, Column, Integer, String, Float

from .engine import get_engine

def get_qualifying_table():

	metadata = MetaData()

	qualifying = Table(
		"qualifying", metadata,
		Column("id", Integer, primary_key=True, autoincrement=True),
		Column("season", Integer, nullable=False),
		Column("round", Integer, nullable=False),
		Column("driver", String, nullable=False),
		Column("q1", Float),
		Column("q2", Float),
		Column("q3", Float),
		Column("position", Float)
	)

	engine = get_engine("qualifying.db")
	metadata.create_all(engine)

	return qualifying