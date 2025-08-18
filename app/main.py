import pandas as pd
import numpy as np
import joblib

from fastf1 import get_events_remaining
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import Engine, Connection

from app.models.q3_prediction import Q3Prediction
from app.core.db.engine import getEngine
from app.core.load_data import DB_NAME
from app.core.db.tables import QUALIFYING_TABLE
import xgboost as xgb

app = FastAPI()

origins = ["http://localhost:5173", "http://127.0.0.1:5173"]

app.add_middleware(
	CORSMiddleware,
	allow_origins = origins,
	allow_credentials = True,
	allow_methods = ["*"],
	allow_headers = ["*"]
)

def formatTime(seconds: float) -> str:
	td = pd.to_timedelta(seconds, unit="s")
	return f"{td.components.minutes:02d}:{td.components.seconds:02d}.{td.components.milliseconds:03d}"

def next_event() -> str | None:
	event = get_events_remaining()
	if event.empty:
		return None
	else:
		next_event = event.iloc[0]
		return next_event["EventName"]

@app.get("/qualifying")
def get_grid():
	round_name: str | None = next_event()
	if round_name is None:
		return { "model_info": "See you next year!" }
	else:
		print(round_name)

	engine: Engine = getEngine(database_name=DB_NAME)
	connection: Connection = engine.connect()
	query: str = f"""
		SELECT * FROM {QUALIFYING_TABLE} WHERE
		season = 2024 AND
		round_name = '{round_name}'
	"""
	data: pd.DataFrame = pd.read_sql(sql=query, con=connection)
	data = data.drop(columns=["id", "round_number", "driver_number", "q3"])
	data = data[~data["q2"].isna()]
	x: pd.DataFrame = data.copy()
	x["season"] = 2025
	x["improvement"] = np.abs(x["q1"] - x["q2"]) / x["q1"]
	encoders: dict = joblib.load(filename="models/le.joblib")
	for feature, le in encoders.items():
		x[feature] = le.transform(x[feature].astype(str))


	model = Q3Prediction()
	model.model = joblib.load(filename="models/xgb_model.joblib")
	if isinstance(model.model, xgb.XGBRegressor):
		y_predicted = model.model.predict(X=x)
		data["q3_predicted"] = y_predicted
		data = data.sort_values(by="q3_predicted").reset_index(drop=True)
		predictions = []
		for index, row in data.iterrows():
			predictions.append({
				"position": index,
				"driver": row["driver_name"],
				"team": row["team"],
				"q3": formatTime(row["q3_predicted"])
			})
		return {
			"model_info": {
				"type": "XGBRegressor",
				"mae": 0.2249,
				"hyperparameters": {
					"n_estimators": 300,
					"max_depth": 8,
					"learning_rate": 0.05
				}
			},
			"predictions": predictions
	}   
	else:
		return { "model_info": "See you next year!" }