import joblib

from fastf1 import get_events_remaining
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.models.q3_prediction import Q3Prediction

app = FastAPI()

origins = ["http://localhost:5173", "http://127.0.0.1:5173"]

app.add_middleware(
	CORSMiddleware,
	allow_origins = origins,
	allow_credentials = True,
	allow_methods = ["*"],
	allow_headers = ["*"]
)

def next_event() -> str | None:
    event = get_events_remaining()
    if event.empty:
        return None
    else:
        next_event = event.iloc[0]
        return next_event["EventName"]

@app.get("/qualifying")
def get_grid():

    season: int = datetime.now().year
    round_name: str | None = next_event()
    if round_name is None:
        return { "model_info": "See you next year!" }
    
    
    model = Q3Prediction()
    model.model = joblib.load(filename="models/xgb_model.joblib")

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
        "predictions": [
            { "position": 1, "driver": "Max Verstappen", "team": "Red Bull Racing" },
            { "position": 2, "driver": "Charles Leclerc", "team": "Ferrari" },
            { "position": 3, "driver": "Lando Norris", "team": "McLaren" }
        ]
    }