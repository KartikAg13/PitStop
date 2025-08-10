from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = ["http://localhost:5173", "http://127.0.0.1:5173"]

app.add_middleware(
	CORSMiddleware,
	allow_origins = origins,
	allow_credentials = True,
	allow_methods = ["*"],
	allow_headers = ["*"]
)

@app.get("/qualifying")
def get_grid():
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