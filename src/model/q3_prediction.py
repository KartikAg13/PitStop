import pandas as pd
import numpy as np

import xgboost as xgb
import joblib

from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score

from typing import cast

from src.data.preprocess import preprocessQualifying

class Q3Prediction:
	
	def __init__(self):
		self.model = None
		self.features = []
		self.test_scores = {}

	
	def featureEngineering(self, data: pd.DataFrame) -> pd.DataFrame:

		data["improvement"] = np.abs(data["q1"] - data["q2"]) / data["q1"]

		return data
	

	def printResults(self, cv_results, top: int) -> None:
		
		data: pd.DataFrame = pd.DataFrame(cv_results)
		data = data.sort_values(by="mean_test_score", ascending=False)
		
		columns = [column for column in data.columns if column.startswith("param_")] + ["mean_test_score", "std_test_score", "rank_test_score"]
		data = data[columns].head(top)

		data.to_csv("xgb_results.csv", index=False)

	
	def trainModel(self, x_train: pd.DataFrame, y_train: pd.Series) -> None:
		
		xgb_param_grid = {
			"n_estimators": [600, 625, 650],
			"max_depth": [2],
			"learning_rate": [0.015, 0.02, 0.025],
			"subsample": [0.6, 0.65, 0.7],
			"colsample_bytree": [0.975, 1.0],
			"gamma": [0.3, 0.4, 0.5],
			"reg_alpha": [0.45, 0.475, 0.5],
			"reg_lambda": [0.1, 0.2, 0.3],
			"min_child_weight": [4, 5, 6],
			"tree_method": ["hist"],
		}
		
		tscv = TimeSeriesSplit(n_splits=10)
		
		self.model = xgb.XGBRegressor(objective="reg:absoluteerror", random_state=50, eval_metric="mae")

		xgb_search = GridSearchCV(
			estimator=self.model,
			param_grid=xgb_param_grid,
			cv=tscv,
			scoring="neg_mean_absolute_error",
			n_jobs=-1,
			verbose=10,
			return_train_score=True,
		)
		xgb_search.fit(x_train, y_train)
		
		self.model = cast(xgb.XGBRegressor, xgb_search.best_estimator_)
		joblib.dump(self.model, "saved_models/xgb_model.joblib")
	
		print("Model trained successfully")
		self.printResults(cv_results=xgb_search.cv_results_, top=25)
		print(f"Best XGBoost Parameters: {xgb_search.best_params_}")
		print(f"Best XGBoost Score: {xgb_search.best_score_}")

	
	def predict(self, x_test, y_test):
		
		if self.model is not None:
			y_predicted = self.model.predict(x_test)
		
		mae = mean_absolute_error(y_test, y_predicted)
		rmse = root_mean_squared_error(y_test, y_predicted)
		r2 = r2_score(y_test, y_predicted)
		
		print(f"Mean Absolute Error: {mae}")
		print(f"Root Mean Squared Error: {rmse}")
		print(f"R2 Score: {r2}")
		
		print("Y Test vs Y Predicted:")
		print(pd.DataFrame({"y_test": y_test, "y_predicted": y_predicted}))
		
		self.test_scores = {"mae": mae, "rmse": rmse, "r2": r2}

	
	def getFeatureImportance(self, x_train: pd.DataFrame):
		
		if self.model is not None:
			importance = self.model.feature_importances_
			
			importance_list = pd.DataFrame({"Feature": self.features, "Importance": importance}).sort_values(by='Importance', ascending=False)
			
			print(importance_list)

	
	def start(self) -> None:
		
		x_train, y_train, x_test, y_test = preprocessQualifying()

		x_train = self.featureEngineering(data=x_train)
		x_test = self.featureEngineering(data=x_test)
		
		self.features = x_train.columns.to_list()
		
		self.trainModel(x_train, y_train)
		
		print("Predict using test:")
		self.predict(x_test, y_test)
		print("Predict using train:")
		self.predict(x_train, y_train)
		
		self.getFeatureImportance(x_train)