import pandas as pd
import matplotlib.pyplot as plt
import xgboost as xgb
import joblib
from sklearn.model_selection import RandomizedSearchCV, GridSearchCV, TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import HistGradientBoostingRegressor, StackingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score
from typing import cast
from src.data.preprocess import preprocessData

class Q3Prediction:
	
	def __init__(self):
		self.xgb_model: xgb.XGBRegressor | None = None
		self.hist_model: HistGradientBoostingRegressor | None = None
		self.model: StackingRegressor | None = None
		self.label_encoders = {}
		self.features = []
		self.test_scores = {}

	
	def featureEngineering(self, data: pd.DataFrame) -> pd.DataFrame:
		# Improvement ratio from q1 to q2
		data['improvement'] = (data['q1'] - data['q2']) / data['q1']

		# Check for possible rain
		data['rain'] = data['rain_flag_q1'].astype(int) | data['rain_flag_q2'].astype(int) | data['rain_flag_q3'].astype(int)

		# Get the average data pattern
		data['avg_air'] = (data['air_temp_q1'] + data['air_temp_q2'] + data['air_temp_q3']) / 3.0
		data['avg_track'] = (data['track_temp_q1'] + data['track_temp_q2'] + data['track_temp_q3']) / 3.0
		data['avg_wspeed'] = (data['wind_speed_q1'] + data['wind_speed_q2'] + data['wind_speed_q3']) / 3.0
		data['avg_wdirection'] = (data['wind_direction_q1'] + data['wind_direction_q2'] + data['wind_direction_q3']) / 3.0
		data['avg_pre'] = (data['pressure_q1'] + data['pressure_q2'] + data['pressure_q3']) / 3.0
		data['avg_hum'] = (data['humidity_q1'] + data['humidity_q2'] + data['humidity_q3']) / 3.0

		return data

	
	def handleCategorical(self, features, data: pd.DataFrame) -> pd.DataFrame:
		for column in features:
			if column not in self.label_encoders:
				self.label_encoders[column] = LabelEncoder()
				data[column] = self.label_encoders[column].fit_transform(data[column].astype(str))
			else:
				data[column] = self.label_encoders[column].transform(data[column].astype(str))
		return data

	
	def trainModel(self, x_train, y_train, x_test, y_test):
		# XGBoost parameter grid
		xgb_param_grid = {
			"n_estimators": [125, 150, 175],
			"max_depth": [4, 5, 6],
			"learning_rate": [0.025, 0.05, 0.075],
			"subsample": [0.825, 0.85, 0.875],
			"colsample_bytree": [0.975, 1.0],
			"gamma": [0.35, 0.4, 0.45],
			"reg_alpha": [0.25, 0.3, 0.35],
			"reg_lambda": [0.125, 0.15, 0.175],
			"min_child_weight": [4, 5, 6],
			"tree_method": ["hist", "auto"]
		}
		
		# HistGradientBoosting parameter grid
		hist_param_grid = {
			"max_iter": [50, 100, 150],
			"max_leaf_nodes": [15, 31, 63],
			"max_depth": [3, 5, 7],
			"learning_rate": [0.01, 0.05, 0.1],
			"l2_regularization": [0, 0.1, 0.5],
			"max_bins": [63, 127, 255],
			"min_samples_leaf": [10, 20, 30]
		}
		
		tscv = TimeSeriesSplit(n_splits=6)
		
		# Tune XGBoost
		print("==========Tuning XGBoost model==========")
		self.xgb_model = xgb.XGBRegressor(objective='reg:absoluteerror', random_state=50, eval_metric='mae')
		xgb_search = GridSearchCV(
			self.xgb_model, xgb_param_grid, cv=tscv, scoring='neg_mean_absolute_error',
			n_jobs=-1, verbose=10
		)
		xgb_search.fit(x_train, y_train)
		self.xgb_model = cast(xgb.XGBRegressor, xgb_search.best_estimator_)
		joblib.dump(self.xgb_model, "xgb_model.joblib")
		
		# Tune HistGradientBoosting
		print("==========Tuning HistGradientBoosting model==========")
		self.hist_model = HistGradientBoostingRegressor(random_state=50)
		hist_search = RandomizedSearchCV(
			self.hist_model, hist_param_grid, n_iter=100, cv=tscv, scoring='neg_mean_absolute_error',
			n_jobs=-1, verbose=10, random_state=50
		)
		hist_search.fit(x_train, y_train)
		self.hist_model = cast(HistGradientBoostingRegressor, hist_search.best_estimator_)
		joblib.dump(self.hist_model, "hist_model.joblib")
		
		# Stacking
		stack = StackingRegressor(
			estimators=[('xgb', self.xgb_model), ('hist', self.hist_model)],
			final_estimator=Ridge()
		)
		stack.fit(x_train, y_train)
		self.model = stack
		print("Model trained successfully")
		
		print("XGB Regressor Model:")
		self.predict(x_test, y_test, model="xgb")
		print(f"Best XGBoost parameters: {xgb_search.best_params_}")
		
		print("Hist Gradient Boosting Model:")
		self.predict(x_test, y_test, model="hist")
		print(f"Best HistGradient parameters: {hist_search.best_params_}")

		self.predict(x_test, y_test)
		
		joblib.dump(self.model, "q3_prediction.joblib")

	
	def predict(self, x_test, y_test, model=None):
		
		if model == "xgb" and self.xgb_model is not None:
			y_predicted = self.xgb_model.predict(x_test)
		
		elif model == "hist" and self.hist_model is not None:
			y_predicted = self.hist_model.predict(x_test)
		
		elif self.model is not None:
			y_predicted = self.model.predict(x_test)
		
		mae = mean_absolute_error(y_test, y_predicted)
		rmse = root_mean_squared_error(y_test, y_predicted)
		r2 = r2_score(y_test, y_predicted)
		
		print(f"Mean Absolute Error: {mae}")
		print(f"Root Mean Squared Error: {rmse}")
		print(f"R2 Score: {r2}")
		print("Y Test vs Y Predicted:")
		print(pd.DataFrame({'y_test': y_test, 'y_predicted': y_predicted}))
		
		if model is None:
			self.test_scores = {"mae": mae, "rmse": rmse, "r2": r2}

	
	def getFeatureImportance(self, x_train: pd.DataFrame):
		
		if self.xgb_model is not None:
			importance = self.xgb_model.feature_importances_
			feature_names = x_train.columns
			
			importance_list = pd.DataFrame({
				'Feature': feature_names,
				'Importance': importance
			}).sort_values(by='Importance', ascending=False)
			print(importance_list)

	
	def start(self) -> None:
		
		data = preprocessData()
		data_copy = data[data['q3'].notna()].copy()
		data_copy = self.featureEngineering(data_copy)
		
		drop_columns = [
			'id', 'driver_number', 'round_number',
			'air_temp_q1', 'track_temp_q1', 'humidity_q1', 'pressure_q1', 'wind_speed_q1', 'wind_direction_q1', 'rain_flag_q1',
			'air_temp_q2', 'track_temp_q2', 'humidity_q2', 'pressure_q2', 'wind_speed_q2', 'wind_direction_q2', 'rain_flag_q2',
			'air_temp_q3', 'track_temp_q3', 'humidity_q3', 'pressure_q3', 'wind_speed_q3', 'wind_direction_q3', 'rain_flag_q3',
		]
		
		data_copy = data_copy.drop(columns=drop_columns)
		
		features = ['driver_name', 'team', 'round_name']
		data_copy = self.handleCategorical(features, data_copy)
		
		train_data = data_copy[data_copy['season'] < 2025]
		test_data = data_copy[data_copy['season'] == 2025]
		
		rookie_list = ['HAD', 'DOO', 'COL', 'ANT', 'LAW', 'BOR']
		test_data = test_data[~test_data['driver_name'].isin(rookie_list)]
		
		y_train = train_data['q3']
		y_test = test_data['q3']
		x_train = train_data.drop(columns=['q3'])
		x_test = test_data.drop(columns=['q3'])
		
		self.features = x_train.columns.to_list()
		
		self.trainModel(x_train, y_train, x_test, y_test)
		
		print("Predict using test:")
		self.predict(x_test, y_test)
		print("Predict using train:")
		
		self.predict(x_train, y_train)
		self.getFeatureImportance(x_train)