import pandas as pd
import matplotlib.pyplot as plt

import xgboost as xgb
import joblib

from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score
from typing import cast

from src.data.preprocess import preprocessData

class Q3Prediction:
	
	def __init__(self):
		self.model: xgb.XGBRegressor | None = None
		self.label_encoders = {}
		self.features = []
		self.test_scores = {}


	def featureEngineering(self, data: pd.DataFrame) -> pd.DataFrame:
		
		# Improvement ratio from q1 to q2
		data['improvement'] = (data['q1'] - data['q2']) / data['q1']

		# Check for possible rain
		data['rain'] = (data['rain_flag_q1'].astype(int) | data['rain_flag_q2'].astype(int) | data['rain_flag_q3'].astype(int))

		# Get the change in weather
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


	def trainModel(self, x_train, y_train):
		
		parameter_grid = {
			"n_estimators": [150, 200, 250],
			"max_depth": [3, 4, 5],
			"learning_rate": [0.01, 0.05, 0.1],
			"subsample": [0.85, 0.9, 0.95],
			"colsample_bytree": [0.9, 0.95, 1.0],
			"gamma": [0.35, 0.4, 0.45],
			"reg_alpha": [0.25, 0.3, 0.35],
			"reg_lambda": [0.15, 0.2, 0.25],
			"min_child_weight": [4, 5, 6]
		}

		self.model = xgb.XGBRegressor(objective='reg:absoluteerror', random_state=50, eval_metric='mae', tree_method='hist')

		tscv = TimeSeriesSplit(n_splits=6)

		grid_search = RandomizedSearchCV(
			self.model, 
			param_distributions=parameter_grid, 
			cv=tscv, 
			scoring='neg_mean_absolute_error', 
			n_jobs=-1, 
			verbose=10, 
			return_train_score=True
		)
		grid_search.fit(x_train, y_train)
		
		self.model = cast(xgb.XGBRegressor, grid_search.best_estimator_)
		print(f"Best score: {grid_search.best_score_}")
		print(f"Best parameters: {grid_search.best_params_}")

		joblib.dump(self.model, "q3_prediction.joblib")

	
	def predict(self, x_test, y_test):

		if self.model is not None:
			y_predicted = self.model.predict(x_test)

		mae = mean_absolute_error(y_test, y_predicted)
		rmse = root_mean_squared_error(y_test, y_predicted)
		r2 = r2_score(y_test, y_predicted)

		print(f"Mean Absolute Error: {mae}")
		print(f"Root Mean Squared Error: {rmse}")
		print(f"R2 Score: {r2}")

		self.test_scores = {
			"mae": mae,
			"rmse": rmse,
			"r2": r2
		}


	def getFeatureImportance(self, x_train: pd.DataFrame): 

		if self.model is not None:
			importance = self.model.feature_importances_
			feature_names = x_train.columns

			importance_list = pd.DataFrame({
				'feature': feature_names,
				'importance': importance 
			}).sort_values(by='importance', ascending=False)

			print(importance_list)


	def start(self) -> None:

		data = preprocessData()
		data_copy = data[data['q3'].notna()].copy()

		data_copy = self.featureEngineering(data_copy)
		drop_columns = [
			'id', 'driver_number', 'round_number',
			'air_temp_q1', 'track_temp_q1', 'humidity_q1',
			'pressure_q1', 'wind_speed_q1', 'wind_direction_q1',
			'rain_flag_q1', 'air_temp_q2', 'track_temp_q2', 'humidity_q2',
			'pressure_q2', 'wind_speed_q2', 'wind_direction_q2',
			'rain_flag_q2', 'air_temp_q3', 'track_temp_q3', 'humidity_q3',
			'pressure_q3', 'wind_speed_q3', 'wind_direction_q3',
			'rain_flag_q3'
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

		# self.model = self.load()
		self.trainModel(x_train, y_train)
		self.predict(x_test, y_test)

		self.getFeatureImportance(x_train)

	
	def load(self):
		self.model = joblib.load("q3_prediction.joblib")
		return self.model
