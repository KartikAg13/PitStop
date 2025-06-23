import pandas as pd
import numpy as np

import torch
import torch.nn as nn
import torch.optim as optim

from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from src.data.preprocess import preprocessData

class Q3Prediction(nn.Module):
	
	def __init__(self, input_size=70):
		# Used to initialize the parent class
		super(Q3Prediction, self).__init__()
		
		# Layer 1
		self.l1 = nn.Linear(in_features=input_size, out_features=32)
		self.a1 = nn.LeakyReLU(negative_slope=0.1)
		
		# Layer 2
		self.l2 = nn.Linear(in_features=32, out_features=16)
		self.a2 = nn.LeakyReLU()

		# Layer 3
		self.l3 = nn.Linear(in_features=16, out_features=1)
		self.a3 = nn.ReLU()

	
	def forward(self, x: torch.Tensor) -> torch.Tensor:
		# Forward Propogation
		z1 = self.l1(x)
		a1 = self.a1(z1)

		z2 = self.l2(a1)
		a2 = self.a2(z2)

		z3 = self.l3(a2)
		a3 = self.a3(z3)
		return a3
	

def featureEngineering(data: pd.DataFrame) -> pd.DataFrame:

	# Track improvement from q1 to q2
	data['improvement'] = (data['q1'] - data['q2']) / data['q1']

	# Check for rain in any session
	data['rain'] = data['rain_flag_q1'].astype(int) | data['rain_flag_q2'].astype(int) | data['rain_flag_q3'].astype(int)

	# Get the average data pattern
	data['avg_air'] = (data['air_temp_q1'] + data['air_temp_q2'] + data['air_temp_q3']) / 3.0
	data['avg_track'] = (data['track_temp_q1'] + data['track_temp_q2'] + data['track_temp_q3']) / 3.0
	data['avg_wspeed'] = (data['wind_speed_q1'] + data['wind_speed_q2'] + data['wind_speed_q3']) / 3.0
	data['avg_wdirection'] = (data['wind_direction_q1'] + data['wind_direction_q2'] + data['wind_direction_q3']) / 3.0
	data['avg_pre'] = (data['pressure_q1'] + data['pressure_q2'] + data['pressure_q3']) / 3.0
	data['avg_hum'] = (data['humidity_q1'] + data['humidity_q2'] + data['humidity_q3']) / 3.0		
	return data


def prep():

	# Get the clean data
	data = preprocessData()

	# Filter the data for only q3 drivers
	data_copy = data[data['q3'].notna()].copy()

	# Create new features
	data_copy = featureEngineering(data=data_copy)

	# Drop columns that are not required
	drop_columns: list[str] = ['id', 'round_number', 'driver_number']
	data_copy = data_copy.drop(columns=drop_columns)

	# Provide labels to the text columns
	text_columns: list[str] = ['round_name', 'driver_name', 'team']
	for column in text_columns:
		label_encoder = LabelEncoder()
		data_copy[column] = label_encoder.fit_transform(data_copy[column])

	# Split the data into train and test
	train_data = data_copy[data_copy['season'] < 2025]
	test_data = data_copy[data_copy['season'] == 2025]

	# Remove the rookies from the test data
	rookie_list: list[str] = ['HAD', 'DOO', 'COL', 'ANT', 'LAW', 'BOR']
	test_data = test_data[~test_data['driver_name'].isin(rookie_list)]

	# Split the data into x and y
	y_train = train_data['q3']
	y_test = test_data['q3']
	x_train = train_data.drop(columns=['q3'])
	x_test = test_data.drop(columns=['q3'])

	# Normalize the data
	x_scaler = StandardScaler()
	x_train = x_scaler.fit_transform(X=x_train)
	x_test = x_scaler.transform(X=x_test)

	# Convert the data to torch tensors
	x_train = torch.tensor(data=x_train, dtype=torch.float32)
	x_test = torch.tensor(data=x_test, dtype=torch.float32)
	y_train = torch.tensor(data=y_train.values, dtype=torch.float32).view(-1, 1)
	y_test = torch.tensor(data=y_test.values, dtype=torch.float32).view(-1, 1)

	return x_train, y_train, x_test, y_test


def train():

	x_train, y_train, x_test, y_test = prep()

	input_size = x_train.shape[1]

	model = Q3Prediction(input_size=input_size)
	criterion = nn.L1Loss()
	optimizer = optim.Adam(model.parameters(), lr=0.01)
	cost = []

	number_of_epochs: int = 10
	for epoch in range(number_of_epochs):
		model.train()
		optimizer.zero_grad()
		outputs = model(x_train)
		loss = criterion(outputs, y_train)
		loss.backward()
		optimizer.step()

		cost.append(loss.item())

		if epoch % 10 == 0:
			print(f"Epoch: {epoch}/{number_of_epochs}, Loss: {loss.item()}")

	model.eval()
	y_predicted = model(x_test)
	y_predicted = y_predicted.detach().numpy()
	y_test = y_test.detach().numpy()
	mae = mean_absolute_error(y_true=y_test, y_pred=y_predicted)
	mse = mean_squared_error(y_true=y_test, y_pred=y_predicted)
	r2 = r2_score(y_true=y_test, y_pred=y_predicted)

	torch.save(model.state_dict(), 'q3_prediction_model.pth')
	print(f"MAE: {mae}, MSE: {mse}, R2 Score: {r2}")
	print(f"y_test: {y_test}, y_pred: {y_predicted}")
	