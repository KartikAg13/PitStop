import pandas as pd

def printUniqueValues(data: pd.DataFrame) -> None:
	for column in data.columns:
		try:
			unique_values = data[column].unique()
			print(f"Column Name: {column}")
			print(f"Data Type: {data[column].dtype}")
			print(f"Unique Values: {len(unique_values)}")
			print(unique_values)
		except Exception as e:
			print(f"Error in printUniqueValues: {e}")


def dataInfo(data: pd.DataFrame) -> None:
	print(data.head())
	print(data.info())
	print(data.describe())