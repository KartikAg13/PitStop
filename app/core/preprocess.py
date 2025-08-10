import pandas as pd

from load_data import getData
from db.tables import QUALIFYING_TABLE
from utils import dataInfo

from sklearn.preprocessing import LabelEncoder

def handleCategorical(features: list[str], data: pd.DataFrame) -> pd.DataFrame:
    for feature in features:
        le = LabelEncoder()
        data[feature] = le.fit_transform(data[feature].astype(str))
    return data


def preprocessQualifying() -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:

	data: pd.DataFrame = getData(table_name=QUALIFYING_TABLE)

	data = data[data["q3"].notna() & data["q1"].notna()]
     
	data = data[~(data["q1"] < data["q3"])]

	drop_columns: list[str] = ["id", "driver_number", "round_number"]
	data = data.drop(columns=drop_columns)

	rookie_list: list[str] = ["HAD", "DOO", "COL", "ANT", "LAW", "BOR"]
	data = data[~data["driver_name"].isin(values=rookie_list)]

	features: list[str] = ['driver_name', 'team', 'round_name']
	data: pd.DataFrame = handleCategorical(features=features, data=data)

	train_data: pd.DataFrame = data[data["season"] < 2025]
	test_data: pd.DataFrame = data[data["season"] == 2025]

	print(f"Train Data Info")
	dataInfo(data=train_data)
	print(f"Test Data Info")
	dataInfo(data=test_data)

	x_train: pd.DataFrame = train_data.drop(columns=["q3"])
	y_train: pd.Series = train_data["q3"]
	x_test: pd.DataFrame = test_data.drop(columns=["q3"])
	y_test: pd.Series = test_data["q3"]
	
	return x_train, y_train, x_test, y_test