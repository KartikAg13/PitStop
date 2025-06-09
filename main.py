from src.data_loader import fetch_weekend_laps

dataframe = fetch_weekend_laps(2024, 'Bahrain', 'R')
print(dataframe.head())

# Not supposed to be here