import sys

from pathlib import Path

project_root = Path(__file__).parents[1]
sys.path.append(str(project_root))

from src.data.fetch_quali import fetchQualifyingData

if __name__ == "__main__":
	try:
		fetchQualifyingData(2023, 2024)	# 2018, some 2019, 2020, 2021, some 2022
	except Exception as e:
		print(f"Error in store_quali.py: {e}")