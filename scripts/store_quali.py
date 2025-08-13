import sys

from pathlib import Path

project_root = Path(__file__).parents[1]
sys.path.append(str(project_root))

from app.core.load_data import fetchQualifyingData

if __name__ == "__main__":
	try:
		fetchQualifyingData(2025, 2025)
	except Exception as e:
		print(f"Error in store_quali.py: {e}")