import pandas as pd
import sys

from pathlib import Path

project_root = Path(__file__).parents[1]
sys.path.append(str(project_root))

from src.model.q3_prediction import getData

if __name__ == "__main__":
	try:
		_ = getData()
	except Exception as e:
		print(f"Error: {e}")