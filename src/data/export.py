import pandas as pd

def convertToExcel(data: pd.DataFrame, file_location: str) -> None:
	# Write the DataFrame to an Excel file using openpyxl
	data.to_excel(excel_writer=file_location, float_format="%.4f", engine="openpyxl")