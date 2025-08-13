import pandas as pd

def convertToExcel(table_name: str, file_location: str) -> None:
	
	from .load_data import getData  
	data: pd.DataFrame = getData(table_name=table_name)
	data.to_excel(excel_writer=file_location, float_format="%.4f", engine="openpyxl", index=False)
