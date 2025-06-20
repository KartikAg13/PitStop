from src.utils.print_utils import dataInfo
from src.data.preprocess import preprocessData

def main() -> None:

	data = preprocessData()
	dataInfo(data)

if __name__ == "__main__":
	main()