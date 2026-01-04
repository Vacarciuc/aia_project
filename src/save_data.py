from enum import Enum
from pandas import DataFrame
from os import path, makedirs

class DataType(Enum):
    DartyData = "darty_data",
    CleanedData = "cleaned_data",
    AnalyzedData = "analyzed_data",

CACHED_DATA = "cached_data"
FORMAT_FILE = "xlsx"

class SaveData:
    def __init__(self, file_name, data_type: DataType):
        self.file_name = file_name
        self.data_type = data_type

    def save(self, data: DataFrame):
        data_path = self._get_path()
        data.to_excel(data_path, index=False)
        return

    def _get_path(self) -> str:
        base_dir = str(path.dirname(__file__))
        folder_path = path.join(base_dir, '../', CACHED_DATA, self.data_type.value[0])
        makedirs(folder_path, exist_ok=True)
        return path.join(folder_path, f"{self.file_name}.{FORMAT_FILE}")




