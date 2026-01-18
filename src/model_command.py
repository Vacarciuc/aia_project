from enum import Enum
from pandas import DataFrame
from src.model_random_forest_regresion import RandomForestTemperatureModel
from src.model_gradient_boosting_regressor import TemperatureGBModel


class ModelType(Enum):
    LINEAR_REGRESSOR = "LR", #easy model
    PROPHET = "PR", #medium complexity model
    RANDOM_FOREST_REGRESSOR = "RFR", #complex model


#@todo improve params for model (test-train split, random state, etc)

class ModelCommand:
    def __init__(self, data: DataFrame):
        self.data = data

    def execute(self, model_type: ModelType):
        if model_type == ModelType.LINEAR_REGRESSOR:
            return self._execute_linear_regressor_model()
        elif model_type == ModelType.PROPHET:
            return self._execute_prophet_model()
        elif model_type == ModelType.RANDOM_FOREST_REGRESSOR:
            return self._execute_random_forest_regressor_model()
        else:
            print("Unknown model type")
            return None


    train_data = 'hist'
    test_data = 'forecast'


    def _execute_linear_regressor_model(self):
        pass

    def _execute_prophet_model(self):
        pass

    def _execute_random_forest_regressor_model(self):
        pass