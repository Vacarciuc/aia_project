from enum import Enum
from pandas import DataFrame
from src.model_random_forest_regresion import RandomForestTemperatureModel
from src.model_gradient_boosting_regressor import TemperatureGBModel


class ModelType(Enum):
    RANDOM_FOREST_REGRESSOR = "RFR",
    GRADIENT_BOOSTING_REGRESSOR = "GBR",

#@todo improve params for model (test-train split, random state, etc)

class ModelCommand:
    def __init__(self, data: DataFrame):
        self.data = data

    def execute(self, model_type: ModelType):
        if model_type == ModelType.RANDOM_FOREST_REGRESSOR:
            self._execute_random_forest_regressor()
        elif model_type == ModelType.GRADIENT_BOOSTING_REGRESSOR:
            self._execute_gradient_boosting_regressor()
        else:
            raise ValueError(f"Unsupported model type: {model_type}")


    def _execute_random_forest_regressor(self):
        model = RandomForestTemperatureModel()
        x_train, x_test, y_train, y_test = model.split_data(self.data)
        model.train(x_train, y_train)
        metrics = model.evaluate(x_test, y_test)
        print(metrics)
        importance = model.feature_importance()
        print(importance.head(10))
        model.plot_real_vs_predicted(x_test, y_test)

    def _execute_gradient_boosting_regressor(self):
        gb_model = TemperatureGBModel()
        x_train, x_test, y_train, y_test = gb_model.split_data(self.data)
        gb_model.train(x_train, y_train)
        metrics_gb = gb_model.evaluate(x_test, y_test)
        print(metrics_gb)
        gb_model.plot_real_vs_predicted(x_test, y_test)
        gb_model.plot_scatter_real_vs_predicted(x_test, y_test)