import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np
import matplotlib.pyplot as plt


class RandomForestTemperatureModel:
    def __init__(
        self,
        target_column: str = "temperature_2m",
        test_size: float = 0.2,
        random_state: int = 42
    ):
        self.target_column = target_column
        self.test_size = test_size
        self.random_state = random_state
        self.model = RandomForestRegressor(
            n_estimators=300,
            max_depth=None,
            min_samples_split=2,
            min_samples_leaf=1,
            random_state=random_state,
            n_jobs=-1
        )

    def split_data(self, df: pd.DataFrame):
        X = df.drop(columns=[self.target_column, "date"])
        y = df[self.target_column]

        split_index = int(len(df) * (1 - self.test_size))

        X_train = X.iloc[:split_index]
        X_test = X.iloc[split_index:]

        y_train = y.iloc[:split_index]
        y_test = y.iloc[split_index:]

        return X_train, X_test, y_train, y_test


    def train(self, x_train: pd.DataFrame, y_train: pd.Series):
        self.model.fit(x_train, y_train)

    def evaluate(self, x_test: pd.DataFrame, y_test: pd.Series) -> dict:
        predictions = self.model.predict(x_test)

        return {
            "MAE": mean_absolute_error(y_test, predictions),
            "RMSE": np.sqrt(mean_squared_error(y_test, predictions)),
            "R2": r2_score(y_test, predictions)
        }

    def feature_importance(self) -> pd.DataFrame:
        return pd.DataFrame({
            "feature": self.model.feature_names_in_,
            "importance": self.model.feature_importances_
        }).sort_values(by="importance", ascending=False)

    def plot_real_vs_predicted(
            self,
            x_test: pd.DataFrame,
            y_test: pd.Series,
            n_points: int = 500
    ):
        predictions = self.model.predict(x_test)

        # Limităm numărul de puncte pentru lizibilitate
        y_test_plot = y_test.iloc[:n_points]
        predictions_plot = predictions[:n_points]

        plt.figure(figsize=(14, 6))
        plt.plot(
            y_test_plot.values,
            label="Valori reale",
            linewidth=2
        )
        plt.plot(
            predictions_plot,
            label="Valori prezise",
            linestyle="--"
        )

        plt.title("Temperatura reală vs temperatura prezisă (2m)")
        plt.xlabel("Observații (timp)")
        plt.ylabel("Temperatura [°C]")
        plt.legend()
        plt.grid(True)
        plt.show()