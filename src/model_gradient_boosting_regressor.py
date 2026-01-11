import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


class TemperatureGBModel:
    def __init__(
        self,
        target_column: str = "temperature_2m",
        test_size: float = 0.2,
        random_state: int = 42
    ):
        self.target_column = target_column
        self.test_size = test_size
        self.random_state = random_state

        self.model = GradientBoostingRegressor(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=4,
            subsample=0.8,
            random_state=random_state
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

    def train(self, X_train, y_train):
        self.model.fit(X_train, y_train)

    def evaluate(self, X_test, y_test) -> dict:
        predictions = self.model.predict(X_test)

        return {
            "MAE": mean_absolute_error(y_test, predictions),
            "RMSE": np.sqrt(mean_squared_error(y_test, predictions)),
            "R2": r2_score(y_test, predictions)
        }

    def plot_real_vs_predicted(
        self,
        X_test,
        y_test,
        n_points: int = 500
    ):
        predictions = self.model.predict(X_test)

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

        plt.title("Temperatura reală vs temperatura prezisă (Gradient Boosting)")
        plt.xlabel("Observații (timp)")
        plt.ylabel("Temperatura [°C]")
        plt.legend()
        plt.grid(True)
        plt.show()

    def plot_scatter_real_vs_predicted(self, X_test, y_test):
        predictions = self.model.predict(X_test)

        plt.figure(figsize=(7, 7))
        plt.scatter(
            y_test,
            predictions,
            alpha=0.4
        )

        min_val = min(y_test.min(), predictions.min())
        max_val = max(y_test.max(), predictions.max())

        plt.plot(
            [min_val, max_val],
            [min_val, max_val],
            linestyle="--"
        )

        plt.xlabel("Temperatură reală [°C]")
        plt.ylabel("Temperatură prezisă [°C]")
        plt.title("Valori reale vs valori prezise – Gradient Boosting")
        plt.grid(True)
        plt.show()
