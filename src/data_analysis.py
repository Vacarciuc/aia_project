from pandas import DataFrame
import matplotlib.pyplot as plt
import seaborn as sns


class Analysis:
    def __init__(self, data: DataFrame):
        self.data = data.copy()

    #@todo implement vertical name of each row
    def summary_statistics(self) -> DataFrame:
        new_data = self._prepare_numeric_data()
        summary = new_data.describe()
        summary.index.name = "statistic"
        return summary

    #@todo implement vertical name of each row
    def correlation_matrix(self) -> DataFrame:
        new_data = self._prepare_numeric_data()
        correlation = new_data.corr()
        correlation.index.name = "feature"
        correlation.columns.name = "feature"

        self._plot_correlation_matrix(correlation)
        return correlation

    def _prepare_numeric_data(self) -> DataFrame:
        new_data = self.data.copy()


        new_data = new_data.drop(
            columns=[
                'date',
                'latitude',
                'longitude'
            ],
            errors='ignore'
        )
        new_data = new_data.select_dtypes(include='number')

        return new_data


    def _plot_correlation_matrix(self, correlation_data: DataFrame):
        plt.figure(figsize=(16, 12))
        sns.heatmap(
            correlation_data,
            cmap='coolwarm',
            annot=True,
            fmt=".2f",
            linewidths=0.5
        )

        plt.title("Correlation Matrix Heatmap")
        plt.tight_layout()
        plt.show()
