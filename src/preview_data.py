from numpy.f2py.auxfuncs import throw_error

from src.graph_enum import GraphType
import pandas as pd
import matplotlib.pyplot as plt


class PreviewData:
    def __init__(self, data):
        self.data = data

    def draw(self, graph_type: GraphType, columns: list[str]):
        if graph_type == GraphType.LINEAR:
            return self._draw_linear(columns)
        elif graph_type == GraphType.SCATTER:
            return self.draw_scatter(columns)
        elif graph_type == GraphType.PIE:
            return self.draw_pie()
        elif graph_type == GraphType.BAR:
            return self.draw_bar()
        elif graph_type == GraphType.HISTOGRAM:
            return self.draw_histogram(columns)
        return None

    def _draw_linear(self, list_columns):
        if len(list_columns) == 0 & len(list_columns) > 2:
            throw_error("Linear graph requires exactly two columns.")
        df = pd.DataFrame(self.data)
        plt.figure(figsize=(10, 6))
        x_columns = list_columns[0]
        y_columns = list_columns[1]
        plt.xlabel(x_columns)
        plt.ylabel(y_columns)
        plt.title(f"Linear Graph for {x_columns} and {y_columns}")
        x_col = df[x_columns]
        y_col = df[y_columns]
        plt.plot(x_col, y_col)
        plt.show()



    def draw_scatter(self, list_columns):
        if len(list_columns) == 0 & len(list_columns) > 2:
            throw_error("Scatter plot requires exactly two columns.")
        x_columns = list_columns[0]
        y_columns = list_columns[1]
        df = pd.DataFrame(self.data)
        plt.figure(figsize=(10, 6))
        plt.xlabel(x_columns)
        plt.ylabel(y_columns)
        plt.title(f"Scatter Plot for {x_columns} and {y_columns}")
        x_col = df[x_columns]
        y_col = df[y_columns]
        plt.scatter(x_col, y_col)
        plt.show()


    def draw_pie(self):
        return self.data

    def draw_bar(self):
        return self.data

    def draw_histogram(self, columns):
        if "date" not in columns:
            throw_error("Histogram requires a date column.")

        date_column = columns["date"]
        df = pd.DataFrame(self.data)
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
        df["year"] = df[date_column].dt.year
        count_by_year = df.groupby("year").size()
        plt.figure(figsize=(10, 6))
        plt.bar(count_by_year.index, count_by_year.values)
        plt.xlabel("An")
        plt.ylabel("Număr de înregistrări")
        plt.title("Histogramă – număr de înregistrări pe an")
        plt.show()