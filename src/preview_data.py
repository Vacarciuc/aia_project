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
            return self._draw_scatter(columns)
        elif graph_type == GraphType.PIE:
            return self.draw_pie()
        elif graph_type == GraphType.BAR:
            return self.draw_bar()
        elif graph_type == GraphType.HISTOGRAM:
            return self._draw_histogram(columns)
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



    def _draw_scatter(self, list_columns):
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

    def _draw_histogram(self, columns):
        if len(columns) == 0 | len(columns) > 1:
            throw_error("Histogram requires exactly one column.")
        column = columns[0]
        df = pd.DataFrame(self.data)
        if column not in df.columns:
            raise ValueError(f"Coloana '{column}' nu există în date.")
        values = pd.to_numeric(df[column], errors="coerce").dropna()
        plt.figure(figsize=(10, 6))
        plt.hist(values, bins=20)
        plt.xlabel(column)
        plt.ylabel("Frecvență")
        plt.title(f"Histogramă – distribuția {column}")
        plt.grid(True, alpha=0.3)
        plt.show()
