from src.graph_enum import GraphType
import pandas as pd
import matplotlib.pyplot as plt


class PreviewData:
    def __init__(self, data):
        self.data = data

    def draw(self, graph_type: GraphType, x_columns, y_columns):
        if graph_type == GraphType.LINEAR:
            return self._draw_linear(self.data, x_columns, y_columns)
        elif graph_type == GraphType.SCATTER:
            return self.draw_scatter()
        elif graph_type == GraphType.PIE:
            return self.draw_pie()
        elif graph_type == GraphType.BAR:
            return self.draw_bar()
        elif graph_type == GraphType.HISTOGRAM:
            return self.draw_histogram()
        return None

    def _draw_linear(self, data, x_columns, y_columns):
        print("hello")
        df = pd.DataFrame(data)
        plt.figure(figsize=(10, 6))
        plt.xlabel(x_columns)
        plt.ylabel(y_columns)
        plt.title(f"Linear Graph for {x_columns} and {y_columns}")
        x_col = df[x_columns]
        y_col = df[y_columns]
        plt.plot(x_col, y_col)
        plt.show()



    def draw_scatter(self):
        return self.data


    def draw_pie(self):
        return self.data

    def draw_bar(self):
        return self.data

    def draw_histogram(self):
        return self.data