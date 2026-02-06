import matplotlib.pyplot as plt
from typing import Any, Tuple
from src.models.init import df, FEATURES, TARGET

def print_day(day: int, model: Any, model_name: str) -> None:
   plot_data = df[FEATURES + [TARGET] + ['hour']].dropna().iloc[(24 * day):(24 * (day + 1))]

   X_day = plot_data[FEATURES]
   y_actual_day = plot_data[TARGET]
   y_pred_day = model.predict(X_day)

   plt.figure(figsize=(8, 4))
   plt.plot(plot_data['hour'], y_actual_day, label='Actual', marker='o')
   plt.plot(plot_data['hour'], y_pred_day, label='Predicted', marker='o')
   plt.xlabel('Hour of day')
   plt.ylabel('Produced Energy (kWh)')
   plt.title(f'Day {day + 1}: Actual vs Predicted ({model_name}) Solar Energy')
   plt.legend()
   plt.grid(True)
   plt.tight_layout()
   plt.show()


from typing import Tuple, Any
import matplotlib.pyplot as plt


def print_days(days_from_to: Tuple[int, int], model: Any, model_name: str) -> None:
   day_from, day_to = days_from_to
   plot_data = df[FEATURES + [TARGET]].dropna().iloc[(24 * day_from):(24 * (day_to + 1))]

   X_day = plot_data[FEATURES]
   y_actual_day = plot_data[TARGET]
   y_pred_day = model.predict(X_day)

   total_hours = range(len(plot_data))

   plt.figure(figsize=(12, 5))
   plt.plot(total_hours, y_actual_day, label='Actual', marker='o')
   plt.plot(total_hours, y_pred_day, label='Predicted', marker='o')

   plt.xlabel(f'Hours elapsed (Starting from Day {day_from + 1})')
   plt.ylabel('Produced Energy (kWh)')
   plt.title(f'Days {day_from + 1} to {day_to + 1}: Actual vs Predicted ({model_name})')

   plt.legend()
   plt.grid(True)
   plt.tight_layout()
   plt.show()
