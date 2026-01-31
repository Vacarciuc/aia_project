import matplotlib.pyplot as plt
from typing import Any
from src.models.init import df, FEATURES, TARGET

def print_day(day: int, model: Any) -> None:
   plot_data = df[FEATURES + [TARGET] + ['hour']].dropna().iloc[(24 * day):(24 * (day + 1))]

   X_day = plot_data[FEATURES]
   y_actual_day = plot_data[TARGET]
   y_pred_day = model.predict(X_day)

   plt.figure(figsize=(8, 4))
   plt.plot(plot_data['hour'], y_actual_day, label='Actual', marker='o')
   plt.plot(plot_data['hour'], y_pred_day, label='Predicted', marker='o')
   plt.xlabel('Hour of day')
   plt.ylabel('Produced Energy (kWh)')
   plt.title(f'Day {day + 1}: Actual vs Predicted Solar Energy')
   plt.legend()
   plt.grid(True)
   plt.tight_layout()
   plt.show()

