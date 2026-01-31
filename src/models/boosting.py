import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import HistGradientBoostingRegressor

from src.models.utils import print_day
from src.models.init import FEATURES, X_train, X_test, y_train, y_test

model = Pipeline(
   steps=[
      ('scaler', StandardScaler()),
      ('gradient_boosting', HistGradientBoostingRegressor(
         learning_rate=0.05,
         max_depth=6,
         max_iter=300,
         random_state=1
      ))
   ]
)

print('Training model...')
model.fit(X_train, y_train)

print('Evaluating model...')
y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print('Features:', FEATURES)
print(f'MAE:  {mae:.4f} kWh')
print(f'RISE: {rmse:.4f} kWh')
print(f'R²:   {r2:.4f}')

print_day(0, model)
print_day(1, model)
print_day(2, model)
