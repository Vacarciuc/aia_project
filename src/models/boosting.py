import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import KFold, cross_validate
from src.models.utils import print_day
from src.models.init import FEATURES, X_train, X_test, y_train, y_test, RANDOM_STATE, K_FOLD_SPLITS

model = Pipeline(
   steps=[
      ('scaler', StandardScaler()),
      ('gradient_boosting', HistGradientBoostingRegressor(
         learning_rate=0.1,  # 0.01 is high perf but R2=0.51 MAE=2.22 RMSE=2.69, 0.1 R2=0.8 MAE=0.91 RMSE=1.7, 0.2 to 0.5 R2=0.8 MAE=0.91 RMSE=1.71
         max_depth=3, # initially 6, tried up to 12 but no effect
         max_iter=50,  # initially 300, reduced to 50 for faster training
         random_state=RANDOM_STATE
      ))
   ]
)

kf = KFold(n_splits=K_FOLD_SPLITS, shuffle=True, random_state=RANDOM_STATE)

print('Performing K-fold cross-validation...')
cv_results = cross_validate(
   model,
   X_train,
   y_train,
   cv=kf,
   scoring={
      "mae": "neg_mean_absolute_error",
      "rmse": "neg_root_mean_squared_error",
      "r2": "r2"
   },
)

cv_mae = -cv_results["test_mae"]
cv_rmse = -cv_results["test_rmse"]
cv_r2 = cv_results["test_r2"]

print("\nK-Fold CV (5 folds) on training set:")
print(f"MAE:  {cv_mae.mean():.4f} ± {cv_mae.std():.4f} kWh")
print(f"RMSE: {cv_rmse.mean():.4f} ± {cv_rmse.std():.4f} kWh")
print(f"R²:   {cv_r2.mean():.4f} ± {cv_r2.std():.4f}")

print('Training model...')
model.fit(X_train, y_train)

print('Evaluating model...')
y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print('Features:', FEATURES)
print(f'MAE:  {mae:.4f} kWh')
print(f'RMSE: {rmse:.4f} kWh')
print(f'R²:   {r2:.4f}')

print_day(0, model)
print_day(1, model)
print_day(2, model)
