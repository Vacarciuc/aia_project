import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.svm import SVR
from src.models.utils import print_day
from sklearn.model_selection import KFold, cross_validate
from src.models.init import FEATURES, X_train, X_test, y_train, y_test, K_FOLD_SPLITS, RANDOM_STATE

model = Pipeline(
   steps=[
      ('scaler', StandardScaler()),
      ('svr', SVR(
         kernel='rbf', # rbf MAE=0.9 RMSE=1.77 R2=0.78, linear MAE=0.95 RMSE=1.78 R2=0.78, poly degree 3 MAE=1.6 RMSE=1.67 R2=0.52
         # degree=3
         C=5, # 10 MAE=0.9 RMSE=1.77 R2=0.78, 5 MAE=0.9 RMSE=1.77 R2=0.78, 15 MAE=0.9 RMSE=1.77 R2=0.78   lower faster
         epsilon=0.2, # 0.1 MAE=0.9 RMSE=1.77 R2=0.78, 0.2 MAE=0.9 RMSE=1.77 R2=0.78, 0.5 MAE=1.05 RMSE=1.778R2=0.78  higher faster
         gamma='scale'
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
