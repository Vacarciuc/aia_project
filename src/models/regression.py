import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import KFold, cross_validate
from src.models.utils import print_day, print_days
from src.models.init import FEATURES, X_train, X_test, y_train, y_test, RANDOM_STATE, K_FOLD_SPLITS

MODEL_NAME = 'Ridge Linear Regression'

model = Pipeline(
   steps=[
      ("scaler", StandardScaler()),
      ("ridge", Ridge(
         alpha=1,
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

print("\nTraining model...")
model.fit(X_train, y_train)

print("Evaluating model...")
y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print("Features:", FEATURES)
print(f"MAE:  {mae:.4f} kWh")
print(f"RMSE: {rmse:.4f} kWh")
print(f"R²:   {r2:.4f}")

coef = model.named_steps["ridge"].coef_
intercept = model.named_steps["ridge"].intercept_

coef_df = pd.DataFrame({"feature": FEATURES, "coef": coef}).sort_values("coef", ascending=False)
print("\nIntercept:", intercept)
print("\nCoefficients (standardized features):")
print(coef_df.to_string(index=False))

print_days((9, 13), model, MODEL_NAME)

# print_day(0, model, MODEL_NAME)
# print_day(1, model, MODEL_NAME)
# print_day(2, model, MODEL_NAME)
