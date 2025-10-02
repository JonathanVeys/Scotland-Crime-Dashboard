from dotenv import load_dotenv 
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import joblib
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split


# -----------------------------
# Load environment and connect to DB
# -----------------------------
load_dotenv()
DB_URL = os.getenv("SUPABASE_DB_URL")
if DB_URL is None:
    raise ValueError("SUPABASE_DB_URL not found in environment variables.")

engine = create_engine(DB_URL)

# -----------------------------
# Query data
# -----------------------------
query = """
SELECT *
FROM ward_crime wc
JOIN ward_education_data we
ON wc.ward_code = we.ward_code AND wc.date = we.date
JOIN ward_employemnt_data wed
ON wc.ward_code = wed.ward_code AND wc.date = wed.date
JOIN ward_population_density wd
ON wc.ward_code = wd.ward_code AND wc.date = wd.date
JOIN ward_code_name wcn
ON wc.ward_code = wcn.ward_code;
"""

df = pd.read_sql(query, engine)

# Remove duplicate columns
df = df.loc[:, ~df.columns.duplicated()]

# Ensure date is datetime
df['date'] = pd.to_datetime(df['date'])

# -----------------------------
# Sort and create lag features
# -----------------------------
df = df.sort_values(['ward_code', 'date'])

# Lag features
df['crime_last_month'] = df.groupby('ward_code')['count'].shift(1)
df['crime_last_two_months'] = df.groupby('ward_code')['count'].shift(2)
df['crime_last_three_months'] = df.groupby('ward_code')['count'].shift(3)

# # Rolling average (optional)
df['crime_3month_avg'] = (
    df.groupby('ward_code')['count']
      .transform(lambda x: x.shift(1).rolling(3).mean())
)
# # Drop rows with missing lag features
df = df.dropna(subset=['crime_last_three_months']).reset_index(drop=True)

# -----------------------------
# Feature engineering
# -----------------------------
# Extract month for seasonality
# df['month'] = df['date'].dt.month
# df = pd.get_dummies(df, columns=['month'], drop_first=True)

# Log transform skewed numeric features
df['pop_density_log'] = np.log1p(df['population_density'])

# Define features and target
exclude_cols = ['ward_name', 'count', 'ward_code', 'date', 'population_density']  # drop raw pop_density
feature_cols = [col for col in df.columns if col not in exclude_cols]
X = df[feature_cols]
y = df['count']  # or np.log1p(df['count']) for skewed target


# -----------------------------
# Chronological train/test split
# -----------------------------

# chronological split, no leakage
train, test = train_test_split(df, test_size=0.1, shuffle=False)

X_train = train[feature_cols]
X_test = test[feature_cols]

y_train = (train['count'])
y_test = (test['count'])

# keep IDs for evaluation
ward_codes_test = test['ward_code']
dates_test = test['date']


# -----------------------------
# Fit linear regression
# -----------------------------

# model = LinearRegression()
# model.fit(X_train, y_train)
# predictions = model.predict(X_test)

rf = RandomForestRegressor(
    n_estimators=500,   # number of trees
    max_depth=None,     # let trees grow until leaves are pure
    random_state=42,    # reproducibility
    n_jobs=-1           # use all cores for speed
)
rf.fit(X_train, y_train)
predictions = rf.predict(X_test)
print(X_test.columns)


# -----------------------------
# Evaluate performance
# -----------------------------
mse = mean_squared_error(y_test, predictions)
rmse = np.sqrt(mse)
print(f"MSE: {mse:.2f}, RMSE: {rmse:.2f}")

# -----------------------------
# Output predictions with ward codes and dates
# -----------------------------
results = pd.DataFrame({
    'ward_code': ward_codes_test.values,
    'date': dates_test.values,
    'predicted_crime': predictions,
    'actual_crime': y_test.values
})


counter = 0
fig, axes = plt.subplots(nrows=2, ncols=2)
axes = axes.flatten()
for ax,(ward_code,grouped_data) in zip(axes, results.groupby(['ward_code'])):
    print(f'Ward code: {ward_code} | MSE: {mean_squared_error(grouped_data["actual_crime"], grouped_data["predicted_crime"])}')
    grouped_data = grouped_data.sort_values(by='date', ascending=True)
    ax.plot(grouped_data['date'], grouped_data['predicted_crime'], label='predicted crime')
    ax.plot(grouped_data['date'], grouped_data['actual_crime'], label='actual crime')
    ax.grid(axis='y')
    ax.legend()
    counter += 1
    if counter == 4:
        break
plt.show()


joblib.dump(rf,'/Users/jonathancrocker/Documents/Python/Scotland Crime Dashboard/src/models/linear_model.pkl')

