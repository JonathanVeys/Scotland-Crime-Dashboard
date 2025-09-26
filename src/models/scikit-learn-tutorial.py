from sklearn.datasets import load_iris
from sklearn.neighbors import KNeighborsRegressor
from sklearn.linear_model import LinearRegression

import matplotlib.pyplot as plt

X, y = load_iris(return_X_y=True)
mod = LinearRegression()
mod.fit(X,y)

predictions = mod.predict(X)
predictions_comparison = zip(predictions, y)

plt.scatter(y, predictions)
plt.show()
