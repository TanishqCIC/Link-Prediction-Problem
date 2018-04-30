# Multiple Linear Regression

# Importing the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


# Importing the dataset
dataset = pd.read_csv('F_Table2.csv')
X = dataset.iloc[:, :-1].values
y = dataset.iloc[:, 13].values


print(X[:30])
print(y[:30])

from sklearn.preprocessing import OneHotEncoder
onehotencoder = OneHotEncoder(categorical_features = [0, 1])
X = onehotencoder.fit_transform(X).toarray()

# Splitting the dataset into the Training set and Test set
from sklearn.cross_validation import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)

print('modelling')
from sklearn.linear_model import LinearRegression
regressor = LinearRegression()
regressor.fit(X_train, y_train)

y_pred = regressor.predict(X_test)
y_pred_new = []
for value in y_pred:
    if value > 0:
        value = 1
        y_pred_new.append(value)
    else:
        y_pred_new.append(-1)

from sklearn.metrics import accuracy_score
print(accuracy_score(y_test, y_pred_new))

plt.scatter(X_train, y_train, color = 'red')
fig = plt.plot(X_train, regressor.predict(X_train), color = 'blue')
fig.savefig('pllot.png')