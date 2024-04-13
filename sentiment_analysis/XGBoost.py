from xgboost import XGBRegressor
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import pickle

df = pd.read_csv("../data/SalesData/biba.csv")

# df = df.append(pd.read_csv('../data/AMZ_Scout_Data_2.csv'), ignore_index=True)
# df = df.append(pd.read_csv('../data/AMZ_Scout_Data_3.csv'))
print(df.info())
# df["Date"] = df["Date"].apply(lambda x: datetime.strptime(x, "%d-%m-%Y"))

# print(type(df["Date"][0]))
# train_df = df
# train_df = df[df["Date"] < datetime(2022, 3, 21)]
# test_df = df[df["Date"] >= datetime(2021, 2, 15)]
train_df = df.iloc[:111]
test_df = df.iloc[111:139]

model = XGBRegressor(n_estimators=100, learning_rate=0.1)
model.fit(train_df[["Price", "Rank"]], train_df["Sales"])
model.save_model("XGBoost_Model_Biba.json")
# pickle.dump(model, open('XGBoost_Model.sav', 'wb'))
# model = pickle.load(open('XGBoost_Model.sav', 'rb'))

model = XGBRegressor()
model.load_model("XGBoost_Model_Biba.json")
# test_df = pd.read_csv('../data/temp.csv')
predictions = model.predict(test_df[["Price", "Rank"]])
# for i, j in zip(predictions,  test_df['Sales']):
#     print(i, j)

from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import math

print("MSE:", mean_squared_error(test_df["Sales"], predictions))
print("RMSE:", math.sqrt(mean_squared_error(test_df["Sales"], predictions)))
print("MAE:", mean_absolute_error(test_df["Sales"], predictions))
print("R2:", r2_score(test_df["Sales"], predictions))

plt.scatter(test_df["Rank"], test_df["Sales"], label="Actual Data")
plt.scatter(test_df["Rank"], predictions, marker="x", label="Estimated Data")
plt.xlabel("BSR Rank")
plt.ylabel("Estimated Sales")
plt.title("BSR-Sales Correlation")
plt.legend()
plt.show()
