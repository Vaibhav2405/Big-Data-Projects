# please run the following code on google colab
import pandas as pd

uberData = pd.read_csv("uber_data.csv")
#print(uberData.head())

uberData['tpep_pickup_datetime'] = pd.to_datetime(uberData['tpep_pickup_datetime'])
uberData['tpep_dropoff_datetime'] = pd.to_datetime(uberData['tpep_dropoff_datetime'])
#print(uberData.info())

datetime_dim = uberData.drop_duplicates(['tpep_pickup_datetime', 'tpep_dropoff_datetime']).reset_index(drop=True)
datetime_dim['pickup_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
datetime_dim['pickup_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
datetime_dim['pickup_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
datetime_dim['pickup_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
datetime_dim['pickup_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

print(datetime_dim.info())
print(uberData.info())