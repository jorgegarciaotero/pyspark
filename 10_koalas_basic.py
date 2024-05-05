import databricks.koalas as ks
import pandas as pd
import numpy as np
from spark.sql import SparkSession

s = ks.Series([1, 2, 3, 4, 5, np.nan, 7, 8, 9, 10])
kdf = ks.DataFrame({
    'a':[1, 2, 3, 4, 5, np.nan, 7, 8, 9, 10],
    'b':[100, 200, 300, 400, 500, np.nan, 700, 800, 900, 1000],
    'c':['one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten']
    },
    index=pd.date_range('20200101', periods=10)
    )

print(s)
print(kdf)


#Convert from pandas DF to koalas DF
dates=pd.date_range('20200101',periods=10)
df=pd.DataFrame(np.random.randn(10,4),index=dates,columns=list('ABCD'))
kdf=ks.from_pandas(pdf)

spark=SparkSession.builder.getOrCreate()
df=spark.createDataFrame(pdf)
sdf.show()

kdf=sdf.to_koalas()
kdf.dtypes
kdf.head()
kdf.describe()
kdf.info()
kdf.index
kdf.columns
kdf.to_numpy()
kdf.T
kdf.sort_index(ascending=False)
kdf.sort_values(by="B")
kdf.dropna(how="any")
kdf.fillna(value=0)
kdf.mean()

kdf.to_csv("test.csv") #save to csv
kdf.read_csv("test.csv") #read from csv
kdf.to_parquet("test.parquet") #save to parquet
kdf.read_parquet("test.parquet") #read from parquet
kdf.to_spark_io("test.json") #save to json
kdf.read_spark_io("test.json") #read from json
kdf.to_spark_io("test.orc") #save to orc
kdf.read_spark_io("test.orc") #read from orc



