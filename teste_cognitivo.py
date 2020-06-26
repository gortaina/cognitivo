from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

spark = SparkSession \
            .builder \
            .appName("Teste Cognitivo.ai") \
            .getOrCreate()

# leitura dos dados
df=spark.read.csv("data/input/users/load.csv", header=True, sep=",")

# leitura dos tipos
with open('config/types_mapping.json', 'r') as S:
    new_types = json.load(S)

# 1.Deduplicação dos dados convertidos
df = df.sort("update_date",ascending=False).dropDuplicates(["id"])    

# 2.Conversão do tipo dos dados deduplicados
for key in new_types:
    df = df.withColumn(key, F.col(key).cast(new_types[key]) )
    
# 3.Conversão do formato dos arquivos    
df.write.parquet("data/output/cognitivo/2020/06/25")    