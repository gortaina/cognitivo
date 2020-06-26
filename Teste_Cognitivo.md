# Requisitos
1. Conversão do formato dos arquivos: Converter o arquivo CSV presente no diretório data/input/users/load.csv, 
para um formato colunar de alta performance de leitura de sua escolha. Justificar brevemente a escolha do formato;

2. Deduplicação dos dados convertidos: No conjunto de dados convertidos haverão múltiplas entradas para um mesmo registro, 
variando apenas os valores de alguns dos campos entre elas. Será necessário realizar um processo de deduplicação destes dados,
 a fim de apenas manter a última entrada de cada registro, usando como referência o id para identificação dos registros duplicados
 e a data de atualização (update_date) para definição do registro mais recente;

3. Conversão do tipo dos dados deduplicados: No diretório config haverá um arquivo JSON de configuração (types_mapping.json),
 contendo os nomes dos campos e os respectivos tipos desejados de output. 
 Utilizando esse arquivo como input, realizar um processo de conversão dos tipos dos campos descritos, no conjunto de dados deduplicados;

# Notas gerais
- Todas as operações devem ser realizadas utilizando Spark. 
O serviço de execução fica a seu critério, podendo utilizar tanto serviços locais como serviços em cloud. 
Justificar brevemente o serviço escolhido (EMR, Glue, Zeppelin, etc.).

- Cada operação deve ser realizada no dataframe resultante do passo anterior, 
podendo ser persistido e carregado em diferentes conjuntos de arquivos após cada etapa ou executados em memória e apenas persistido após operação final.

- Você tem liberdade p/ seguir a sequência de execução desejada;

- Solicitamos a transformação de tipos de dados apenas de alguns campos. Os outros ficam a seu critério

- O arquivo ou o conjunto de arquivos finais devem ser compactados e enviados por e-mail.


# Solução:

Foi feita em modo local utilizado o Spark 3.0.0 e o Jupyter Notebook, ambos do Miniconda que só tem os pacotes necessários.
Há junto com esse notebook, o arquivo **teste_cognitivo.py** para ser submetido como job no Spark com o comando:
>$SPARK_HOME/bin/spark-submit --master local[2] teste_cognitivo.py


Como dito nas notas gerais sobre a execução desejada, sigo a sequência abaixo:
    
1. Deduplicação dos dados convertidos

2. Conversão do tipo dos dados deduplicados

3. Conversão do formato dos arquivos - escolhi o Parquet por sua eficiência de leitura e armazenamento. Seu formatos de arquivos consistem em grupos de linhas, cabeçalho e rodapé e, em cada grupo de linhas, os dados nas mesmas colunas são armazenados juntos, falando nisso o armazendamento de dados no HDFS tem uma replicação de no mínimo 3 copias fora os custos de processamento, I/O, rede, por ter uma boa taxa de compressão e I/O, ele reduz e muito os custos.

4. Leitura do arquivo pós processado - incluí para efeitos de teste


```python
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
```


```python
spark = SparkSession \
            .builder \
            .appName("Teste Cognitivo.ai") \
            .getOrCreate()
```


```python
df=spark.read.csv("data/input/users/load.csv", header=True, sep=",")
```


```python
with open('config/types_mapping.json', 'r') as S:
    new_types = json.load(S)
```


```python
df.printSchema()
```

    root
     |-- id: string (nullable = true)
     |-- name: string (nullable = true)
     |-- email: string (nullable = true)
     |-- phone: string (nullable = true)
     |-- address: string (nullable = true)
     |-- age: string (nullable = true)
     |-- create_date: string (nullable = true)
     |-- update_date: string (nullable = true)
    



```python
#Quantidade de registros
df.count()
```




    6




```python
#Mostrando até 10º linha se houver
df.show(n=10)
```

    +---+--------------------+--------------------+---------------+--------------------+---+--------------------+--------------------+
    | id|                name|               email|          phone|             address|age|         create_date|         update_date|
    +---+--------------------+--------------------+---------------+--------------------+---+--------------------+--------------------+
    |  1|david.lynch@cogni...|         David Lynch|(11) 99999-9997|Mulholland Drive,...| 72|2018-03-03 18:47:...|2018-03-03 18:47:...|
    |  1|david.lynch@cogni...|         David Lynch|(11) 99999-9998|Mulholland Drive,...| 72|2018-03-03 18:47:...|2018-04-14 17:09:...|
    |  2|sherlock.holmes@c...|     Sherlock Holmes|(11) 94815-1623|221B Baker Street...| 34|2018-04-21 20:21:...|2018-04-21 20:21:...|
    |  3|spongebob.squarep...|Spongebob Squarep...|(11) 91234-5678|124 Conch Street,...| 13|2018-05-19 04:07:...|2018-05-19 04:07:...|
    |  1|david.lynch@cogni...|         David Lynch|(11) 99999-9999|Mulholland Drive,...| 72|2018-03-03 18:47:...|2018-05-23 10:13:...|
    |  3|spongebob.squarep...|Spongebob Squarep...|(11) 98765-4321|122 Conch Street,...| 13|2018-05-19 04:07:...|2018-05-19 05:08:...|
    +---+--------------------+--------------------+---------------+--------------------+---+--------------------+--------------------+
    


### 1. Deduplicação dos dados convertidos


```python
df = df.sort("update_date",ascending=False).dropDuplicates(["id"])
```


```python
#Mostrando até 10º linha se houver e sem truncar os dados
df.show(n=10, truncate=False)
```

    +---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
    |id |name                              |email                |phone          |address                                       |age|create_date               |update_date               |
    +---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
    |3  |spongebob.squarepants@cognitivo.ai|Spongebob Squarepants|(11) 98765-4321|122 Conch Street, Bikini Bottom, Pacific Ocean|13 |2018-05-19 04:07:06.854752|2018-05-19 05:08:07.964752|
    |1  |david.lynch@cognitivo.ai          |David Lynch          |(11) 99999-9999|Mulholland Drive, Los Angeles, CA, US         |72 |2018-03-03 18:47:01.954752|2018-05-23 10:13:59.594752|
    |2  |sherlock.holmes@cognitivo.ai      |Sherlock Holmes      |(11) 94815-1623|221B Baker Street, London, UK                 |34 |2018-04-21 20:21:24.364752|2018-04-21 20:21:24.364752|
    +---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
    


### 2. Conversão do tipo dos dados deduplicados

 Substituindo o tipo de dado das colunas segundo arquivo types_mapping.json


```python
for key in new_types:
    df = df.withColumn(key, F.col(key).cast(new_types[key]) )
```

### 3. Conversão do formato dos arquivos
Escolhi o Parquet por sua eficiência de leitura e armazenamento. Seu formatos de arquivos consistem em grupos de linhas, cabeçalho e rodapé e, em cada grupo de linhas, os dados nas mesmas colunas são armazenados juntos, falando nisso o armazendamento de dados no HDFS tem uma replicação de no mínimo 3 copias fora os custos de processamento, I/O, rede, por ter uma boa taxa de compressão e I/O, ele reduz e muito os custos.


```python
df.write.parquet("data/output/cognitivo/2020/06/25")
```

### 4. Leitura do arquivo pós processado


```python
dfp=spark.read.parquet("data/output/cognitivo/2020/06/25")
```


```python
dfp.printSchema()
```

    root
     |-- id: string (nullable = true)
     |-- name: string (nullable = true)
     |-- email: string (nullable = true)
     |-- phone: string (nullable = true)
     |-- address: string (nullable = true)
     |-- age: integer (nullable = true)
     |-- create_date: timestamp (nullable = true)
     |-- update_date: timestamp (nullable = true)
    



```python
#Quantidade de registros
dfp.count()
```




    3




```python
dfp.show(n=10, truncate=False)
```

    +---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
    |id |name                              |email                |phone          |address                                       |age|create_date               |update_date               |
    +---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
    |3  |spongebob.squarepants@cognitivo.ai|Spongebob Squarepants|(11) 98765-4321|122 Conch Street, Bikini Bottom, Pacific Ocean|13 |2018-05-19 04:07:06.854752|2018-05-19 05:08:07.964752|
    |2  |sherlock.holmes@cognitivo.ai      |Sherlock Holmes      |(11) 94815-1623|221B Baker Street, London, UK                 |34 |2018-04-21 20:21:24.364752|2018-04-21 20:21:24.364752|
    |1  |david.lynch@cognitivo.ai          |David Lynch          |(11) 99999-9999|Mulholland Drive, Los Angeles, CA, US         |72 |2018-03-03 18:47:01.954752|2018-05-23 10:13:59.594752|
    +---+----------------------------------+---------------------+---------------+----------------------------------------------+---+--------------------------+--------------------------+
    

