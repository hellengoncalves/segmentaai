from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from airflow.hooks.base import BaseHook

# 1. Sessão Spark

aws_conn = BaseHook.get_connection("aws_default")
AWS_ACCESS_KEY = aws_conn.login
AWS_SECRET_KEY = aws_conn.password

spark = SparkSession.builder \
    .appName("ETL") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.jars", "/caminho/para/ojdbc8.jar") \
    .getOrCreate()

# 2. ETL

df1 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_1.csv")
df2 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_2.csv")
df3 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_3.csv")
df4 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_4.csv")
df5 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_5.csv")
df6 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_6.csv")
df7 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_7.csv")
df8 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_8.csv")
df9 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_9.csv")
df10 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_10.csv")
df11 = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").csv("s3://segmentaai/telemetria_11.csv")

df_uniao = (
    df1
    .unionAll(df2)
    .unionAll(df3)
    .unionAll(df4)
    .unionAll(df5)
    .unionAll(df6)
    .unionAll(df7)
    .unionAll(df8)
    .unionAll(df9)
    .unionAll(df10)
    .unionAll(df11)
)

df_telemetria = (
    df_uniao
    .filter(F.col('clienteid').isNotNull())
    .select(
        F.col("clienteid").alias("cd_cliente"),
        F.col("eventduration").alias("duracao_evento"),
        F.col("moduloid").alias("id_modulo"),
        F.col("productlineid").alias("id_linha_produto"),
        F.col("referencedatestart").alias("dt_referencia_inicio"),
        F.col("slotid").alias("id_slot"),
        F.col("statuslicenca").alias("status_licenca"),
        F.col("tcloud").alias("tcloud"),
        F.col("clienteprime").alias("cliente_prime")
    )
)

# 3. Conexão Oracle
oracle_conn = BaseHook.get_connection("oracle_conn")

oracle_url = f"jdbc:oracle:thin:@//{oracle_conn.host}:{oracle_conn.port}/{oracle_conn.schema}"
oracle_properties = {
    "user": oracle_conn.login,
    "password": oracle_conn.password,
    "driver": "oracle.jdbc.OracleDriver"
}

# 4. Escrita Final
df_telemetria.write \
    .jdbc(url=oracle_url, table="TB_TELEMETRIA", mode="overwrite", properties=oracle_properties)

spark.stop()