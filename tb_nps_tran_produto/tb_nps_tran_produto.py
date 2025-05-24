from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

df_nps_tran_produto = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3://segmentaai/nps_transacional_produto.csv")
    .filter(
        (F.col('Cód. T').isNotNull())
    )
    .select(
        F.col("Cód. T").alias("cd_cliente"),
        F.col("Data da Resposta").alias("dt_resposta"),
        F.col("Linha de Produto").alias("ds_linha_produto"),
        F.col("Nome do Produto").alias("nm_produto"),
        F.col("Nota").alias("nota"),
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
df_nps_tran_produto.write \
    .jdbc(url=oracle_url, table="TB_NPS_TRAN_PRODUTO", mode="overwrite", properties=oracle_properties)

spark.stop()