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
    .config("spark.jars", "/home/hellen/spark/jars/ojdbc8.jar") \
    .getOrCreate()

# 2. ETL

df_tickets = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3://segmentaai/tickets.csv")
    .filter(F.col('CODIGO_ORGANIZACAO').isNotNull())
    .select(
        F.col("CODIGO_ORGANIZACAO").alias("cd_cliente"),
        F.col("NOME_GRUPO").alias("nm_grupo"),
        F.col("TIPO_TICKET").alias("ds_ticket"),
        F.col("STATUS_TICKET").alias("st_ticket"),
        F.col("DT_CRIACAO").alias("dt_criacao"),
        F.col("DT_ATUALIZACAO").alias("dt_atualizacao"),
        F.col("BK_TICKET").alias("bk_ticket"),
        F.col("PRIORIDADE_TICKET").alias("ds_prio_ticket")
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
df_tickets.write \
    .jdbc(url=oracle_url, table="TB_TICKET", mode="overwrite", properties=oracle_properties)

spark.stop()