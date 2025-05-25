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
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.jars", "/home/hellen/spark/jars/hadoop-aws-3.3.4.jar,/home/hellen/spark/jars/aws-java-sdk-bundle-1.12.489.jar") \
    .config("spark.jars", "/home/hellen/spark/jars/hadoop-aws-3.3.4.jar,/home/hellen/spark/jars/aws-java-sdk-bundle-1.12.489.jar,/home/hellen/spark/jars/ojdbc8.jar") \
    .getOrCreate()

# 2. ETL

df_contratos = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3a://segmentaai/dados_clientes.csv")
    .filter(F.col('CD_CLIENTE').isNotNull())
    .select(
        F.col("CD_CLIENTE").alias("cd_cliente"),
        F.coalesce(F.col("DS_PROD"), F.lit('-')).alias("produto"),
        F.coalesce(F.col("DS_LIN_REC"), F.lit('-')).alias("linha_receita"),
        F.coalesce(F.col("MARCA_TOTVS"), F.lit('-')).alias("marca_totvs"),
        F.coalesce(F.col("MODAL_COMERC"), F.lit('-')).alias("modalidade_comercial"),
        F.coalesce(F.col("PERIODICIDADE"), F.lit('-')).alias("periodicidade"),
        F.coalesce(F.col("SITUACAO_CONTRATO"), F.lit('-')).alias("situacao_contrato"),
        F.coalesce(F.col("VL_TOTAL_CONTRATO"), F.lit(0)).alias("valor_total_contrato"),
        F.coalesce(F.col("DT_ASSINATURA_CONTRATO"), F.lit('1900-01-01')).alias("data_assinatura_contrato")
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
df_contratos.write \
    .jdbc(url=oracle_url, table="TB_CONTRATOS", mode="overwrite", properties=oracle_properties)

spark.stop()