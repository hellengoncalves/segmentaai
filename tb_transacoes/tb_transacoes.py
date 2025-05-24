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

df_transacoes = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3://segmentaai/historico.csv")
    .filter(
        (F.col('NR_PROPOSTA').isNotNull())
    )
    .select(
        F.col("NR_PROPOSTA").alias("id_proposta"),
        F.col("ITEM_PROPOSTA").alias("id_item_proposta"),
        F.col("DT_UPLOAD").alias("dt_upload"),
        F.col("HOSPEDAGEM").alias("ds_hospedagem"),
        F.col("CD_CLI").alias("cd_cliente"),
        F.col("CD_PROD").alias("cd_prod"),
        F.coalesce(F.col("QTD"), F.lit(0)).alias("qt_solicitada"),
        F.coalesce(F.col("MESES_BONIF"), F.lit(0)).alias("meses_bonificacao"),
        F.coalesce(F.col("VL_PCT_DESC_TEMP"), F.lit(0)).alias("vl_pct_desc_temp"),
        F.coalesce(F.col("VL_PCT_DESCONTO"), F.lit(0)).alias("vl_pct_desc"),
        F.coalesce(F.col("PRC_UNITARIO"), F.lit(0)).alias("vl_preco_unitario"),
        F.coalesce(F.col("VL_DESCONTO_TEMPORARIO"), F.lit(0)).alias("vl_desc_temp"),
        F.coalesce(F.col("VL_TOTAL"), F.lit(0)).alias("vl_total"),
        F.coalesce(F.col("VL_FULL"), F.lit(0)).alias("vl_cheio"),
        F.coalesce(F.col("VL_DESCONTO"), F.lit(0)).alias("vl_desconto")
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
df_transacoes.write \
    .jdbc(url=oracle_url, table="TB_TRANSACOES", mode="overwrite", properties=oracle_properties)

spark.stop()