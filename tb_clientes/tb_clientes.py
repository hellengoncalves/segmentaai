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

df_base_clientes = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3://segmentaai/clientes_desde.csv")
    .filter(F.col('CLIENTE').isNotNull())
    .select(
        F.col('CLIENTE').alias('cd_cliente'),
        F.col('CLIENTE_DESDE').alias('dt_abertura')
    )
)

df_contratacoes_12m = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3://segmentaai/contratacoes_ultimos_12_meses.csv")
    .filter(F.col('CD_CLIENTE').isNotNull())
    .select(
        F.col('CD_CLIENTE').alias('cd_cliente'),
        F.col('QTD_CONTRATACOES_12M').alias('qt_contratacao_12m'),
        F.col('VLR_CONTRATACOES_12M').alias('vl_contratacao_12m')
    )
)

df_consolidado = (
    df_base_clientes.alias('a')
    .join(
        df_contratacoes_12m.alias('b'),
        ['cd_cliente'],
        'left'
    )
    .select(
        F.col('a.cd_cliente'),
        F.col('a.dt_abertura'),
        F.coalesce(F.col('b.qt_contratacao_12m'), 0).alias('qt_contratacao_12m'),
        F.coalesce(F.col('b.vl_contratacao_12m'), 0).alias('vl_contratacao_12m')
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
df_consolidado.write \
    .jdbc(url=oracle_url, table="TB_CLIENTES", mode="overwrite", properties=oracle_properties)

spark.stop()