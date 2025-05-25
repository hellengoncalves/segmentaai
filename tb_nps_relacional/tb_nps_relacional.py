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
    .config("spark.jars", "/home/hellen/spark/jars/ojdbc8.jar") \
    .getOrCreate()

# 2. ETL

df_nps_relacional = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3://segmentaai/nps_relacional.csv")
    .filter(
        (F.col('metadata_codcliente').isNotNull())
    )
    .select(
        F.col("metadata_codcliente").alias("cd_cliente"),
        F.col("respondedAt").alias("dt_resposta"),
        F.lit('relacional').alias('categoria'),
        F.col("resposta_NPS").alias("resposta_nps"),
        F.when(F.col('resposta_NPS').isNull(), 'Não Avaliado')
            .when(F.col('resposta_NPS') <= 6, 'Detrator')
            .when((F.col('resposta_NPS') >= 7) & (F.col('resposta_NPS') <= 8), 'Neutro')
            .when(F.col('resposta_NPS') >= 9, 'Promotor')
            .otherwise('Não Avaliado')
            .alias('classificacao_nps'),
        F.col("resposta_unidade").alias("resposta_unidade"),
        F.col("Nota_SupTec_Agilidade").alias("nota_suporte_agilidade"),
        F.col("Nota_SupTec_Atendimento").alias("nota_suporte_atendimento"),
        F.col("Nota_Comercial").alias("nota_comercial"),
        F.col("Nota_Custos").alias("nota_custos"),
        F.col("Nota_AdmFin_Atendimento").alias("nota_adm_financeiro_atendimento"),
        F.col("Nota_Software").alias("nota_software"),
        F.col("Nota_Software_Atualizacao").alias("nota_software_atualizacao")
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
df_nps_relacional.write \
    .jdbc(url=oracle_url, table="TB_NPS_RELACIONAL", mode="overwrite", properties=oracle_properties)

spark.stop()