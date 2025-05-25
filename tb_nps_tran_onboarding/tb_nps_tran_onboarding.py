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

df_nps_tran_onboarding = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3://segmentaai/nps_transacional_onboarding.csv")
    .filter(
        (F.col('Cod Cliente').isNotNull())
    )
    .select(
        F.col("Cod Cliente").alias("cd_cliente"),
        F.col("Data de resposta").alias("dt_resposta"),
        F.col("Em uma escala de 0 a 10, quanto você recomenda o Onboarding da TOTVS para um amigo ou colega?.").alias("nota_recomendacao_onboarding"),
        F.col("Em uma escala de 0 a 10, o quanto você acredita que o atendimento CS Onboarding ajudou no início da sua trajetória com a TOTVS?").alias("nota_ajuda_cs_onboarding"),
        F.col("- Duração do tempo na realização da reunião de Onboarding;").alias("duracao_reuniao_onboarding"),
        F.col("- Clareza no acesso aos canais de comunicação da TOTVS;").alias("clareza_canais_comunicacao"),
        F.col("- Clareza nas informações em geral transmitidas pelo CS que lhe atendeu no Onboarding;").alias("clareza_informacoes_cs"),
        F.col("- Expectativas atendidas no Onboarding da TOTVS.").alias("expectativas_atendidas_onboarding"),
        F.when(F.col('- Expectativas atendidas no Onboarding da TOTVS.').isNull(), 'Não Avaliado')
            .when(F.col('- Expectativas atendidas no Onboarding da TOTVS.') <= 6, 'Detrator')
            .when((F.col('- Expectativas atendidas no Onboarding da TOTVS.') >= 7) & (F.col('- Expectativas atendidas no Onboarding da TOTVS.') <= 8), 'Neutro')
            .when(F.col('- Expectativas atendidas no Onboarding da TOTVS.') >= 9, 'Promotor')
            .otherwise('Não Avaliado')
            .alias('classificacao_nps')
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
df_nps_tran_onboarding.write \
    .jdbc(url=oracle_url, table="TB_NPS_TRAN_ONBOARDING", mode="overwrite", properties=oracle_properties)

spark.stop()