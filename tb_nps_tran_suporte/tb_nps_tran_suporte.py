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

df_nps_tran_suporte = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3a://segmentaai/nps_transacional_suporte.csv")
    .filter(
        (F.col('cliente').isNotNull())
    )
    .select(
        F.col("cliente").alias("cd_cliente"),
        F.col("ticket").alias("ticket"),
        F.col("resposta_NPS").alias("resposta_nps"),
        F.when(F.col('resposta_NPS').isNull(), 'Não Avaliado')
            .when(F.col('resposta_NPS') <= 6, 'Detrator')
            .when((F.col('resposta_NPS') >= 7) & (F.col('resposta_NPS') <= 8), 'Neutro')
            .when(F.col('resposta_NPS') >= 9, 'Promotor')
            .otherwise('Não Avaliado')
            .alias('classificacao_nps'),
        F.col("grupo_NPS").alias("grupo_nps"),
        F.col("Nota_ConhecimentoAgente").alias("nota_conhecimento_agente"),
        F.col("Nota_Solucao").alias("nota_solucao"),
        F.col("Nota_TempoRetorno").alias("nota_tempo_retorno"),
        F.col("Nota_Facilidade").alias("nota_facilidade"),
        F.col("Nota_Satisfacao").alias("nota_satisfacao")
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
df_nps_tran_suporte.write \
    .jdbc(url=oracle_url, table="TB_NPS_TRAN_SUPORTE", mode="overwrite", properties=oracle_properties)

spark.stop()