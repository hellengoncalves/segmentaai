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

df_nps_tran_aquisicao = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3a://segmentaai/nps_transacional_aquisicao.csv")
    .withColumnRenamed("C�d. Cliente", "Cód. Cliente")
    .filter(
        (F.col('Cód. Cliente').isNotNull())
    )
    .select(
        F.col("Cód. Cliente").alias("cd_cliente"),
        F.col("Data da Resposta").alias("dt_resposta"),
        F.col("Nota NPS").alias("nota_nps"),
        F.when(F.col('Nota NPS').isNull(), 'Não Avaliado')
            .when(F.col('Nota NPS') <= 6, 'Detrator')
            .when((F.col('Nota NPS') >= 7) & (F.col('Nota NPS') <= 8), 'Neutro')
            .when(F.col('Nota NPS') >= 9, 'Promotor')
            .otherwise('Não Avaliado')
            .alias('classificacao_nps'),
        F.col("Nota Agilidade").alias("nota_agilidade"),
        F.col("Nota Conhecimento").alias("nota_conhecimento"),
        F.col("Nota Custo").alias("nota_custo"),
        F.col("Nota Facilidade").alias("nota_facilidade"),
        F.col("Nota Flexibilidade").alias("nota_flexibilidade")
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
df_nps_tran_aquisicao.write \
    .jdbc(url=oracle_url, table="TB_NPS_TRAN_AQUISICAO", mode="overwrite", properties=oracle_properties)

spark.stop()