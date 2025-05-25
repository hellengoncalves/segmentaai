from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from airflow.hooks.base import BaseHook
from pyspark.sql.window import Window

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

df_base_clientes = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3a://segmentaai/clientes_desde.csv")
    .filter(F.col('CLIENTE').isNotNull())
    .select(
        F.col('CLIENTE').alias('cd_cliente'),
        F.col('CLIENTE_DESDE').alias('dt_abertura')
    ).distinct()
)

df_contratacoes_12m = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3a://segmentaai/contratacoes_ultimos_12_meses.csv")
    .filter(F.col('CD_CLIENTE').isNotNull())
    .select(
        F.col('CD_CLIENTE').alias('cd_cliente'),
        F.col('QTD_CONTRATACOES_12M').alias('qt_contratacao_12m'),
        F.col('VLR_CONTRATACOES_12M').alias('vl_contratacao_12m')
    ).distinct()
)

window_spec = Window.partitionBy("CD_CLIENTE").orderBy(F.col("FAT_FAIXA").desc())

df_dados_gerais = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3a://segmentaai/dados_clientes.csv")
    .filter(
        (F.col('CD_CLIENTE').isNotNull()) &
        (F.col('CIDADE').isNotNull()) &
        (F.col('CD_CLIENTE') != 'TDCG0P')
    )
    .select(
        F.col("CD_CLIENTE").alias("cd_cliente"),
        F.coalesce(F.col("CIDADE"), F.lit('-')).alias("nm_cidade"),
        F.coalesce(F.col("DS_CNAE"), F.lit('-')).alias("ds_cnae"),
        F.coalesce(F.col("DS_SEGMENTO"), F.lit('-')).alias("ds_segmento"),
        F.coalesce(F.col("DS_SUBSEGMENTO"), F.lit('-')).alias("ds_subsegmento"),
        F.coalesce(F.col("FAT_FAIXA"), F.lit('-')).alias("vl_faixa_faturamento"),
        F.coalesce(F.col("PAIS"), F.lit('-')).alias("nm_pais"),
        F.coalesce(F.col("UF"), F.lit('-')).alias("nm_uf"),
        F.row_number().over(window_spec).alias('rn')
    ).distinct()
)

df_dados_gerais_filtro = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3a://segmentaai/dados_clientes.csv")
    .filter(
        (F.col('CD_CLIENTE').isNotNull()) &
        (F.col('CIDADE').isNotNull()) &
        (F.col('CD_CLIENTE') == 'TDCG0P')
    )
    .select(
        F.col("CD_CLIENTE").alias("cd_cliente"),
        F.coalesce(F.col("CIDADE"), F.lit('-')).alias("nm_cidade"),
        F.coalesce(F.col("DS_CNAE"), F.lit('-')).alias("ds_cnae"),
        F.coalesce(F.col("DS_SEGMENTO"), F.lit('-')).alias("ds_segmento"),
        F.coalesce(F.col("DS_SUBSEGMENTO"), F.lit('-')).alias("ds_subsegmento"),
        F.coalesce(F.col("FAT_FAIXA"), F.lit('-')).alias("vl_faixa_faturamento"),
        F.coalesce(F.col("PAIS"), F.lit('-')).alias("nm_pais"),
        F.coalesce(F.col("UF"), F.lit('-')).alias("nm_uf"),
        F.row_number().over(window_spec).alias('rn')
    ).distinct()
)

df_dados_completos = (df_dados_gerais.unionAll(df_dados_gerais_filtro).select(F.col('*')).distinct())

df_mrr = (
    spark.read
    .option("header", "true") 
    .option("delimiter", ";") 
    .option("inferSchema", "true") 
    .option("encoding", "UTF-8")
    .csv("s3a://segmentaai/mrr.csv")
    .filter(
        (F.col('CLIENTE').isNotNull())
    )
    .select(
        F.col('CLIENTE').alias("cd_cliente"),
        F.col('MRR_12M').alias('vl_mrr_12m')
    ).distinct() 
)

df_consolidado = (
    df_base_clientes.alias('a')
    .join(
        df_contratacoes_12m.alias('b'),
        ['cd_cliente'],
        'left'
    )
    .join(
        df_dados_completos.alias('c'),
        (F.col('a.cd_cliente') == F.col('c.cd_cliente')) & (F.col('c.rn') == 1),
        'left'
    )
    .join(
        df_mrr.alias('d'),
        ['cd_cliente'],
        'left'
    )
    .filter(F.col('c.rn') == 1)
    .select(
        F.col('a.cd_cliente'),
        F.col('a.dt_abertura'),
        F.coalesce(F.col('b.qt_contratacao_12m'), F.lit(0)).alias('qt_contratacao_12m'),
        F.coalesce(F.col('b.vl_contratacao_12m'), F.lit(0)).alias('vl_contratacao_12m'),
        F.coalesce(F.col("c.nm_cidade"), F.lit('-')).alias("nm_cidade"),
        F.coalesce(F.col("c.ds_cnae"), F.lit('-')).alias("ds_cnae"),
        F.coalesce(F.col("c.ds_segmento"), F.lit('-')).alias("ds_segmento"),
        F.coalesce(F.col("c.ds_subsegmento"), F.lit('-')).alias("ds_subsegmento"),
        F.coalesce(F.col("c.vl_faixa_faturamento"), F.lit('-')).alias("vl_faixa_faturamento"),
        F.coalesce(F.col("c.nm_pais"), F.lit('-')).alias("id_pais"),
        F.coalesce(F.col("c.nm_uf"), F.lit('-')).alias("nm_uf"),
        F.coalesce(F.col('d.vl_mrr_12m'), F.lit(0)).alias('vl_mrr_12m')
    ).distinct()
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
df_consolidado.write.jdbc(
    url=oracle_url,
    table="TB_CLIENTES",
    mode="overwrite",
    properties=oracle_properties,
    truncate=True
)

spark.stop()