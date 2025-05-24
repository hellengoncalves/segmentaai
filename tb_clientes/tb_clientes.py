
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Criar sessão Spark com suporte a S3 e Excel
spark = SparkSession.builder \
    .appName("ExcelToTable") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
    .enableHiveSupport() \
    .getOrCreate()

# 2. Caminho do arquivo Excel no S3
excel_path = "s3a://nome-do-bucket/caminho/arquivo.xlsx"

# 3. Leitura do arquivo Excel
df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("dataAddress", "'Sheet1'!A1") \
    .option("useHeader", "true") \
    .option("inferSchema", "true") \
    .option("addColorColumns", "false") \
    .load(excel_path)

# 4. Transformações com functions as F
df_transformed = df.withColumn("nova_coluna", F.upper(F.col("nome_coluna_existente"))) \
                   .filter(F.col("outra_coluna") > 100)

# 5. Salvar como tabela
df_transformed.write.mode("overwrite").saveAsTable("nome_da_tabela_final")