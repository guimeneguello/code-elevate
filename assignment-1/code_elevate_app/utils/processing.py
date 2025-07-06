#classe que realiza alguns processamentos dos dados referentes a camada silver e persiste o arquivo parquet
import pyspark.sql.functions as f
import unicodedata
import logging
from pyspark.sql.types import StringType

class Transformer():
    def __init__(self, spark):
        self.spark = spark

    def read_bronze(self, path_to_bronze):
        #ler do paruet bronze
        try:
            logging.info("Lendo parquet da tabela bronze")

            df_bronze = self.spark.read.parquet(path_to_bronze)

            logging.info("Tabela bronze lida com sucesso")

            return df_bronze
        except Exception as e:
            logging.error("Erro ao ler o arquivo parquet bronze.")
            raise e
        
    def transform(self, df):
        try:
            logging.info("Realizando tratamentos no dataframe")

            def remover_acentos(s):
                if s is None:
                    return s
                return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
            remover_acentos = f.udf(remover_acentos, StringType())

            df_silver = df.withColumn(
                'DT_REFE', f.date_format(f.to_timestamp(f.col('DATA_INICIO'), 'MM-dd-yyyy H:mm'), 'yyyy-MM-dd')) \
                .withColumn('DISTANCIA', f.when((f.col('DISTANCIA') <= 0) | f.col('DISTANCIA').isNull(), 'Nao-consta').otherwise(f.col('DISTANCIA'))) \
                .fillna(value='Nao-consta', subset=['CATEGORIA', 'LOCAL_INICIO', 'LOCAL_FIM', 'PROPOSITO'])

            for col in ['CATEGORIA', 'LOCAL_INICIO', 'LOCAL_FIM', 'PROPOSITO']:
                df_silver = df_silver.withColumn(col, remover_acentos(f.col(col)))

            #df_silver.show()
            return df_silver
        
        except Exception as e:
            logging.error("Erro processando os dados na camada silver.")
            raise e
        
    def write_silver(self, df_silver, path_to_silver):
        try:
            logging.info("Persistindo tabela silver")

            df_silver.write.mode("overwrite").parquet(f"{path_to_silver}")

            logging.info("Tabela silver persistida com sucesso!")
        except Exception as e:
            logging.error("Erro gravando os dados na camada silver.")
            raise e