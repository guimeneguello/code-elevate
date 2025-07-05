#classe principal orquestradora
from pyspark.sql import SparkSession
import sys
import logging
from code_elevate_app.utils.reader import Reader
from code_elevate_app.utils.processing import Transformer
from code_elevate_app.utils.aggregator import Aggregator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)


class Motor:
    BRONZE_PATH = "table_destinations/info_transportes.parquet"
    SILVER_PATH = "table_destinations/info_transportes_silver.parquet"
    GOLD_PATH = "table_destinations/info_corridas_do_dia.parquet"

    def __init__(self, csv_path, spark):
        self.csv_path = csv_path
        self.spark = spark
        self.reader = Reader(spark, csv_path)
        self.processing = Transformer(spark)
        self.aggregator = Aggregator(spark)

    def executor(self):
        try:
            logging.info("Iniciando aplicação")

            logging.info("Iniciando tratamentos referente à camada bronze")
            df_transportes = self.reader.csv_reader()
            self.reader.write_bronze(df_transportes, self.BRONZE_PATH)

            logging.info("Iniciando tratamentos referente à camada silver")
            df_bronze = self.processing.read_bronze(self.BRONZE_PATH)
            df_silver = self.processing.transform(df_bronze)
            self.processing.write_silver(df_silver, self.SILVER_PATH)

            logging.info("Iniciando tratamentos referente à camada gold")
            df_silver = self.aggregator.read_silver(self.SILVER_PATH)
            df_gold = self.aggregator.aggregations(df_silver)
            self.aggregator.write_gold(df_gold, self.GOLD_PATH)

            logging.info("Concluído o processamento dos dados! Carregue a tabela gold para análise.")
            logging.info("Finalizando aplicação...")

        except Exception as e:
            logging.error("Erro ao iniciar a aplicação")
            raise e
        
def main(csv_path):
    import os

    os.environ['HADOOP_HOME'] = 'C:\\hadoop'
    os.environ['PATH'] += os.pathsep + 'C:\\hadoop\\bin'

    spark = SparkSession.builder.appName("assignment1").config("spark.hadoop.hadoop.native.lib", "false").getOrCreate()
    motor = Motor(csv_path=csv_path, spark=spark)
    motor.executor()
    spark.stop()

if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            print("Usage: python -m code_elevate_app.main <csv+path>")
        else:
            main(sys.argv[1])
    except Exception as e:
        print("Erro ao executar o script:")
        print(e)
        sys.exit(1)