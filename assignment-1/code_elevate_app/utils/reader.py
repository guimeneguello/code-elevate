import logging
#classe que lê o arquivo csv de origen e persiste na camada bronze
class Reader():
    def __init__(self, spark, path):
        self.spark = spark
        self.path = path

    def csv_reader(self):
        try:
            logging.info(f"Lendo arquivo CSV {self.path}")

            df_transportes = self.spark.read.csv(self.path,
                                                 sep=';',
                                                 header=True,
                                                 inferSchema=True
                                            )

            logging.info("Arquivo CSV lido com sucesso!")

            return df_transportes
        
        except Exception as e:
            logging.error("Erro ao ler o arquivo CSV.")
            raise e

    def write_bronze(self, df, path_to_bronze):
        try:
            logging.info("Persistindo tabela bronze")

            df.write.mode("overwrite").parquet(f"{path_to_bronze}")

            logging.info("Tabela bronze persistida com sucesso!")
        except Exception as e:
            logging.error("Erro gravando os dados na camada bronze.")
            raise e