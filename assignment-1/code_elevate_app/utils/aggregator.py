#classe que realize agregações referente a camada gold e persiste o arquivo parquet final
import pyspark.sql.functions as f
import logging

class Aggregator():
    def __init__(self, spark):
        self.spark = spark

    def read_silver(self, path_to_silver):
        try:
            logging.info("Lendo parquet da tabela silver")

            df_silver = self.spark.read.parquet(path_to_silver)

            logging.info("Tabela silver lida com sucesso")

            return df_silver
        except Exception as e:
            logging.error("Erro ao ler o arquivo parquet silver.")
            raise e
        
    def aggregations(self, df_silver):
        try:
            logging.info("Lendo parquet da tabela silver")

            info_corridas_do_dia = df_silver.groupBy("DT_REFE").agg(
                f.count('*').alias('QT_CORR'),
                f.count(f.when(f.col('CATEGORIA') == 'Negocio', True)).alias('QT_CORR_NEG'),
                f.count(f.when(f.col('CATEGORIA') == 'Pessoal', True)).alias('QT_CORR_PESS'),
                f.round(f.max('DISTANCIA'), 1).alias('VL_MAX_DIST'),
                f.round(f.min('DISTANCIA'), 1).alias('VL_MIN_DIST'),
                f.round(f.avg('DISTANCIA'), 1).alias('VL_AVG_DIST'),
                f.count(f.when(f.col('PROPOSITO') == 'Reuniao', True)).alias('QT_CORR_REUNI'),
                f.count(f.when(f.col('PROPOSITO') != 'Reuniao', True)).alias('QT_CORR_NAO_REUNI'),
            )

            #info_corridas_do_dia = info_corridas_do_dia.orderBy('DT_REFE')

            #info_corridas_do_dia.show()

            logging.info("Agregações realizadas com sucesso!")

            return info_corridas_do_dia
        except Exception as e:
            logging.error("Erro realizando as agregações no dataframe.")
            raise e
        
    def write_gold(self, df_gold, path_to_gold):
        try:
            logging.info("Persistindo tabela gold")

            df_gold.write.mode("overwrite").parquet(f"{path_to_gold}")

            logging.info("Tabela gold persistida com sucesso")
        except Exception as e:
            logging.error("Erro gravando os dados na camada gold.")
            raise e