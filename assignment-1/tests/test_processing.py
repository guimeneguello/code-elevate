import unittest
from pyspark.sql import SparkSession
from code_elevate_app.utils.processing import Transformer
from pyspark.sql import Row
import re

class TestTransformer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        cls.transformer = Transformer(cls.spark, cls.logger)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform_removes_accents(self): #testa se a transformação remove acentos
        data = [Row(CATEGORIA="Ônibus", LOCAL_INICIO="São Paulo", LOCAL_FIM="João Pessoa", PROPOSITO="Viágem", DATA_INICIO="01-01-2020 8:00", DISTANCIA=10)]
        df = self.spark.createDataFrame(data)
        df_transformed = self.transformer.transform(df)
        row = df_transformed.first()
        self.assertEqual(row.CATEGORIA, "Onibus")
        self.assertEqual(row.LOCAL_INICIO, "Sao Paulo")
        self.assertEqual(row.LOCAL_FIM, "Joao Pessoa")
        self.assertEqual(row.PROPOSITO, "Viagem")

    def test_transform_handles_nulls(self): #testa se a transformação lida com valores nulos
        data = [Row(CATEGORIA=None, LOCAL_INICIO=None, LOCAL_FIM=None, PROPOSITO=None, DATA_INICIO="01-01-2020 8:00", DISTANCIA=None)]
        df = self.spark.createDataFrame(data)
        df_transformed = self.transformer.transform(df)
        row = df_transformed.first()
        self.assertEqual(row.CATEGORIA, "Nao-consta")
        self.assertEqual(row.LOCAL_INICIO, "Nao-consta")
        self.assertEqual(row.LOCAL_FIM, "Nao-consta")
        self.assertEqual(row.PROPOSITO, "Nao-consta")
        self.assertEqual(row.DISTANCIA, "Nao-consta")

    def test_dt_refe_format(self): # Checa se DT_REFE está no formato yyyy-MM-dd
        data = [Row(CATEGORIA="Ônibus", LOCAL_INICIO="São Paulo", LOCAL_FIM="João Pessoa", PROPOSITO="Viágem", DATA_INICIO="01-01-2020 8:00", DISTANCIA=10)]
        df = self.spark.createDataFrame(data)
        df_transformed = self.transformer.transform(df)
        row = df_transformed.first()
        self.assertTrue(re.match(r"^\d{4}-\d{2}-\d{2}$", row.DT_REFE))

if __name__ == "__main__":
    unittest.main()
