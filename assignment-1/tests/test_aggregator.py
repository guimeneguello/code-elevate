import unittest
from pyspark.sql import SparkSession, Row
from code_elevate_app.utils.aggregator import Aggregator
import os

class TestAggregator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("test-aggregator").getOrCreate()
        cls.aggregator = Aggregator(cls.spark)
        # Cria um DataFrame de teste para silver
        cls.test_silver = "test_silver.parquet"
        data = [
            Row(DT_REFE="2020-01-01", CATEGORIA="Negocio", PROPOSITO="Reunião", DISTANCIA=10),
            Row(DT_REFE="2020-01-01", CATEGORIA="Pessoal", PROPOSITO="Viagem", DISTANCIA=20),
            Row(DT_REFE="2020-01-02", CATEGORIA="Negocio", PROPOSITO="Reunião", DISTANCIA=30),
        ]
        df = cls.spark.createDataFrame(data)
        df.write.mode("overwrite").parquet(cls.test_silver)

    @classmethod #apaga os arquivos criados após os testes
    def tearDownClass(cls):
        cls.spark.stop()
        if os.path.exists(cls.test_silver):
            import shutil
            shutil.rmtree(cls.test_silver)
        if os.path.exists("test_gold.parquet"):
            import shutil
            shutil.rmtree("test_gold.parquet")

    def test_read_silver(self): #Verifica se o DataFrame foi lido corretamente
        df = self.aggregator.read_silver(self.test_silver)
        self.assertEqual(df.count(), 3)

    def test_aggregations(self): #Verifica se as agregações estão corretas
        df = self.aggregator.read_silver(self.test_silver)
        df_gold = self.aggregator.aggregations(df)
        result = {row.DT_REFE: row for row in df_gold.collect()}
        self.assertIn("2020-01-01", result)
        self.assertIn("2020-01-02", result)
        self.assertEqual(result["2020-01-01"].QT_CORR, 2)
        self.assertEqual(result["2020-01-02"].QT_CORR, 1)

    def test_write_gold(self): #Verifica se o arquivo parquet gold foi criado
        df = self.aggregator.read_silver(self.test_silver)
        df_gold = self.aggregator.aggregations(df)
        self.aggregator.write_gold(df_gold, "test_gold.parquet")
        self.assertTrue(os.path.exists("test_gold.parquet"))

if __name__ == "__main__":
    unittest.main()
