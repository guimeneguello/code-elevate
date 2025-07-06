import unittest
from pyspark.sql import SparkSession
from code_elevate_app.utils.reader import Reader
import os

class TestReader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("test-reader").getOrCreate()
        # Cria um CSV de teste
        cls.test_csv = "test_info_transportes.csv"
        with open(cls.test_csv, "w", encoding="utf-8") as f:
            f.write("CATEGORIA;LOCAL_INICIO;LOCAL_FIM;PROPOSITO;DATA_INICIO;DISTANCIA\n")
            f.write("Ônibus;São Paulo;João Pessoa;Viágem;01-01-2020 8:00;10\n")
        cls.reader = Reader(cls.spark, cls.test_csv)

    @classmethod #apaga os arquivos criados após os testes
    def tearDownClass(cls):
        cls.spark.stop()
        if os.path.exists(cls.test_csv):
            os.remove(cls.test_csv)
        if os.path.exists("test_bronze.parquet"):
            import shutil
            shutil.rmtree("test_bronze.parquet")

    def test_csv_reader(self): # Verifica se o DataFrame foi lido corretamente
        df = self.reader.csv_reader()
        self.assertEqual(df.count(), 1)
        row = df.first()
        self.assertEqual(row.CATEGORIA, "Ônibus")
        self.assertEqual(row.LOCAL_INICIO, "São Paulo")
        self.assertEqual(row.LOCAL_FIM, "João Pessoa")
        self.assertEqual(row.PROPOSITO, "Viágem")
        self.assertEqual(row.DISTANCIA, 10)

    def test_bronze_persister(self): # Verifica se o arquivo parquet foi criado
        df = self.reader.csv_reader()
        self.reader.write_bronze(df, "test_bronze.parquet")
        self.assertTrue(os.path.exists("test_bronze.parquet"))

if __name__ == "__main__":
    unittest.main()
