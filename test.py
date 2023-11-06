import unittest
from pyspark.sql import SparkSession
import mylib.lib as lib


class PySparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize a Spark session
        cls.spark = (
            SparkSession.builder.master("local[*]").appName("PySparkTest").getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        cls.spark.stop()

    # Test the load_data function
    def test_load_data(self):
        
        columns_to_select = [
            "country",
            "country_long",
            "name",
            "capacity_mw",
            "primary_fuel",
        ]
        # Path to the dataset
        test_data_path = "dataset/global_power_plant_database.csv"

        # Use the function to load the data
        df = lib.load_data(self.spark, test_data_path, columns=columns_to_select)

        # Check if the DataFrame is not empty
        self.assertFalse(df.head(1) == [])

        # Check if the correct columns are selected
        selected_columns = df.columns
        self.assertListEqual(selected_columns, columns_to_select)


if __name__ == "__main__":
    unittest.main()
