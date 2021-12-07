import findspark
findspark.init() 

import sys
import os
sys.path.append(os.path.abspath('src'))

#Required imports
import unittest
from pyspark.sql import SparkSession
from chispa import *
from main import filter_countries,rename_col



#Unit Testing the Filter and Rename functions for the main script
class TestMain(unittest.TestCase):
    
    #Setting up a spark session
    def setUp(self):
      self.spark = SparkSession.builder.getOrCreate()

    #Unit Testing the filter function by checking that the row contents are the same in
    #actual and expected dataset
    def test_filter_countries(self):
        source_data = [
            (1,'France'),
            (2,'Netherlands'),
            (3,'United Kingdom'),
            (4,'United States'),
            (5,'Netherlands')
        ]

        source_df = self.spark.createDataFrame(source_data, ['id', 'country'])
        country_list = ['United Kingdom', 'Netherlands']

        actual_df = filter_countries(source_df,country_list)

        expected_data = [
            (2,'Netherlands'),
            (3,'United Kingdom'),
            (5,'Netherlands')
        ]

        expected_df = self.spark.createDataFrame(expected_data, ['id', 'country'])

        #comparing the two datasets
        assert_df_equality(actual_df,expected_df)

    #Unit Testing the Rename function by checking that the schema is the same
    def test_rename_col(self):
        source_data = [
            (1,'Test1','Test2')
        ]

        source_df = self.spark.createDataFrame(source_data, ['id', 'country','first_name'])
        actual_df = rename_col(source_df,'id','id_new')

        expected_data = [
            (1,'Test1','Test2')
        ]

        expected_df = self.spark.createDataFrame(expected_data, ['id_new', 'country','first_name'])

        #comparing the two datasets
        assert_df_equality(actual_df, expected_df)



