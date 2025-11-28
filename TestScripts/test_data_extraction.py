import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import paramiko
import pytest
import inspect
from CommonUtilities.utilities import *

import logging

# Logging configution
logging.basicConfig(
    filename="Logs/etljob.log",
    filemode="w" ,
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("connect_to_mysql_database")
class TestDataExtraction:
    @pytest.mark.DataExtraction
    def test_DE_from_sales_data_between_source_and_staging(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name

            logger.info(f"Test case '{test_case_name}' execution has started...")
            sales_data_from_Linux_server()
            verify_expected_as_file_to_actual_as_database("TestData/sales_data.csv","csv",connect_to_mysql_database,"staging_sales",test_case_name=test_case_name)
            logger.info(f"Test case '{test_case_name}' execution has completed...")
        except Exception as e:
            logger.error(f"Test case '{test_case_name}' execution has failed: {e}")
            pytest.fail(f"Test case '{test_case_name}' failed")


    '''
    @pytest.mark.DataExtraction
    def test_DE_from_product_data_between_source_and_staging(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            logger.info(f"Test case '{test_case_name}' execution has started...")

            db_engine_actual = connect_to_mysql_database
            query_actual = """select * from staging_product"""
            #verify_expected_as_S3_to_actual_as_db(bucket_name_expected, file_key_expected, db_engine_actual,
                                                  query_actual)

            logger.info(f"Test case '{test_case_name}' execution has completed...")
        except Exception as e:
            logger.error(f"Test case '{test_case_name}' execution has failed: {e}")
            pytest.fail(f"Test case '{test_case_name}' failed")

    '''

    @pytest.mark.DataExtraction
    def test_DE_from_supplier_data_between_source_and_staging(self, connect_to_mysql_database):
        pass

    @pytest.mark.DataExtraction
    def test_DE_from_inventory_data_between_source_and_staging(self, connect_to_mysql_database):
        pass

    @pytest.mark.DataExtraction
    def test_DE_from_stores_data_between_source_and_staging(self,connect_to_oracle_database, connect_to_mysql_database,):
        try:
            logger.info("Test case execution for store data extraction has started...")

            query_expected = """select * from stores"""
            query_actual = """select * from staging_stores"""
            verify_expected_as_database_to_actual_as_database(connect_to_oracle_database, query_expected,
                                                              connect_to_mysql_database, query_actual)
            logger.info("Test case execution for store data extraction has completed...")
        except Exception as e:
            logger.error(f"Test case execution for store data extraction has failed{e}...")
            pytest.fail("Test case execution for store data extraction has failed")

