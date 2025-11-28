import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import paramiko
import pytest
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



class TestDataTransformation:
    @pytest.mark.smoke
    @pytest.mark.regresison
    def test_DT_Filter_Sales(self, connect_to_mysql_database):
            try:
                logger.info("Test case execution for filter transformation has started...")

                query_expected = """select * from staging_sales where sale_date >= '2024-09-10'"""
                query_actual= """select * from intermediate_filtered_sales"""
                verify_expected_as_database_to_actual_as_database(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
                logger.info("Test case execution for filter transformation has completed...")
            except Exception as e:
                logger.error(f"Test case execution for filter transformation has failed{e}...")
                pytest.fail("Test case execution for filter transformation has failed")

    @pytest.mark.smoke
    def test_DT_Router_High_Region__Sales(self, connect_to_mysql_database):
            try:
                logger.info("Test case execution for Router_High_Region transformation has started...")

                query_expected = """select * from intermediate_filtered_sales where region='High'"""
                query_actual= """select * from intermediate_high_sales"""
                verify_expected_as_database_to_actual_as_database(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
                logger.info("Test case execution for Router_High_Region transformation has completed...")
            except Exception as e:
                logger.error(f"Test case execution for Router_High_Region transformation has failed{e}...")
                pytest.fail("Test case execution for Router_High_Region transformation has failed")

    def test_DT_Router_Low_Region__Sales(self, connect_to_mysql_database):
            try:
                logger.info("Test case execution for Router_Low_Region transformation has started...")

                query_expected = """select * from intermediate_filtered_sales where region='Low'"""
                query_actual= """select * from intermediate_low_sales"""
                verify_expected_as_database_to_actual_as_database(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
                logger.info("Test case execution for Router_Low_Region transformation has completed...")
            except Exception as e:
                logger.error(f"Test case execution for Router_Low_Region transformation has failed{e}...")
                pytest.fail("Test case execution for Router_Low_Region transformation has failed")
