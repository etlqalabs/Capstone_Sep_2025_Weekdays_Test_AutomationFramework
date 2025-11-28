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



class TestDataLoading:
    def test_DL_Monthly_sales_summary(self, connect_to_mysql_database):
            try:
                logger.info("Test case execution for monthly sales summary load has started...")

                query_expected = """select product_id,total_sales, year,month from intermediate_monthly_sales_summary_source order by product_id"""
                query_actual= """select product_id,total_sales, year,month from monthly_sales_summary order by product_id"""
                verify_expected_as_database_to_actual_as_database(connect_to_mysql_database,query_expected,connect_to_mysql_database,query_actual)
                logger.info("Test case execution for monthly sales summary load has completed...")
            except Exception as e:
                logger.error(f"Test case execution for monthly sales summary load has failed{e}...")
                pytest.fail("Test case execution for monthly sales summary load has failed")

    def test_DL_fact_Sales(self, connect_to_mysql_database):
        pass

    def test_DL_fact_inventory(self, connect_to_mysql_database):
        pass

    def test_DL_inventory_level_by_stores(self, connect_to_mysql_database):
        pass