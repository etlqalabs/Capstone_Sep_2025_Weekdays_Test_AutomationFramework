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
from CommonUtilities.utilities import *


class TestDataQuality:

    @pytest.mark.DataQuality
    def test_DQ_Sales_csv_duplicate_check(self):
        try:
            logger.info("Duplicate check for sales_data has started...")
            duplicate_status = check_for_duplicates_across_all_the_columns("TestData/sales_data.csv","csv")
            print(duplicate_status)
            assert duplicate_status," there are duplicate in the sales data file"
            logger.info("Duplicate check for sales_data has completed ...")
        except Exception as e:
            logger.error(f"Duplicate check for sales_data has failed...,{e}")
            pytest.fail()

    @pytest.mark.DataQuality
    def test_duplicates_check_all_other_file_across_columns(self):
        pass
    @pytest.mark.skip
    def test_duplicates_check_all_other_file_for_speciifc_columns(self):
        pass

    @pytest.mark.skip
    def test_duplicates_check_all_targe_tables_across_columns(self):
        pass

    @pytest.mark.skip
    def test_duplicates_check_all_targe_tables_specifci_columns(self):
        pass

    @pytest.mark.DataQuality
    def test_DQ_Sales_csv_null_values_check(self):
        try:
            logger.info("Null check for sales_data has started...")
            null_value_status = check_for_null_values("TestData/sales_data.csv","csv")
            logger.info(f"is there a null in the file: {null_value_status}")
            assert null_value_status," there are Null in the sales data file"
            logger.info("Null check for sales_data has completed...")
        except Exception as e:
            logger.error(f"Null check for sales_data has failed.{e}")
            pytest.fail()

    @pytest.mark.DataQuality
    def test_DQ_Sales_csv_file_availabilty(self):
        try:
            logger.info("file availability check for sales_data has started...")
            assert check_file_existence("TestData/sales_data.csv"),"File doe not exist in location"
            logger.info("file availability check for sales_data has completed...")
        except Exception as e:
            logger.error(f"file availability check for sales_data has failed {e}")
            pytest.fail()

    # Test case to check tables availbility in database
    @pytest.mark.DataQuality
    def test_mysql_tables_exist(self,connect_to_mysql_database):
        expected_table_list = ["fact_inventory", "fact_sales", "monthly_sales_summary", "inventory_levels_by_store"]

        missing_tables_list = database_tables_exist(
            database_engine=connect_to_mysql_database,
            expected_tables_list=expected_table_list,
            database_name="retaildwh"
        )
        assert len(missing_tables_list) == 0, f"Missing tables: {missing_tables_list}"
