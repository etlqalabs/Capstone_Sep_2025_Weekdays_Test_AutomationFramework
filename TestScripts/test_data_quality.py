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

    @pytest.mark.order(5)
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

    @pytest.mark.order(4)
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
    @pytest.mark.order(3)
    @pytest.mark.DataQuality
    def test_mysql_tables_exist(self,connect_to_mysql_database):
        expected_table_list = ["fact_inventory", "fact_sales", "monthly_sales_summary", "inventory_levels_by_store"]

        missing_tables_list = database_tables_exist(
            database_engine=connect_to_mysql_database,
            expected_tables_list=expected_table_list,
            database_name="retaildwh"
        )
        assert len(missing_tables_list) == 0, f"Missing tables: {missing_tables_list}"

    @pytest.mark.order(2)
    @pytest.mark.DataQuality
    def test_referentialIntegrity_product_id_between_staging_and_target(self,connect_to_mysql_database):
        source_query ="""select product_id from staging_product order by product_id"""
        target_query = """select product_id from fact_sales order by product_id"""
        df_not_matched = check_referential_integrity(
            source_conn = connect_to_mysql_database,
            target_conn = connect_to_mysql_database,
            source_query = source_query,
            target_query = target_query,
            key_column = 'product_id',
            csv_path = "Differences/not_matching_product_data.csv"
            )
        assert df_not_matched.empty,"There are product_id valure in the target that do not exist in the source"

    @pytest.mark.order(1)
    def test_referentialIntegrity_store_id_between_Oracle_stores_and_target_mysql(self,connect_to_oracle_database,connect_to_mysql_database):
        source_query ="""select store_id from stores_bkp order by store_id"""
        target_query = """select store_id from fact_sales order by store_id"""
        df_not_matched = check_referential_integrity(
            source_conn = connect_to_oracle_database,
            target_conn = connect_to_mysql_database,
            source_query = source_query,
            target_query = target_query,
            key_column = 'store_id',
            csv_path = "Differences/not_matching_store_data.csv"
            )
        assert df_not_matched.empty,"There are store_id valure in the target that do not exist in the source"
