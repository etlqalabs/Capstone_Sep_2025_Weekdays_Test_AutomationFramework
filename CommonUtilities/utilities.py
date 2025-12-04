import os.path

import boto3
from io import StringIO
import pandas as pd
import pytest
from sqlalchemy import create_engine
import cx_Oracle
import paramiko

from Configuration.etlconfig import *
import logging

# Logging configution
logging.basicConfig(
    filename="Logs/etljob.log",
    filemode="w" ,
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)


def sales_data_from_Linux_server():
    # download the sales frile form Linux server to local via SFTP/ssh
    try:
        logger.info("Sales file from Linux server doenload started...")
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        ssh_client.connect(hostname,username=username,password=password)
        sftp = ssh_client.open_sftp()
        sftp.get(remote_file_path,local_file_path)
        sftp.close()
        ssh_client.close()
        logger.info("Sales file from Linux server doenload completed...")
    except Exception as e:
        logger.error("Error while donloading the sales file from Linux server.")



def verify_expected_as_file_to_actual_as_database(file_path, file_type, db_engine, table_name,test_case_name):
        try:
            if file_type == 'csv':
                df_expected = pd.read_csv(file_path)
            elif file_type == 'json':
                df_expected = pd.read_json(file_path)
            elif file_type == 'xml':
                df_expected = pd.read_xml(file_path, xpath=".//item")
            else:
                raise ValueError(f"unsupported file type passed{file_path}")
            logger.info(f"Expected data in the file is {df_expected}")
            query_actual = f"select * from {table_name}"
            df_actual = pd.read_sql(query_actual, db_engine)
            logger.info(f"Actual data in the file is {df_actual}")


            # expected minus atcual data (  extra data in expected )
            df_extra = df_expected[~df_expected.apply(tuple, axis=1).isin(df_actual.apply(tuple, axis=1))]
            df_extra.to_csv(f"Differences/extra_rows_in_expected_{test_case_name}.csv",index=False)

            # actual data minus expected (  extra data in actual )
            df_missing = df_actual[~df_actual.apply(tuple, axis=1).isin(df_expected.apply(tuple, axis=1))]
            df_missing.to_csv(f"Differences/extra_rows_in_actual_{test_case_name}.csv",index=False)

            assert df_extra.empty,(
                f"{test_case_name} : extra rows found in {df_extra} \n"
                f"check Differences/extra_rows_in_expected{test_case_name}.csv"
             )

            assert df_missing.empty, (
                f"{test_case_name} : extra rows found in {df_missing} \n"
                f"check Differences/extra_rows_in_actual_{test_case_name}.csv"
            )
        except Exception as e:
            logger.error(f"there is exception raised while check{e}")
            pytest.fail()



def verify_expected_as_database_to_actual_as_database(db_engine_expected,query_expected,db_engine_actual,query_actual):
    df_expected = pd.read_sql(query_expected, db_engine_expected)
    logger.info(f"Expected data in the file is {df_expected}")
    df_actual= pd.read_sql(query_actual, db_engine_actual)
    logger.info(f"Actual data in the file is {df_actual}")

    assert df_actual.equals(df_expected), f"expeected data in {query_expected} does not match with actual data in {query_actual}"

def check_for_duplicates_across_all_the_columns(file_path, file_type):
      try:
        if file_type == 'csv':
            df_data = pd.read_csv(file_path)
        elif file_type == 'json':
            df_data = pd.read_json(file_path)
        elif file_type == 'xml':
            df_data = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed{file_path}")
        #logger.info(f"data in the file is {df_data}")

        if df_data.duplicated().any():
            return False
        else:
            return True
      except Exception as e:
        logger.error(f"Error while reading the file {file_path}")

def check_for_duplicates_for_specific_columns(file_path, file_type,column_name):
    pass


def check_for_duplicates_for_database_table(db_engine,table_name):
    pass


def check_for_null_values(file_path, file_type):
    try:
        if file_type == 'csv':
            df_data = pd.read_csv(file_path)
        elif file_type == 'json':
            df_data = pd.read_json(file_path)
        elif file_type == 'xml':
            df_data = pd.read_xml(file_path, xpath=".//item")
        else:
            raise ValueError(f"unsupported file type passed{file_path}")
        #logger.info(f"data in the file is {df_data}")

        if df_data.isnull().values.any():
            return False
        else:
            return True
    except Exception as e:
        logger.error(f"Error while reading the file {file_path}")

def check_file_existence(file_path):
    try:
        if os.path.isfile(file_path):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"file :{file_path} does not exist {e}")

def check_file_size(file_path):
    try:
        if os.path.getsize(file_path) !=0:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"file :{file_path} is zero byte file {e}")


def database_tables_exist(database_engine, expected_tables_list, database_name):
            query = f"""
                SELECT TABLE_NAME
                FROM information_schema.tables
                WHERE table_schema = '{database_name}'"""

            df = pd.read_sql(query, database_engine)
            actual_tables_list = df['TABLE_NAME'].tolist()
            logger.info(f"the actual table is are : {actual_tables_list}")

            missing_tables_list = []
            for table in expected_tables_list:
                if table not in actual_tables_list:
                    missing_tables_list.append(table)
            return missing_tables_list


# S3 conectivity

'''
# initialize the connection
s3 = boto3.client("s3")
def read_file_from_s3(bucket_name,file_key):
    # fetch the csv file from S3
    try:
        response = s3.get_object(Bucket=bucket_name,Key=file_key)
        csv_content = response['Body'].read().decode('utf-8')
        data = StringIO(csv_content)
        df = pd.read_csv(data)
        return df
    except Exception as e:
        logger.error(f"exception raised while reading from S3 {e}", exc_info=True)

def verify_expected_as_S3_to_actual_as_db(bucket_name_expected,file_key_expected,db_engine_actual,query_actual):
    df_expected = read_file_from_s3(bucket_name_expected,file_key_expected)
    df_actual = pd.read_sql(query_actual,db_engine_actual)
    assert df_actual.equals(df_expected),f"expected data {df_expected} doesn not match with actual data{df_actual}"

'''


def check_referential_integrity(source_conn,target_conn,source_query,target_query,key_column,csv_path):
    try:
        logger.info(f"Running source query: {source_query}")
        df_source = pd.read_sql(source_query,source_conn)
        logger.info(f"Running target query: {target_query}")
        df_target = pd.read_sql(target_query, target_conn)
        logger.info(f"Comparing key column: {key_column}")
        df_not_matched = df_target[~df_target[key_column].isin(df_source[key_column])]
        df_not_matched.to_csv(csv_path,index=False)
        return df_not_matched
    except Exception as e:
        logger.error(f"Error during referential integerity check {e}")





