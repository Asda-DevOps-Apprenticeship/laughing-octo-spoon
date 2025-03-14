import os
import requests
import yaml
import json
import psycopg2
from databricks import sql
import pandas as pd
from datetime import datetime, date
from app.models import *
from sqlalchemy import func, and_
from sqlalchemy.orm import aliased
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from psycopg2 import extras
# from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config as DatabricksConfig
from delta.tables import DeltaTable



def merge_data_to_databricks_table(dataframe, table_name, match_column):
    config = DatabricksConfig(
        host=f'https://{os.environ.get("DATABRICKS_SERVER_HOSTNAME")}',
        token=os.environ.get("DATABRICKS_TOKEN"),
        cluster_id=os.environ.get('DATABRICKS_CLUSTER_ID') 
    )
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    
    spark_df = spark.createDataFrame(dataframe)
    full_table_name = f'custanwo.customer_transformation.{table_name}'
    
    logging.info(f"Merging data into table: {full_table_name}")
    
    try:
        # Load Delta Table
        delta_table = DeltaTable.forName(spark, full_table_name)
        
        # Perform Merge Operation
        merge_result = delta_table.alias("tgt").merge(
            spark_df.alias("src"),
            f"tgt.{match_column} = src.{match_column}"  # Matching condition
        ).whenMatchedUpdate(set={
            "deletion_flag": "src.deletion_flag",
            'deletion_date': 'src.deletion_date' # Update deletion_flag
        }).execute()
                
        logging.info(f"Data successfully merged into table '{full_table_name}'.")
    
    except Exception as e:
        logging.error(f"Error merging data into table '{full_table_name}': {str(e)}")
    
    spark.stop()
        

def write_data_to_databricks_table(dataframe, table_name):
    config = DatabricksConfig(
        host=f'https://{os.environ.get("DATABRICKS_SERVER_HOSTNAME")}',
        token=os.environ.get("DATABRICKS_TOKEN"),
        cluster_id=os.environ.get('DATABRICKS_CLUSTER_ID') 
        )
    spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()
    
    spark_df = spark.createDataFrame(dataframe)
    table_name = f'custanwo.customer_transformation.{table_name}'
    print(table_name)
    try:
        spark_df.write.format('delta').mode('append').saveAsTable(table_name)
        logging.info(f"Data successfully written to table '{table_name}'.")
    except Exception as e:
        logging.error(f"Error writing data to table '{table_name}': {str(e)}")
    spark.stop()
        

def validate_env_vars():
    required_vars = ['CLIENT_SECRET', 'API_KEY', 'SCOPES', 'DATASET_ID', 'IMS_ORG', 'SANDBOX_NAME','GDPR_CLIENT_SECRET','GDRP_API_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

def generate_access_token():
    validate_env_vars()
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    client_secret = os.environ['CLIENT_SECRET']
    client_id = os.environ['API_KEY']
    scope = os.environ['SCOPES']
    payload = {"client_id": client_id, "scope": scope, "client_secret": client_secret,
               'grant_type': 'client_credentials'}
    
    # Retry mechanism
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retry))
    
    try:
        response = session.post('https://ims-na1.adobelogin.com/ims/token/v3', headers=headers, data=payload)
        response.raise_for_status()
        return response.json()['access_token']
    except requests.exceptions.RequestException as e:
        logging.error(f"Error generating access token: {e}")
        raise

def generate_access_token_cdp_gdpr_execution():
    validate_env_vars()
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    client_secret = os.environ['GDPR_CLIENT_SECRET']
    client_id = os.environ['GDRP_API_KEY']
    scope = os.environ['SCOPES']
    payload = {"client_id": client_id, "scope": scope, "client_secret": client_secret,
               'grant_type': 'client_credentials'}


    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retry))

    try:
        response = session.post('https://ims-na1.adobelogin.com/ims/token/v3', headers=headers, data=payload)
        response.raise_for_status()
        return response.json()['access_token']
    except requests.exceptions.RequestException as e:
        logging.error(f"Error generating access token: {e}")
        raise

def customer_table_daily_run_cdd_tables():
    start_time = time.time()
    
    try:
        connection = sql.connect(
            server_hostname=os.environ.get("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.environ.get("DATABRICKS_HTTP_PATH"),
            access_token=os.environ.get("DATABRICKS_TOKEN")
        )
        with open('./app/sql_queries/cust_gdpr_table.sql', 'r') as file:
            query = file.read()
            logging.info(f"Executing query: {query}")
        
        with connection.cursor() as cursor:
            cursor.execute(query)
        
            daily_gdpr_run_objects = []
            chunk_count = 0
            while True:
                result = cursor.fetchmany(1000)
                if not result:
                    break
                chunk_count += 1
                daily_gdpr_run_objects.extend([
                    {'singl_profl_id': row['singl_profl_id'], 'wallet_id': row['wallet_id'], 'query_execution_date': row['query_execution_date']}
                    for row in result
                ])
                

                logging.info(f"Processed chunk {chunk_count} with {len(result)} records.")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

    finally:
        if 'connection' in locals() and connection:
            connection.close()

    end_time = time.time()
    logging.info(f"Time taken: {end_time - start_time} seconds")
    
    df_daily_gdpr_run = pd.DataFrame(daily_gdpr_run_objects)    
    return ', '.join([f"'{obj['singl_profl_id']}'" for obj in daily_gdpr_run_objects]), df_daily_gdpr_run

def profile_store_table_get_gdpr_deletions():
    result = customer_table_daily_run_cdd_tables()
    if result is None:
        logging.error("Failed to retrieve data from customer_table_daily_run_cdd_tables.")
        return
    single_profile_list, daily_run_data_frame = result
    
    if not single_profile_list:
        logging.error("No SINGLEPROFILEID_LIST returned from customer_table_daily_run_cdd_tables.")
        return
    
    with open('./app/sql_queries/profile_store_table.sql', 'r') as file:
        query_template = file.read()      
    query = query_template.format(PROFILE_SNAPSHOT_DATASET=os.getenv('PROFILE_SNAPSHOT_DATASET'), SINGLEPROFILEID_LIST=single_profile_list)   
    prod_connection = None
     
    try:
        prod_connection = psycopg2.connect(
            user=os.environ.get('IMS_ORG'),
            password=generate_access_token(),
            host=os.environ.get('HOST'),
            port=os.environ.get('PORT'),
            database=os.environ.get('PRODDB')
        )
        with prod_connection.cursor(cursor_factory=extras.DictCursor) as prod_cursor:
            prod_cursor.execute(query)
            profile_table_objects = []
            chunk_count = 0
            while True:
                result = prod_cursor.fetchmany(1000)
                if not result:
                    break
                chunk_count += 1

                profile_table_objects.extend([
                    {'singl_profl_id': row['singl_profl_id']}
                    for row in result
                ])
                logging.info(f"Processed chunk {chunk_count} with {len(result)} records.")
                                
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None
   
    finally:
        if 'prod_connection' in locals() and prod_connection:
            prod_connection.close()
            
    df_profile_table = pd.DataFrame([obj['singl_profl_id'] for obj in profile_table_objects])
    df_profile_table.rename(columns={0: 'singl_profl_id'}, inplace=True)
    df_profile_table['execution_date'] = datetime.today().date()
    user_deletion = df_profile_table.merge(daily_run_data_frame, on='singl_profl_id', how='inner')
    user_deletion.drop(columns=['query_execution_date'], inplace=True)
    user_deletion['deletion_date'] = datetime.today().date()
    user_deletion['deletion_flag'] = False
    
    write_data_to_databricks_table(df_profile_table, 'gdpr_profile_export_snapshot')
    write_data_to_databricks_table(user_deletion, 'gdpr_user_deletions')
    
    
        
def gdpr_deletions_api_call(chunk):
    try : 
    
        company_context_data = [
            {"namespace": "imsOrgID", "value": "B9CB1CFE53309CAD0A490D45@AdobeOrg"}
        ]

        include_data = ["profileService", "aepDataLake", "identity"]

        df_company_contexts = pd.DataFrame(company_context_data)
        df_include = pd.DataFrame(include_data, columns=["include"])

        grouped_users = chunk.groupby('key').agg({
            'action': lambda x: list(x),
            'namespace': lambda x: list(x),
            'value': lambda x: list(x),
            'type': lambda x: list(x)
        }).reset_index()

        users = []
        for _, row in grouped_users.iterrows():
            user = {
                "key": row['key'],
                "action": row['action'],
                "userIDs": [
                    {"namespace": ns, "value": val, "type": typ}
                    for ns, val, typ in zip(row['namespace'], row['value'], row['type'])
                ]
            }
            users.append(user)
            
        company_contexts = df_company_contexts.to_dict(orient='records')

        include = df_include['include'].tolist()

        regulation = "gdpr"

        final_json = {
            "companyContexts": company_contexts,
            "users": users,
            "include": include,
            "regulation": regulation
        
        }
        payload = json.dumps(final_json)
        access_token = generate_access_token_cdp_gdpr_execution()
        url = os.getenv('PRIVACY_END_POINT')
        headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': 'Bearer {access_token}'.format(access_token = access_token),
    'x-api-key': os.environ.get('GDRP_API_KEY'),
    'x-gw-ims-org-id': os.environ.get('IMS_ORG')
                    }
        
        response = requests.request("POST", url, headers=headers, data=payload)
        
        if response.status_code == 202:
            
            response_data = response.json()
            request_id = response_data['requestId']
            total_records = response_data['totalRecords']
            jobs = response_data['jobs']

            logging.info(f"Deletion request successful. Request ID: {request_id}")
            logging.info(f"Total records processed: {total_records}")

            # Save job details to the database
            for job in jobs:
                job_id = job['jobId']
                user_key = job['customer']['user']['key']



            # Create a DataFrame from the jobs data
            jobs_df = pd.DataFrame(jobs)
            jobs_df['execution_date'] = datetime.today().date()
                        
            write_data_to_databricks_table(jobs_df, 'gdpr_deletion_jobs')

            
            user_deletions_df = pd.DataFrame(users)
            user_deletions_df['deletion_flag'] = True
            user_deletions_df['singl_profl_id'] = user_deletions_df['key']
            user_deletions_df['deletion_date'] = datetime.today().date()
            user_deletions_df = user_deletions_df[['deletion_flag', 'singl_profl_id','deletion_date']]
            
            merge_data_to_databricks_table(user_deletions_df, 'gdpr_user_deletions', 'singl_profl_id')
            
            return True
        else:
            logging.error(f"Deletion request failed. Status code: {response.status_code}, Response: {response.text}")
            return False

    except Exception as e:
        logging.error(f"Error in gdpr_deletions_api_call: {e}", exc_info=True)
        db.session.rollback()  
        return False
    
    
def execute_gdpr_deletions_cdp(delete_date=None, flash=None):
    if not delete_date:
        logging.warning("No delete_date provided. Exiting function.")
        if flash:
            flash("No date provided for deletions.", "warning")
        return
    
    try:
        
        connection = sql.connect(
            server_hostname=os.environ.get("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.environ.get("DATABRICKS_HTTP_PATH"),
            access_token=os.environ.get("DATABRICKS_TOKEN")
        )
        with open('./app/sql_queries/gdpr_user_deletions_date.sql', 'r') as file:
            query_template = file.read()
            query = query_template.format(delete_date=delete_date)
            logging.info(f"Executing query: {query}")
            
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            user_deletions_list = cursor.fetchall()
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

    finally:
        if 'connection' in locals() and connection:
            connection.close()

    try:
        
        df = pd.DataFrame(user_deletions_list)

        if df.empty:
            logging.info(f"No user deletions to process for {delete_date}")
            if flash:
                flash(f"No user deletions to process for {delete_date}.", "info")
            return

        # Prepare data for API call
        df['key'] = df[0]
        df['action'] = 'delete'
        df['namespace'] = 'SPID'
        df['value'] = df[0]
        df['type'] = 'custom'


        # Process deletions in chunks
        chunk_size = 800
        for start in range(0, len(df), chunk_size):
            end = min(start + chunk_size, len(df))
            chunk = df.iloc[start:end]
            gdpr_deletions_api_call(chunk)

        logging.info(f"Successfully processed GDPR deletions for {delete_date}")
        if flash:
            flash(f"Successfully processed GDPR deletions for {delete_date}.", "success")

    except Exception as e:
        logging.error(f"Error in execute_gdpr_deletions_cdp: {e}", exc_info=True)
        if flash:
            flash(f"An error occurred while processing deletions: {str(e)}", "danger")
        raise



def auto_execute_gdpr_deletions_cdp(delete_date=None):
    profile_store_table_get_gdpr_deletions()
    
    if not delete_date:
        logging.warning("No delete_date provided. Exiting function.")
        return
    try:
        
        connection = sql.connect(
            server_hostname=os.environ.get("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.environ.get("DATABRICKS_HTTP_PATH"),
            access_token=os.environ.get("DATABRICKS_TOKEN")
        )
        with open('./app/sql_queries/gdpr_user_deletions_date.sql', 'r') as file:
            query_template = file.read()
            query = query_template.format(delete_date=delete_date)
            logging.info(f"Executing query: {query}")
            
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            user_deletions_list = cursor.fetchall()
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

    finally:
        if 'connection' in locals() and connection:
            connection.close()
        
        # Check the number of records due for deletion
        record_count = len(user_deletions_list)
        if record_count >= 1000:
            logging.info(f"More than 1000 records ({record_count}) found for {delete_date}. Manual intervention required.")
            return

        logging.info(f"Fewer than 1000 records ({record_count}) found for {delete_date}. Proceeding with automatic deletion.")

    try:
        df = pd.DataFrame(user_deletions_list)

        if df.empty:
            logging.info(f"No user deletions to process for {delete_date}")
            return

        # Prepare data for API call
        df['key'] = df[0]
        df['action'] = 'delete'
        df['namespace'] = 'SPID'
        df['value'] = df[0]
        df['type'] = 'custom'


        # Process deletions in chunks
        chunk_size = 800
        for start in range(0, len(df), chunk_size):
            end = min(start + chunk_size, len(df))
            chunk = df.iloc[start:end]
            success = gdpr_deletions_api_call(chunk)  #

            if not success:
                logging.error(f"Failed to process chunk {start}-{end} for {delete_date}")
                return

        logging.info(f"Successfully processed GDPR deletions for {delete_date}")
  
    except Exception as e:
        logging.error(f"Error in execute_gdpr_deletions_cdp: {e}", exc_info=True)
        raise



       
def get_spids_count_by_gdprdate():
    start_time = time.time()
    
    try:
        connection = sql.connect(
            server_hostname=os.environ.get("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.environ.get("DATABRICKS_HTTP_PATH"),
            access_token=os.environ.get("DATABRICKS_TOKEN")
        )
        with open('./app/sql_queries/gdpr_user_deletions.sql', 'r') as file:
            query = file.read()
            logging.info(f"Executing query: {query}")
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            spids_by_date = cursor.fetchall()
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

    finally:
        if 'connection' in locals() and connection:
            connection.close()

    end_time = time.time()
    logging.info(f"Time taken: {end_time - start_time} seconds")
    total_count = sum(row['cnt'] for row in spids_by_date)
    return total_count, spids_by_date
