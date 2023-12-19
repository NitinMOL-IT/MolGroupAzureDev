import azure.functions as func
import azure.durable_functions as df
import logging
import json
from . import CO_process as CP
from .az_datalake_activity import DataLakeUtility
import os
from datetime import datetime
import pandas as pd

obj_DataLakeUtility = DataLakeUtility()

today_date = datetime.now().strftime('%Y-%m-%d')
blob_path = f"Log_Folder/{today_date}"
destination_container_name = "nonprdfocusbisto/ABLOG_FINAL/Cylindrical_Oil"


def main(name: str) -> str:

    log_Activity = []
    try:
        log_Activity.append("Activity function has been started")
        obj_DataLakeUtility = DataLakeUtility()

        log_Activity.append("Downloading Files to temp Folder")
        CO_Initial_file_download = obj_DataLakeUtility.download_blob("nonprdfocusbisto",
                                                        "ABLOG_FINAL/Cylindrical_Oil/Initial_Data/CO_Initial_Data.csv",
                                                        "CO_Initial_Data.csv")
        CO_Incremental_file_download = obj_DataLakeUtility.download_blob("nonprdfocusbisto",
                                                        "ABLOG_FINAL/Cylindrical_Oil/Incremental_Data/CO_Incremental_Data.csv",
                                                        "CO_Incremental_Data.csv")
        
        log_Activity.append("Downloading completed Files to temp Folder")

        # File paths
        CO_Initial_file_path = CO_Initial_file_download['absolute_path']
        CO_Incremental_file_path = CO_Incremental_file_download['absolute_path']

        # Logging
        logging.info('Data Fetching and Processing has been started')
        log_Activity.append('Data Fetching and Processing has been started')

        # Processing the CSV files using pandas
        CO_Incremental_Data_Processed = CP.CO_Process_first(CO_Incremental_file_path)

        CO_Incremental_Data_Processed["reporttime"] = pd.to_datetime(CO_Incremental_Data_Processed["reporttime"])
        min_date_inc_data = CO_Incremental_Data_Processed['reporttime'].min()

        CO_Initial_Data = pd.read_csv(CO_Initial_file_download['absolute_path'])
        CO_Initial_Data["reporttime"] = pd.to_datetime(CO_Initial_Data["reporttime"],format='%d-%m-%Y %H:%M:%S')
        CO_Initial_Data_Filtered = CO_Initial_Data[CO_Initial_Data["reporttime"] < min_date_inc_data]

        Final_Data = pd.concat([CO_Initial_Data_Filtered, CO_Incremental_Data_Processed])

        Final_Processed_Data = CP.CO_Process_Second(Final_Data)

        temp_final_file_loc = os.path.join(obj_DataLakeUtility.temp_directory,'CO_Initial_Data.csv')

        Final_Processed_Data.to_csv(temp_final_file_loc,index=False)
        
        # Upload the Final CSV to Azure Blob Storage
        logging.info('Overwriting final file in destination container')
        log_Activity.append('Overwriting final file in destination container')
        final_file_upload = obj_DataLakeUtility.upload_blob(contr=destination_container_name, blob_path="Initial_Data",temp_file_abs_path=temp_final_file_loc,
                                                            blob_name="CO_Initial_Data.csv")
        logging.info('Final file uploaded successfully')
        log_Activity.append('Final file uploaded successfully')

        obj_DataLakeUtility.delete_dir() 

        logging.info('--------------------------------')
        logging.info("Copy & Delete blob function is initializing...")
        log_Activity.append('--------------------------------')
        log_Activity.append("Copy & Delete blob function is initializing...")
        
        obj_DataLakeUtility.copy_and_delete()

        log_Activity.append("Copy & Delete blob function is completed successfully")
        log_Activity.append('--------------------------------')

    except Exception as e:
        logging.exception(str(e))
        log_Activity.append(e)
    finally:
        Activity_Function_Log_file_Upload = obj_DataLakeUtility.upload_log(log_data_string=str(log_Activity),blob_name="Activity_Function_Log.log",blob_path=blob_path,destination_container_name=destination_container_name)

    return f"Hello {name}!"
