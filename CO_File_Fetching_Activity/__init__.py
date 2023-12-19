# This function is not intended to be invoked directly. Instead it will be
# triggered by an orchestrator function.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import azure.functions as func
import azure.durable_functions as df
from . import az_datalake_activity as azd
import os
from datetime import datetime
import logging
import pymysql
import paramiko
import pandas as pd
from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta
from io import StringIO


def main(name: str) -> str:
        today_date = datetime.now().strftime('%Y-%m-%d')
        blob_path = "ABLOG_FINAL/Cylindrical_Oil"
        destination_container_name = "nonprdfocusbisto"
        logging.info("Activity Function Started")
        try:
            obj_DataLakeUtility = azd.DataLakeUtility()

            # Get the path to the SSH key file
            ssh_key_path = os.path.join(os.getcwd(), 'CO_File_Fetching_Activity/pro_key.pem')

            # Read the contents of the SSH key file
            with open(ssh_key_path, 'r') as file:
                private_key = file.read()

            # Create a Paramiko SSH client
            ssh = paramiko.SSHClient()

            # Automatically add the server's host key (this is insecure and you should verify the host key in a production environment)
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            mypkey = paramiko.RSAKey(file_obj=StringIO(private_key))
            sql_hostname = os.environ.get('CO_sql_hostname')
            sql_username = os.environ.get('CO_sql_username')
            sql_password = os.environ.get('CO_sql_password')
            sql_main_database = os.environ.get('CO_sql_main_database')
            sql_port = 3306
            ssh_host = os.environ.get('CO_ssh_host')
            ssh_user = os.environ.get('CO_ssh_user')
            ssh_port = 22
            sql_ip = os.environ.get('CO_sql_ip')

            logging.info("Started Tunneling")
            with SSHTunnelForwarder(
                    (ssh_host, ssh_port),
                    ssh_username=ssh_user,
                    ssh_pkey=mypkey,
                    remote_bind_address=(sql_hostname, sql_port)) as tunnel:
                    # For Local Connection
                    conn = pymysql.connect(host='127.0.0.1', user=sql_username,
                            passwd=sql_password, db=sql_main_database,
                            port=tunnel.local_bind_port)
                    # For Azure connection
                    logging.info("Connected to Database")

                    logging.info("Creating Cursor")
                    cursor = conn.cursor()

                    # Execute SQL queries to retrieve data
                    logging.info("Executing SQL queries")
                    cursor.execute("""SELECT
                                    D.imono,
                                    v.vesselname,
                                    vlm_VT.title AS VesselKind,
                                    vlm_SY.title AS Shipyard,
                                    vlm_OC.title AS MOLOwned_Chartered,
                                    vlm_CH.title AS Charterer,
                                    v.deldate AS DeliveryDate,
                                    vlm_SMC.title AS ShipManagement,
                                    vlm_MEM.title AS ME_Name,
                                    v.metype AS ME_Type,
                                    D.reporttime,
                                    D.spantime,
                                    D.spantype,
                                    D.eventid,
                                    E.aveoutput,
                                    c2.qa AS FeedRate
                                    FROM (
                                    SELECT * 
                                    FROM focussyfp.dailynoondata
                                    WHERE eventtype in ('5','6','10','13','16')
                                    ) D
                                    LEFT JOIN focussyfp.dailyengineinfo E 
                                    on D.eventid  = E.eventid 
                                    LEFT JOIN focussyfp.cyloilfeedrate c2 
                                    on c2.imono = D.imono AND c2.reporttime = D.reporttime 
                                    LEFT JOIN focuscommon.vesselinfo v 
                                    on D.imono = v.imono 
                                    LEFT JOIN focuscommon.vesselinfoitemmaster vlm_VT
                                    on vlm_VT.itemno = 'VESSEL_TYPE' AND vlm_VT.value = v.typeofvessel  
                                    LEFT JOIN focuscommon.vesselinfoitemmaster vlm_CH
                                    on vlm_CH.itemno = 'CHARTERER' AND vlm_CH.value = v.charterer
                                    LEFT JOIN focuscommon.vesselinfoitemmaster vlm_SY
                                    on vlm_SY.itemno = 'SHIPYARD' AND vlm_SY.value = v.shipyard 
                                    LEFT JOIN focuscommon.vesselinfoitemmaster vlm_SMC
                                    on vlm_SMC.itemno = 'SHIP_MANAGEMENT_COMPANY' AND vlm_SMC.value = v.shipmanagement
                                    LEFT JOIN focuscommon.vesselinfoitemmaster vlm_MEM
                                    on vlm_MEM.itemno = 'ME_MAKER' AND vlm_MEM.value = v.mename
                                    LEFT JOIN focuscommon.vesselinfoitemmaster vlm_OC
                                    on vlm_OC.itemno = 'MOL_OWNED' AND vlm_OC.value = v.molowned
                                    WHERE D.reporttime >= CURDATE() - INTERVAL 3 DAY""")

                    # Retrieve column names
                    column_names = [col[0] for col in cursor.description]

                    # Fetch data with column names
                    data = cursor.fetchall()

                    # Convert the data into a pandas DataFrame
                    logging.info("Converting data into pandas Data")
                    # Convert the data into a pandas DataFrame with column names
                    result = pd.DataFrame(data, columns=column_names)

                    # Define the path where you want to save the CSV file
                    logging.info("Defining CSV File Path for uploading")
                    csv_file_path = os.path.join(obj_DataLakeUtility.temp_directory, 'data.csv')
                    result.to_csv(csv_file_path,index=False)
                    logging.info('Dataframe to CSV Done')

                    # Get yesterday's date
                    yesterday = datetime.now() - timedelta(days=1)

                    # Format the date as needed (e.g., in YYYYMMDD format)
                    date_suffix = yesterday.strftime("%Y%m%d")

                    # Construct the new blob name with yesterday's date
                    new_blob_name = "CO_Incremental_Data.csv"

                    obj_DataLakeUtility.upload_blob(destination_container_name,blob_path='ABLOG_FINAL/Cylindrical_Oil/Incremental_Data',temp_file_abs_path=csv_file_path, blob_name=new_blob_name)

                    conn.close()
                    logging.info("Closing Connection")
                    obj_DataLakeUtility.delete_dir() 
                    logging.info("Deleting Temp Dir")


        except Exception as e:
            logging.error(e)
            return (str(e))

        return f"Hello {name}!"