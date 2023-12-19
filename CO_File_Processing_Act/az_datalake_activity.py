from azure.storage.blob import BlobServiceClient
import tempfile
import os
import shutil
import datetime
import logging
from uuid import uuid4


class DataLakeUtility:

    def __init__(self,uuid=False):
        self.local_source_file = None
        if uuid:
            self.guid = uuid
        else:
            self.guid = str(uuid4())
        self.root_folder = tempfile.gettempdir()
        self.dynamic_folder = self.guid
        self.temp_directory = os.path.join(self.root_folder, "AzureBlobTemp", self.dynamic_folder)
        os.makedirs(self.temp_directory, exist_ok=True)
        self.connection_string = os.environ.get("ADLS_QRT_CON_STRING")
        
    """
        |//////////////////////////////////////////////////////////////////////////////////////////////////|
        |              Connect Azure                                                                       |
        |//////////////////////////////////////////////////////////////////////////////////////////////////|
    
    """
    def connect_azure(self):        
        try:
            
            blob_srv = BlobServiceClient.from_connection_string(self.connection_string)
            return blob_srv
        except Exception as e:
            logging.error(str(e))
            raise Exception("BlobService Initialization failed")

    """
        |//////////////////////////////////////////////////////////////////////////////////////////////////|
        |              Download Blob to App Service Plan temp directory from Blob Storage                  |
        |//////////////////////////////////////////////////////////////////////////////////////////////////|
    
    """
    def download_blob(self, contr: str, blob_path: str, file_name: str):
        #started_at = datetime.now()
        try:
            blob_srv = self.connect_azure()
            self.local_source_file = os.path.join(self.temp_directory, file_name)
            blob_client_instance = blob_srv.get_blob_client(contr, blob_path, snapshot=None)
            os.makedirs(self.temp_directory, exist_ok=True)
            with open(self.local_source_file, "wb") as my_blob:
                blob_data = blob_client_instance.download_blob()
                blob_data.readinto(my_blob)
                response_dict = {"RefId": self.guid, "Message": 'Blob Download Successfully',
                                "absolute_path": self.local_source_file}
                logging.info(response_dict)
                return response_dict
        except Exception as e:
            #ended_at = datetime.now()
            self.delete_dir()
            raise Exception(str(e))


    """
        |//////////////////////////////////////////////////////////////////////////////////////////////////|
        |              Upload File from App Service Plan temp directory to Blob Storage                    |
        |//////////////////////////////////////////////////////////////////////////////////////////////////|
    
    """            
    def upload_blob(self, contr: str, blob_path: str, temp_file_abs_path: str, blob_name: str):
        #started_at = datetime.now()
        try:
            blob_srv = self.connect_azure()
            blob_client_instance = blob_srv.get_blob_client(contr, blob_path + "/" + blob_name)
            blob_path = '@' + contr + '/' + blob_path + '/' + blob_name
            with open(temp_file_abs_path, mode="rb") as data:
                blob_client_instance.upload_blob(data, overwrite=True)
            response_dict = {"RefId": self.guid, "Message": 'Blob Upload Successfully',
                            "absolute_path": temp_file_abs_path, "blob_path": blob_path}
            logging.info(response_dict)
            return response_dict
        except Exception as e:
            # ended_at = datetime.now()
            self.delete_dir()
            raise Exception(str(e)) 
        
    """
        |//////////////////////////////////////////////////////////////////////////////////////////////////|
        |              Upload Log File from App Service Plan root folder to Blob Storage                   |
        |//////////////////////////////////////////////////////////////////////////////////////////////////|
    
    """            
    # def upload_log(self, contr: str, blob_path: str, temp_file_abs_path: str, blob_name: str):
    #     #started_at = datetime.datetime.now()
    #     try:
    #         blob_srv = self.connect_azure()
    #         blob_client_instance = blob_srv.get_blob_client(contr, blob_path + "/" + blob_name)
    #         blob_path = '@' + contr + '/' + blob_path + '/' + blob_name
    #         with open(temp_file_abs_path, mode="rb") as data:
    #             blob_client_instance.upload_blob(data, overwrite=True)
    #         response_dict = {"RefId": self.guid, "Message": 'Blob Upload Successfully',
    #                         "absolute_path": temp_file_abs_path, "blob_path": blob_path}
    #         logging.info(response_dict)
    #         return response_dict
    #     except Exception as e:
    #         # ended_at = datetime.now()
    #         self.delete_dir()
    #         raise Exception(str(e)) 
        
    def upload_log(self,log_data_string,blob_name,blob_path,destination_container_name):
        try:
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            # container_client = blob_service_client.get_container_client(self.container_name)
            var_blob_name = f"{blob_name}"
            blob_client = blob_service_client.get_blob_client(destination_container_name,blob_path + "/" + var_blob_name)

            # with open(var_blob_name,"w") as f:
            #     f.write(log_data_dict)
            # f.close()
            # with open(var_blob_name, mode="rb") as data:
            #     blob_client.upload_blob(data, overwrite=True)           
            
            blob_client.upload_blob(log_data_string, overwrite=True)
        except Exception as e:
            logging.error(str(e)) 
        return "Log Uploaded Successfully"
        
    """
        |//////////////////////////////////////////////////////////////////////////////////////////////////|
        |              Delete Temp Directory                                                               |
        |//////////////////////////////////////////////////////////////////////////////////////////////////|
    
    """            
    def delete_dir(self):        
        if self.temp_directory is not None:
            if os.path.exists(self.temp_directory): 
                shutil.rmtree(self.temp_directory)
                del_loc_dict = {"Type": "Success", "Location": self.temp_directory}
            else:
                del_loc_dict = {"Type": "Success", "Location": self.temp_directory,"Msg":"File already deleted"} 
        return  del_loc_dict  
    
    
    def copy_and_delete(self):
        source_container_name = "nonprdfocusbisto"
        destination_container_name_archive = "nonprdfocusbisto/ABLOG_FINAL/Cylindrical_Oil/Archive Folder"
        # Get today's date
        today = datetime.date.today()

        # Calculate tomorrow's date by adding one day to today
        logging.info("getting yesterday's date for folder naming")
        yesterday = today - datetime.timedelta(days=1)

        # Format tomorrow's date as a string in "YYYY-MM-DD" format
        logging.info("Initializing Destination folder and blob names")
        destination_folder = yesterday.strftime("%Y-%m-%d")
        source_blob_names = ["ABLOG_FINAL/Cylindrical_Oil/Incremental_Data/CO_Incremental_Data.csv"]

        try:
            # Initialize Azure Blob Service Clients for source and destination containers
            logging.info("Initializing Function")
            logging.info("Initializing Function")
            blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            logging.info("Initializing Blob Service Clients")
            logging.info("Initializing Blob Service Clients")
            source_container_client = blob_service_client.get_container_client(source_container_name)
            destination_container_client = blob_service_client.get_container_client(destination_container_name_archive)

            # Copy files from source to destination folder and delete them from the source container
            logging.info("Copying files from source to destination folder")
            for blob_name in source_blob_names:
                source_blob_client = source_container_client.get_blob_client(blob_name)
                destination_blob_name = f"{destination_folder}/{'CO_Incremental_Data.csv'}"
                destination_blob_client = destination_container_client.get_blob_client(destination_blob_name)

                result = destination_blob_client.start_copy_from_url(source_blob_client.url)
                
                if(result['copy_status']=='success'):
                    # Delete the source blob after copying
                    logging.info("Deleting source blob")
                    source_blob_client.delete_blob()

            logging.info("Files copied and deleted successfully")
        except Exception as e:
            logging.exception(str(e))
        
        return "Files copied and deleted successfully."
    

# if __name__ == '__main__':
#     obj_DataLakeUtility = DataLakeUtility()

#     temp_final_file_loc = os.path.join(obj_DataLakeUtility.temp_directory,'qrt_final.csv')
#     final_data_after_merging_last_14_days.to_csv(temp_final_file_loc, index=False)

#     final_file_upload = obj_DataLakeUtility.upload_blob(contr=destination_container_name, blob_path="QRTFILE_FINAL",temp_file_abs_path=temp_final_file_loc,
#                                                     blob_name="qrt_final.csv")



