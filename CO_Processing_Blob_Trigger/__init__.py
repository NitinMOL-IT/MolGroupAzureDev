import logging
import azure.functions as func
import azure.durable_functions as df

async def main(myblob: func.InputStream, starter: str) -> None:
    logging.info("Python blob trigger function processed blob")  

    client = df.DurableOrchestrationClient(starter)
    instance_id = await client.start_new('CO_File_Processing_Orches')