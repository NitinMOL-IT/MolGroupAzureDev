# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json
import asyncio
import azure.functions as func
import azure.durable_functions as df

# define the asynchronous orchestrator function
def orchestrator_function(context: df.DurableOrchestrationContext):
    # call the activity function asynchronously
    yield context.call_activity("DG_Blob_Activity", "Activity Started")

# create the main orchestrator
main = df.Orchestrator.create(orchestrator_function)
