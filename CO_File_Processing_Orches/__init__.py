import logging
import json
import asyncio
import azure.functions as func
import azure.durable_functions as df

# define the asynchronous orchestrator function
def orchestrator_function(context: df.DurableOrchestrationContext):
    # call the activity function asynchronously
    yield context.call_activity("CO_File_Processing_Act", "Activity Started")

# create the main orchestrator
main = df.Orchestrator.create(orchestrator_function)

