import azure.functions as func
import logging

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="blobforcsv/arriving/{name}.csv",
                               connection="accountforcsvtosql_STORAGE") 
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob - here I made a change vol2"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
