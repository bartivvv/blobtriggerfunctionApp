import azure.functions as func
import logging
import os
import pymssql
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

app = func.FunctionApp()

# Define Key Vault and secret names
secret_name_username = "passwordDB"
secret_name_password = "usernameDB"

# Create a SecretClient using the DefaultAzureCredential
def get_key_vault_secret(secret_name):
    key_vault_url = "https://keyvaultlearnblobtrigger.vault.azure.net/" # Replace with your Key Vault URL
    credential = DefaultAzureCredential()

    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
    secret_value = secret_client.get_secret(secret_name).value

    return secret_value

@app.blob_trigger(arg_name="myblob", path="blobforcsv/arriving/{name}.csv",
                               connection="accountforcsvtosql_STORAGE") 
def blob_trigger(myblob: func.InputStream, context: func.Context):

    # env_variable_name = 'SQLCONNSTR_MyDbConnectionString'
    
    # sql_connection_string = os.environ.get(env_variable_name)

    # logging.info(f"DB string: {sql_connection_string}")

    # logging.info("Environment Variables:")
    # for key, value in os.environ.items():
    #     logging.info(f"{key} = {value}")
    
    logging.info("Starting Azure Function execution")

    try:

        csv_content = myblob.read().decode('utf-8').splitlines()
    # Connect to SQL Database
        server = 'serverforsqltolearn.database.windows.net'
        database = 'outputdatabase'
        username = get_key_vault_secret(secret_name_username)
        password = get_key_vault_secret(secret_name_password)
        conn = pymssql.connect(server=server, user=username, password=password, database=database)
        # conn = pyodbc.connect(sql_connection_string)
        cursor = conn.cursor()

        # Assuming your CSV has headers that match the database table columns
        headers = csv_content[0].split(',')
        columns = ', '.join(headers)

        # Process CSV data and insert into SQL Database
        for row in csv_content[1:]:
            values = ', '.join([f"'{value}'" for value in row.split(',')])
            query = f"INSERT INTO TestingTable ({columns}) VALUES ({values})"
            cursor.execute(query)

        # Commit changes and close connections
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error(manually): {str(e)}")

    # Log information about the processed blob
    logging.info(f"Python blob trigger function finished")
