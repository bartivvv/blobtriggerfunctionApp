import azure.functions as func
import logging
import pyodbc

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="blobforcsv/arriving/{name}.csv",
                               connection="accountforcsvtosql_STORAGE") 
def blob_trigger(myblob: func.InputStream, context: func.Context):
    sql_connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:serverforsqltolearn.database.windows.net,1433;Database=outputdatabase;Uid=bartix381;Pwd=Krzychui123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    csv_content = myblob.read().decode('utf-8').splitlines()
 # Connect to SQL Database
    conn = pyodbc.connect(sql_connection_string)
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

    # Log information about the processed blob
    logging.info(f"Python blob trigger function processed blob")
