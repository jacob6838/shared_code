import csv
import json
import uuid
import tempfile
from datetime import datetime
import logging

from azure.storage.blob import BlobServiceClient, BlobClient, generate_blob_sas, BlobSasPermissions
from azure.identity import ManagedIdentityCredential

# --------------------------------------------------------------------------------------------------------------------------------
# Create blob client
# --------------------------------------------------------------------------------------------------------------------------------
def createBlobClient(
    funcData: dict, 
    conn_str: str, 
    container_name: str, 
    file_path: str, 
    managed_credential: ManagedIdentityCredential, 
    client_type: str) -> BlobClient:

    try:
        serviceClient = BlobServiceClient.from_connection_string(conn_str, credential=managed_credential)
        blobClient = serviceClient.get_blob_client(container=container_name, blob=file_path)

        return blobClient
    except Exception as _e:
        raise RuntimeError('Exiting due to unable to create {:s} blob client: {:s}. Error message: {:s}'.format(client_type, json.dumps(funcData), str(_e))) from _e


# --------------------------------------------------------------------------------------------------------------------------------
# Download file from blob client
# --------------------------------------------------------------------------------------------------------------------------------
def downloadFile(funcData: dict, blobClient: BlobClient) -> str:
    try:
        localFileName = getUniqueTempFileName()
        with open(localFileName, 'wb') as download_file:
            download_file.write(blobClient.download_blob().readall())
        return localFileName
    except Exception as _e:
        raise RuntimeError('Exiting due to unable to download source file: {:s}. Error message: {:s}'.format(json.dumps(funcData), str(_e))) from _e


# --------------------------------------------------------------------------------------------------------------------------------
# Create and upload results object
# --------------------------------------------------------------------------------------------------------------------------------
def createAndUploadResults(funcData: dict, customResults: dict, resultsBlobClient: BlobClient):
    results = createResultsObj(funcData, customResults)
    uploadResults(funcData, resultsBlobClient, results)

# --------------------------------------------------------------------------------------------------------------------------------
# Upload results data to results blob client
# --------------------------------------------------------------------------------------------------------------------------------
def uploadResults(funcData: dict, resultsBlobClient: BlobClient, results_obj: dict):
    try:
        results_string = json.dumps(results_obj, default=default_json_encoder)
        resultsBlobClient.upload_blob(results_string, overwrite=True)
    except Exception as _e:
        raise RuntimeError('Exiting due to unable to upload results file: {:s}. Error message: {:s}'.format(json.dumps(funcData), str(_e))) from _e

# --------------------------------------------------------------------------------------------------------------------------------
# override default json encoding for datetimes
# --------------------------------------------------------------------------------------------------------------------------------
def default_json_encoder(o):
    if isinstance(o, (datetime.datetime)):
        return o.strftime("%Y-%m-%dT%H:%M:%SZ")

# --------------------------------------------------------------------------------------------------------------------------------
# Create results object
# --------------------------------------------------------------------------------------------------------------------------------
def createResultsObj(funcData: dict, customResults: dict):
    results = funcData.copy()
    executionResults = customResults
    results['execution_results'] = executionResults
    return results



# --------------------------------------------------------------------------------------------------------------------------------
# Create results file name + path from funcData with format function-name/dataset/datatype/yyyy/mm/dd/file_name.ext
# --------------------------------------------------------------------------------------------------------------------------------
def generateResultsBlobPath(funcData, modifiedDate):
    functionName = funcData.get('functionName', 'unknown-function')
    dataSet = funcData.get('dataSet', 'unknown-dataset')
    dataType = funcData.get('dataType', 'unknown-data-type')
    
    time_based_path = modifiedDate.strftime("%Y/%m/%d")
    results_file_name = 'results.json'
    results_file_path = '/'.join([functionName, dataSet, dataType, time_based_path, results_file_name])
    return results_file_path

# --------------------------------------------------------------------------------------------------------------------------------
# Create unique temporary file name
# --------------------------------------------------------------------------------------------------------------------------------
def getUniqueTempFileName():
    localFile = tempfile.gettempdir() + '/' + str(uuid.uuid4())
    return localFile

# --------------------------------------------------------------------------------------------------------------------------------
# Read csv file to dict with latin encoding
# --------------------------------------------------------------------------------------------------------------------------------
def readCsvFile(local_file_name):
    csvData = list(csv.DictReader(open(local_file_name, 'r', encoding='latin')))
    return csvData