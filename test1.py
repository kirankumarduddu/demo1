import requests
import json
from azure.storage.blob import ContainerClient

# Replace this with your Azure API endpoint
AZURE_API_ENDPOINT = 'https://qtitfhirpoc1-srvc1.fhir.azurehealthcareapis.com'

# Replace this with your Azure client ID
AZURE_CLIENT_ID = '963d6e4f-2755-4ee7-8707-7b7904a2bedf'

# Replace this with your Azure client secret
AZURE_CLIENT_SECRET = 'omI8Q~veHhuUsrgsz7YYn.BHTJDbNpG2XGAopa9Q'

# Get an access token from Azure's auth endpoint
def get_access_token():
    url = f'https://login.microsoftonline.com/511cb4e0-c8a3-472f-8b65-e47ddb154beb/oauth2/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': AZURE_CLIENT_ID,
        'client_secret': AZURE_CLIENT_SECRET,
        'resource': AZURE_API_ENDPOINT,
    }
    response = requests.post(url, data=data)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(f'Error getting access token: {response.text}')

# Set the URL for the FHIR converter service
converter_url = "https://qtitfhirpoc1-srvc1.fhir.azurehealthcareapis.com/$convert-data"

# Set the connection string for the Azure Blob storage account
connection_string = 'DefaultEndpointsProtocol=https;AccountName=test4cloudshell4qti;AccountKey=huXa7rYBaB0aotwqbdOyikGlLo0dwgf0FZ5UieLI6Ekl8kjOkqUAJ9IkxYqNCYwJSMVIaOWSPTs5+AStQj4IWA==;EndpointSuffix=core.windows.net'

# Connect to the Azure Blob storage account
container_client = ContainerClient.from_connection_string(connection_string, "ccda-xml-input")

# Connect to the Azure Blob storage account
output_container_client = ContainerClient.from_connection_string(connection_string, "export-fhir")

# Connect to the Azure Blob storage account for error logs
error_container_client = ContainerClient.from_connection_string(connection_string, "error")

# Get a list of all the blobs in the container and print the blob names
blob_list = container_client.list_blobs()
print("Blob list")

# Function to convert CCDA XML to FHIR JSON and upload to output container
def convert_and_upload():
    print("Starting convert_and_upload function")
    # Get a list of all the blobs in the container and print the blob names
    blob_list = list(container_client.list_blobs())
    # Iterate over the list of blobs
    for blob in blob_list:
        print(f"Processing blob {blob.name}")
        # Check if the blob has a XML file extension
        if blob.name.endswith(".xml"):
            # Download the CCDA XML file from the Blob container
            blob_client = container_client.get_blob_client(blob.name)
            xml = blob_client.download_blob().readall().decode('utf-8')
            # Encode the XML data as base64
            xml_base64 = xml.encode('utf-8').decode('utf-8')

            # Set up the parameters for the conversion
            parameters = {
                "resourceType": "Parameters",
                "parameter": [
                    {
                        "name": "inputData",
                        "valueString": xml_base64
                    },
                    {
                        "name": "inputDataType",
                        "valueString": "Ccda"
                    },
                    {
                        "name": "templateCollectionReference",
                        "valueString": "fhirconvertor.azurecr.io/fhirconvertor@sha256:adc0a4c1a2f74bd28014dfe5566196a3e3aa801ae4ec85429956927383a66b03"
                    },
                    {
                        "name": "rootTemplate",
                        "valueString": "CCD"
                    },
                ]
            }
            headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {get_access_token()}'
            }

            # Send the conversion request to the Azure Container Registry
            response = requests.post(converter_url, data=json.dumps(parameters), headers=headers)
            print(response.text)

            # Check the response status code to make sure the conversion was successful
            if response.status_code == 200:
                # Get the FHIR JSON data from the response
                fhir_json = response.json()

                # Upload the FHIR JSON data to the output container
                output_blob_client = output_container_client.get_blob_client(blob.name.replace(".xml", ".json"))
                output_blob_client.upload_blob(json.dumps(fhir_json), overwrite=True)
            else:
                # If there was an error converting the CCDA XML to FHIR JSON, write the error message to a file
                with open("error.txt", "w") as file:
                    file.write(f'Error converting CCDA XML to FHIR JSON: {response.text}')
                    # Upload the error message to the error container
                    blob_name = blob.name.replace(".xml", ".txt")
                    index = 0
                    while True:
                        try:
                            error_container_client = ContainerClient.from_connection_string(connection_string, "error")
                            error_blob_client = error_container_client.get_blob_client(blob_name)
                            error_blob_client.upload_blob(f'Error converting CCDA XML to FHIR JSON: {response.text}'.encode('utf-8'))
                            break
                        except Exception as e:
                            index += 1
                            blob_name = f"{blob.name.replace('.xml', '')}_{index}.txt'"
                            print(f"Error uploading error message to blob storage: {e}")
                            print(f"Trying again with blob name {blob_name}")
        else:
            # If the blob is not a XML file, write an error message to a file
            with open("error.txt", "w") as file:
                file.write(f'Error: {blob.name} is not a XML file')
                # Upload the error message to the error container
                blob_name = blob.name.replace(".xml", ".txt")
                index = 0
                while True:
                    try:
                        error_container_client = ContainerClient.from_connection_string(connection_string, "error")
                        error_blob_client = error_container_client.get_blob_client(blob_name)
                        error_blob_client.upload_blob(f'Error: {blob.name} is not a XML file'.encode('utf-8'))
                        break
                    except Exception as e:
                        index += 1
                        blob_name = f"{blob.name.replace('.xml', '')}_{index}.txt'"
                        print(f"Error uploading error message to blob storage: {e}")
                        print(f"Trying again with blob name {blob_name}")

# Call the convert_and_upload function
convert_and_upload()

