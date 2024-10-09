import requests
import yaml
import json
import logging
import os

POSTMAN_API_KEY = os.getenv("POSTMAN_API_KEY")
OPENAPI_SPEC_PATH = os.getenv("OPENAPI_SPEC_PATH")
WORKSPACE_NAME = os.getenv("WORKSPACE_NAME")
COLLECTION_NAME = "MOSTLY AI"
BASE_URL = "https://api.getpostman.com"
headers = {
    "X-API-Key": POSTMAN_API_KEY,
    "Content-Type": "application/json"
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

def get_workspace_id(name):
    logger.info(f"Searching for workspace '{name}'")
    response = requests.get(f"{BASE_URL}/workspaces", headers=headers)
    workspaces = response.json()["workspaces"]
    for workspace in workspaces:
        if workspace["name"].lower() == name.lower():
            logger.info(f"Workspace '{name}' found with ID: {workspace['id']}")
            return workspace["id"]
    raise Exception(f"Workspace '{name}' not found")

def check_if_collection_exists(name, workspace_id):
    logger.info(f"Searching for collection '{name}' in workspace '{workspace_id}'")
    response = requests.get(f"{BASE_URL}/collections?workspace={workspace_id}", headers=headers)
    collections = response.json()["collections"]
    for collection in collections:
        if collection["name"] == name:
            logger.info(f"Collection '{name}' found with ID: {collection['id']}")
            return collection["id"]
    return None

def import_openapi_spec(spec, workspace_id):
    logger.info(f"Importing OpenAPI spec to workspace '{workspace_id}'")
    payload = {
        "type": "string",
        "input": json.dumps(spec),
        "options": {
            "folderStrategy": "Tags"
        }
    }
    response = requests.post(f"{BASE_URL}/import/openapi?workspace={workspace_id}", headers=headers, json=payload)
    result = response.json()
    if 'error' in result:
        raise Exception(f"Error importing OpenAPI spec: {result['error']['message']}")

    collection_id = result["collections"][0]["id"]
    logger.info(f"Succesfully imported OpenAPI spec with ID: {collection_id}")    
    return collection_id

def reorganize_folders(collection_id):
    collection_url = f"{BASE_URL}/collections/{collection_id}"
    collection_response = requests.get(collection_url, headers=headers)
    collection = collection_response.json()

    logger.info(f"Reorganizing folders in collection '{collection_id}'")
    generators_source_tables_folder = find_folder(collection, "Source Tables")
    if generators_source_tables_folder:
        move_folder(collection, "Source Columns", generators_source_tables_folder['id'])
        move_folder(collection, "Source Foreign Keys", generators_source_tables_folder['id'])

    generators_folder = find_folder(collection, "Generators")
    synthetic_datasets_folder = find_folder(collection, "Synthetic Datasets")
    move_folder(collection, "Source Tables", generators_folder['id'])
    move_folder(collection, "Generator Training", generators_folder['id'])
    move_folder(collection, "Synthetic Tables", synthetic_datasets_folder['id'])
    move_folder(collection, "Synthetic Generation", synthetic_datasets_folder['id'])

    update_url = f"{BASE_URL}/collections/{collection_id}"
    logger.info(f"Updating collection '{collection_id}' with reorganized folders")
    update_response = requests.put(update_url, headers=headers, json=collection)
    
    if update_response.status_code != 200:
        raise Exception(f"Error updating collection: {update_response.text}. Status code: {update_response.status_code}. You can try deleting the collection and reimporting the OpenAPI spec using this script from scratch.")
    return

def find_folder(collection, folder_name):
    for item in collection['collection']['item']:
        if item['name'] == folder_name:
            logger.info(f"Folder '{folder_name}' found with ID: {item['id']}")
            return item
    logger.warning(f"Folder '{folder_name}' not found")
    return None

def move_folder(collection, folder_name, parent_folder_id):
    folder_to_move = None
    logger.info(f"Moving folder '{folder_name}' to folder '{parent_folder_id}'")
    for index, item in enumerate(collection['collection']['item']):
        if item['name'] == folder_name:
            folder_to_move = item
            logger.info(f"Folder '{folder_name}' found at index {index}. Deleting it from root")
            del collection['collection']['item'][index]
            break
    
    if folder_to_move:
        for item in collection['collection']['item']:
            if item['id'] == parent_folder_id:
                logger.info(f"Folder '{folder_name}' added to folder '{parent_folder_id}'")
                item['item'].append(folder_to_move)
                break

def delete_collection(collection_id):
    logger.info(f"Deleting collection '{collection_id}'")
    response = requests.delete(f"{BASE_URL}/collections/{collection_id}", headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error deleting collection: {response.text}. Status code: {response.status_code}")
    return

if __name__ == "__main__":
    try:
        with open(OPENAPI_SPEC_PATH, 'r') as file:
            openapi_spec = yaml.safe_load(file)
        workspace_id = get_workspace_id(WORKSPACE_NAME)
        collection_id = check_if_collection_exists(COLLECTION_NAME, workspace_id)
        if collection_id:
            delete_collection(collection_id)
        collection_id = import_openapi_spec(openapi_spec, workspace_id)
        reorganize_folders(collection_id)

    except Exception as e:
        print(f"An error occurred: {str(e)}")