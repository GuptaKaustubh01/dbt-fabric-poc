# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ###### **Imports libraries for authentication, Azure Key Vault access, Fabric integration, HTTP requests, and concurrency utilities.**
# ###### 

# CELL ********************

import msal
import requests
import sempy.fabric as fabric

from concurrent.futures import ThreadPoolExecutor

import os
import fsspec
import requests
from datetime import datetime
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Retrieves the current Fabric workspace ID using the workspace name from the environment.**

# CELL ********************

def get_workspace_id_by_name(workspace_name):
    """
    To get the workspace_id by passing workspace name.
   
    Args:
        String: workspace_name
       
    Returns:
        String: workspace_id
    """

    workspaces = fabric.list_workspaces()
    workspace_row = workspaces[workspaces.Name == workspace_name]
    workspace_id = workspace_row['Id'].iloc[0]
    
    return workspace_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_workspace_id():
    """
    To get the workspace_id by passing workspace name.
   
    Args:
        String: workspace_name
       
    Returns:
        String: workspace_id
    """

    workspace_name = mssparkutils.env.getWorkspaceName()
    workspaces = fabric.list_workspaces()
    workspace_row = workspaces[workspaces.Name == workspace_name]
    workspace_id = workspace_row['Id'].iloc[0]
    
    return workspace_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Retrieves the current Fabric notebook's workspace name and ID.**

# CELL ********************

def get_current_workspace_details():
    """
    To get the workspace_id and workspace_name of the notebook.
   
    Args:
        none
       
    Returns:
        String: workspace_id
        String: workspace_name
    """
    workspaces_df = fabric.list_workspaces()
    workspaces_list = workspaces_df["Name"].tolist()
    current_workspace_id = fabric.get_notebook_workspace_id()  
    workspace_row = workspaces_df[workspaces_df.Id == current_workspace_id]
    current_workspace_name = workspace_row['Name'].iloc[0]
    return current_workspace_name,current_workspace_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Determines the current environment (dev/stage/prod) based on the Fabric workspace name.**

# CELL ********************

# def get_environment():
#     """
#     To get the lakehouse_id by passing lakehouse_name within the workspace.
   
#     Args:
#         String: name of the lakehouse
       
#     Returns:
#         String: lakehouse_id
#     """
#     workspaces_df = fabric.list_workspaces()
#     workspaces_list = workspaces_df["Name"].tolist()
#     current_workspace_id = fabric.get_notebook_workspace_id()  
#     workspace_row = workspaces_df[workspaces_df.Id == current_workspace_id]
#     current_workspace_name = workspace_row['Name'].iloc[0]

#     dev_workspaces = [   'ws-cn-data-platform-dev-bronze'
#                         , 'ws-cn-data-platform-dev-silver'
#                         , 'ws-cn-data-platform-dev-gold'
#                         , 'parameterTesting-Dev']

#     stage_workspaces = [    'ws-cn-data-platform-stage-bronze'
#                         , 'ws-cn-data-platform-stage-silver'
#                         , 'ws-cn-data-platform-stage-gold'
#                         , 'parameterTesting-Stage' ]

#     prod_workspaces = [    'ws-cn-data-platform-prod-bronze'
#                         , 'ws-cn-data-platform-prod-silver'
#                         , 'ws-cn-data-platform-prod-gold']
#     if current_workspace_name in prod_workspaces:
#         environment = 'prod'
#     elif current_workspace_name in stage_workspaces:
#         environment = 'stage'
#     else:
#         environment = 'dev'
#     return environment


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_environment():
    # instead of listing all workspaces
    workspace_display_name = notebookutils.runtime.context.get("currentWorkspaceName", "")
    
    if "prod" in workspace_display_name.lower():
        return "prod"
    elif "stage" in workspace_display_name.lower():
        return "stage"
    else:
        return "dev"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

curr_env = get_environment()
base_uri = f"https://kv-cn-dp-{curr_env}-gwc-001.vault.azure.net/"
if curr_env == 'stage':
    base_uri = f"https://kv-cn-dp-stg-gwc-001.vault.azure.net/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Retrieves service principal credentials (tenant ID, client ID, client secret) securely from Azure Key Vault.**

# CELL ********************

tenant_id = mssparkutils.credentials.getSecret(base_uri,'service-principal-tenant-id')
client_id = mssparkutils.credentials.getSecret(base_uri,'service-principal-client-id')
client_secret = mssparkutils.credentials.getSecret(base_uri,'service-principal-client-secret')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Generates and returns a Fabric API authentication bearer token using MSAL with service principal credentials.**

# CELL ********************

def generate_token():    
    """
    To generate the bearer token for Fabric Authentication.
   
    Args:
        None
       
    Returns:
        Dict: headers required for authentication
    """
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential= client_secret ,
    )

    scope = ["https://api.fabric.microsoft.com/.default"]
    result = app.acquire_token_for_client(scopes=scope)

    if "access_token" not in result:
        raise Exception("Could not obtain access token")

    access_token = result["access_token"]


    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    return headers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_email_access_token():
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
    token = credential.get_token("https://graph.microsoft.com/.default")
    access_token = token.token
    headers = {"Authorization": f"Bearer {access_token}"}
    
    print("Authenticated successfully\n")
    return headers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###### **Retrieves a lakehouse ID by name within the current Fabric workspace via the Fabric REST API.**

# CELL ********************

def get_lakehouse_id(lakehouse_name):
    """
    To get the lakehouse_id by passing lakehouse_name within the workspace.
   
    Args:
        String: name of the lakehouse
       
    Returns:
        String: lakehouse_id
    """
    workspace_id = get_workspace_id()
    list_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    headers = generate_token()
    response = requests.get(list_url, headers=headers)
    fabric_items = response.json()['value']
    fabric_lakehouses = {item["displayName"]: item["id"] for item in fabric_items}
    lakehouse_id = fabric_lakehouses[lakehouse_name]
    return lakehouse_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_fabric_item_id(lakehouse_name, workspace_name):
    """
    To get the lakehouse_id by passing lakehouse_name within the workspace.
   
    Args:
        String: name of the lakehouse
       
    Returns:
        String: lakehouse_id
    """
    workspace_id = get_workspace_id_by_name(workspace_name)
    list_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    headers = generate_token()
    response = requests.get(list_url, headers=headers)
    fabric_items = response.json()['value']
    fabric_lakehouses = {item["displayName"]: item["id"] for item in fabric_items}
    lakehouse_id = fabric_lakehouses[lakehouse_name]
    return lakehouse_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
