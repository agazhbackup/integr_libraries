# Databricks notebook source
# MAGIC %md
# MAGIC This script allows you to autheticate with Power BI API and execute get/post/delete requests.
# MAGIC
# MAGIC You can trigger this notebooks with `dbutils.notebook.run()` or from workflows with specified parameters
# MAGIC
# MAGIC API response can be passed on to other notebooks.
# MAGIC
# MAGIC !! Works with EE PBI API Service principal.
# MAGIC
# MAGIC Requirements:
# MAGIC - azure-identity package
# MAGIC - lp14dv key vault
# MAGIC
# MAGIC Link to MS API documentation: https://docs.microsoft.com/en-us/rest/api/power-bi

# COMMAND ----------

from azure.identity import ClientSecretCredential
import requests
import io
import json
import ast

# COMMAND ----------

### EE PBI API Service principal attributes

tenant_id = '3596192b-fdf5-4e2c-a6fa-acb706c963d8'
client_id = 'ab68979e-6b50-424c-8488-96ad83da5282'
client_secret = dbutils.secrets.get(scope = "lp14dv", key = "powerbi-api-client-secret")

# COMMAND ----------

# get authentication token

auth_class = ClientSecretCredential(tenant_id = tenant_id, client_id = client_id, client_secret = client_secret)
token = auth_class.get_token("https://analysis.windows.net/powerbi/api/.default").token

# COMMAND ----------

### widgets - fill in necessary 

dbutils.widgets.text('groupId', '')
dbutils.widgets.text('datasetId', '')
dbutils.widgets.text('reportId', '')
dbutils.widgets.text('refreshId', '')
dbutils.widgets.text('api_uri', '')
dbutils.widgets.text('headers', "{'key': 'value'}")
dbutils.widgets.text('body', "{'key': 'value'}")
dbutils.widgets.text('tables', '', label="e.g.: Customer, Products")
dbutils.widgets.dropdown('api_method', 'post',  ['post', 'get', 'delete', 'refresh_pbi', 'cancel_pbi_refresh'])
dbutils.widgets.dropdown('refresh_mode', 'enhanced',  ['enhanced', 'regular'])

groupId = dbutils.widgets.get('groupId')
datasetId = dbutils.widgets.get('datasetId')
reportId = dbutils.widgets.get('reportId')
refreshId = dbutils.widgets.get('refreshId')
api_uri = dbutils.widgets.get('api_uri')
headers = dbutils.widgets.get('headers')
body = dbutils.widgets.get('body')
tables = [table.strip() for table in dbutils.widgets.get('tables').split(",")]
api_method = dbutils.widgets.get('api_method')
refresh_mode = dbutils.widgets.get('refresh_mode')

# COMMAND ----------

def check_tables_list(lst):
  """
  Help function to check if tables parameter is blank
  """
  return not (len(lst) == 1 and lst[0] == "")

# COMMAND ----------

### do not change Authorization key in header

header_auth = {'Authorization': f'Bearer {token}'}

# COMMAND ----------

# convert string headers and body to dict

headers = ast.literal_eval(headers)
body = ast.literal_eval(body)

# concatenate auth and other headers

headers = {**header_auth, **headers}

# COMMAND ----------

### replace placeholders with actual values

api_uri = api_uri.format(groupId = groupId, datasetId=datasetId, reportId=reportId, refreshId=refreshId)

# COMMAND ----------

### function for refreshing Power BI dataset

def refresh_pbi(groupId = groupId, datasetId = datasetId, tables = tables, refresh_mode=refresh_mode, **kwargs):
  
  uri = 'https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes'.format(groupId=groupId, datasetId=datasetId)
  
  if refresh_mode == 'enhanced':

    body_func = {'retryCount': 2, 'type': 'Full'}
    body_func = {**body, **body_func}

    if check_tables_list(tables):
      init_list = []
  
      for table in tables:
        init_list += [{'table': table}]

      body_tables = {'objects': init_list}
      body_func = {**body_func, **body_tables}

  elif refresh_mode == 'regular':
    body_func = {}
  
  else:
    raise ValueError('refresh_mode can be either enhanced or regular')
  
  response = requests.post(uri, headers=headers, json=body_func)
    
  return response

# COMMAND ----------

### function for cancelling refresh of Power BI dataset

def cancel_pbi_refresh(groupId = groupId, datasetId = datasetId, **kwargs):
  
  get_uri = 'https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes'.format(groupId=groupId, datasetId=datasetId)

  get_response = requests.get(get_uri, headers=headers, json=body).json()

  get_response_in_progress = []
    
  for i in [x for x in get_response['value'] if x['status'] == 'Unknown']: 
    get_response_in_progress.append(i['requestId'])
      
  for i in get_response_in_progress:

    uri = 'https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/refreshes/{refreshId}'.format(groupId=groupId, datasetId=datasetId, refreshId=i)
    response = requests.delete(uri, headers=headers, json=body)
    
  return response

# COMMAND ----------

### takes method from widgets and assigns valid function

switcher = {
  'get': requests.get,
  'post': requests.post,
  'delete': requests.delete,
  'refresh_pbi': refresh_pbi,
  'cancel_pbi_refresh': cancel_pbi_refresh
}

# COMMAND ----------

if api_method.lower() in switcher:
  
  response = switcher[api_method.lower()](url=api_uri, headers=headers, json=body)
  
else:
  
  dbutils.notebook.exit('Provide valid method. Supported requests methods: [get, post, delete]; supported custom functions: [refresh_pbi, cancel_pbi_refresh]')

# COMMAND ----------

### reponse can passed on to other notebooks

try: 
  response_json = response.json()
except Exception as e: 
  response_json = ""

if response.status_code in [200, 202]:
  
  dbutils.notebook.exit(response_json)
  
else:
  
  response_json = {'value': [{'status': 'Failed', 'text': response.text}]}
  
  dbutils.notebook.exit(response_json)