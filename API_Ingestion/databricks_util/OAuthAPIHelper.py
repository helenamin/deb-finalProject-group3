# Databricks notebook source
def getOAuthToken(user,password,token_url):
  import requests
  import json
  import base64

  # Populate these values with the values obtained during OAuth setup. 
  message = user +':'+password
  message_bytes = message.encode('ascii')
  base64_bytes = base64.b64encode(message_bytes)
  AUTH = 'Basic ' + base64_bytes.decode('ascii')

  # Prepare the payload for the REST request to obtain JWT authorization token. 
  HEADERS = {'Authorization':AUTH,
           'Content-Type':'application/x-www-form-urlencoded'
          }
  PAYLOAD = 'grant_type=client_credentials'

  # Get OAuth JWT token.
  response = requests.request("POST", token_url, headers=HEADERS, data=PAYLOAD)

  # Store JWT token value in string object for later use.
  TOKEN = json.loads(response.text)['access_token']
  return TOKEN
  

# COMMAND ----------

def getOAuthTokenandType(user,key,token_url):
  import requests
  import json
  import base64

  # Populate these values with the values obtained during OAuth setup. 
  password = dbutils.secrets.get(scope='azure-key-vault',key=key)
  message = user +':'+password
  message_bytes = message.encode('ascii')
  base64_bytes = base64.b64encode(message_bytes)
  AUTH = 'Basic ' + base64_bytes.decode('ascii')

#   # Prepare the payload for the REST request to obtain JWT authorization token. 
#   HEADERS = {'Authorization':AUTH,
#            'Content-Type':'application/x-www-form-urlencoded'
#           }
#   PAYLOAD = 'grant_type=client_credentials'

#   # Get OAuth JWT token.
#   response = requests.request("POST", token_url, headers=HEADERS, data=PAYLOAD)

#   # Store JWT token value in string object for later use.
#   TOKEN = json.loads(response.text)['access_token']
#   TOKEN_TYPE = json.loads(response.text)['token_type']
  return message

# COMMAND ----------


