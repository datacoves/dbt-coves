from typing import Dict
import requests, json, os
from requests.exceptions import RequestException
from dotenv import load_dotenv

load_dotenv()

# Dev imports
import ipdb

class DbtCovesExtractException(Exception):
    pass

DBT_CMD_LIST_SOURCES = "dbt ls --resource-type source"

try:
    airbyte_host = os.environ['AIRBYTE_API_HOST']
    airbyte_port = os.environ['AIRBYTE_API_PORT']
    airbyte_api_root = os.environ['AIRBYTE_API_ROOT']
    airbyte_api_base_endpoint = f"http://{airbyte_host}:{airbyte_port}/{airbyte_api_root}"
except KeyError as e:
    raise DbtCovesExtractException(f'Missing env variable for configuration: {e}')

print(airbyte_api_base_endpoint)




def airbyte_api_call(
        endpoint: str, call_keyword: str, request_body: Dict[str, str] = None
    ):
        """
        Generic `api caller` for contacting Airbyte
        """
        try:
            if request_body is not None:
                response = requests.post(endpoint, json=request_body)
            else:
                response = requests.post(endpoint)

            if response.status_code == 200:
                response_json = json.loads(response.text)[call_keyword]
                if len(response_json) <= 1:
                    return response_json[0] or False
                else:
                    return response_json
            else:
                raise RequestException(
                    f"Unexpected status code from airbyte: {response.status_code}"
                )
        except RequestException as e:
            raise DbtCovesExtractException("Airbyte API error: " + e)


