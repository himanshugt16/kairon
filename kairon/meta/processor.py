import asyncio
import json
from typing import Text, List
from urllib.parse import urljoin
import requests
from kairon import Utility
from kairon.shared.rest_client import AioRestClient
from urllib.parse import quote


class MetaProcessor:

    def __init__(self, access_token: Text, catalog_id:Text):
        self.catalog_id = catalog_id
        self.meta_url = Utility.environment['meta']['url']
        self.access_token = access_token
        self.headers = {}
        self.processed_data = []

    def preprocess_data(self, data: list, method: Text):
        for item in data:
            transformed_item = {"retailer_id": item["id"]}

            if method == "UPDATE":
                transformed_item["data"] = {}
                if "title" in item:
                    transformed_item["data"]["name"] = item["title"]
                if "description" in item:
                    transformed_item["data"]["description"] = item["description"]
                if "availability" in item:
                    transformed_item["data"]["availability"] = item["availability"]
                if "condition" in item:
                    transformed_item["data"]["condition"] = item["condition"]
                if "link" in item:
                    transformed_item["data"]["url"] = item["link"]
                if "image_link" in item:
                    transformed_item["data"]["image_url"] = item["image_link"]
                if "price" in item:
                    transformed_item["data"]["price"] = int(item["price"])
                if "brand" in item:
                    transformed_item["data"]["brand"] = item["brand"]

            else:
                transformed_item["data"] = {
                    "name": item["title"],
                    "currency": "INR",
                    "description": item["description"],
                    "availability": item["availability"],
                    "condition": item["condition"],
                    "url": item["link"],
                    "image_url": item["image_link"],
                    "price": int(item["price"]),
                    "brand": item["brand"],
                }

            transformed_item["method"] = method
            transformed_item["item_type"] = "PRODUCT_ITEM"
            self.processed_data.append(transformed_item)

        return self.processed_data

    def preprocess_delete_data(self, remaining_ids: List):
        """
        Creates a payload for deleting stale records from the catalog.

        Args:
            remaining_ids: List of primary keys that need to be deleted.

        Returns:
            Dict: Payload containing the list of delete operations.
        """
        return [{"retailer_id": id, "method": "DELETE"} for id in remaining_ids]

    async def push_meta_catalog(self, processed_data: list):
        """
        Sync the data to meta when event type is 'push_menu'
        """
        try:

            req = quote(json.dumps(self.processed_data))
            base_url = "https://graph.facebook.com/v21.0/1880697869060042/batch"
            url = f"{base_url}?item_type=PRODUCT_ITEM&requests={req}"

            data = {
                "access_token": self.access_token,
            }

            try:
                response = await asyncio.to_thread(requests.post, url, headers={}, data=data)
                response.raise_for_status()
                print("Status Code:", response.status_code)
                print("Response JSON:", response.json())
                print("Successfully synced push menu data to meta.")
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err}")
            except requests.exceptions.RequestException as req_err:
                print(f"An error occurred: {req_err}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
        except Exception as e:
            print(f"Error syncing push menu data: {e}")
            raise e

    async def update_meta_catalog(self, processed_data: list):
        """
        Sync the data to meta when event type is 'push_menu'
        """
        try:
            req = quote(json.dumps(self.processed_data))
            base_url = "https://graph.facebook.com/v21.0/1880697869060042/batch"
            url = f"{base_url}?item_type=PRODUCT_ITEM&requests={req}"

            data = {
                "access_token": self.access_token,
            }

            try:
                response = await asyncio.to_thread(requests.post, url, headers={}, data=data)
                response.raise_for_status()
                print("Status Code:", response.status_code)
                print("Response JSON:", response.json())
                print("Successfully synced push menu data to meta.")
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err} - Response: {response.text}")
            except requests.exceptions.RequestException as req_err:
                print(f"An error occurred: {req_err}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
        except Exception as e:
            print(f"Error syncing push menu data: {e}")
            raise e


    async def delete_meta_catalog(self, delete_payload: list):
        """
        Sync the data to meta when event type is 'push_menu'
        """
        try:

            req = quote(json.dumps(delete_payload))
            base_url = "https://graph.facebook.com/v21.0/1880697869060042/batch"
            url = f"{base_url}?requests={req}"

            data = {
                "access_token": self.access_token,
            }

            try:
                response = await asyncio.to_thread(requests.post, url, headers={}, data=data)
                response.raise_for_status()
                print("Status Code:", response.status_code)
                print("Response JSON:", response.json())
                print("Successfully deleted data from meta.")
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err}")
            except requests.exceptions.RequestException as req_err:
                print(f"An error occurred: {req_err}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
        except Exception as e:
            print(f"Error deleting data from meta: {e}")
            raise e