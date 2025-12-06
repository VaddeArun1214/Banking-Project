import logging
import json
import os
import io
from urllib.parse import urlparse

import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient


def main(myQueueItem: bytes):
    logging.info("Service Bus Trigger started")

    # -------------------------
    # 1. Decode Service Bus msg
    # -------------------------
    message_body = myQueueItem.decode("utf-8")
    data = json.loads(message_body)

    blob_url = data["blob_url"]
    logging.info(f"Blob URL received: {blob_url}")

    # -----------------------------------------
    # 2. Extract container + blob name from URL
    # -----------------------------------------
    parsed = urlparse(blob_url)
    path_parts = parsed.path.split("/")

    container_name = path_parts[1]
    blob_name = "/".join(path_parts[2:])

    logging.info(f"Container: {container_name}, Blob: {blob_name}")

    # -------------------------
    # 3. Read blob CSV content
    # -------------------------
    blob_service = BlobServiceClient.from_connection_string(
        os.environ["AzureWebJobsStorage"]
    )
    blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)

    stream = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(stream))

    logging.info(f"Rows read: {len(df)}")

    # -------------------------
    # 4. Clean data
    # -------------------------
    df = df.drop_duplicates()
    df = df.fillna("")

    # -------------------------
    # 5. Insert into Cosmos DB
    # -------------------------
    cosmos = CosmosClient(os.environ["COSMOS_CONN"])
    database = cosmos.get_database_client(os.environ["COSMOS_DB_NAME"])
    
    # Insert into Processeddata
    container = database.get_container_client("Processeddata")

    rows = df.to_dict(orient="records")

    for row in rows:
        row["id"] = str(row["TransactionID"])       # required by Cosmos
        container.upsert_item(row)

    logging.info("Data inserted into Cosmos DB successfully")
