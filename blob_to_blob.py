import os
import time
from azure.storage.blob import BlobServiceClient, BlobClient

from log_utils import log


def main(**kwargs):
    source_conn_string = kwargs['source_conn_string']
    target_conn_string = kwargs['target_conn_string']
    source_container_name = kwargs['source_container_name']
    target_container_name = kwargs['target_container_name']

    temp_dir = kwargs['temp_dir'] if 'temp_dir' in kwargs else '.'
    estensioni = kwargs['estensioni'] if 'estensioni' in kwargs else None

    target_client = BlobServiceClient.from_connection_string(conn_str=target_conn_string)
    target_container = target_client.get_container_client(target_container_name)

    source_client = BlobServiceClient.from_connection_string(conn_str=source_conn_string)
    source_container = source_client.get_container_client(source_container_name)

    # CYCLE OVER BLOB
    for input_blob in source_container.list_blobs():
        blob_name = input_blob['name']
        if estensioni and blob_name.split('.')[-1] not in estensioni:
            log.debug(f"Skipping blob {blob_name} for unsupported extension")
            continue

        log.info(f"Processing blob: {blob_name}...")
        file_name = os.path.join(temp_dir, blob_name)
        blob = source_container.get_blob_client(blob=blob_name)
        try:
            tic = time.clock()
            with open(file_name, "wb") as my_blob:
                my_blob.write(blob.download_blob().readall())

            time_ela = toc - tic
            log.info("Blob saved to temporary directory in {} seconds.".format(str(time_ela)))
        except Exception as e:
            log.error(f"Failed to save {blob_name}, skipping upload.")
            log.debug(str(e))
            continue

        log.info("Writing blob to target container...")
        try:
            tic = time.clock()
            with open(file_name, "rb") as my_blob:
                target_container.upload_blob(blob_name, my_blob)
            toc = time.clock()
            time_ela = toc - tic
            log.info("Upload completed in {} seconds.".format(str(time_ela)))
        except Exception as e:
            log.error(f"Failed to upload {blob_name}")
            log.debug(str(e))


if __name__ == "__main__":
    # MANDATORY
    ## Source info
    source_conn_string = ""
    source_container_name = ""
    ## Destination info
    target_container_name = ""
    target_conn_string = ""

    # OPTIONAL
    temp_dir = "to_upload"
    estensioni = ['zip']

    main(
        source_conn_string=source_conn_string,
        target_conn_string=target_conn_string,
        source_container_name=source_container_name,
        target_container_name=target_container_name,
        temp_dir=temp_dir,
        estensioni=estensioni
    )
