import os
import time
from azure.storage.blob import BlobServiceClient, BlobClient

from log_utils import log


def main(**kwargs):
    target_conn_string = kwargs['target_conn_string']
    target_container_name = kwargs['target_container_name']
    input_dir = kwargs['input_dir']

    target_client = BlobServiceClient.from_connection_string(conn_str=target_conn_string)
    target_container = target_client.get_container_client(target_container_name)

    estensioni = kwargs['estensioni'] if 'estensioni' in kwargs else None

    for file in os.listdir(input_dir):
        if estensioni and file.split('.')[-1] not in estensioni: continue

        tic = time.clock()
        log.info(f"Processing file: {file}")
        try:
            with open(os.path.join(input_dir, file), 'rb') as fb:
                target_container.upload_blob(file, fb)

            toc = time.clock()
            time_ela = toc - tic
            log.info("Upload completed in {} seconds.".format(str(time_ela)))
        except:
            log.error(f"Failed to upload {file}")


if __name__ == "__main__":
    # MANDATORY
    target_conn_string = ""
    target_container_name = "container1"
    input_dir = "to_upload"

    # OPTIONAL
    estensioni = ['zip']

    main(
        target_conn_string=target_conn_string,
        target_container_name=target_container_name,
        temp_dir=input_dir,
        estensioni=estensioni
    )