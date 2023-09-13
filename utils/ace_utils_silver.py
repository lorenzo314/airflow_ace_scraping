# ONLY AUTOMATIC DOWNLOAD IMPLEMENTED FOR NOW
# NO ARGUMENTS PASSED TO THE MAIN FUNCTION
# TODO: Implement manual download
# TODO: Settle the args issue in DAGS
# TODO: Clarify if it is possible to use DAG params with TaskFlow

import pathlib
import pandas as pd
import json
import os
import os.path
from tqdm.auto import tqdm
from datetime import datetime
from urllib import request
import re
import requests

from airflow.decorators import task

# Necessary to do pip install --upgrade google-cloud-storage
# See https://stackoverflow.com/questions/50840511/google-cloud-import-storage-cannot-import-storage
from google.cloud import storage


@task()
def aggregate_all_devices(passed_arguments_dict: dict):
    measuring_devices = passed_arguments_dict["measuring_devices"]
    silver_directory_path = passed_arguments_dict["silver_directory_path"]

    rootlist = ["_donnees_epam.txt",
                "_donnees_magnetometer.txt",
                "_donnees_sis.txt",
                "_donnees_swepam.txt"
                ]

    x = list_blobs(passed_arguments_dict)

    print("NUM BLOBS")
    print(len(x))

    blob_names = []
    for i in x:
        for j in rootlist:
            if j in i:
                blob_names.append(i)

    print("BLOBS USED")
    for i in blob_names:
        print(i)

    storage_client = storage.Client()

    bucket = storage_client.bucket(passed_arguments_dict["bucket_name"])

    silver_data_file_list = []

    for device in measuring_devices:
        if device in 'mag':
            nasa_device = "magnetometer"
        else:
            nasa_device = device

        print("BLOBS")
        print(nasa_device)

        lines_by_day = {}
        header = None
        count = 0

        for source_blob_name in blob_names:
            if nasa_device in source_blob_name:
                print(source_blob_name)
                _, name = os.path.split(source_blob_name)
                blob = bucket.blob(source_blob_name)
                destination_file_name = os.path.join(silver_directory_path, name)
                blob.download_to_filename(destination_file_name)

                if count == 0:
                    header = get_header_from_file(destination_file_name)
                    count += 1

                with open(destination_file_name) as f:
                    for line in f:
                        if not line.startswith(("#", ":")):
                            measurement_date = line[:10]
                            if measurement_date not in lines_by_day:
                                lines_by_day[measurement_date] = [line]
                            else:
                                old_list = lines_by_day[measurement_date]
                                old_list.append(line)
                                old_list = deduplicate_list(old_list)
                                old_list.sort()
                                lines_by_day[measurement_date] = old_list

        # Use dictionary to write files
        output_path = silver_directory_path
        for measurement_date in lines_by_day:
            measurement_date_with_dash = measurement_date.replace(" ", "-")
            foo = device + "_" + measurement_date_with_dash + ".txt"
            path = os.path.join(output_path, foo)

            print("Writing output", path)

            if os.path.isfile(path):
                os.remove(path)

            write_aggregated_file(path, header, lines_by_day[measurement_date])

            silver_data_file_list.append(path)

    passed_arguments_dict["silver_output_files"] = silver_data_file_list

    return passed_arguments_dict


@task()
def upload_silver(passed_arguments_dict: dict):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    # BUCKET NAME RANDOMLY GENERATED
    #
    # v3mhxhemaey1ohkyil8v
    #
    ##################################

    # TODO: fill
    bucket_name = passed_arguments_dict["bucket_name"]

    # There are several files to upload, one for each instrument
    output_files = passed_arguments_dict["silver_output_files"]

    print('____UPLOADING SILVER FILES___________')
    for i in output_files:
        print(i)
    print('_______________________________')

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    print(type(bucket))

    for i in output_files:
        destination_blob_name = i
        print(destination_blob_name)
        blob = bucket.blob(destination_blob_name)

        generation_match_precondition = 0
        source_file_name = i

        try:
            blob.upload_from_filename(
                source_file_name,
                if_generation_match=generation_match_precondition
            )

            print(
                f"File {source_file_name} uploaded to {destination_blob_name}."
            )
        except:
            print(f"Could not upload {source_file_name}")

    return passed_arguments_dict


def get_header_from_file(file):
    """
    Retrieve header from a file

    Parameters
    ----------
    file : str
        One of the files in a device directory.

    Returns
    -------
    header : list of str
        List of string containing the lines of the header.

    """

    header = []
    with open(file) as f:
        for line in f:
            if line.startswith(("#", ":")):
                header.append(line)
    return header


def deduplicate_list(input_list):
    """
    Take a list of all data lines for a given day and remove duplicates

    Parameters
    ----------
    input_list : list
        list of all data lines for a given day.

    Returns
    -------
    List
        List of data lines without duplicates.

    """
    return list(dict.fromkeys(input_list))


def write_aggregated_file(path, header, data_lines):
    """
    Write file with a header at the beginning followed by lines of data

    Parameters
    ----------
    path : str
        Complete path name to the aggregated file.
    header : list of str
        List of string containing the lines of the header.
    data_lines : list
        A list of all lines containing data.

    Returns
    -------
    Write the file.

    """

    with open(path, "w") as file:
        for header_lines in header:
            file.writelines(header_lines)
        for data_line in data_lines:
            file.writelines(data_line)


# Taken from
# \\wsl$\Ubuntu-22.04\home\lorenzo\spaceable\python-storage\samples\snippets\storage_list_files.py
def list_blobs(passed_arguments_dict: dict):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    bucket_name = passed_arguments_dict["bucket_name"]

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    # Note: The call returns a response only when the iterator is consumed.
    ret_blobs_names = []
    for blob in blobs:
        # print(blob.name)
        ret_blobs_names.append(blob.name)

    return ret_blobs_names
