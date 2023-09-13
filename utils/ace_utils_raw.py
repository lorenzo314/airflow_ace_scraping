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
def initialize_date_and_directory_path():
    """
    Developed to test the DAG in local environment
    TODO: it will be necessary to settle the args issue in DAGS

    The code is from the check_passes_arguments function for the
    automatic download case
    """

    # default: download the daily file
    start_date = datetime.now()

    end_date = None

    raw_directory_path = \
        "/home/lorenzo/spaceable/airflow_ace_scraping/raw_data"

    silver_directory_path = \
        "/home/lorenzo/spaceable/airflow_ace_scraping/silver_data"

    source = "https://services.swpc.noaa.gov/text/"
    # address Arnaud

    monthly = None

    # A DECORATED FUNCTION SHOULD RETURN A DICTIONARY,
    # OTHERWISE IT GIVES ERRORS
    return {
        "start_date": start_date,
        "end_date": end_date,
        "source": source,
        "raw_directory_path": raw_directory_path,
        "silver_directory_path": silver_directory_path,
        "monthly": monthly,
    }


@task()
def get_dates_in_time_interval(passed_arguments_dict: dict):
    def serialize_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError("Type not serializable")

    """
    Get the dates over the selected time interval.
    For manual mode, frequency is either month or day.
    For automatic mode, frequency is day.
    For automatic mode, the right time bound is equal to the left time bound.

    Parameters
    ----------
    passed_arguments_dict : a dictionary containing
    1 start_date : datetime.datetime
        the left bound for generating scraping time interval.
    2 end_date : datetime.datetime
        Right bound for generating scraping time interval.
    3 monthly : bool
        If True, frequency is month (manual mode).
        If False, frequency is day.
        If False and end_date is None,
        the right time bound is equal to the left time bound

    Returns
    -------
    A dictionary containing the 3 keys in input plus
    dates_in_time_interval : DatetimeIndex
    The range of equally spaced time points between start_date and end_date.
    """

    start_date = passed_arguments_dict["start_date"]
    end_date = passed_arguments_dict["end_date"]
    monthly = passed_arguments_dict["monthly"]

    if monthly is True:
        dates_in_time_interval = pd.date_range(
            start_date, end_date, freq='MS'
        )
    else:
        if end_date is None:
            end_date = start_date

        dates_in_time_interval = pd.date_range(
            start_date, end_date, freq='d'
        )

    # Transform the DatetimeIndex type in a type that can be serialized
    dates_in_time_interval = list(dates_in_time_interval)
    tmp_list = [i.to_pydatetime() for i in dates_in_time_interval]
    dates_in_time_interval = \
        [json.dumps(i, default=serialize_datetime) for i in tmp_list]

    passed_arguments_dict["dates_in_time_interval"] = dates_in_time_interval

    return passed_arguments_dict


@task
def get_measuring_devices(passed_arguments_dict: dict):
    measuring_devices = ['mag', 'swepam', 'epam', 'sis']

    passed_arguments_dict["measuring_devices"] = measuring_devices

    return passed_arguments_dict


@task
def get_bucket_name(passed_arguments_dict: dict):
    bucket_name = "v3mhxhemaey1ohkyil8v"

    passed_arguments_dict["bucket_name"] = bucket_name

    return passed_arguments_dict


@task()
def define_url_format(passed_arguments_dict: dict):
    """
    Build complete urls for each date and each measuring device,
    with format depending on the data source.

    Parameters
    ----------
    passed_arguments_dict : dict
        Contains:
        1: Website address for data scraping.
        2: dates_in_time_interval : DatetimeIndex
        Range of equally spaced time points between start_date and end_date
        3: measuring_devices : list
        A list of measuring devices: mag', 'swepam', 'epam', 'sis'.
        4: directory_path : str
        The directory path

    Raises
    ------
    ValueError
        Warning about potential mix-up between sources and file_name format.

    Returns
    -------
    list_url : list
        A list of all the complete urls for each date and each measuring devices.
    """

    source = passed_arguments_dict["source"]
    dates_in_time_interval = passed_arguments_dict["dates_in_time_interval"]
    measuring_devices = passed_arguments_dict["measuring_devices"]
    directory_path = passed_arguments_dict["raw_directory_path"]

    list_url = []
    for device in measuring_devices:
        # Next 4 lines because on one url, the device is called "magnetometer",
        # and on the other "mag"
        if device in "mag":
            nasa_device = "magnetometer"
        else:
            nasa_device = device

        for date in dates_in_time_interval:
            # for automatic download
            # ONLY AUTOMATIC DOWNLOAD IMPLEMENTED FOR NOW
            if source == 'https://services.swpc.noaa.gov/text/':
                # date_format = "%Y-%m-%d_%H-%M-%S"
                # formatted_date = date.strftime(date_format)
                remote_file_name = "ace-" + nasa_device + ".txt"
                url = source + remote_file_name
                list_url.append(url)
            # for manual download of monthly files
            elif source == "https://sohoftp.nascom.nasa.gov/sdb/goes/ace/monthly/":
                date_format = "%Y%m"
                interval = "1h"
                formatted_date = date.strftime(date_format)
                file_name = \
                    formatted_date + "_ace_" + device + "_" + interval + ".txt"

                # create list of url for files not in the directory
                if not os.path.isfile(directory_path + device + '/' + file_name):
                    url = source + file_name
                    list_url.append(url)
            # for manual download of daily files
            elif source == "https://sohoftp.nascom.nasa.gov/sdb/goes/ace/daily/":
                date_format = "%Y%m%d"
                interval = get_interval(device)
                formatted_date = date.strftime(date_format)
                file_name = \
                    formatted_date + "_ace_" + device + "_" + interval + ".txt"

                # create list of url for files not in the directory
                if not os.path.isfile(
                        directory_path + device + '/' + file_name):
                    url = source + file_name
                    list_url.append(url)
            else:
                raise ValueError("Your local source does match any default sources,"
                                 " please check sources ")

    passed_arguments_dict["list_url"] = list_url

    return passed_arguments_dict


@task()
def download_data(passed_arguments_dict: dict):
    """
    Download data files from all the urls into the saving directories.
    Track download progress

    Parameters
    ----------
    passed_arguments_dict: dict
    A dictionary containing
        1 list_url : list
            arg of all the complete urls for each date and each measuring devices.
        2 directory_path : str
            Path pointing to the directories where files are saved.
        3 start_date : datetime.datetime
            Left bound for generating scraping time interval.
            Used to build the file_name in automatic mode.
        4 monthly : bool, optional
            DESCRIPTION. The default is False.
            NOT USED IN AUTOMATIC MODE

    Returns
    -------
    Write .txt files and save them in the selected directories.
    """

    start_date = passed_arguments_dict["start_date"]
    directory_path = passed_arguments_dict["raw_directory_path"]
    list_url = passed_arguments_dict["list_url"]

    output_files = []

    if list_url:
        for url in tqdm(list_url):
            check_url = is_url(url)
            if not check_url:
                print(url + " doesn't exist")
            else:
                if "daily" in directory_path or "monthly" in directory_path:
                    # manual case
                    # NOT IMPLEMENTED FOR NOW
                    file_name = url.split('/')[-1]
                    # device = file_name.split("_")[2]
                else:
                    # automatic case
                    date_format = "%Y%m%dT%H%M%S"
                    formatted_date = start_date.strftime(date_format)
                    device = re.split(r'[\-.]+', url)[-2]
                    file_name = formatted_date + "_donnees_" + device + ".txt"

                # Name of output file
                outname = os.path.join(directory_path, file_name)

                # Creates an empty file with that name
                # exist_ok=True by default, but specify it anyway
                # It means that if it already exists it does not give an error
                pathlib.Path(outname, exist_ok=True).touch()

                # grab the file
                request.urlretrieve(url, outname)

                output_files.append(outname)

    passed_arguments_dict["output_files"] = output_files

    return passed_arguments_dict


def is_url(url):
    """
    Check if the passed url exists or not

    INPUT:
    param: url (str)
    url to test

    OUTPUT: True if the url exists, False if not
    """
    r = requests.get(url)
    if r.status_code == 429:
        print('Retry URL checking (429)')
        time.sleep(5)
        return is_url(url)
    elif r.status_code == 404:
        return False
    else:
        return True


@task()
def save_passed_arguments_locally(passed_arguments_dict: dict):
    date_time = datetime.now()
    str_date_time = date_time.strftime("%d%m%YT%H%M%S")
    str_date_time = f"{str_date_time}.txt"
    output_file = os.path.join(
        passed_arguments_dict["raw_directory_path"],
        str_date_time
    )

    with open(output_file, "w") as file:
        if passed_arguments_dict["start_date"] is not None:
            file.write(f'start_date {passed_arguments_dict["start_date"].strftime("%m/%d/%Y")}\n')
        else:
            file.write(f'start_date {str(None)}\n')

        if passed_arguments_dict["end_date"] is not None:
            file.write(
                f'end_date {passed_arguments_dict["end_date"].strftime("%m/%d/%Y")}\n'
            )
        else:
            file.write(f'end_date {str(None)}\n')

        if passed_arguments_dict["source"] is not None:
            file.write(f'source {passed_arguments_dict["source"]}\n')
        else:
            file.write(f'source {str(None)}\n')

        if passed_arguments_dict["raw_directory_path"] is not None:
            file.write(f'directory_path {passed_arguments_dict["raw_directory_path"]}\n')
        else:
            file.write(f'directory_path {str(None)}\n')

        if passed_arguments_dict["dates_in_time_interval"] is not None:
            dates_in_time_interval = passed_arguments_dict["dates_in_time_interval"]
            assert type(dates_in_time_interval) == list
            for i in dates_in_time_interval:
                assert type(i) == str
                file.write(f'dates_in_time_interval {i}\n')
        else:
            file.write(f'dates_in_time_interval {str(None)}\n')

        if passed_arguments_dict["measuring_devices"] is not None:
            measuring_devices = passed_arguments_dict["measuring_devices"]
            assert type(measuring_devices) == list
            for i in measuring_devices:
                file.write(f'measuring device {i}\n')
        else:
            file.write(f'measuring devices {str(None)}\n')

        if passed_arguments_dict["list_url"] is not None:
            list_url = passed_arguments_dict["list_url"]
            assert type(list_url) == list
            for i in list_url:
                file.write(f'url {i}\n')
        else:
            file.write(f'list url {str(None)}\n')

        if passed_arguments_dict["output_files"] is not None:
            output_files = passed_arguments_dict["output_files"]
            assert type(output_files) == list
            for i in output_files:
                file.write(f'output file {i}\n')
        else:
            file.write(f'output_files {str(None)}\n')

    return passed_arguments_dict


# Copied from
# spaceable\python-storage\samples\snippets\storage_upload_file.py
@task()
def upload_raw(passed_arguments_dict: dict):
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
    output_files = passed_arguments_dict["output_files"]

    print('_______________________________')
    for i in output_files:
        print(i)
    print('_______________________________')

    # TODO; fill
    # destination_blob_name = .....

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for i in output_files:
        destination_blob_name = i
        blob = bucket.blob(destination_blob_name)

        generation_match_precondition = 0

        source_file_name = i

        blob.upload_from_filename(
            source_file_name,
            if_generation_match=generation_match_precondition
        )

        print(
            f"File {source_file_name} uploaded to {destination_blob_name}."
        )

    return passed_arguments_dict

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    # generation_match_precondition = 0

    # blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)
    #
    # print(
    #     f"File {source_file_name} uploaded to {destination_blob_name}."
    # )
