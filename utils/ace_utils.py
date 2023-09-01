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

    directory_path = \
        "/home/lorenzo/spaceable/airflow_ace_scraping/test_data"

    source = "https://services.swpc.noaa.gov/text/"
    # address Arnaud

    monthly = None

    # A DECORATED FUNCTION SHOULD RETURN A DICTIONARY,
    # OTHERWISE IT GIVES ERRORS
    return {
        "start_date": start_date,
        "end_date": end_date,
        "source": source,
        "directory_path": directory_path,
        "monthly": monthly
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
    directory_path = passed_arguments_dict["directory_path"]

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
    directory_path = passed_arguments_dict["directory_path"]
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
        passed_arguments_dict["directory_path"],
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

        if passed_arguments_dict["directory_path"] is not None:
            file.write(f'directory_path {passed_arguments_dict["directory_path"]}\n')
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

#############################################################################
#
# ORIGINAL CODE
#
#############################################################################


def check_passed_arguments(argv):
    """
    Check the number of passed arguments.
    Check if the date arguments have the right format.
    Check if the correct download directory is used is manual mode.
    Raise error if not.
    Program runs in automatic mode if no dates/path arguments are passed.
    Program runs in manual mode if 2 dates and a path are passed as arguments.
    Change type of arguments, return website address for data scraping
    and directory path according to the chosen scraping mode.

    Parameters
    ----------
    argv : str
        0th argument is the name of the script, it is dropped
        1st and 2nd arguments are optional and refer to the bounds of a time interval.
        3rd argument is optional and refers to the directory to save the data.

    Raises
    ------
    ValueError
        Error messages and recommendation to user.

    Returns
    -------

    start_date : datetime.datetime
        Left bound for generating scraping time interval.
    end_date : datetime.datetime
        Right bound for generating scraping time interval.
    source : str
        website address for data scraping.
    directory_path : str
        Path pointing to the parent directory (set to be in gitlab in automatic mode )
        where the directories are created.
    monthly : bool
        Used in manual mode to distinguish between download of monthly data and download of daily data.
    """

    #  Pass command-line arguments to the script.
    arguments = argv[1:]

    print("ARGUMENTS", arguments)
    print(len(arguments))

    if len(arguments) != 0 and 3 < len(arguments) < 6:
        raise ValueError(
            "Wrong number of arguments passed, expected 0, or 2 dates and a save_path,"
            " or 2 dates a save_path and a list of SWE features,"
            " or 2 dates a save_path, a list of SWE features and a sampling frequency"
        )
    # No argument besides "main.py" is passed,
    # default: download the daily file
    elif len(arguments) == 0:
        start_date = datetime.now()
        end_date = None

        directory_path = pathlib.Path(__file__).parent.parent / 'data'

        source = "https://services.swpc.noaa.gov/text/"
        # address Arnaud
        # Automatic download, the only one implemented

        monthly = None
    else:
        start_date, end_date, source, directory_path, monthly =\
            None, None, None, None, None
        # TODO: fix the rgs issue, for now we assume no args
        # aux = pathlib.Path(__file__).parent.parent / "data"
        # if pathlib.Path(aux) / "data_aggregating" in arguments[2] \
        #         or pathlib.Path(aux) / "data_scraping" in arguments[2]:
        #     raise ValueError("You cannot use default directory in manual mode,"
        #                      " please select another directory")
        # elif not os.path.isdir(arguments[2]):
        #     raise ValueError("Path does not exist")
        # else:
        #     try:
        #         start_date = datetime.strptime(arguments[0], "%Y-%m")
        #         end_date = datetime.strptime(arguments[1], "%Y-%m")
        #         source = "https://sohoftp.nascom.nasa.gov/sdb/goes/ace/monthly/"
        #         monthly = True
        #     except ValueError:
        #         try:
        #             start_date = \
        #                 datetime.strptime(arguments[0], "%Y-%m-%d_%H:%M:%S")
        #             end_date = \
        #                 datetime.strptime(arguments[1], "%Y-%m-%d_%H:%M:%S")
        #             source = \
        #                 "https://sohoftp.nascom.nasa.gov/sdb/goes/ace/daily/"
        #             monthly = False
        #         except ValueError:
        #             raise ValueError(
        #                 "Could not parse date,"
        #                 " expected format is : YYYY-MM-dd_HH:MM:SS"
        #                 " (ex: 2022-06-22_13:52:45)")
        #
        #     if (start_date > datetime.now()) and (end_date > datetime.now()):
        #         raise ValueError("Neither dates has come yet")
        #
        #     if start_date > end_date:
        #         raise ValueError("start_date should be before end date")
        #
        #     if end_date > datetime.now():
        #         end_date = datetime.now()
        #         print("end_date has not come yet,"
        #               " data is downloaded from start_date"
        #               "to most recent date online")
        #
        #     directory_path = arguments[2]

    return start_date, end_date, source, directory_path, monthly


# def create_directory(directory_path, measuring_devices, monthly=False):
#     """
#     For automatic mode: creates data/ directory to save scraped data files from source and one directory
#     to save processed (aggregated) data for each measuring device
#     For manual mode in the directory passed as parameters, creates
#     monthly / and a daily / directories, creates subdirectories
#     for each measuring device in these directories
#
#     Parameters
#     ----------
#     directory_path : str
#         Path pointing to the parent directory (set to be in gitlab in automatic mode )
#         where the directories are created.
#     measuring_devices : list
#         A list of measuring devices: mag', 'swepam', 'epam', 'sis'
#     monthly : bool
#         Default value is false, True when date arguments are passed in month format
#
#     Returns
#     -------
#     data_scraping_directory : str
#         Return parents directories of the measuring devices directories
#
#     """
#     # create a new directory for each measuring device
#     for device in measuring_devices:
#         if device in "mag":
#             nasa_device = "magnetometer"
#         else:
#             nasa_device = device
#         # automatic mode
#         if directory_path == pathlib.Path(__file__).parent.parent / 'data':
#
#             # Create directory to scrape data
#             data_scraping_directory = str(directory_path) + '/data_scraping/'
#             # data_device = os.path.join(data_scraping_directory, device, "")
#             data_device = os.path.join(data_scraping_directory, nasa_device, "")
#             if not os.path.exists(data_device):
#                 os.makedirs(data_device)
#
#             # Create directory to process data
#             data_aggregating_directory = str(directory_path) + '/data_aggregating/'
#             data_device = os.path.join(data_aggregating_directory, nasa_device, "")
#             if not os.path.exists(data_device):
#                 os.makedirs(data_device)
#
#         # manual mode
#         else:
#             if monthly:
#                 data_scraping_directory = str(directory_path) + '/monthly/'
#             else:
#                 data_scraping_directory = str(directory_path) + '/daily/'
#
#             data_device = os.path.join(data_scraping_directory, device, "")
#             if not os.path.exists(data_device):
#                 os.makedirs(data_device)
#
#     return data_scraping_directory

# def get_dates_in_time_interval(start_date, end_date, monthly):
#     """
#     Get the dates over the selected time interval.
#     For manual mode, frequency is either month or day.
#     For automatic mode, frequency is day.
#     For automatic mode, the right time bound is equal to the left time bound.
#
#     Parameters
#     ----------
#     start_date : datetime.datetime
#         Left bound for generating scraping time interval.
#     end_date : datetime.datetime
#         Right bound for generating scraping time interval.
#     monthly : bool
#         If True, frequency is month (manual mode).
#         If False, frequency is day.
#         If False and end_date is None,
#         the right time bound is equal to the left time bound
#
#     Returns
#     -------
#     dates_in_time_interval : DatetimeIndex
#         Return the range of equally spaced time points between start_date and end_date.
#
#     """
#     if monthly is True:
#         dates_in_time_interval = pd.date_range(start_date, end_date, freq='MS')
#     else:
#         if end_date is None:
#             end_date = start_date
#
#         dates_in_time_interval = pd.date_range(start_date, end_date, freq='d')
#
#     return dates_in_time_interval


# def define_url_format(source, dates_in_time_interval, measuring_devices, directory_path):
#     """
#     Build complete urls for each date and each measuring device,
#     with format depending on the data source.
#
#     Parameters
#     ----------
#     source : str
#         Website address for data scraping.
#     dates_in_time_interval : DatetimeIndex
#         Range of equally spaced time points between start_date and end_date
#     measuring_devices : list
#         A list of measuring devices: mag', 'swepam', 'epam', 'sis'.
#     directory_path : str
#         The directory path
#
#     Raises
#     ------
#     ValueError
#         Warning about potential mix-up between sources and file_name format.
#
#     Returns
#     -------
#     list_url : list
#         A list of all the complete urls for each date and each measuring devices.
#     """
#
#     list_url = []
#     for device in measuring_devices:
#         # Next 4 lines because on one url, the device is called "magnetometer",
#         # and on the other "mag"
#         if device in "mag":
#             nasa_device = "magnetometer"
#         else:
#             nasa_device = device
#
#         for date in dates_in_time_interval:
#             # for automatic download
#             if source == "https://services.swpc.noaa.gov/text/":
#                 # date_format = "%Y-%m-%d_%H-%M-%S"
#                 # formatted_date = date.strftime(date_format)
#                 remote_file_name = "ace-" + nasa_device + ".txt"
#                 url = source + remote_file_name
#                 list_url.append(url)
#
#             # for manual download of monthly files
#             elif source == "https://sohoftp.nascom.nasa.gov/sdb/goes/ace/monthly/":
#                 date_format = "%Y%m"
#                 interval = "1h"
#                 formatted_date = date.strftime(date_format)
#                 file_name = \
#                     formatted_date + "_ace_" + device + "_" + interval + ".txt"
#
#                 # create list of url for files not in the directory
#                 if not os.path.isfile(directory_path + device + '/' + file_name):
#                     url = source + file_name
#                     list_url.append(url)
#
#             # for manual download of daily files
#             elif source == "https://sohoftp.nascom.nasa.gov/sdb/goes/ace/daily/":
#                 date_format = "%Y%m%d"
#                 interval = get_interval(device)
#                 formatted_date = date.strftime(date_format)
#                 file_name = \
#                     formatted_date + "_ace_" + device + "_" + interval + ".txt"
#
#                 # create list of url for files not in the directory
#                 if not os.path.isfile(
#                         directory_path + device + '/' + file_name):
#                     url = source + file_name
#                     list_url.append(url)
#             else:
#                 raise ValueError("Your local source does match any default sources,"
#                                  " please check sources ")
#
#     return list_url

# def is_url(url):
#     """
#     Check if the passed url exists or not
#
#     INPUT:
#     :param: url (str)      url to test
#
#     OUTPUT: True if the url exists, False if not
#     """
#     r = requests.get(url)
#     if r.status_code == 429:
#         print('Retry URL checking (429)')
#         time.sleep(5)
#         return is_url(url)
#     elif r.status_code == 404:
#         return False
#     else:
#         return True
