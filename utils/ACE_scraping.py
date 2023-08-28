import pathlib
import pandas as pd
import utils
import os
from tqdm.auto import tqdm
import requests
from datetime import datetime
from urllib import request
import re

from airflow.decorators import task


@task()
def download_data(list_url, directory_path, start_date, monthly=False):
    """
    Download data files from all the urls into the saving directories.
    Track download progress

    Parameters
    ----------
    list_url : list
        List of all the complete urls for each dates and each measuring devices.
    directory_path : str
        Path pointing to the directories where files are saved.
    start_date : datetime.datetime
        Left bound for generating scraping time interval. Used to build the file_name in automatic mode
    monthly : bool, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    Write .txt files and save them in the selected directories.

    """
    if list_url:
        for url in tqdm(list_url):
            check_url = utils.is_url(url)
            if not check_url:
                print(url + " doesn't exist")
            else:
                if ("daily" in directory_path) or ("monthly" in directory_path):
                    # manual case
                    file_name = url.split('/')[-1]
                    device = file_name.split("_")[2]
                else:
                    # automatic case
                    date_format = "%Y-%m-%d_%H-%M-%S"
                    formatted_date = start_date.strftime(date_format)
                    device = re.split(r'[\-.]+', url)[-2]
                    file_name = formatted_date + "_donnees_" + device + ".txt"

                # grab the file
                request.urlretrieve(url, directory_path + "/" + device + "/" + file_name)

# --------------------------------------------------------------------
#
# ORIGINAL CODE
#
# --------------------------------------------------------------------

# -*- coding: utf-8 -*-
"""
Created on Mon May  9 16:40:34 2022

@author: célineong
"""


# import pathlib
# # import pandas as pd
# import utils
# import os
# from tqdm.auto import tqdm
# import requests
# from datetime import datetime
# from urllib import request
# import re

def check_passed_arguments(argv):
    """
    Check the number of passed arguments. Check if the date arguments have the right format. Check if the correct download directory is used is manual mode. Raise error if not.
    Program runs in automatic mode if no dates/path arguments are passed.
    Program runs in manual mode if 2 dates and a path are passed as arguments.
    Change type of arguments, return website address for data scraping and directory path according to the chosen scraping mode.

    Parameters
    ----------
    argv : str
        0th argument is the name of the script (ACE_main.py), it is dropped
        1st and 2nd arguments are optional and refer to the bounds of a time interval.
        3rd argument is optional and refers to the directory to save the data.

    Raises
    ------
    ValueError
        Error messages and recommandation to user.

    Returns
    -------

    start_date : datetime.datetime
        Left bound for generating scraping time interval.
    end_date : datetime.datetime
        Right bound for generating scraping time interval.
    source : str
        website address for data scraping.
    directory_path : str
        Path pointing to the parent directory (set to be in gitlab in automatic mode ) where the directories are created.
    monthly : bool
        Used in manual manual mode to distinguish between download of monthly data and download of daily data.

    """

    #  Pass command-line arguments to the script.
    arguments = argv[1:]
    if ((len(arguments) !=0) and (3 < len(arguments) < 6)):
        #(len(arguments) != 3) and (len(arguments) != 4) and (len(arguments) != 5) and (len(arguments) != 6)):
        raise ValueError("Wrong number of arguments passed, expected 0, or 2 dates and a save_path, or 2 dates a save_path and a list of SWE features, or 2 dates a save_path, a list of SWE features and a sampling frequency")

    #No argument besides "main.py" is passed, default: download the daily file
    elif len(arguments) == 0:
        start_date = datetime.now()
        end_date = None
        directory_path = pathlib.Path(__file__).parent.parent / 'data'
        source = "https://services.swpc.noaa.gov/text/" # address Arnaud
        monthly = None

    else:
        if str(pathlib.Path(__file__).parent.parent / 'data' / 'data_aggregating') in arguments[2]  or str(pathlib.Path(__file__).parent.parent / 'data' / 'data_scraping') in arguments[2]:
            raise ValueError("You cannot use default directory in manual mode, please select another directory")
        elif not os.path.isdir(arguments[2]):
            raise ValueError("Path does not exist")
        else:
            try:
                start_date = datetime.strptime(arguments[0],"%Y-%m")
                end_date = datetime.strptime(arguments[1],"%Y-%m")
                source = "https://sohoftp.nascom.nasa.gov/sdb/goes/ace/monthly/"
                monthly = True
            except:
                try:
                    start_date = datetime.strptime(arguments[0],"%Y-%m-%d_%H:%M:%S")
                    end_date = datetime.strptime(arguments[1],"%Y-%m-%d_%H:%M:%S")
                    source = "https://sohoftp.nascom.nasa.gov/sdb/goes/ace/daily/"
                    monthly = False
                except ValueError:
                    raise ValueError("Could not parse date, expected format is : YYYY-MM-dd_HH:MM:SS (ex: 2022-06-22_13:52:45)")
            if (start_date > datetime.now()) and (end_date > datetime.now()):
                raise ValueError("Neither dates has come yet")
            if start_date > end_date:
                raise ValueError("start_date should be before end date")
            if end_date > datetime.now():
                end_date = datetime.now()
                print("end_date has not come yet, data is downloaded from start_date to most recent date online")
            directory_path = arguments[2]

    return start_date, end_date, source, directory_path, monthly

def create_directory(directory_path, measuring_devices, monthly=False):
    """
    For automatic mode: creates data/ directory to save scraped data files from source and one directory
    to save processed (aggregated) data for each measuring device
    For manual mode: in the passed directory, creates a monthly/ and a daily/ directories, creates subdirectories for each measuring device in these directories

    Parameters
    ----------
    directory_path : str
        Path pointing to the parent directory (set to be in gitlab in automatic mode ) where the directories are created.
    measuring_devices : list
        List of measuring devices: mag', 'swepam', 'epam', 'sis'
    monthly : bool
        Default value is false, True when date arguments are passed in month format

    Returns
    -------
    data_scraping_directory : str
        Return parents directories of the measuring devices directories

    """
    #create a new directory for each measuring device
    for device in measuring_devices:
        if device in ('mag'):
            nasa_device = "magnetometer"
        else:
            nasa_device = device
        # automatic mode
        if directory_path == pathlib.Path(__file__).parent.parent / 'data':

        # Create directory to scrape data
            data_scraping_directory = str(directory_path) + '/data_scraping/'
            #data_device = os.path.join(data_scraping_directory, device, "")
            data_device = os.path.join(data_scraping_directory, nasa_device, "")
            if not os.path.exists(data_device):
                os.makedirs(data_device)

        # Create directory to process data
            data_aggregating_directory = str(directory_path) + '/data_aggregating/'
            data_device = os.path.join(data_aggregating_directory, nasa_device, "")
            if not os.path.exists(data_device):
                os.makedirs(data_device)

        # manual mode
        else:
            if monthly:
                data_scraping_directory = str(directory_path) + '/monthly/'
            else:
                data_scraping_directory = str(directory_path) + '/daily/'

            data_device = os.path.join(data_scraping_directory, device, "")
            if not os.path.exists(data_device):
                os.makedirs(data_device)

    return data_scraping_directory


def get_dates_in_time_interval(start_date, end_date, monthly):
    """
    Get the dates over the selected time interval. For manual mode, frequency is either month or day. For automatic mode, frequency is day. For automatic mode, the right time bound is equal to the left time bound.

    Parameters
    ----------
    start_date : datetime.datetime
        Left bound for generating scraping time interval.
    end_date : datetime.datetime
        Right bound for generating scraping time interval.
    monthly : bool
        If True, frequency is month (manual mode). If False, frequency is day. If False and end_date is None, the right time bound is equal to the left time bound

    Returns
    -------
    dates_in_time_interval : DatetimeIndex
        Return the range of equally spaced time points between start_date and end_date.

    """
    if monthly==True:
        dates_in_time_interval= pd.date_range(start_date, end_date, freq = 'MS')
    else:
        if end_date == None:
            end_date = start_date
        dates_in_time_interval= pd.date_range(start_date, end_date, freq = 'd')

    return dates_in_time_interval

def define_url_format(source, dates_in_time_interval, measuring_devices, directory_path):
    """
    Build complete urls for each dates and each measuring devices, with format depending on the data source.

    Parameters
    ----------
    source : str
        Website address for data scraping.
    dates_in_time_interval : DatetimeIndex
        Range of equally spaced time points between start_date and end_date..
    measuring_devices : list
        List of measuring devices: mag', 'swepam', 'epam', 'sis'.

    Raises
    ------
    ValueError
        Warning about potential mix-up between sources and file_name format.

    Returns
    -------
    list_url : list
        List of all the complete urls for each dates and each measuring devices.

    """

    list_url = []
    for device in measuring_devices:
        # Next 4 lines because on one url, the device is called "magnetometer", and on the other "mag"
        if device in ('mag'):
            nasa_device = "magnetometer"
        else:
            nasa_device = device

        for date in dates_in_time_interval:

            # for automatic download
            if source == "https://services.swpc.noaa.gov/text/":
                date_format ="%Y-%m-%d_%H-%M-%S"
                formatted_date = date.strftime(date_format)
                remote_file_name = "ace-" + nasa_device + ".txt"
                url = source + remote_file_name
                list_url.append(url)

            # for manual download of monthly files
            elif source == "https://sohoftp.nascom.nasa.gov/sdb/goes/ace/monthly/":
                date_format ="%Y%m"
                interval = "1h"
                formatted_date = date.strftime(date_format)
                file_name =  formatted_date + "_ace_" + device + "_" + interval + ".txt"
                # create list of url for files not in the directory
                if not os.path.isfile(directory_path + device + '/' + file_name):
                    url = source + file_name
                    list_url.append(url)

            # for manual download of daily files
            elif source == "https://sohoftp.nascom.nasa.gov/sdb/goes/ace/daily/":
                date_format = "%Y%m%d"
                interval = get_interval(device)
                formatted_date = date.strftime(date_format)
                file_name =  formatted_date + "_ace_" + device + "_" + interval + ".txt"
                # create list of url for files not in the directory
                if not os.path.isfile(directory_path + device + '/' + file_name):
                    url = source + file_name
                    list_url.append(url)

            else:
                raise ValueError("Your local source does match any default sources, please check sources ")

    return  list_url

def download_data(list_url, directory_path, start_date, monthly=False):
    """
    Download data files from all the urls into the saving directories. Track download progress

    Parameters
    ----------
    list_url : list
        List of all the complete urls for each dates and each measuring devices.
    directory_path : str
        Path pointing to the directories where files are saved.
    start_date : datetime.datetime
        Left bound for generating scraping time interval. Used to build the file_name in automatic mode
    monthly : bool, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    Write .txt files and save them in the selected directories.

    """
    if list_url:
        for url in tqdm(list_url):
            check_url = utils.is_url(url)
            if not check_url:
               print(url + " doesn't exist")
            else:
                if ("daily" in directory_path) or ("monthly" in directory_path):
                    # manual case
                    file_name = url.split('/')[-1]
                    device = file_name.split("_")[2]
                else:
                    # automatic case
                    date_format ="%Y-%m-%d_%H-%M-%S"
                    formatted_date = start_date.strftime(date_format)
                    device = re.split(r'[\-.]+', url)[-2]
                    file_name = formatted_date +"_donnees_"+ device + ".txt"

                # grab the file
                request.urlretrieve(url, directory_path + "/" + device + "/" + file_name)

def get_interval(device):
    """
    Get the time interval of the measuring device

    Parameters
    ----------
    device : str
        Measuring device(s) of ACE (mag/swepam/epam/sis).

    Returns
    -------
    str
        String of the interval.

    """
    if device in ('mag', 'swepam'):
        return '1m'
    else:
        return '5m'