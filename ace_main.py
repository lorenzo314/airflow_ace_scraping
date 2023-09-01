import pendulum

from airflow.decorators import dag

import ace_utils as au


@dag(
    schedule="35 8 * * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="UTC"),
    catchup=False,
    tags=["ace_scraping"]
)
def ace_scraping():
    # @task()
    # def print_dates(passed_arguments_dict_p: dict):
    #     print("hello")
    #     dates_in_time_interval_p = passed_arguments_dict_p["dates_in_time_interval"]
    #     print("--------------------------")
    #     for d in dates_in_time_interval_p:
    #         print(d)
    #         print(type(d))
    #         print("--------------------------")
    #     print("--------------------------")
    #     print(type(dates_in_time_interval_p))
    #     print(len(dates_in_time_interval_p))
    #     print("--------------------------")



    # Get the start and en dates and the directory path
    # Retrieves the current day as start date and One as end date since
    # this is done in automatic mode
    # The directory path is a local one
    passed_arguments_dict = au.initialize_date_and_directory_path()

    # Get the date range to query
    # In automatic mode it is just the current date
    passed_arguments_dict = \
        au.get_dates_in_time_interval(passed_arguments_dict)

    # Get the measuring devices
    passed_arguments_dict = \
        au.get_measuring_devices(passed_arguments_dict)

    # Get the list of URLs
    passed_arguments_dict = au.define_url_format(passed_arguments_dict)

    # Download the data
    passed_arguments_dict = au.download_data(passed_arguments_dict)

    # Save parameters on local file
    au.save_passed_arguments_locally(passed_arguments_dict)

    # print_dates(passed_arguments_dict)
    # au.show_passed_arguments(passed_arguments_dict)
    # au.save_passed_arguments_locally(passed_arguments_dict)
    # dates_in_time_interval = passed_arguments_dict["dates_in_time_interval"]
    # passed_arguments_dict["measuring_devices"] = measuring_devices


ace_scraping()

#
#
#
#
# ______________________________________________________________________________
#
# ORIGINAL CODE
#
# ------------------------------------------------------------------------------
# -*- coding: utf-8 -*-
# """
# Created on Wed May 25 14:58:53 2022
#
# @author: Céline ONG
#
#
# """
#
# #---------------------------------------------------- MINI USER'S GUIDE ----------------------------------------------------
#
# # For manual download, make sure the saving directory already exists before passing its path in arguments (it is not created automatically)
#
# # for manual scraping mode enter:
# #start_date ("%Y-%m-%d_%H:%M:%S"), end_date ("%Y-%m-%d_%H:%M:%S") and complete path to saving directory (on your computer)
#
# # to resample data, also pass SWE features in ' ' and sample_frequency
# # command example:
#
# #  python ACE_main.py 2022-06-22_01:23:12 2022-06-23_17:55:30 C:\Users\célineong\Desktop\Projets\pipeline_scraping_ACE_data\data\manual 'Bx, By, Bulk Speed' 'mean, std', 2H
#
# # NB for resampling, use date of format ("%Y-%m-%d_%H:%M:%S"), not ("%Y-%m")
#
# #-------------------------------------------------------------------------------------------------------------------------
#
# #%% IMPORTING LIBRARIES
#
# from ACE_scraping import check_passed_arguments, create_directory, get_dates_in_time_interval, define_url_format, download_data
# from ACE_resampling import get_resampled_data
# from ACE_processing import aggregate_all_devices
# import pathlib
# import sys
# from datetime import datetime
#
# measuring_devices = ['mag', 'swepam', 'epam', 'sis']
#
#
# # TODO
# # reampling > 1d doesn't work. For sample_fre = m, we get 31 lines with same values as for sample_freq=d, instead of 1 line
# # ACE_main.py 2022-03-01_00:00:00 2023-04-01_23:59:00 'C:\Users\célineong\OneDrive - SpaceAble\Bureau\Projets\pipeline_scraping_ACE_data\data\manual' 'Bz' 'mean' 'm'
#
# def pipeline_scrape_ACE_data(measuring_devices,
#                              directory_path= pathlib.Path(__file__).parent.parent / 'data',
#                              return_df=True):
#     """
#
#
#     Parameters
#     ----------
#     measuring_devices : list of strings
#         List of measuring devices: mag', 'swepam', 'epam', 'sis'
#     directory_path : pathlib.WindowsPath, optional
#         Path pointing to the parent directory (set to be in gitlab in automatic mode ) where the directories are created. The default is pathlib.Path(__file__).parent.parent / 'data'.
#     return_df : bool, optional
#         Keyword. Set to True to return a dataframe of resampled ACE features. The default is True.
#
#     Returns
#     -------
#     df_ACE : Pandas dataframe and optional csv file.
#         Timeseries of the resampled selected features.
#
#     """
#
#     # ----- retrieving arguments passed in command line
#     args = sys.argv
#
#     # ----- checking arguments passed in command line
#     start_date, end_date, source, directory_path, monthly =  check_passed_arguments(args)
#
#     # ----- creating directories to save scraped data
#     data_scraping_directory  = create_directory(directory_path, measuring_devices, monthly=monthly)
#
#     # ----- getting the dates over the selected time interval
#     dates_in_time_interval = get_dates_in_time_interval(start_date, end_date, monthly = monthly)
#
#     # ----- Building complete urls for each dates and each measuring devices, with format depending on the data source
#     list_url = define_url_format(source, dates_in_time_interval, measuring_devices, data_scraping_directory)
#
#     # ----- downloading data files from all the urls into the saving directories
#     download_data(list_url, data_scraping_directory, start_date, monthly=monthly)
#
#     # ---- optionnal resampling of the manually downloaded data
#
#     # ---- identifying arguments
#     if len(args) > 4:
#         try: # to resample data, dates should be following format (monthly format not allowed)
#             start_date = datetime.strptime(args[1],"%Y-%m-%d_%H:%M:%S")
#         except ValueError:
#             raise ValueError("To resample data, start_date and end_date should have format : YYYY-MM-dd_HH:MM:SS (ex: 2022-06-22_13:52:45)")
#         features = args[4]
#         stat = args[5]
#         sample_freq = args[6]
#
#         # resampling
#         df_ACE = get_resampled_data(measuring_devices,
#                                input_path=data_scraping_directory,
#                                output_path = directory_path,
#                                start_date=start_date, end_date=end_date,
#                                features=features,
#                                stat=stat, sample_freq=sample_freq)
#
#         if return_df:
#             return df_ACE
#
#
#     # ----- aggregating data by day, only for automatic mode
#     if end_date == None:
#         aggregate_all_devices(directory_path, measuring_devices)
#
# pipeline_scrape_ACE_data(measuring_devices)
