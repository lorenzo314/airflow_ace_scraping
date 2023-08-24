"""
TODO: put doc
"""
# from datetime import datetime, timedelta
# from textwrap import dedent

from datetime import datetime
import pathlib
import sys
import pendulum

from airflow.decorators import dag  #, task

from ACE_scraping import (
    check_passed_arguments
    , create_directory
    , get_dates_in_time_interval
    , define_url_format
    , download_data
)
from ACE_resampling import get_resampled_data
from ACE_processing import aggregate_all_devices

@dag(
    # TODO : put the real values for scheduling
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)


def pipeline_scrape_ace_data(
    measuring_devices,
    directory_path= pathlib.Path(__file__).parent.parent / 'data',
    return_df=True):

    """
    Parameters
    ----------
    measuring_devices : list of strings
        List of measuring devices: mag', 'swepam', 'epam', 'sis'
    directory_path : pathlib.WindowsPath, optional
        Path pointing to the parent directory 
        (set to be in gitlab in automatic mode )
        where the directories are created.
        The default is pathlib.Path(__file__).parent.parent / 'data'.
    return_df : bool, optional
        Keyword. Set to True to return a dataframe of resampled ACE features. The default is True.

    Returns
    -------
    df_ace : Pandas dataframe and optional csv file.
        Timeseries of the resampled selected features.

    """

    # ----- retrieving arguments passed in command line
    args = sys.argv

    # ----- checking arguments passed in command line
    start_date, end_date, source, directory_path, monthly = \
        check_passed_arguments(args)

    # ----- creating directories to save scraped data
    data_scraping_directory  = create_directory(directory_path, measuring_devices, monthly=monthly)

    # ----- getting the dates over the selected time interval
    dates_in_time_interval = get_dates_in_time_interval(start_date, end_date, monthly = monthly)

    # ----- Building complete urls for each dates and each measuring devices,
    # with format depending on the data source
    list_url = define_url_format(
        source,
        dates_in_time_interval,
        measuring_devices,
        data_scraping_directory)

    # ----- downloading data files from all the urls into the saving directories
    download_data(
        list_url,
        data_scraping_directory,
        start_date,
        monthly=monthly)

    # ---- optionnal resampling of the manually downloaded data

    # TODO: what to use instead of args in DAGs ?
    # ---- identifying arguments
    if len(args) > 4:
        try: # to resample data, dates should be following format (monthly format not allowed)
            start_date = datetime.strptime(args[1],"%Y-%m-%d_%H:%M:%S")
        except ValueError as exc:
            raise ValueError(
                "To resample data, start_date and end_date should have" \
                "format : YYYY-MM-dd_HH:MM:SS (ex: 2022-06-22_13:52:45)"
            ) from exc
        features = args[4]
        stat = args[5]
        sample_freq = args[6]
        # resampling
        df_ace = get_resampled_data(
            measuring_devices,
            input_path=data_scraping_directory,
            output_path = directory_path,
            start_date=start_date, end_date=end_date,
            features=features,
            stat=stat, sample_freq=sample_freq)

        if return_df:
            return df_ace


    # ----- aggregating data by day, only for automatic mode
    if end_date is None:
        aggregate_all_devices(directory_path, measuring_devices)
