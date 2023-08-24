
"""

TODO: write doc 

"""

import os
from pathlib import Path

from airflow.decorators import dag, task

@task()
def aggregate_all_devices(directory_path, measuring_devices):
    """
    Loop the aggregation function over all measuring devices

    Parameters
    ----------
    directory_path : pathlib.WindowsPath
        Path to the parent directory containing the scraping and aggregating directories .
    measuring_devices : list of str
        List of the measuring devices.

    Returns
    -------
    None.

    """
    for device in measuring_devices:
        if device in ('mag'):
            nasa_device = "magnetometer"
        else:
            nasa_device = device
        ACE_scrapped_path = str(directory_path) + "/data_scraping/" + nasa_device + "/"
        ACE_aggregated_path = str(directory_path) + '/data_aggregating/' + nasa_device + '/'
        aggregate_device_data(ACE_scrapped_path, ACE_aggregated_path, nasa_device)


@task()
def aggregate_device_data(input_path, output_path, device, time_scale="H", 
                          start_date=None, end_date=None):
    """
    Aggregate data for one device over 24 hours for all the files saved in the input directory (data_scraping), generate .txt file and save it in the output directory (data_aggregating)

    Parameters
    ----------
    input_path : str
        DESCRIPTION.
    output_path : str
        DESCRIPTION.
    device : str
        DESCRIPTION.
    time_scale : str, optional
        DESCRIPTION. The default is "H".
    start_date : Datetime.datetime or None, optional
        Left bound for generating scraping time interval. The default is None.
    end_date : Datetime.datetime or None, optional
        Right bound for generating scraping time interval. The default is None.

    Returns
    -------
    Write daily aggregated files for one device.

    """
    
    os.chdir(input_path)
    
    list_of_n_files = list(Path('.').glob('**/*.txt'))
    number_files = len(list_of_n_files)
    one_file = list_of_n_files[0]

    # Store the header in a list
    header = get_header_from_file(one_file)
    
    # Scan all data and organize lines in a dictionary by date
    lines_by_day = {}
    count = 0
    for each_file in list_of_n_files:
        count += 1
        print("Processing " + output_path+ " file " + str(count) + " of " + str(number_files) + " files")
        with open(each_file) as f:
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
    for measurement_date in lines_by_day:
        measurement_date_with_dash = measurement_date.replace(" ", "-")
        path = output_path +device+"_"+measurement_date_with_dash+".txt"
        write_aggregated_file(path,header,lines_by_day[measurement_date])

@task()    
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
             if line.startswith(("#",":")):
                 header.append(line)
    return header


@task()
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
        list of all lines containing data.

    Returns
    -------
    Write the file.

    """
    
    with open(path, "w") as file:
        for header_lines in header:
            file.writelines(header_lines)
        for data_line in data_lines:
            file.writelines(data_line)


@task()
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