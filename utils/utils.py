import urllib.request
import requests
from requests.exceptions import ConnectionError
import time


def is_url(url):
    """
    Check if the passed url exists or not

    INPUT:
    :param: url (str)      url to test

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
