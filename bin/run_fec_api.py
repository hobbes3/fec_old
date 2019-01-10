#!/usr/bin/env python

# hobbes3
# Example from https://docs.python.org/2.7/howto/urllib2.html

import urllib
import urllib2
import os
import time
import json
import sys
import logging
import logging.handlers
from multiprocessing.dummy import Pool as ThreadPool

import fec_api

# Constants

URL_BASE = "https://api.open.fec.gov/v1"
URL_PARAMETERS = {
    "api_key": fec_api.api_key,
    "per_page": 100,
    "cycle": 2016,
    "is_notice": False,
    "sort": "expenditure_date",
}

THREADS = 8

LOG_ROTATION_LOCATION = "/Users/hobbes3/gdrive/TEMP/"
LOG_ROTATION_BYTES = 25 * 1024 * 1024
LOG_ROTATION_LIMIT = 10000

# Before retrying first wait 1 second, then another 1, then another 1, then 30, then 60, then finally retry every 300 seconds
RETRY_SLEEP = [1, 1, 1, 30, 60, 300]

REQUESTS = [
    {
        "url": "/schedules/schedule_e/",
        "filename": "clinton_schedule_e.json",
        "parameters": {
            "candidate_id": "P00003392"
        }
    },
    {
        "url": "/schedules/schedule_e/",
        "filename": "trump_schedule_e.json",
        "parameters": {
            "candidate_id": "P80001571"
        }
    },
    #{
    #    "url": "/schedules/schedule_e/",
    #    "filename": "obama_schedule_e.json",
    #    "parameters": {
    #        "candidate_id": "P80003338"
    #    }
    #},
    #{
    #    "url": "/schedules/schedule_e/",
    #    "filename": "romney_schedule_e.json",
    #    "parameters": {
    #        "candidate_id": "P80003353"
    #    }
    #},
    #{
    #    "url": "/schedules/schedule_e/",
    #    "filename": "mccain_schedule_e.json",
    #    "parameters": {
    #        "candidate_id": "P80002801"
    #    }
    #},
    #{
    #    "url": "/schedules/schedule_e/",
    #    "filename": "bush_schedule_e.json",
    #    "parameters": {
    #        "candidate_id": "P00003335"
    #    }
    #},
    #{
    #    "url": "/schedules/schedule_e/",
    #    "filename": "kerry_schedule_e.json",
    #    "parameters": {
    #        "candidate_id": "P80000235"
    #    }
    #},
    #{
    #    "url": "/schedules/schedule_e/",
    #    "filename": "gore_schedule_e.json",
    #    "parameters": {
    #        "candidate_id": "P80000912"
    #    }
    #},
]

logger = logging.getLogger('logger_debug')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("[%(levelname)s] (%(threadName)-10s) %(message)s"))
logger.addHandler(ch)

# http://stackoverflow.com/a/7047765/1150923
def tail(f, window=20):
    """
    Returns the last `window` lines of file `f` as a list.
    """
    if window == 0:
        return []
    BUFSIZ = 1024
    f.seek(0, 2)
    bytes = f.tell()
    size = window + 1
    block = -1
    data = []
    while size > 0 and bytes > 0:
        if bytes - BUFSIZ > 0:
            # Seek back one whole BUFSIZ
            f.seek(block * BUFSIZ, 2)
            # read BUFFER
            data.insert(0, f.read(BUFSIZ))
        else:
            # file too small, start from begining
            f.seek(0,0)
            # only read what was not read
            data.insert(0, f.read(bytes))
        linesFound = data[0].count('\n')
        size -= linesFound
        bytes -= BUFSIZ
        block -= 1
    return ''.join(data).splitlines()[-window:]

def retry(retries, error_msg):
    sleep_sec = RETRY_SLEEP[retries]

    logger.debug(error_msg.replace("_SEC_", str(sleep_sec)))

    time.sleep(sleep_sec)

    if retries >= len(RETRY_SLEEP) - 1:
        retries = len(RETRY_SLEEP) - 1
    else:
        retries += 1

    return retries

def run_fec_api(request):
    #import pdb; pdb.set_trace()

    filename = LOG_ROTATION_LOCATION + request["filename"]

    logger_file = logging.getLogger('logger_' + request["filename"])
    logger_file.setLevel(logging.DEBUG)
    handler = logging.handlers.RotatingFileHandler(filename, maxBytes=LOG_ROTATION_BYTES, backupCount=1000)
    logger_file.addHandler(handler)

    logger.debug("Opening file %s" % filename)

    # Python file rotated logging adds an extra empty line at the end of the file
    with open(filename, "a+") as f:
        lines = tail(f, 2)

    lines = [x for x in lines if x != ""]

    #import pdb; pdb.set_trace()

    if lines == []:
        last_indexes = {}
    else:
        # End of a file looks like
        # {"pagination":{"per_page":100,"count":3344,"last_indexes":null,"pages":34},"api_version":"1.0","results":[]}
        parsed_last_json = json.loads(lines[-1])

        if parsed_last_json["pagination"]["last_indexes"] is None:
            logger.info("Done for %s" % filename)
            return
        # Otherwise it looks like
        # {"pagination":{"count":11611,"per_page":100,"last_indexes":{"last_expenditure_amount":2.31,"last_index":286395610},"pages":117},"results": ...
        else:
            last_indexes = parsed_last_json["pagination"]["last_indexes"]

    while last_indexes is not None:
        parameters = URL_PARAMETERS.copy()
        parameters.update(request["parameters"])
        parameters.update(last_indexes)

        url_parameters = urllib.urlencode(parameters, doseq=True)
        full_url = URL_BASE + request["url"] + "?" + url_parameters

        retries = 0

        while retries >= 0:
            logger.debug(full_url)

            try:
                response = urllib2.urlopen(full_url)
                data = response.read().strip()

                parsed_json = json.loads(data)

                last_indexes = parsed_json["pagination"]["last_indexes"]

                if last_indexes is None:
                    logger.info("Done for %s" % filename)
                    return
                else:
                    # Write to file
                    logger_file.info(data)

                retries = -1
            except urllib2.HTTPError as e:
                error_msg = "Error code: %s - sleeping for _SEC_ seconds(s)" % e.code
                retries = retry(retries, error_msg)
                pass
            except:
                error_msg = "Unexpected error: %s - sleeping for _SEC_ seconds(s)" % sys.exc_info()[0]
                retries = retry(retries, error_msg)
                pass

#test = {
#    "url": "/schedules/schedule_e/",
#    "filename": "clinton_schedule_e.json",
#    "parameters": {
#        "candidate_id": "P00003392"
#    }
#}
#
#run_fec_api(test)

# http://stackoverflow.com/a/28463266/1150923
pool = ThreadPool(THREADS)

results = pool.map(run_fec_api, REQUESTS)
pool.close()
pool.join()
