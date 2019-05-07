# Imports
import pdb
import json
import os
import pandas as pd
from pandas.io.json import json_normalize
import dask.dataframe as dd # TODO read the json better
import dask.bag as db
import numpy as np
import logging
import time

SUFFICIENT_LINES = 5*10**3
TL = 'data/thousandlines.sbp.json'

# Construct filepath, ensure it's json
DATADIR = 'data'
FILENAME = '00ec3270-7061-4d3d-a570-47b1eb753e04_PK40ac4c023b714c8aaaf2377242a2b1d3_msg74decoded.sbp.json'
FILEPATH = os.path.join(os.path.curdir, DATADIR, FILENAME)
assert(os.path.splitext(FILEPATH)[1] == '.json')

# Setup logger
def make_logger():
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(FORMAT)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return(logger)

def load_data_some(log):
    log.debug("Starting load_data_some()")
    cols = ['tow', 'sat', 'const', 'rng']
    log.debug("Collecting columns: {}".format(cols))

    # Use vanilla pandas to load the data
    start_time = time.time()
    df = pd.DataFrame(columns=cols)
    with open(FILEPATH) as f:
        log.info("Opened file {}".format(FILEPATH))
        i = 0
        for line in f.readlines():
            msg = json.loads(line)
            assert(msg['msg_type'] == 74)
            tow = msg['header']['t']['tow']
            for obs in msg['obs']:
                sat = obs['sid']['sat']
                const = obs['sid']['code']
                rng = obs['P']
                row = [tow, sat, const, rng]
                df.loc[i] = row
                i = i + 1
                if i % 1000 == 0: log.debug("Loaded {} lines".format(i))
            if i > SUFFICIENT_LINES: break

    log.info("Read {} lines into a DataFrame".format(i+1))
    end_time = time.time()
    time_took = end_time - start_time
    log.debug("Took {} ms to load {} lines".format(time_took, i+1))

    return(df)

def load_data_all(log):
    log.debug("Starting load_data()")
    cols = ['tow', 'sat', 'const', 'rng']
    log.debug("Collecting columns: {}".format(cols))

    # Use Dask to load the nested JSON
    bag = db.read_text(FILEPATH).map(json.loads)
    df = bag.map(lambda r:
                [
                    dict(
                        tow     = r['header']['t']['tow'],
                        sat     = o['sid']['sat'],
                        const   = o['sid']['code'],
                        rng     = o['P']
                    )
                    for o in r['obs']
                ]
            ).flatten()

    return(df)

def plot(log, df):
    log.debug("Starting plot()")
    pdb.set_trace()

# Main
def main(log):
    df = load_data_some(log)
    plot(log, df)

if __name__ == '__main__':
    # TODO commandline args
    log = make_logger()
    main(log)

""" Assumptions
"""
