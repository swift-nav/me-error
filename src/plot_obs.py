# Imports
import pdb
import json
import os
import pandas as pd
import matplotlib.pyplot as plt
from pandas.io.json import json_normalize
import dask.dataframe as dd
import dask.array as da
import dask.bag as db
from dask.diagnostics import ProgressBar
import numpy as np
import logging
import time

SUFFICIENT_LINES = 5*10**3
TL = 'data/thousandlines.sbp.json'

pbar = ProgressBar()
pbar.register()

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
    log.debug("Starting load_data_all()")

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

def plot_all(log, bag):
    log.debug("Starting plot_all()")
    st, cn = 19, 0
    log.debug("Downfiltering to satelite {} const {}".format(st,cn))
    df1 = bag.filter(
            lambda r: (r['sat'] == st) & (r['const'] == cn)
            ).pluck('rng')

    # https://stackoverflow.com/questions/38086741/how-to-draw-a-histogram-in-dask
    # df1.visualize(filename='computation_visualization.svg')
    # pdb.set_trace()
    # mx = df1.max().compute()
    # mn = df1.min().compute()
    # dld = bag.to_delayed()[0]
    # da1 = da.from_delayed(
    #         dld,
    #         shape = (100,),
    #         dtype = type(df1.take(1)[0])
    #         # dtype = np.uint32
    #         )

    log.debug("Computing frequencies of pseudoranges...")
    frq = df1.frequencies().compute()
    log.debug("Plotting the pseudorange frequencies...")

    fig = plt.figure()
    ax = plt.subplot(111)

    ax.scatter(*zip(*frq))
    tit = 'Pseudorange counts for sat {} and const {}'.format(st, cn)
    plt.title(tit)
    plt.xlabel('Pseudorange (2cm)')
    plt.ylabel('Count')
    fig.savefig('plt1.png')

    # df2 = df1.to_dataframe().compute()
    # log.debug("Downfiltered! Starting plot...")
    # fig, ax = plt.subplots()
    # df3 = df2[1:100]
    # n, bins, patches = ax.hist(df3)
    # plt.show()

def plot_some(log, df):
    log.debug("Starting plot_some()")
    pdb.set_trace()

# Main
def main(log):
    df = load_data_all(log)
    plot_all(log, df)
    # df = load_data_some(log)
    # plot_some(log, df)

if __name__ == '__main__':
    # TODO commandline args
    log = make_logger()
    main(log)

""" Assumptions
"""
