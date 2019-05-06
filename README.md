# StochaSITL
Stochastic Software In The Loop

## Requirements
Built and tested on Mac OsX Mojave 10.14.3 (18D109) with Intel i7.
Requires
1. AWS CLI
2. `virtualenv --version` 16.0.0
3. `python3 -V` Python 3.7.3

## Install
1. `virtualenv venv -p python3`
2. `source ./venv/bin/activate`
3. TODO: install requirements.txt

## Get data
1. Authenticate with AWS
2. `mkdir ./data`
3. `./src/get_ex_sbp.sh` and wait for the file to download (~44M)
4. `./src/extract_msg74.sh` and wait ~5 sec for the conversion (~124M)

# Overview
Parse ME output, specifically pseudoranges

Learn state and error models

For a drive, perturb ME output via state & error models
