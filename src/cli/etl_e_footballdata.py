#!/usr/bin/env python3

### Soccer Predictions  - ETL 
### Utilities Football Data Org
### By Edgar Daniel

# Code creating a client and getting soccer leagues  data from the public API 
# from football data org. and creating usable raw data tables for modelling 
# and analytics usage. 


### -------------------------------------------------------------------------------
### Needed libraries --------------------------------------------------------------

from src.etl.etl_utils_footballdata import *
import os
import argparse

# Log dirs and make sure exist 
DIR_LOG = "./logs/"
FILE_LOG = "footballdataorg.log"
os.makedirs(DIR_LOG, exist_ok=True)
os.makedirs(os.path.dirname(DIR_LOG + FILE_LOG), exist_ok=True)


### -------------------------------------------------------------------------------
### Main --------------------------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Get Raw data from Football Data Org for a given League and Seasons"
    )

    # Positional (always required)
    parser.add_argument("LEAGUE", type=str, help="League of interest.")
    parser.add_argument("SEASON", type=int, help="Season year")
    parser.add_argument("OUT_DIR", type=str, help="Output directory path..")

    args = parser.parse_args()


    # Logger 
    import logging

    logger = logging.getLogger(f"etl-footballdata-{args.LEAGUE}-{args.SEASON}")
    logger.setLevel(logging.INFO)

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    formatter = logging.Formatter(fmt)

    console = logging.StreamHandler()
    console.setFormatter(formatter)

    file = logging.FileHandler(DIR_LOG + FILE_LOG)
    file.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file)


    # Get API token from Football Data Org 
    token_env = os.getenv("API_TOKEN_FUT_DATA", "").strip()
    if not token_env:
        raise SystemExit("Set API_TOKEN_FUT_DATA env var to your football-data.org API token.")

    logger.info("--------------------------------------------------------------------------")
    logger.info(f"Starting data calling for league {args.LEAGUE} and season {args.SEASON}.")

    try:

        artifacts_out = download_competition_season(
            token=token_env,
            competition=args.LEAGUE,
            season_year=args.SEASON,
            out_dir=args.OUT_DIR,
        )

        logger.info(f"Data retrived for league {args.LEAGUE} and season {args.SEASON}. \n Saved Items: \n ")

        # Print saved Items 
        for k, v in artifacts_out.items():
            logger.info(f"{k}: {v}")

    except Exception as e:
        logger.info(f"Error getting data: \n: {e}")

    

