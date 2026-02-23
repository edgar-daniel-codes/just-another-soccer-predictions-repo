#!/usr/bin/env python3

### Soccer Predictions - ETL 
### Footbal Data UK transformation 
### By Edgar Daniel

# Auxiliar code for transforming and formating of the historical raw data 
# for the files downloaded from Football data UK.
# 


RAW_DATA_DIR = "./data/raw/footballdata_uk/"
PROC_DATA_DIR = "./data/proc/footballdata_uk/"
CLEAN_DATA_DIR = "./data/clean/footballdata_uk/"

RAW_HISTORIC_FILENAME = "footballdatauk_historic.csv"

DIR_LOG = "./logs/"
FILE_LOG = "footballdataorg.log"



### -------------------------------------------------------------------------------
### Needed libraries --------------------------------------------------------------

import pandas as pd
import numpy as np 
import os
from pathlib import Path
from typing import Any



# Make sure directories exist 
os.makedirs(DIR_LOG, exist_ok=True)
os.makedirs(os.path.dirname(DIR_LOG + FILE_LOG), exist_ok=True)
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROC_DATA_DIR, exist_ok=True)
os.makedirs(CLEAN_DATA_DIR, exist_ok=True)


### -------------------------------------------------------------------------------
### Load and Normalizing   --------------------------------------------------------


def _concat_files(RAW_DATA_DIR:str,RAW_HISTORIC_FILENAME:str, logger:Any)->None:

    raw_dir = Path(RAW_DATA_DIR)
    out_path = raw_dir / RAW_HISTORIC_FILENAME
    first_write = True

    country_list = os.listdir(RAW_DATA_DIR)

    # Given that from original source all data files are in csv
    # We just take the .csv files. 

    logger.info("Starting concatenation loop---")

    for country in country_list:

        logger.info(f"--- Starting loop for country {country}")

        country_dir = raw_dir / country
        if not country_dir.is_dir():
            continue

        for csv_path in country_dir.rglob("*.csv"):

            logger.info(f"-- Appending file {csv_path}")

            try:
                for chunk in pd.read_csv(
                    csv_path,
                    chunksize=200_000,
                    encoding="latin-1",
                    on_bad_lines="skip",
                    engine="python",
                ):
                    #chunk["country"] = country
                    #chunk["source_file"] = csv_path.name
                    chunk.to_csv(
                        out_path,
                        mode="w" if first_write else "a",
                        index=False,
                        header=first_write,
                    )
                    first_write = False

                logger.info(f"- File {csv_path} appended.")

            except Exception as e:
                logger.info(f"- Failed file {csv_path} append. \n {e}")
                continue



def _get_column_stats(df:pd.DataFrame, alpha:float, logger:Any)-> pd.DataFrame:

    col_list = df.columns.to_list()

    df = df.drop_duplicates()

    n = len(df)

    stats_dict = []

    for c in col_list:
        nas = int(df[c].isna().sum())
        stats_dict.append(
            {
                "column":c, 
                "null_count":nas, 
                "null_pct":round(nas/n,4)
            }
        )


    stats_df = pd.DataFrame(stats_dict)
    stats_df = stats_df.query(f""" null_pct <= {1-alpha} """)
    print(stats_df)



if __name__=="__main__":

    # Logger 
    import logging

    logger = logging.getLogger(f"etl-footballdata-uk-transformation")
    logger.setLevel(logging.INFO)

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    formatter = logging.Formatter(fmt)

    console = logging.StreamHandler()
    console.setFormatter(formatter)

    file = logging.FileHandler(DIR_LOG + FILE_LOG)
    file.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file)



    # File Creation 
    #_concat_files(RAW_DATA_DIR,RAW_HISTORIC_FILENAME, logger)

    # Standarize common columns 

    chunks = pd.read_csv(
        RAW_DATA_DIR+RAW_HISTORIC_FILENAME,
        encoding="latin-1",
        engine="python",
        on_bad_lines="skip",
        chunksize=500_000,
        #low_memory=False,
    )

    df = pd.concat(chunks, ignore_index=True, sort=False)


    _get_column_stats(df, alpha = 0.9, logger=logger)


