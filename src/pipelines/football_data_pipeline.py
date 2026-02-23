#!/usr/bin/env python3

### Soccer Predictions - ETL 
### Utilities Football Pipelines 
### By Edgar Daniel

# Auxiliar code for orchestrating the data pipeline flow for Football Data Extraction
# Usage
# python src/pipelines/football_data_pipeline.py


### -------------------------------------------------------------------------------
### Needed libraries --------------------------------------------------------------

import subprocess

### -------------------------------------------------------------------------------
### Parameters --------------------------------------------------------------------



LEAGUES = ['PL','BL1', 'PD', 'SA', 'FL1', 'ELC']

YEARS = list(range(2014, 2024))

### -------------------------------------------------------------------------------
### Main Pipeline  ----------------------------------------------------------------


## Data Extraction 
for league in LEAGUES:
    for year in YEARS:
        subprocess.run(
            [
                "python",
                "-m",
                "src.cli.etl_e_footballdata",
                league,
                str(year),
                "./data/raw/footballdata/",
            ],
            check=False,
        )



## Data Transformation 



