
# Just Another Soccer Predictions Repo (WIP)

The present repository aims to create data pipelines and machine learning and statistical models to generate descriptive statistics, analytics, insights, and predictions related to soccer matches and players around the globe. It uses public data sources and delivers multiple dashboard visualizations to present results to stakeholders and the general public interested in using analytics for decision-making and understanding sports. 

## About this repo

The following is the folder structure:

WIP

### Leagues Coverage 

The products and results vary depending on the trusted data sources for each league and the data availability.


**Football Data Org**

The following leagues are available from the Football Data API:
    - Premier League



## Data Pipelines 

In the following sections we define the exact ETL process and usage of the scripts top get the dta from each source with examples for some leagues and seasons. 

### Football Data Org 

**Extract**

The script, *etl_Utils_footballdata.py*, contains the necessary functions to create the API client, retrieve soccer football data, and perform a simple transformation to save it into usable raw tables. 

Running *etl_utils_footballdata.py* with the league and season parameters and output paths, as shown in the code snippet, loads the raw data into our data folders. 

Where PL = Premier League, 2023 the season year and "./data/raw/footballdata/" the output directory.

```bash 
python ./src/etl_e_footbaldata.py PL 2023 ./data/raw/footballdata/
```



