
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

### Extract

**Football Data Org **

The script, *etl_Utils_footballdata.py*, contains the necessary functions to create the API client, retrieve soccer football data, and perform a simple transformation to save it into usable raw tables. 

Running *etl_utils_footballdata.py* with the league and season parameters and output paths, as shown in the code snippet, loads the raw data into our data folders. 

Where PL = Premier League, 2023 the season year and "./data/raw/footballdata/" the output directory.

```bash 
python ./src/etl_e_footbaldata.py PL 2023 ./data/raw/footballdata/
```



** 

The [Football Data UK](https://www.football-data.co.uk/englandm.php) is one of the oldest and more reliable data soirces for open data in football data for matches , and given the partnerships with betting pages, the databases included odds payment (useful to infere implicit probability and simulate portfolio scenarios). To extract the data, it is used the following scripts based on web scrapping for getting the .csv and .txt files. 

Firs we use the 'etl_utils_footballdata_uk_get_links.py' script to get all avilable leagues and tags:

```bash 
mkdir ./data/raw/footballdata_uk/
curl -fsSL "$URL" | python src/etl/etl_utils_footballdata_uk_get_links.py "https://www.football-data.co.uk" > ./data/raw/footballdata_uk/links.txt
```


Here are the following url for each League DataBase:

```txt
section,label,url
Main Leagues,England Football Results,https://www.football-data.co.uk/englandm.php
Main Leagues,Scotland Football Results,https://www.football-data.co.uk/scotlandm.php
Main Leagues,Germany Football Results,https://www.football-data.co.uk/germanym.php
Main Leagues,Italy Football Results,https://www.football-data.co.uk/italym.php
Main Leagues,Spain Football Results,https://www.football-data.co.uk/spainm.php
Main Leagues,France Football Results,https://www.football-data.co.uk/francem.php
Main Leagues,Netherlands Football Results,https://www.football-data.co.uk/netherlandsm.php
Main Leagues,Belgium Football Results,https://www.football-data.co.uk/belgiumm.php
Main Leagues,Portugal Football Results,https://www.football-data.co.uk/portugalm.php
Main Leagues,Turkey Football Results,https://www.football-data.co.uk/turkeym.php
Main Leagues,Greece Football Results,https://www.football-data.co.uk/greecem.php
Extra Leagues,Argentina,https://www.football-data.co.uk/argentina.php
Extra Leagues,Austria,https://www.football-data.co.uk/austria.php
Extra Leagues,Brazil,https://www.football-data.co.uk/brazil.php
Extra Leagues,China,https://www.football-data.co.uk/china.php
Extra Leagues,Denmark,https://www.football-data.co.uk/denmark.php
Extra Leagues,Finland,https://www.football-data.co.uk/finland.php
Extra Leagues,Ireland,https://www.football-data.co.uk/ireland.php
Extra Leagues,Japan,https://www.football-data.co.uk/japan.php
Extra Leagues,Mexico,https://www.football-data.co.uk/mexico.php
Extra Leagues,Norway,https://www.football-data.co.uk/norway.php
Extra Leagues,Poland,https://www.football-data.co.uk/poland.php
Extra Leagues,Romania,https://www.football-data.co.uk/romania.php
Extra Leagues,Russia,https://www.football-data.co.uk/russia.php
Extra Leagues,Sweden,https://www.football-data.co.uk/sweden.php
Extra Leagues,Switzerland,https://www.football-data.co.uk/switzerland.php
Extra Leagues,USA,https://www.football-data.co.uk/usa.php
```

For each League link, we run the following code './lib/etl_footballdata_uk_massive_download.zsh' on terminal to mass download all available files: 

```bash 
chmod +x ./lib/etl_footballdata_uk_massive_download.zsh
./lib/etl_footballdata_uk_massive_download.zsh ./data/raw/footballdata_uk/links.txt
```




