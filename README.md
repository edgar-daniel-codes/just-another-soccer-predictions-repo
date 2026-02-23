
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
curl -fsSL "$URL" | python src/etl/etl_utils_footballdata_uk_get_links.py "https://www.football-data.co.uk" > ./data/raw/footballdata_uk/links.txt
```


Here are the following url for each League DataBase:

```txt

section,label,url
Main Leagues,England Football Results,https://www.football-data.co.uk/data.php/englandm.php
Main Leagues,Scotland Football Results,https://www.football-data.co.uk/data.php/scotlandm.php
Main Leagues,Germany Football Results,https://www.football-data.co.uk/data.php/germanym.php
Main Leagues,Italy Football Results,https://www.football-data.co.uk/data.php/italym.php
Main Leagues,Spain Football Results,https://www.football-data.co.uk/data.php/spainm.php
Main Leagues,France Football Results,https://www.football-data.co.uk/data.php/francem.php
Main Leagues,Netherlands Football Results,https://www.football-data.co.uk/data.php/netherlandsm.php
Main Leagues,Belgium Football Results,https://www.football-data.co.uk/data.php/belgiumm.php
Main Leagues,Portugal Football Results,https://www.football-data.co.uk/data.php/portugalm.php
Main Leagues,Turkey Football Results,https://www.football-data.co.uk/data.php/turkeym.php
Main Leagues,Greece Football Results,https://www.football-data.co.uk/data.php/greecem.php
Extra Leagues,Argentina Football Results,https://www.football-data.co.uk/data.php/Argentina.php
Extra Leagues,Austria Football Results,https://www.football-data.co.uk/data.php/Austria.php
Extra Leagues,Brazil Football Results,https://www.football-data.co.uk/data.php/Brazil.php
Extra Leagues,China Football Results,https://www.football-data.co.uk/data.php/China.php
Extra Leagues,Denmark Football Results,https://www.football-data.co.uk/data.php/Denmark.php
Extra Leagues,Finland Football Results,https://www.football-data.co.uk/data.php/Finland.php
Extra Leagues,Ireland Football Results,https://www.football-data.co.uk/data.php/Ireland.php
Extra Leagues,Japan Football Results,https://www.football-data.co.uk/data.php/Japan.php
Extra Leagues,Mexico Football Results,https://www.football-data.co.uk/data.php/Mexico.php
Extra Leagues,Norway Football Results,https://www.football-data.co.uk/data.php/Norway.php
Extra Leagues,Poland Football Results,https://www.football-data.co.uk/data.php/Poland.php
Extra Leagues,Romania Football Results,https://www.football-data.co.uk/data.php/Romania.php
Extra Leagues,Russia Football Results,https://www.football-data.co.uk/data.php/Russia.php
Extra Leagues,Sweden Football Results,https://www.football-data.co.uk/data.php/Sweden.php
Extra Leagues,Switzerland Football Results,https://www.football-data.co.uk/data.php/Switzerland.php
Extra Leagues,USA Football Results,https://www.football-data.co.uk/data.php/USA.php
```

For each League link, we run the following code './lib/etl_footballdata_uk_massive_download.zsh' on terminal to mass download all available files: 

```bash 
chmod +x ./lib/etl_footballdata_uk_massive_download.zsh
./lib/etl_footballdata_uk_massive_download.zsh ./data/raw/footballdata_uk/links.txt
```




