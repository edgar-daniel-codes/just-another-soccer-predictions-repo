
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

**Football Data Org**

The script, *etl_Utils_footballdata.py*, contains the necessary functions to create the API client, retrieve soccer football data, and perform a simple transformation to save it into usable raw tables. 

Running *etl_utils_footballdata.py* with the league and season parameters and output paths, as shown in the code snippet, loads the raw data into our data folders. 

Where PL = Premier League, 2023 the season year and "./data/raw/footballdata/" the output directory.

```bash 
python ./src/etl_e_footbaldata.py PL 2023 ./data/raw/footballdata/
```



**Football Data UK**

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


**Statsbom Open Data**

Hudl Statsbomb is one of the world's leading football analytics and data collection companies. By opening match, player, and event data via their open data repository, they hope to encourage the development of new analytical insights from football leagues for enthusiasts and professionals alike. 

First, we pull the open data provided on the [Official Statsbomb Repository](https://github.com/statsbomb/open-data/tree/master/data). 


```bash 
mkdir -p data/raw/statsbomb_open_repo
cd data/raw/statsbomb_open_repo

git init open-data
cd open-data

git remote add origin https://github.com/statsbomb/open-data.git
git config core.sparseCheckout true

printf "data/\n" > .git/info/sparse-checkout

git fetch --depth=1 origin master
git checkout master

cd ..
mv open-data/data/* .
rm -r open-data
```

Now , with the python code 'etl_t_statsbomb_from_json_to_parquet.py' process the original JSON files downloaded before into a parquet files to a further data exploration and cleaning at scale. 

```bash
python src/etl/etl_t_statsbomb_from_json_to_parquet.py --data-root data/raw/statsbomb_open_repo/ --out-root data/proc/statsbomb_open_repo/ --log-dir logs/ --log-file statsbomb.log
```

### Transform 

#### Football Data UK

First , we execute the code 'etl_t_statsbomb_from_json_to_parquet.py' the data from Full time, half-time and complementary data into a parquet structure open to cleaning and feature engineering: 

```bash
python src/etl/etl_t_footballdata_uk_preproc.py
```

**Football Feature Engineering — Code & Feature Reference - Pipeline Mechanics**

After this, we use '' to clean the original results data into a clean data and creating two derivative tables: the position of teams by date and fitness and a full results and data engineered features ready to model trainiing and fitting. 


`cast_columns` → type coercion + LEAGUE fill  
`add_match_meta` → per-match derived columns  
`build_features` → rates, momentum, multi-window rolling  
`infer_league_table` → cumulative season standings  
`save_parquet` → partitioned output (LEAGUE / YEAR)

Rolling via `_rolling_team`: `groupby(team) → shift(1) → rolling(k).mean()`  
`shift(1)` enforces strict temporal leakage prevention.  
Windows default: `k ∈ {3, 5, 10}`

```bash
python src/etl/etl_t_footballdata_uk_proc.py
```

Here a brief description of the geenrated results:

**Features:**

- Match Meta
| Feature | Description |
|---|---|
| `SEASON` | Football season label (e.g. `2023/24`, July cutoff) |
| `YEAR / MONTH / WEEK` | Date decomposition |
| `FT/HT_TOTAL_GOALS` | Sum of both teams' goals |
| `FT/HT_GOAL_DIFF` | Home minus away goals |
| `HOME/AWAY_POINTS` | Points earned per match (3/1/0) |
| `BTTS` | Both teams scored flag |
| `OVER_2.5 / OVER_3.5` | Total goals threshold flags |
| `CLEAN_SHEET_H/A` | Zero goals conceded flag |
| `HT_FT_SAME` | Half-time result held at full-time |
| `HOME/AWAY_COMEBACK` | Result reversed from HT to FT |

- Shot & Discipline Rates

| Feature | Formula |
|---|---|
| `HOME/AWAY_SHOT_ACC` | `HST / HS` |
| `HOME/AWAY_CONV_RATE` | `Goals / HST` |
| `HOME/AWAY_FOUL_RATE` | `Fouls / Shots` |
| `HOME/AWAY_CARD_RATE` | `(Y+R) / Fouls` |
| `SHOT_SHARE_HOME` | `HS / (HS+AS)` |
| `SHOT_ON_T_SHARE_H` | `HST / (HST+AST)` |
| `CORNER_DIFF` | `HC - AC` |

- Half-Time Momentum

| Feature | Description |
|---|---|
| `HT_LEAD_HOME/AWAY` | Team leading at HT |
| `HT_LEVEL` | Scores level at HT |
| `SH_GOALS_H/A` | Second-half goals (FT − HT) |

- Rolling Windows (`k ∈ {3, 5, 10}`)

| Feature | Description |
|---|---|
| `HOME/AWAY_ROLLk_GF` | Avg goals scored last k matches |
| `HOME/AWAY_ROLLk_GA` | Avg goals conceded last k matches |
| `HOME/AWAY_ROLLk_SHOTS` | Avg shots last k matches |
| `HOME/AWAY_ROLLk_SHOTS_T` | Avg shots on target last k matches |
| `HOME/AWAY_ROLLk_PTS` | Avg points last k matches (form proxy) |
| `HOME/AWAY_ROLLk_xGD` | `ROLL_GF − ROLL_GA` (attack/defense balance) |
| `ROLLk_STRENGTH_DIFF` | `HOME_PTS − AWAY_PTS` (relative form) |

- League Table (`infer_league_table`)

| Feature | Description |
|---|---|
| `MP / W / D / L` | Matches played, won, drawn, lost |
| `GF / GA / GD` | Goals for, against, differential |
| `PTS / PPG` | Points, points per game |
| `POSITION` | League rank by points |
| `FORM_PTS / FORM_STR` | Points + W/D/L string over last `FITNESS_K` matches |
| `HOME_*/AWAY_*` | All above split by venue |



## Data Analytics and Modelling 




## Predictive Models 





