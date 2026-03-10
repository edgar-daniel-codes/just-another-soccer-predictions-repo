#!/usr/bin/env python3

### Football Data UK
### Processing, League Table Inference & Feature Engineering
### By Edgar Daniel

"""
Input:  Three parquet files (FT / HT / COMPLEMENT) with varying column availability
Output: Per-source processed matches, inferred league tables, and feature tables
        all partitioned by LEAGUE / YEAR
"""

### -------------------------------------------------------------------------------
### Needed libraries --------------------------------------------------------------

import pandas as pd
import numpy as np
from pathlib import Path
from etl_utils_general import *

### -------------------------------------------------------------------------------
### Parameters --------------------------------------------------------------------

RAW_DATA_DIR   = "./data/raw/footballdata_uk/"
PROC_DATA_DIR  = "./data/proc/footballdata_uk/"
CLEAN_DATA_DIR = "./data/clean/footballdata_uk/"

PROC_HISTORIC_FILENAME_COMPLEMENT = "footballdatauk_historic_matches_complement"
PROC_HISTORIC_FILENAME_HT         = "footballdatauk_historic_matches_ht"
PROC_HISTORIC_FILENAME_FT         = "footballdatauk_historic_matches_ft"

INT_COLS_FT = [
    "FT_HOME_GOALS", "FT_AWAY_GOALS"
]
INT_COLS_HT = INT_COLS_FT + [
    "HT_HOME_GOALS", "HT_AWAY_GOALS"
]
INT_COLS_COMPLEMENT = INT_COLS_HT + [
    "HS", "AS", "HST", "AST", "HF", "AF", "HC", "AC", "HY", "AY", "HR", "AR"
]

# Source registry: (filename, int_cols, has_ht, has_shots)
SOURCES = {
    "ft":         (PROC_HISTORIC_FILENAME_FT,         INT_COLS_FT,         False, False),
    "ht":         (PROC_HISTORIC_FILENAME_HT,         INT_COLS_HT,         True,  False),
    "complement": (PROC_HISTORIC_FILENAME_COMPLEMENT, INT_COLS_COMPLEMENT, True,  True ),
}

POINTS_MAP  = {"H": (3, 0), "D": (1, 1), "A": (0, 3)}
FITNESS_K   = 5
MIN_PERIODS = 1
WINDOWS     = [3, 4, 5]

DIR_LOG  = Path("./logs/")
FILE_LOG = "footballdatauk.log"

safe_mkdir(Path(CLEAN_DATA_DIR))
safe_mkdir(DIR_LOG)

### -------------------------------------------------------------------------------
### Functions ---------------------------------------------------------------------

def cast_columns(df: pd.DataFrame, int_cols: list[str]) -> pd.DataFrame:
    df["DATE"]   = pd.to_datetime(df["DATE"], format="mixed")
    df["LEAGUE"] = np.where(
        df["LEAGUE"].astype(str).str.strip() == "",
        df["COUNTRY"].astype(str) + " " + df["DIV"].astype(str),
        df["LEAGUE"]
    )
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    return df


def add_match_meta(df: pd.DataFrame, has_ht: bool, has_shots: bool) -> pd.DataFrame:
    df["YEAR"]   = df["DATE"].dt.year
    df["MONTH"]  = df["DATE"].dt.month
    df["WEEK"]   = df["DATE"].dt.isocalendar().week.astype(int)
    df["SEASON"] = df["DATE"].apply(
        lambda d: f"{d.year}/{str(d.year+1)[-2:]}" if d.month >= 7
                  else f"{d.year-1}/{str(d.year)[-2:]}"
    )

    df["FT_TOTAL_GOALS"] = df["FT_HOME_GOALS"] + df["FT_AWAY_GOALS"]
    df["FT_GOAL_DIFF"]   = df["FT_HOME_GOALS"] - df["FT_AWAY_GOALS"]
    df["HOME_POINTS"]    = df["FT_RESULT"].map(lambda r: POINTS_MAP.get(r, (0, 0))[0])
    df["AWAY_POINTS"]    = df["FT_RESULT"].map(lambda r: POINTS_MAP.get(r, (0, 0))[1])
    df["HOME_WIN"]       = (df["FT_RESULT"] == "H").astype(int)
    df["AWAY_WIN"]       = (df["FT_RESULT"] == "A").astype(int)
    df["DRAW"]           = (df["FT_RESULT"] == "D").astype(int)
    df["BTTS"]           = ((df["FT_HOME_GOALS"] > 0) & (df["FT_AWAY_GOALS"] > 0)).astype(int)
    df["OVER_2_5"]       = (df["FT_TOTAL_GOALS"] > 2.5).astype(int)
    df["OVER_3_5"]       = (df["FT_TOTAL_GOALS"] > 3.5).astype(int)
    df["CLEAN_SHEET_H"]  = (df["FT_AWAY_GOALS"] == 0).astype(int)
    df["CLEAN_SHEET_A"]  = (df["FT_HOME_GOALS"] == 0).astype(int)

    if has_ht:
        df["HT_TOTAL_GOALS"] = df["HT_HOME_GOALS"] + df["HT_AWAY_GOALS"]
        df["HT_GOAL_DIFF"]   = df["HT_HOME_GOALS"] - df["HT_AWAY_GOALS"]
        df["HT_FT_SAME"]     = (df["HT_RESULT"] == df["FT_RESULT"]).astype(int)
        df["HOME_COMEBACK"]  = ((df["HT_RESULT"] == "A") & (df["FT_RESULT"] == "H")).astype(int)
        df["AWAY_COMEBACK"]  = ((df["HT_RESULT"] == "H") & (df["FT_RESULT"] == "A")).astype(int)
        df["SH_GOALS_H"]     = df["FT_HOME_GOALS"] - df["HT_HOME_GOALS"]
        df["SH_GOALS_A"]     = df["FT_AWAY_GOALS"] - df["HT_AWAY_GOALS"]
        df["HT_LEAD_HOME"]   = (df["HT_HOME_GOALS"] > df["HT_AWAY_GOALS"]).astype(int)
        df["HT_LEAD_AWAY"]   = (df["HT_HOME_GOALS"] < df["HT_AWAY_GOALS"]).astype(int)
        df["HT_LEVEL"]       = (df["HT_HOME_GOALS"] == df["HT_AWAY_GOALS"]).astype(int)
        df["HT_RESULT_NUM"]  = df["HT_RESULT"].map({"H": 1, "D": 0, "A": -1})

    if has_shots:
        df["SH_TOTAL"]         = df["HS"]  + df["AS"]
        df["ST_TOTAL"]         = df["HST"] + df["AST"]
        df["HOME_SHOT_ACC"]    = np.where(df["HS"]  > 0, df["HST"] / df["HS"],  np.nan)
        df["AWAY_SHOT_ACC"]    = np.where(df["AS"]  > 0, df["AST"] / df["AS"],  np.nan)
        df["HOME_CONV_RATE"]   = np.where(df["HST"] > 0, df["FT_HOME_GOALS"] / df["HST"], np.nan)
        df["AWAY_CONV_RATE"]   = np.where(df["AST"] > 0, df["FT_AWAY_GOALS"] / df["AST"], np.nan)
        df["HOME_FOUL_RATE"]   = np.where(df["HS"]  > 0, df["HF"] / df["HS"],  np.nan)
        df["AWAY_FOUL_RATE"]   = np.where(df["AS"]  > 0, df["AF"] / df["AS"],  np.nan)
        df["HOME_CARD_RATE"]   = np.where(df["HF"]  > 0, (df["HY"] + df["HR"]) / df["HF"], np.nan)
        df["AWAY_CARD_RATE"]   = np.where(df["AF"]  > 0, (df["AY"] + df["AR"]) / df["AF"], np.nan)
        df["SHOT_SHARE_HOME"]  = np.where(df["SH_TOTAL"] > 0, df["HS"]  / df["SH_TOTAL"], np.nan)
        df["SHOT_ON_T_SHARE_H"]= np.where(df["ST_TOTAL"] > 0, df["HST"] / df["ST_TOTAL"], np.nan)
        df["CORNER_DIFF"]      = df["HC"] - df["AC"]

    df["FT_RESULT_NUM"] = df["FT_RESULT"].map({"H": 1, "D": 0, "A": -1})
    return df


def infer_league_table(df: pd.DataFrame) -> pd.DataFrame:
    records = []
    for (league, season), grp in df.groupby(["LEAGUE", "SEASON"]):
        teams = set(grp["HOME_TEAM"]) | set(grp["AWAY_TEAM"])
        for team in teams:
            home = grp[grp["HOME_TEAM"] == team]
            away = grp[grp["AWAY_TEAM"] == team]

            gf  = home["FT_HOME_GOALS"].sum() + away["FT_AWAY_GOALS"].sum()
            ga  = home["FT_AWAY_GOALS"].sum() + away["FT_HOME_GOALS"].sum()
            w   = home["HOME_WIN"].sum()       + away["AWAY_WIN"].sum()
            d   = home["DRAW"].sum()           + away["DRAW"].sum()
            l   = home["AWAY_WIN"].sum()       + away["HOME_WIN"].sum()
            mp  = len(home) + len(away)
            pts = w * 3 + d

            team_matches = pd.concat([
                home[["DATE", "FT_RESULT"]].assign(SIDE="H"),
                away[["DATE", "FT_RESULT"]].assign(SIDE="A")
            ]).sort_values("DATE").tail(FITNESS_K)

            def result_pts(row):
                return POINTS_MAP.get(row["FT_RESULT"], (0, 0))[0 if row["SIDE"] == "H" else 1]

            form_pts = team_matches.apply(result_pts, axis=1).sum()
            form_str = "".join(
                "W" if result_pts(r) == 3 else ("D" if result_pts(r) == 1 else "L")
                for _, r in team_matches.iterrows()
            )

            records.append({
                "LEAGUE": league, "SEASON": season, "TEAM": team,
                "MP": mp, "W": w, "D": d, "L": l,
                "GF": gf, "GA": ga, "GD": gf - ga,
                "PTS": pts,
                "PPG": round(pts / mp, 3) if mp > 0 else 0,
                "FORM_PTS": form_pts, "FORM_STR": form_str,
                "HOME_MP": len(home), "HOME_W": home["HOME_WIN"].sum(),
                "HOME_D": home["DRAW"].sum(),
                "HOME_GF": home["FT_HOME_GOALS"].sum(),
                "HOME_GA": home["FT_AWAY_GOALS"].sum(),
                "AWAY_MP": len(away), "AWAY_W": away["AWAY_WIN"].sum(),
                "AWAY_D": away["DRAW"].sum(),
                "AWAY_GF": away["FT_AWAY_GOALS"].sum(),
                "AWAY_GA": away["FT_HOME_GOALS"].sum(),
            })

    df_table = pd.DataFrame(records)
    df_table["POSITION"] = (
        df_table.groupby(["LEAGUE", "SEASON"])["PTS"]
        .rank(method="min", ascending=False)
        .astype(int)
    )
    return df_table.sort_values(["LEAGUE", "SEASON", "POSITION"])


def _rolling_team(df_sorted: pd.DataFrame, team_col: str, goals_for_col: str,
                  goals_against_col: str, side: str, k: int,
                  shots_col: str = None, shots_t_col: str = None) -> pd.DataFrame:
    g      = df_sorted.groupby(team_col)
    prefix = f"{side}_ROLL{k}"

    df_sorted[f"{prefix}_GF"]  = g[goals_for_col].transform(lambda x: x.shift(1).rolling(k, MIN_PERIODS).mean())
    df_sorted[f"{prefix}_GA"]  = g[goals_against_col].transform(lambda x: x.shift(1).rolling(k, MIN_PERIODS).mean())
    df_sorted[f"{prefix}_PTS"] = g[f"{side}_POINTS"].transform(lambda x: x.shift(1).rolling(k, MIN_PERIODS).mean())

    if shots_col and shots_t_col:
        df_sorted[f"{prefix}_SHOTS"]   = g[shots_col].transform(lambda x: x.shift(1).rolling(k, MIN_PERIODS).mean())
        df_sorted[f"{prefix}_SHOTS_T"] = g[shots_t_col].transform(lambda x: x.shift(1).rolling(k, MIN_PERIODS).mean())

    return df_sorted


def build_features(df: pd.DataFrame, has_shots: bool,
                   windows: list[int] = WINDOWS) -> pd.DataFrame:
    df = df.sort_values(["LEAGUE", "DATE"]).copy()

    for k in windows:
        df = _rolling_team(df, "HOME_TEAM", "FT_HOME_GOALS", "FT_AWAY_GOALS",
                           "HOME", k,
                           shots_col="HS" if has_shots else None,
                           shots_t_col="HST" if has_shots else None)
        df = _rolling_team(df, "AWAY_TEAM", "FT_AWAY_GOALS", "FT_HOME_GOALS",
                           "AWAY", k,
                           shots_col="AS" if has_shots else None,
                           shots_t_col="AST" if has_shots else None)

        df[f"HOME_ROLL{k}_xGD"]      = df[f"HOME_ROLL{k}_GF"] - df[f"HOME_ROLL{k}_GA"]
        df[f"AWAY_ROLL{k}_xGD"]      = df[f"AWAY_ROLL{k}_GF"] - df[f"AWAY_ROLL{k}_GA"]
        df[f"ROLL{k}_STRENGTH_DIFF"] = df[f"HOME_ROLL{k}_PTS"] - df[f"AWAY_ROLL{k}_PTS"]

    return df


def save_parquet(df: pd.DataFrame, base_dir: str, filename: str,
                 partition_cols: list[str]) -> None:
    out = Path(base_dir) / filename
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, partition_cols=partition_cols, index=False)


### -------------------------------------------------------------------------------
### Main --------------------------------------------------------------------------

def main() -> None:
    import logging

    logger = logging.getLogger("etl-footballdatauk-transform")
    logger.setLevel(logging.INFO)
    fmt       = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    formatter = logging.Formatter(fmt)
    console   = logging.StreamHandler()
    console.setFormatter(formatter)
    file_h    = logging.FileHandler(DIR_LOG / FILE_LOG)
    file_h.setFormatter(formatter)
    logger.addHandler(console)
    logger.addHandler(file_h)

    for source, (filename, int_cols, has_ht, has_shots) in SOURCES.items():
        logger.info(f"── Processing source: {source} ──")

        try:
            df_raw = pd.read_parquet(PROC_DATA_DIR + filename)
            logger.info(f"[{source}] Loaded: {df_raw.shape}")
        except Exception as e:
            logger.error(f"[{source}] Load failed: {e}")
            raise

        try:
            df = cast_columns(df_raw.copy(), int_cols)
            df = add_match_meta(df, has_ht=has_ht, has_shots=has_shots)
            logger.info(f"[{source}] Cast + meta done: {df.shape}")
        except Exception as e:
            logger.error(f"[{source}] Cast/meta failed: {e}")
            raise

        try:
            save_parquet(df, PROC_DATA_DIR, f"matches_processed_{source}", ["LEAGUE", "YEAR"])
            logger.info(f"[{source}] Saved: matches_processed_{source}")
        except Exception as e:
            logger.error(f"[{source}] Save matches failed: {e}")
            raise

        try:
            df_table = infer_league_table(df)
            save_parquet(df_table, PROC_DATA_DIR, f"league_table_{source}", ["LEAGUE"])
            logger.info(f"[{source}] Saved: league_table_{source} — {df_table.shape}")
        except Exception as e:
            logger.error(f"[{source}] League table failed: {e}")
            raise

        try:
            df_features = build_features(df.copy(), has_shots=has_shots)
            save_parquet(df_features, CLEAN_DATA_DIR, f"matches_features_{source}", ["LEAGUE", "YEAR"])
            logger.info(f"[{source}] Saved: matches_features_{source} — {df_features.shape}")
        except Exception as e:
            logger.error(f"[{source}] Features failed: {e}")
            raise

    logger.info("All sources processed successfully.")


if __name__ == "__main__":
    main()