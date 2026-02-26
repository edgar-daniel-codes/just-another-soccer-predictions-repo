#!/usr/bin/env python3

### Soccer Predictions - ETL 
### Footbal Data UK Column format  
### By Edgar Daniel

# Auxiliar code for transforming and formating of the historical raw data 
# by filtering common columns and mantain a more standarized files. 
# 


### -------------------------------------------------------------------------------
### Needed libraries --------------------------------------------------------------


from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

### -------------------------------------------------------------------------------
### Main Functions and classes ----------------------------------------------------


@dataclass(frozen=True)
class StandardizedResult:
    """Container for standardized outputs."""
    df: pd.DataFrame
    shared_columns: List[str]


def _clean_raw_column_name(col: str) -> str:
    """
    Clean a raw CSV column header.

    :param col: Raw column header as read by pandas.
    :return: Cleaned column header.
    """
    if col is None:
        return ""
    # Remove BOM artifacts and normalize whitespace
    cleaned = str(col).replace("\ufeff", "").replace("ï»¿", "").strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def _infer_season_from_path(file_path: str) -> Optional[str]:
    """
    Infer season string from common Football-Data folder patterns like /2526/ (=> 2025/2026).

    :param file_path: CSV path or source_file-like string.
    :return: Season in 'YYYY/YYYY' format if found, else None.
    """
    # Common pattern on football-data: .../mmz4281/2526/T1.csv
    match = re.search(r"/(\d{4})/", file_path.replace("\\", "/"))
    if not match:
        return None

    yy1 = int(match.group(1)[:2])   # 25
    yy2 = int(match.group(1)[2:])   # 26

    # Heuristic: interpret 00-79 as 2000s, 80-99 as 1900s (rare in this dataset use-case)
    start_year = 2000 + yy1 if yy1 <= 79 else 1900 + yy1
    end_year = 2000 + yy2 if yy2 <= 79 else 1900 + yy2
    return f"{start_year}/{end_year}"


def _infer_league_from_div(div_value: Optional[str]) -> Optional[str]:
    """
    Infer league identifier from Div.

    Note: Football-Data defines Div as "League Division" (a division code like E0, SP1, T1, etc.).
    We store that code as LEAGUE by default.

    :param div_value: Division code from the dataset.
    :return: League code.
    """
    if div_value is None:
        return None
    div_value = str(div_value).strip()
    return div_value if div_value else None


def _build_column_alias_map() -> Dict[str, str]:
    """
    Build a mapping from many observed Football-Data column headers to standardized names.

    Standardized names are UPPER_SNAKE_CASE.

    :return: Alias map {raw_name -> standardized_name}.
    """
    # Core identity columns
    alias_map = {
        "Country": "COUNTRY",
        "country": "COUNTRY",
        "League": "LEAGUE",
        "Season": "SEASON",
        "source_file": "SOURCE_FILE",
        "SourceFile": "SOURCE_FILE",
        "Div": "DIV",

        # Match time
        "Date": "DATE",
        "Time": "TIME",

        # Teams
        "Home": "HOME_TEAM",
        "Away": "AWAY_TEAM",
        "HomeTeam": "HOME_TEAM",
        "AwayTeam": "AWAY_TEAM",

        # Full-time goals/results (Football-Data notes: FTHG and HG; FTAG and AG; FTR and Res)
        "FTHG": "FT_HOME_GOALS",
        "HG": "FT_HOME_GOALS",
        "FTAG": "FT_AWAY_GOALS",
        "AG": "FT_AWAY_GOALS",
        "FTR": "FT_RESULT",
        "Res": "FT_RESULT",

        # Half-time goals/results
        "HTHG": "HT_HOME_GOALS",
        "HTAG": "HT_AWAY_GOALS",
        "HTR": "HT_RESULT",

        # Pinnacle opening odds (PSH/PSD/PSA) and closing odds (PSCH/PSCD/PSCA)
        "PSH": "PSH",
        "PSD": "PSD",
        "PSA": "PSA",
        "PSCH": "PSCH",
        "PSCD": "PSCD",
        "PSCA": "PSCA",

        # Market max/avg (opening + closing)
        "MaxH": "MAXH",
        "MaxD": "MAXD",
        "MaxA": "MAXA",
        "AvgH": "AVGH",
        "AvgD": "AVGD",
        "AvgA": "AVGA",
        "MaxCH": "MAXCH",
        "MaxCD": "MAXCD",
        "MaxCA": "MAXCA",
        "AvgCH": "AVGCH",
        "AvgCD": "AVGCD",
        "AvgCA": "AVGCA",

        # Betfair Exchange closing odds
        "BFECH": "BFECH",
        "BFECD": "BFECD",
        "BFECA": "BFECA",

        # Bet365 closing 1X2 odds
        "B365CH": "B365CH",
        "B365CD": "B365CD",
        "B365CA": "B365CA",
    }

    return alias_map


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column headers to UPPER_SNAKE_CASE using an alias map.

    :param df: Raw dataframe.
    :return: Dataframe with standardized column names (where recognized).
    """
    alias_map = _build_column_alias_map()

    df.columns = (
            df.columns
            .str.replace("\ufeff", "", regex=False)
            .str.replace("ï»¿", "", regex=False)
            .str.strip()
        )

    cleaned_cols = [_clean_raw_column_name(c) for c in df.columns]
    df = df.copy()
    df.columns = cleaned_cols

    rename_dict: Dict[str, str] = {}
    for c in df.columns:
        if c in alias_map:
            rename_dict[c] = alias_map[c]

    df = df.rename(columns=rename_dict)

    # Drop purely "Unnamed: n" columns (common CSV artifacts)
    unnamed_cols = [c for c in df.columns if re.fullmatch(r"Unnamed: \d+", str(c))]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)

    # If renaming created duplicate columns (e.g., Div and ï»¿Div -> DIV), keep the first
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    return df


def _standardize_semantics(
    df: pd.DataFrame,
    file_path: str,
    default_country: Optional[str],
) -> pd.DataFrame:
    """
    Standardize semantics: COUNTRY/LEAGUE/SEASON derivation, DATE parsing.

    :param df: Dataframe with standardized column names.
    :param file_path: Source file path for inference.
    :param default_country: Country value to use if missing in file.
    :return: Dataframe with standardized semantics.
    """
    df = df.copy()

    # SOURCE_FILE
    if "SOURCE_FILE" not in df.columns:
        df["SOURCE_FILE"] = str(file_path)

    # COUNTRY
    if "COUNTRY" not in df.columns:
        df["COUNTRY"] = default_country

    # SEASON
    if "SEASON" not in df.columns:
        inferred_season = _infer_season_from_path(str(file_path))
        df["SEASON"] = inferred_season

    # LEAGUE: if already present keep; else derive from DIV when available
    if "DIV" not in df.columns:
        if "LEAGUE" in df.columns:
            # Use the division code as a league identifier
            df["DIV"] = df["COUNTRY"] + df["LEAGUE"]
        else:
            df["DIV"] = None

    # DATE parsing (Football-Data notes: dd/mm/yy) where possible
    if "DATE" in df.columns:
        df["DATE"] = df["DATE"].astype("str").fillna("")
    #    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce", dayfirst=True)

    # TIME: keep as string (times can be missing/blank)
    if "TIME" in df.columns:
        df["TIME"] = df["TIME"].astype(str).replace({"nan": None}).where(df["TIME"].notna(), None)

    return df


def standardize_football_data_csvs(
    csv_files: Sequence[str],
    default_country: Optional[str] = "Turkey",
    keep_only_core: bool = False,
) -> StandardizedResult:
    """
    Read a list of Football-Data-style CSVs, standardize columns/semantics, and compute shared columns.

    Outputs:
      - A concatenated standardized dataframe (outer union of columns)
      - A list of columns shared across ALL standardized dataframes

    :param csv_files: List of CSV file paths.
    :param default_country: Default country to set when COUNTRY is absent.
    :param keep_only_core: If True, restrict each dataframe to a conservative "core" schema before concatenation.
    :return: StandardizedResult(df=combined_df, shared_columns=shared_cols).
    """
    core_cols = [
        "COUNTRY", "LEAGUE", "SEASON", "DATE", "TIME",
        "HOME_TEAM", "AWAY_TEAM",
        "FT_HOME_GOALS", "FT_AWAY_GOALS", "FT_RESULT",
        "HT_HOME_GOALS", "HT_AWAY_GOALS", "HT_RESULT",
        # commonly requested odds features
        "PSH", "PSD", "PSA",
        "PSCH", "PSCD", "PSCA",
        "MAXH", "MAXD", "MAXA", "AVGH", "AVGD", "AVGA",
        "MAXCH", "MAXCD", "MAXCA", "AVGCH", "AVGCD", "AVGCA",
        "BFECH", "BFECD", "BFECA",
        "B365CH", "B365CD", "B365CA",
        "SOURCE_FILE",
    ]

    standardized_dfs: List[pd.DataFrame] = []

    for file_path in csv_files:
        # utf-8-sig handles BOM cleanly across these feeds
        raw_df = pd.read_csv(file_path, encoding="utf-8-sig", low_memory=False)
        
        
        df = _standardize_columns(raw_df)
        df = _standardize_semantics(df, str(file_path), default_country=default_country)

        if keep_only_core:
            existing_core = [c for c in core_cols if c in df.columns]
            df = df[existing_core]

        standardized_dfs.append(df)

    # Shared columns across all standardized dfs
    column_sets = [set(d.columns) for d in standardized_dfs]
    shared_cols = sorted(set.intersection(*column_sets)) if column_sets else []

    # Concatenate (outer union)
    combined_df = pd.concat(standardized_dfs, axis=0, ignore_index=True, sort=False)

    return StandardizedResult(df=combined_df, shared_columns=shared_cols)

