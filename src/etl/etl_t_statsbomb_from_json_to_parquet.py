#!/usr/bin/env python3

### Soccer Predictions - ETL 
### Statsbomb Transform 
### By Edgar Daniel

# Auxiliar code for transforming raw JSON files from 
# Statsbomb Open Data into raw pre-processed parquet files 
# 


# Expected repo layout (StatsBomb Open Data):
#- data/competitions.json
#- data/matches/<competition_id>/<season_id>.json
#- data/events/<match_id>.json
#- data/lineups/<match_id>.json
#- data/three-sixty/<match_id>.json



### -------------------------------------------------------------------------------
### Needed libraries --------------------------------------------------------------

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import uuid
import pyarrow as pa
import pyarrow.parquet as pq

### -------------------------------------------------------------------------------
### Function and classes ----------------------------------------------------------


def write_dataset_batch(
    df: pd.DataFrame,
    out_dir: Path,
    partition_cols: List[str],
) -> None:
    """
    Append a batch to a partitioned Parquet dataset.

    :param df: Batch dataframe.
    :param out_dir: Dataset directory.
    :param partition_cols: Partition columns.
    """
    safe_mkdir(out_dir)

    # Ensure partition cols exist
    for c in partition_cols:
        if c not in df.columns:
            df[c] = pd.NA

    # Important: keep nullable columns (default behavior)
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Unique filename per batch avoids collisions
    pq.write_to_dataset(
        table,
        root_path=str(out_dir),
        partition_cols=partition_cols,
        basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
    )



def read_json(path: Path) -> Any:
    """
    Read a JSON file.

    :param path: Path to JSON file.
    :return: Parsed JSON object.
    """
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def safe_mkdir(path: Path) -> None:
    """
    Create a directory if it does not exist.

    :param path: Directory path.
    """
    path.mkdir(parents=True, exist_ok=True)


def flatten_records(records: List[Dict[str, Any]], sep: str = ".") -> pd.DataFrame:
    """
    Flatten a list of nested dict records into a DataFrame using dot-notation columns.

    :param records: List of dict-like records.
    :param sep: Separator for nested keys.
    :return: Flattened pandas DataFrame.
    """
    if not records:
        return pd.DataFrame()
    return pd.json_normalize(records, sep=sep)


def json_dumps_or_none(value: Any) -> Optional[str]:
    """
    JSON-serialize a value; return None for null-like values.

    :param value: Any Python object.
    :return: JSON string or None.
    """
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError:
        return None


def align_columns(df: pd.DataFrame, all_columns: List[str]) -> pd.DataFrame:
    """
    Align DataFrame columns to a target superset (missing columns become null).

    :param df: Input DataFrame.
    :param all_columns: Target superset column list.
    :return: DataFrame with exactly all_columns in that order.
    """
    for col in all_columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df[all_columns]


def write_partitioned_parquet(
    df: pd.DataFrame,
    out_dir: Path,
    partition_cols: List[str],
) -> None:
    """
    Write DataFrame as a Parquet dataset partitioned by the given columns.

    Arrow will keep fields nullable by default; missing values become nulls.

    :param df: DataFrame to write.
    :param out_dir: Output dataset directory.
    :param partition_cols: Columns to partition by.
    """
    safe_mkdir(out_dir)

    # Ensure partition columns exist; if not, create them as null to avoid crashes.
    for c in partition_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df.to_parquet(
        out_dir,
        engine="pyarrow",
        index=False,
        partition_cols=partition_cols,
    )


def build_match_index(matches_root: Path) -> pd.DataFrame:
    """
    Build an index DataFrame mapping match_id -> competition_id, season_id.

    :param matches_root: data/matches directory.
    :return: DataFrame with columns: match_id, competition_id, season_id.
    """
    rows: List[Dict[str, Any]] = []

    if not matches_root.exists():
        return pd.DataFrame(columns=["match_id", "competition_id", "season_id"])

    for comp_dir in matches_root.iterdir():
        if not comp_dir.is_dir():
            continue
        comp_id = comp_dir.name

        for season_file in comp_dir.glob("*.json"):
            season_id = season_file.stem
            try:
                matches = read_json(season_file)
            except json.JSONDecodeError:
                continue

            if not isinstance(matches, list):
                continue

            for m in matches:
                match_id = m.get("match_id")
                if match_id is None:
                    continue
                rows.append(
                    {
                        "match_id": int(match_id),
                        "competition_id": int(comp_id) if str(comp_id).isdigit() else comp_id,
                        "season_id": int(season_id) if str(season_id).isdigit() else season_id,
                    }
                )

    if not rows:
        return pd.DataFrame(columns=["match_id", "competition_id", "season_id"])

    return pd.DataFrame(rows).drop_duplicates(subset=["match_id"])


def load_competitions(data_root: Path) -> pd.DataFrame:
    """
    Load competitions.json into a DataFrame.

    :param data_root: Root data directory (contains competitions.json).
    :return: Competitions DataFrame.
    """
    competitions_path = data_root / "competitions.json"
    if not competitions_path.exists():
        return pd.DataFrame()

    competitions = read_json(competitions_path)
    if not isinstance(competitions, list):
        return pd.DataFrame()

    return flatten_records(competitions)


def export_matches_batched(matches_root: Path, out_root: Path) -> None:
    """
    Export matches by (competition_id/season_id) file, writing each file as a batch.

    :param matches_root: data/matches directory.
    :param out_root: Output root directory.
    """
    if not matches_root.exists():
        return

    for comp_dir in matches_root.iterdir():
        if not comp_dir.is_dir():
            continue
        comp_id = int(comp_dir.name) if comp_dir.name.isdigit() else comp_dir.name

        for season_file in comp_dir.glob("*.json"):
            season_id = int(season_file.stem) if season_file.stem.isdigit() else season_file.stem

            try:
                matches = read_json(season_file)
            except json.JSONDecodeError:
                continue

            if not isinstance(matches, list) or not matches:
                continue

            df = flatten_records(matches)
            df["competition_id"] = comp_id
            df["season_id"] = season_id

            write_dataset_batch(
                df=df,
                out_dir=out_root / "matches",
                partition_cols=["competition_id", "season_id"],
            )


def export_events_batched(
    events_root: Path,
    match_index: pd.DataFrame,
    out_root: Path,
    batch_size_matches: int = 50,
) -> None:
    """
    Export events in batches of match files.

    :param events_root: data/events directory.
    :param match_index: match_id -> competition_id, season_id mapping.
    :param out_root: Output root directory.
    :param batch_size_matches: Number of matches per in-memory batch.
    """
    if not events_root.exists():
        return

    idx = match_index.set_index("match_id", drop=False) if not match_index.empty else None

    batch_events: List[pd.DataFrame] = []
    batch_ff: List[pd.DataFrame] = []

    def flush() -> None:
        if batch_events:
            df_events = pd.concat(batch_events, ignore_index=True, sort=False)
            write_dataset_batch(
                df=df_events,
                out_dir=out_root / "events",
                partition_cols=["competition_id", "season_id"],
            )
            batch_events.clear()

        if batch_ff:
            df_ff = pd.concat(batch_ff, ignore_index=True, sort=False)
            write_dataset_batch(
                df=df_ff,
                out_dir=out_root / "events_shot_freeze_frame",
                partition_cols=["competition_id", "season_id"],
            )
            batch_ff.clear()

    count = 0

    for event_file in events_root.glob("*.json"):
        match_id_str = event_file.stem
        if not match_id_str.isdigit():
            continue
        match_id = int(match_id_str)

        try:
            events = read_json(event_file)
        except json.JSONDecodeError:
            continue

        if not isinstance(events, list) or not events:
            continue

        # Freeze-frame rows
        ff_rows: List[Dict[str, Any]] = []
        for e in events:
            shot = e.get("shot")
            if isinstance(shot, dict):
                freeze_frame = shot.get("freeze_frame")
                if isinstance(freeze_frame, list):
                    for p in freeze_frame:
                        if not isinstance(p, dict):
                            continue
                        ff_rows.append(
                            {
                                "match_id": match_id,
                                "event_id": e.get("id"),
                                "player_id": (p.get("player") or {}).get("id"),
                                "player_name": (p.get("player") or {}).get("name"),
                                "teammate": p.get("teammate"),
                                "keeper": p.get("keeper"),
                                "location": json_dumps_or_none(p.get("location")),
                                "position_id": (p.get("position") or {}).get("id"),
                                "position_name": (p.get("position") or {}).get("name"),
                            }
                        )

        df = flatten_records(events)
        df["match_id"] = match_id

        if idx is not None and match_id in idx.index:
            df["competition_id"] = idx.loc[match_id, "competition_id"]
            df["season_id"] = idx.loc[match_id, "season_id"]
        else:
            df["competition_id"] = pd.NA
            df["season_id"] = pd.NA

        # JSON-stringify nested object/list columns to keep schema stable-ish
        for col in df.columns:
            if df[col].dtype != "object":
                continue
            sample = df[col].dropna().head(25).tolist()
            if any(isinstance(v, (dict, list)) for v in sample):
                df[col] = df[col].map(json_dumps_or_none)

        batch_events.append(df)

        if ff_rows:
            ff_df = pd.DataFrame(ff_rows)
            if idx is not None and match_id in idx.index:
                ff_df["competition_id"] = idx.loc[match_id, "competition_id"]
                ff_df["season_id"] = idx.loc[match_id, "season_id"]
            else:
                ff_df["competition_id"] = pd.NA
                ff_df["season_id"] = pd.NA
            batch_ff.append(ff_df)

        count += 1
        if count % batch_size_matches == 0:
            flush()

    flush()


def load_lineups(
    lineups_root: Path,
    match_index: pd.DataFrame,
) -> pd.DataFrame:
    """
    Load lineups into a players-level table (one row per player in lineup per match/team).

    :param lineups_root: data/lineups directory.
    :param match_index: match_id -> competition_id, season_id mapping.
    :return: Lineups players DataFrame.
    """
    idx = match_index.set_index("match_id", drop=False) if not match_index.empty else None
    rows: List[Dict[str, Any]] = []

    if not lineups_root.exists():
        return pd.DataFrame()

    for lineup_file in lineups_root.glob("*.json"):
        match_id_str = lineup_file.stem
        if not match_id_str.isdigit():
            continue
        match_id = int(match_id_str)

        try:
            lineup = read_json(lineup_file)
        except json.JSONDecodeError:
            continue

        if not isinstance(lineup, list):
            continue

        for team in lineup:
            team_id = (team.get("team") or {}).get("id")
            team_name = (team.get("team") or {}).get("name")
            players = team.get("lineup")
            if not isinstance(players, list):
                continue

            for p in players:
                if not isinstance(p, dict):
                    continue
                rows.append(
                    {
                        "match_id": match_id,
                        "team_id": team_id,
                        "team_name": team_name,
                        "player_id": (p.get("player") or {}).get("id"),
                        "player_name": (p.get("player") or {}).get("name"),
                        "position_id": (p.get("position") or {}).get("id"),
                        "position_name": (p.get("position") or {}).get("name"),
                        "jersey_number": p.get("jersey_number"),
                        "country_id": (p.get("country") or {}).get("id"),
                        "country_name": (p.get("country") or {}).get("name"),
                        "cards": json_dumps_or_none(p.get("cards")),
                        "positions": json_dumps_or_none(p.get("positions")),
                    }
                )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if idx is not None:
        df = df.merge(
            match_index[["match_id", "competition_id", "season_id"]],
            on="match_id",
            how="left",
        )
    else:
        df["competition_id"] = pd.NA
        df["season_id"] = pd.NA

    return df


def load_three_sixty(
    three_sixty_root: Path,
    match_index: pd.DataFrame,
) -> pd.DataFrame:
    """
    Load StatsBomb 360 into a player-level frames table (one row per visible player per frame).

    :param three_sixty_root: data/three-sixty directory.
    :param match_index: match_id -> competition_id, season_id mapping.
    :return: 360 frames DataFrame.
    """
    idx = match_index.set_index("match_id", drop=False) if not match_index.empty else None
    rows: List[Dict[str, Any]] = []

    if not three_sixty_root.exists():
        return pd.DataFrame()

    for fpath in three_sixty_root.glob("*.json"):
        match_id_str = fpath.stem
        if not match_id_str.isdigit():
            continue
        match_id = int(match_id_str)

        try:
            frames = read_json(fpath)
        except json.JSONDecodeError:
            continue

        if not isinstance(frames, list):
            continue

        for fr in frames:
            fr_id = fr.get("id")
            event_uuid = fr.get("event_uuid")
            visible_area = json_dumps_or_none(fr.get("visible_area"))
            freeze_frame = fr.get("freeze_frame")

            if not isinstance(freeze_frame, list):
                continue

            for p in freeze_frame:
                if not isinstance(p, dict):
                    continue
                rows.append(
                    {
                        "match_id": match_id,
                        "frame_id": fr_id,
                        "event_uuid": event_uuid,
                        "visible_area": visible_area,
                        "player_id": (p.get("player") or {}).get("id"),
                        "player_name": (p.get("player") or {}).get("name"),
                        "teammate": p.get("teammate"),
                        "keeper": p.get("keeper"),
                        "location": json_dumps_or_none(p.get("location")),
                        "position_id": (p.get("position") or {}).get("id"),
                        "position_name": (p.get("position") or {}).get("name"),
                    }
                )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if idx is not None:
        df = df.merge(
            match_index[["match_id", "competition_id", "season_id"]],
            on="match_id",
            how="left",
        )
    else:
        df["competition_id"] = pd.NA
        df["season_id"] = pd.NA

    return df


def parse_args() -> argparse.Namespace:
    """
    Parse CLI args.

    :return: Parsed args.
    """
    parser = argparse.ArgumentParser(description="Convert StatsBomb Open Data JSON to Parquet datasets.")
    parser.add_argument(
        "--data-root",
        type=str,
        required=True,
        help="Path to StatsBomb open-data 'data' directory.",
    )
    parser.add_argument(
        "--out-root",
        type=str,
        required=True,
        help="Output directory where Parquet datasets will be written.",
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        required=True,
        help="Log directory.",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        required=True,
        help="Log file name.",
    )

    return parser.parse_args()

def main() -> None:
    """
    CLI entrypoint.
    """
    
    args =  parse_args()

    data_root = Path(args.data_root).expanduser().resolve()
    out_root = Path(args.out_root).expanduser().resolve()
    
    DIR_LOG = Path(args.log_dir).expanduser().resolve()
    FILE_LOG = args.log_file

    safe_mkdir(out_root)
    safe_mkdir(DIR_LOG)


    # Logger 
    import logging

    logger = logging.getLogger("etl-statsbomb-transform")
    logger.setLevel(logging.INFO)

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    formatter = logging.Formatter(fmt)

    console = logging.StreamHandler()
    console.setFormatter(formatter)

    file = logging.FileHandler(DIR_LOG / FILE_LOG)
    file.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file)

    # Input roots
    matches_root = data_root / "matches"
    events_root = data_root / "events"
    lineups_root = data_root / "lineups"
    three_sixty_root = data_root / "three-sixty"

    # 1) Build match index (match_id -> competition_id, season_id)
    logger.info("Starting match index ---")
    try:
        match_index = build_match_index(matches_root)
        logger.info("Match index completed. ")
    except Exception as e:
        logger.warning(f"Error in match index. \n {e}")


    # 2) Competitions
    logger.info("Starting competitions ---")
    try:
        competitions_df = load_competitions(data_root)
        if not competitions_df.empty:
            write_dataset_batch(competitions_df, out_root / "competitions", partition_cols=[])

        logger.info("Competitions completed. ")
    except Exception as e:
        logger.warning(f"Error in competitions. \n {e}")

    

    # 3) Matches
    logger.info("Starting matches loading ---")
    try:
        export_matches_batched(matches_root, out_root)
        logger.info("Match loading completed. ")
    except Exception as e:
        logger.warning(f"Error in match loading. \n {e}")
        


    # 4) Events + shot freeze-frame subtable
    logger.info("Starting events ---")
    try:
        export_events_batched(events_root, match_index, out_root, batch_size_matches=100)
        logger.info("Events completed. ")
    except Exception as e:
        logger.warning(f"Error in match index. \n {e}")
        

    # 5) Lineups (players-level)
    logger.info("Line ups index ---")
    try:
        lineups_players_df = load_lineups(lineups_root, match_index)
        if not lineups_players_df.empty:
            write_partitioned_parquet(
                lineups_players_df,
                out_root / "lineups_players",
                partition_cols=["competition_id", "season_id"],
            )
        logger.info("   Line ups completed. ")
    except Exception as e:
        logger.warning(f"Error in Line ups. \n {e}")
        

    # 6) StatsBomb 360 (frames players-level)
    logger.info("Starting StatsBomb 360  ---")
    try:
        three_sixty_df = load_three_sixty(three_sixty_root, match_index)
        if not three_sixty_df.empty:
            write_partitioned_parquet(
                three_sixty_df,
                out_root / "three_sixty_frames",
                partition_cols=["competition_id", "season_id"],
            )
        logger.info("StatsBomb 360  completed. ")
    except Exception as e:
        logger.warning(f"Error in StatsBomb 360 . \n {e}")
        


    # Emit match index for debugging / joins
    logger.info("Starting match index ---")
    try:
        if not match_index.empty:
            write_partitioned_parquet(
                match_index,
                out_root / "match_index",
                partition_cols=["competition_id", "season_id"],
            )
        logger.info("Match index completed. ")
    except Exception as e:
        logger.warning(f"Error in match index. \n {e}")
        

    logger.info(f"Done. Parquet datasets written to: {out_root}")


if __name__ == "__main__":
    main()
