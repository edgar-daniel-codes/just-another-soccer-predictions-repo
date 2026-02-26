#!/usr/bin/env python3

### Soccer Predictions - ETL 
### Statsbomb Extraction 
### By Edgar Daniel

# Auxiliar code for downloading all available data from StatsBomb 
# Open Data Repository with file handling and grouping
# via statsbombpy


#What it pulls:
#- competitions (competition_id, season_id catalog)
#- matches per (competition_id, season_id)
#- events per match_id
#- lineups per match_id (when available)
#- frames per match_id (freeze-frame / 360-like frames when available)

#Output Files:
#<out_dir>/
#  competitions.parquet
#  matches/competition_id=XX/season_id=YY/matches.parquet
#  events/match_id=ZZZZZZ/events.parquet
#  lineups/match_id=ZZZZZZ/lineups.parquet
#  frames/match_id=ZZZZZZ/frames.parquet
#logs/
#  statsbomb_failed_tasks.jsonl
  


### -------------------------------------------------------------------------------
### Needed libraries --------------------------------------------------------------


from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from statsbombpy import sb


@dataclass(frozen=True)
class EtlConfig:
    """
    ETL configuration.

    :param out_dir: Root output directory.
    :param workers: Thread pool size for per-match pulls.
    :param sleep_seconds: Sleep between requests to be polite.
    :param max_retries: Retries for transient failures.
    :param retry_backoff_seconds: Base exponential backoff seconds.
    """

    out_dir: Path
    workers: int = 8
    sleep_seconds: float = 0.15
    max_retries: int = 6
    retry_backoff_seconds: float = 1.6


def _ensure_dir(path: Path) -> None:
    """
    Ensure directory exists.

    :param path: Directory path.
    """
    path.mkdir(parents=True, exist_ok=True)


def _write_parquet(df: pd.DataFrame, path: Path) -> None:
    """
    Write DataFrame to parquet.

    :param df: DataFrame to write.
    :param path: Output file path.
    """
    _ensure_dir(path.parent)
    df.to_parquet(path, index=False)


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    """
    Append a JSON payload to a JSONL file.

    :param path: JSONL file path.
    :param payload: Payload dict.
    """
    _ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _with_retries(cfg: EtlConfig, fn, meta: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Execute callable returning a DataFrame with retries + exponential backoff.

    :param cfg: ETL config.
    :param fn: Callable returning a DataFrame.
    :param meta: Metadata for logging on failure.
    :return: DataFrame or None on permanent failure.
    """
    last_error: Optional[str] = None
    for attempt in range(cfg.max_retries + 1):
        try:
            time.sleep(cfg.sleep_seconds)
            df = fn()
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            return df
        except Exception as exc:  # noqa: BLE001
            last_error = repr(exc)
            if attempt >= cfg.max_retries:
                _append_jsonl(
                    cfg.out_dir.parent / "logs" / "statsbomb_failed_tasks.jsonl",
                    {**meta, "attempts": attempt + 1, "error": last_error},
                )
                return None
            time.sleep((cfg.retry_backoff_seconds**attempt))


def pull_competitions(cfg: EtlConfig) -> pd.DataFrame:
    """
    Pull the competitions catalog.

    :param cfg: ETL config.
    :return: Competitions DataFrame.
    """
    out_path = cfg.out_dir / "competitions.parquet"
    if out_path.exists():
        return pd.read_parquet(out_path)

    df = _with_retries(cfg, lambda: sb.competitions(), {"task": "competitions"})
    if df is None or df.empty:
        raise RuntimeError("Failed to fetch StatsBomb competitions catalog.")
    _write_parquet(df, out_path)
    return df


def pull_matches(cfg: EtlConfig, competition_id: int, season_id: int) -> Optional[pd.DataFrame]:
    """
    Pull matches for a given competition_id and season_id.

    :param cfg: ETL config.
    :param competition_id: StatsBomb competition_id.
    :param season_id: StatsBomb season_id.
    :return: Matches DataFrame or None if failed.
    """
    out_path = (
        cfg.out_dir
        / "matches"
        / f"competition_id={competition_id}"
        / f"season_id={season_id}"
        / "matches.parquet"
    )
    if out_path.exists():
        return pd.read_parquet(out_path)

    df = _with_retries(
        cfg,
        lambda: sb.matches(competition_id=competition_id, season_id=season_id),
        {"task": "matches", "competition_id": competition_id, "season_id": season_id},
    )
    if df is None:
        return None
    _write_parquet(df, out_path)
    return df


def pull_events(cfg: EtlConfig, match_id: int) -> None:
    """
    Pull events for a match.

    :param cfg: ETL config.
    :param match_id: StatsBomb match_id.
    """
    out_path = cfg.out_dir / "events" / f"match_id={match_id}" / "events.parquet"
    if out_path.exists():
        return

    df = _with_retries(cfg, lambda: sb.events(match_id=match_id), {"task": "events", "match_id": match_id})
    if df is not None and not df.empty:
        df["match_id"] = match_id
        _write_parquet(df, out_path)


def pull_lineups(cfg: EtlConfig, match_id: int) -> None:
    """
    Pull lineups for a match (when available).

    :param cfg: ETL config.
    :param match_id: StatsBomb match_id.
    """
    out_path = cfg.out_dir / "lineups" / f"match_id={match_id}" / "lineups.parquet"
    if out_path.exists():
        return

    df = _with_retries(cfg, lambda: sb.lineups(match_id=match_id), {"task": "lineups", "match_id": match_id})
    if df is not None and not df.empty:
        df["match_id"] = match_id
        _write_parquet(df, out_path)


def pull_frames(cfg: EtlConfig, match_id: int) -> None:
    """
    Pull frames / freeze-frame style data for a match (when available).

    :param cfg: ETL config.
    :param match_id: StatsBomb match_id.
    """
    out_path = cfg.out_dir / "frames" / f"match_id={match_id}" / "frames.parquet"
    if out_path.exists():
        return

    df = _with_retries(cfg, lambda: sb.frames(match_id=match_id), {"task": "frames", "match_id": match_id})
    if df is not None and not df.empty:
        df["match_id"] = match_id
        _write_parquet(df, out_path)


def _iter_comp_seasons(comps: pd.DataFrame) -> List[Tuple[int, int]]:
    """
    Build unique (competition_id, season_id) pairs.

    :param comps: Competitions DataFrame.
    :return: List of (competition_id, season_id).
    """
    required = {"competition_id", "season_id"}
    missing = required - set(comps.columns)
    if missing:
        raise ValueError(f"Competitions DF missing columns: {sorted(missing)}")

    pairs = (
        comps[["competition_id", "season_id"]]
        .dropna()
        .drop_duplicates()
        .astype({"competition_id": "int64", "season_id": "int64"})
        .itertuples(index=False, name=None)
    )
    return list(pairs)


def run_etl(cfg: EtlConfig, logger: Any) -> None:
    """
    Run full StatsBomb Open Data ETL.

    :param cfg: ETL config.
    """


    logger.info("Ensure directories ---")
    try:
        _ensure_dir(cfg.out_dir)
        comps = pull_competitions(cfg)
        pairs = _iter_comp_seasons(comps)
        logger.info("Correct directories.")
    except Exception as e:
        logger.warining(f"Failling ensuring directories. \n {e}")


    logger.info("Starting matches pulling ---")
    try:
        match_ids: List[int] = []
        for competition_id, season_id in pairs:
            mdf = pull_matches(cfg, competition_id, season_id)
            if mdf is None or mdf.empty:
                continue
            if "match_id" in mdf.columns:
                match_ids.extend(mdf["match_id"].dropna().astype(int).tolist())

        match_ids = sorted(set(match_ids))
        logger.info("Match pulling ended.")
    except:
        logger.warining(f"Failling match pulling. \n {e}")


    logger.info("Starting pull events ---")
    try: 
        with ThreadPoolExecutor(max_workers=cfg.workers) as ex:
            futures = []
            for mid in match_ids:
                futures.append(ex.submit(pull_events, cfg, mid))
                futures.append(ex.submit(pull_lineups, cfg, mid))
                futures.append(ex.submit(pull_frames, cfg, mid))

            for _ in as_completed(futures):
                pass

        logger.info("Event pulling event.")

    except: 
        logger.warining(f"Failling event pulling. \n {e}")


def parse_args() -> argparse.Namespace:
    """
    Parse CLI args.

    :return: Parsed args.
    """
    p = argparse.ArgumentParser()
    p.add_argument("out_dir", type=str, help="Output directory, e.g. ./data/raw/statsbomb_open/")
    p.add_argument("log_dir", type=str, help="Log directory.")
    p.add_argument("log_file", type=str, help="Log file.")
    p.add_argument("--workers", type=int, default=8, help="Thread pool size.")
    p.add_argument("--sleep", type=float, default=0.15, help="Sleep seconds between calls.")
    p.add_argument("--retries", type=int, default=6, help="Max retries per request.")
    return p.parse_args()


def main() -> None:
    """
    CLI entry point.
    """

    args = parse_args()

    cfg = EtlConfig(
        out_dir=Path(args.out_dir).expanduser().resolve(),
        workers=args.workers,
        sleep_seconds=args.sleep,
        max_retries=args.retries,
    )
    
    DIR_LOG = args.log_dir
    FILE_LOG = args.log_file

    # Make sure directories exist 

    _ensure_dir(Path(DIR_LOG))

    # Logger 
    import logging

    logger = logging.getLogger("etl-statsbomb-extract")
    logger.setLevel(logging.INFO)

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    formatter = logging.Formatter(fmt)

    console = logging.StreamHandler()
    console.setFormatter(formatter)

    file = logging.FileHandler(DIR_LOG + FILE_LOG)
    file.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file)

    
    run_etl(cfg, logger)


if __name__ == "__main__":

    main()

