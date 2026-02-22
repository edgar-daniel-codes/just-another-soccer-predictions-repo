
#!/usr/bin/env python3

### Soccer Predictions  - ETL 
### Utilities Football Data Org
### By Edgar Daniel

# Code creating a client and getting soccer leagues  data from the public API 
# from football data org. and creating usable raw data tables for modelling 
# and analytics usage. 

"""
football-data.org (v4) league/season downloader.

What it does (for a given competition code or id):
- Fetch competition metadata (including available seasons if present)
- Fetch season-scoped resources:
  - matches
  - standings
  - teams
  - scorers
- Persist:
  - raw JSON (for reproducibility)
  - normalized tables (CSV + Parquet) for analytics
"""


### -------------------------------------------------------------------------------
### Needed libraries --------------------------------------------------------------


from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import requests


### -------------------------------------------------------------------------------
### Classes and functions  --------------------------------------------------------


BASE_URL = "https://api.football-data.org/v4"


@dataclass(frozen=True)
class FootballDataClient:
    """
    Client for football-data.org v4.

    :param token: API token for football-data.org (X-Auth-Token).
    :param requests_per_minute: Throttle to respect rate limits (free tier is commonly 10/min).
    :param timeout_seconds: Request timeout in seconds.
    :param max_retries: Max retries for transient errors (timeouts/5xx/429).
    :param backoff_seconds: Base backoff in seconds (exponential).
    """

    token: str
    requests_per_minute: int = 10
    timeout_seconds: int = 30
    max_retries: int = 5
    backoff_seconds: float = 1.5

    def __post_init__(self) -> None:
        if not self.token or not isinstance(self.token, str):
            raise ValueError("token must be a non-empty string")

    @property
    def _min_interval_seconds(self) -> float:
        return 60.0 / float(self.requests_per_minute)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Perform a GET request with throttling + retries.

        :param path: API path starting with '/', e.g. '/competitions/PL'.
        :param params: Query string parameters.
        :return: Parsed JSON response.
        """
        url = f"{BASE_URL}{path}"
        headers = {"X-Auth-Token": self.token}

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                # Throttle
                time.sleep(self._min_interval_seconds)

                resp = requests.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self.timeout_seconds,
                )

                # Retry-worthy statuses
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(
                        f"HTTP {resp.status_code} for {url}: {resp.text[:200]}",
                        response=resp,
                    )

                resp.raise_for_status()
                return resp.json()

            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt >= self.max_retries:
                    break

                # exponential backoff + jitterless (simple, deterministic)
                sleep_s = self.backoff_seconds * (2.0**attempt)
                time.sleep(sleep_s)

        raise RuntimeError(f"Request failed after retries: {url}") from last_error


def _safe_write_json(obj: Dict[str, Any], path: Path) -> None:
    """
    Write JSON to disk.

    :param obj: JSON-serializable dict.
    :param path: Output file path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _normalize_matches(matches_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Normalize /competitions/{code}/matches response into a flat table.

    :param matches_json: Raw JSON response.
    :return: DataFrame of matches.
    """
    matches = matches_json.get("matches", [])
    rows = []
    for m in matches:
        score = m.get("score", {}) or {}
        ft = (score.get("fullTime") or {}) if isinstance(score.get("fullTime"), dict) else {}
        ht = (score.get("halfTime") or {}) if isinstance(score.get("halfTime"), dict) else {}

        home = m.get("homeTeam") or {}
        away = m.get("awayTeam") or {}

        rows.append(
            {
                "match_id": m.get("id"),
                "utc_date": m.get("utcDate"),
                "status": m.get("status"),
                "matchday": m.get("matchday"),
                "stage": m.get("stage"),
                "group": m.get("group"),
                "season_id": (m.get("season") or {}).get("id"),
                "season_start_date": (m.get("season") or {}).get("startDate"),
                "season_end_date": (m.get("season") or {}).get("endDate"),
                "home_team_id": home.get("id"),
                "home_team_name": home.get("name"),
                "away_team_id": away.get("id"),
                "away_team_name": away.get("name"),
                "ft_home": ft.get("home"),
                "ft_away": ft.get("away"),
                "ht_home": ht.get("home"),
                "ht_away": ht.get("away"),
                "winner": score.get("winner"),
                "duration": score.get("duration"),
                "referees": "; ".join([r.get("name", "") for r in (m.get("referees") or []) if r.get("name")]),
            }
        )
    return pd.DataFrame(rows)


def _normalize_teams(teams_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Normalize /competitions/{code}/teams response into a flat table.

    :param teams_json: Raw JSON response.
    :return: DataFrame of teams.
    """
    teams = teams_json.get("teams", [])
    rows = []
    for t in teams:
        area = t.get("area") or {}
        rows.append(
            {
                "team_id": t.get("id"),
                "team_name": t.get("name"),
                "short_name": t.get("shortName"),
                "tla": t.get("tla"),
                "crest": t.get("crest"),
                "address": t.get("address"),
                "website": t.get("website"),
                "founded": t.get("founded"),
                "club_colors": t.get("clubColors"),
                "venue": t.get("venue"),
                "area_name": area.get("name"),
                "area_code": area.get("code"),
            }
        )
    return pd.DataFrame(rows)


def _normalize_scorers(scorers_json: Dict[str, Any]) -> pd.DataFrame:
    """
    Normalize /competitions/{code}/scorers response into a flat table.

    :param scorers_json: Raw JSON response.
    :return: DataFrame of scorers.
    """
    scorers = scorers_json.get("scorers", [])
    rows = []
    for s in scorers:
        player = s.get("player") or {}
        team = s.get("team") or {}
        rows.append(
            {
                "player_id": player.get("id"),
                "player_name": player.get("name"),
                "player_position": player.get("position"),
                "player_nationality": player.get("nationality"),
                "team_id": team.get("id"),
                "team_name": team.get("name"),
                "goals": s.get("goals"),
                "assists": s.get("assists"),
                "penalties": s.get("penalties"),
            }
        )
    return pd.DataFrame(rows)


def _normalize_standings(standings_json: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Normalize /competitions/{code}/standings response.

    :param standings_json: Raw JSON response.
    :return: (tables_df, rows_df)
      - tables_df: one row per standings table
      - rows_df: one row per team within a table
    """
    standings = standings_json.get("standings", [])
    tables = []
    rows = []

    for table_idx, st in enumerate(standings):
        tables.append(
            {
                "table_idx": table_idx,
                "stage": st.get("stage"),
                "type": st.get("type"),
                "group": st.get("group"),
            }
        )

        for r in st.get("table", []) or []:
            team = r.get("team") or {}
            rows.append(
                {
                    "table_idx": table_idx,
                    "position": r.get("position"),
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                    "played_games": r.get("playedGames"),
                    "form": r.get("form"),
                    "won": r.get("won"),
                    "draw": r.get("draw"),
                    "lost": r.get("lost"),
                    "points": r.get("points"),
                    "goals_for": r.get("goalsFor"),
                    "goals_against": r.get("goalsAgainst"),
                    "goal_difference": r.get("goalDifference"),
                }
            )

    return pd.DataFrame(tables), pd.DataFrame(rows)


def download_competition_season(
    token: str,
    competition: str,
    season_year: Optional[int],
    out_dir: str,
) -> Dict[str, Path]:
    """
    Download the whole available dataset for a competition and season.

    "competition" can be a code (e.g., 'PL', 'BL1') or numeric id as string.
    "season_year" is the starting year of the season (e.g., 2023 for 2023/24).
    If season_year is None, uses the competition's currentSeason start year when available.

    :param token: football-data.org API token.
    :param competition: Competition code or id (string).
    :param season_year: Season start year (int) or None for current.
    :param out_dir: Output directory path.
    :return: Dict mapping artifact names to file patif __name__ == "__main__":
    # Usage example:
    # export FOOTBALL_DATA_TOKEN="..."
    token_env = os.getenv("FOOTBALL_DATA_TOKEN", "").strip()
    if not token_env:
        raise SystemExit("Set FOOTBALL_DATA_TOKEN env var to your football-data.org API token.")

    # Examples:
    # Premier League: "PL"
    # Bundesliga: "BL1"
    # season_year=2023 means 2023/24
    artifacts_out = download_competition_season(
        token=token_env,
        competition="PL",
        season_year=2023,
        out_dir="./football_data_dump",
    )

    for k, v in artifacts_out.items():
        print(f"{k}: {v}")

hs.
    """
    client = FootballDataClient(token=token)
    root = Path(out_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)

    # 1) Competition metadata (catalog + available seasons when present)
    comp_json = client.get(f"/competitions/{competition}")

    # Determine default season year if not provided
    if season_year is None:
        current = comp_json.get("currentSeason") or {}
        if isinstance(current, dict) and current.get("startDate"):
            season_year = int(str(current["startDate"])[:4])
        else:
            # fallback: try first season entry if present, else raise
            seasons = comp_json.get("seasons") or []
            if seasons and isinstance(seasons, list) and seasons[0].get("startDate"):
                season_year = int(str(seasons[0]["startDate"])[:4])
            else:
                raise ValueError(
                    "season_year is None and no currentSeason/seasons startDate available for this competition"
                )

    # 2) Season-scoped endpoints
    matches_json = client.get(f"/competitions/{competition}/matches", params={"season": season_year})
    standings_json = client.get(f"/competitions/{competition}/standings", params={"season": season_year})
    teams_json = client.get(f"/competitions/{competition}/teams", params={"season": season_year})
    scorers_json = client.get(f"/competitions/{competition}/scorers", params={"season": season_year})

    # 3) Persist raw JSON
    slug = f"{competition}_season_{season_year}"
    raw_dir = root / slug / "raw"
    _safe_write_json(comp_json, raw_dir / "competition.json")
    _safe_write_json(matches_json, raw_dir / "matches.json")
    _safe_write_json(standings_json, raw_dir / "standings.json")
    _safe_write_json(teams_json, raw_dir / "teams.json")
    _safe_write_json(scorers_json, raw_dir / "scorers.json")

    # 4) Normalize + persist tables
    tbl_dir = root / slug / "tables"
    tbl_dir.mkdir(parents=True, exist_ok=True)

    matches_df = _normalize_matches(matches_json)
    teams_df = _normalize_teams(teams_json)
    scorers_df = _normalize_scorers(scorers_json)
    standings_tables_df, standings_rows_df = _normalize_standings(standings_json)

    # Write CSV + Parquet (Parquet is better for analytics)
    artifacts: Dict[str, Path] = {}

    def _write(df: pd.DataFrame, name: str) -> None:
        csv_path = tbl_dir / f"{name}.csv"
        pq_path = tbl_dir / f"{name}.parquet"
        df.to_csv(csv_path, index=False)
        df.to_parquet(pq_path, index=False)
        artifacts[f"{name}_csv"] = csv_path
        artifacts[f"{name}_parquet"] = pq_path

    _write(matches_df, "matches")
    _write(teams_df, "teams")
    _write(scorers_df, "scorers")
    _write(standings_tables_df, "standings_tables")
    _write(standings_rows_df, "standings_rows")

    artifacts["competition_json"] = raw_dir / "competition.json"
    artifacts["matches_json"] = raw_dir / "matches.json"
    artifacts["standings_json"] = raw_dir / "standings.json"
    artifacts["teams_json"] = raw_dir / "teams.json"
    artifacts["scorers_json"] = raw_dir / "scorers.json"

    return artifacts


