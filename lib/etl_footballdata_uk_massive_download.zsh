#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <input.csv>" >&2
  exit 1
fi

csv_file="$1"
base_dir="./data/raw/footballdata_uk"

mkdir -p "$base_dir"

tail -n +2 "$csv_file" | while IFS=',' read -r section label url; do
  label="${label//$'\r'/}"
  url="${url//$'\r'/}"
  label="${label#"${label%%[![:space:]]*}"}"
  label="${label%"${label##*[![:space:]]}"}"
  url="${url#"${url%%[![:space:]]*}"}"
  url="${url%"${url##*[![:space:]]}"}"

  league_folder="$(echo "$label" | sed 's/ Football Results$//' | tr '[:upper:]' '[:lower:]' | tr ' ' '_' )"
  target_dir="${base_dir}/${league_folder}"
  mkdir -p "$target_dir"

  wget -r -np \
    -A "*.csv,*.CSV,*.txt,*.TXT,*.xlsx,*.XLSX" \
    --user-agent="Mozilla/5.0 (X11; Linux x86_64)" \
    --referer="https://www.football-data.co.uk/" \
    --wait=2 --random-wait --limit-rate=500k \
    --retry-connrefused --timeout=20 \
    -P "$target_dir" "$url"

  mapfile -d '' files < <(find "$target_dir" -type f \( -iname '*.csv' -o -iname '*.txt' -o -iname '*.xlsx' \) -print0 | sort -z)

  j=1
  for f in "${files[@]}"; do
    ext="${f##*.}"
    ext="$(printf '%s' "$ext" | tr '[:upper:]' '[:lower:]')"
    new_name="${league_folder}_$(printf '%02d' "$j").${ext}"
    mv -f -- "$f" "${target_dir}/${new_name}"
    ((j++))
  done

  find "$target_dir" -type d -empty -delete
done
