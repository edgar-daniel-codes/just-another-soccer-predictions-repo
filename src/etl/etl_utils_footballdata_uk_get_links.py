#!/usr/bin/env python3

### Soccer Predictions - ETL 
### Utilities Football Pipelines 
### By Edgar Daniel

# Auxiliar code for getting all available data dumps for each league from 
# https://www.football-data.co.uk/data.php
# Usage
# curl -fsSL "$URL" | python src/etl/etl_utils_footballdata_uk_get_links.py "$URL" 
# > ./data/raw/footballdata_uk/links.txt
#


### -------------------------------------------------------------------------------
### Needed libraries --------------------------------------------------------------

import re, sys, csv
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = sys.argv[1].rstrip("/")  

soup = BeautifulSoup(sys.stdin.read(), "lxml")


### -------------------------------------------------------------------------------
### Main Pipeline  ----------------------------------------------------------------


def norm(s):
    return re.sub(r"\s+", " ", (s or "").strip())

def find_heading(text):
    t = soup.find(string=re.compile(rf"\b{re.escape(text)}\b", re.I))
    return t.parent if t else None

def collect(h1, h2=None):
    start = find_heading(h1)
    if not start:
        return []

    out = []
    node = start

    while True:
        node = node.find_next()
        if not node:
            break

        if h2 and node.find(string=re.compile(rf"^\s*{re.escape(h2)}\s*$", re.I)):
            break

        if getattr(node, "name", None) == "a":
            label = norm(node.get_text(" ", strip=True))
            href = (node.get("href") or "").strip()

            if label and href:
                # resolve relative links
                href = urljoin(BASE_URL + "/", href)

                # filter by base URL
                if href.startswith(BASE_URL):
                    out.append((label, href))

    # deduplicate
    seen = set()
    ded = []
    for x in out:
        if x not in seen:
            seen.add(x)
            ded.append(x)

    return ded

### Main Collection 

try:
    main = collect("Main Leagues", "Extra Leagues")
    print("Main Leagues Collected")
except Exception as e:
    print(f"Error collecting Main Leagues: \n: {e}")

try:
    extra = collect("Extra Leagues")
    print("Extra Leagues Collected")
except Exception as e:
    print(f"Error collecting Extra Leagues: \n: {e}")


### -------------------------------------------------------------------------------
### Save the link into file   -----------------------------------------------------

try:
    w = csv.writer(sys.stdout)
    w.writerow(["section", "label", "url"])

    for label, href in main:
        w.writerow(["Main Leagues", label, href])

    for label, href in extra:
        w.writerow(["Extra Leagues", label, href])

    print("Links saved into given file path.")

except Exception as e:
    print(f"Error writing links. \n {e}")
