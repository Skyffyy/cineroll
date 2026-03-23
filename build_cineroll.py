"""
╔══════════════════════════════════════════════════════════════════╗
║  CineRoll Builder                                                ║
║  Downloads IMDb datasets → injects data → outputs cineroll.html ║
╠══════════════════════════════════════════════════════════════════╣
║  Requirements:                                                   ║
║    pip install pandas                                            ║
║                                                                  ║
║  Usage:                                                          ║
║    python build_cineroll.py                                      ║
║                                                                  ║
║  Output:                                                         ║
║    cineroll.html  — open in any browser, no server needed        ║
╚══════════════════════════════════════════════════════════════════╝

HOW THE SELECTION WORKS
──────────────────────
IMDb's genre field on each title contains up to 3 genres, e.g. "Drama,Crime,Thriller".
We expand this so that every movie is counted towards ALL of its genres.
Then for each genre we independently pick up to PER_GENRE_LIMIT movies by bayesian score.
The final list is the union (deduped by tconst).

This means:
  • Drama  → up to 1 000 movies
  • Comedy → up to 1 000 movies
  • Horror → up to 1 000 movies
  … and so on for every genre in the dataset.

Total will typically be 8 000 – 15 000 movies depending on MIN_VOTES.
"""

import gzip
import json
import math
import os
import sys
import io
import time
import urllib.request

# Configure UTF-8 output on Windows
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

import pandas as pd

# ════════════════════════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════════════════════════
MIN_VOTES       = 100      # minimum IMDb vote count (lower = more obscure films)
MIN_RATING      = 3.5      # minimum IMDb average rating
PER_GENRE_LIMIT = 300      # max movies to keep per genre (before dedup)
CACHE_DIR       = "./imdb_cache"
TEMPLATE        = "cineroll_template.html"
OUT_FILE        = "cineroll.html"
HDREZKA         = "https://hdrezka.ag/search/?q="

# ════════════════════════════════════════════════════════════════
#  GENRE EMOJI AND BACKGROUND COLOR
# ════════════════════════════════════════════════════════════════
EMOJI = {
    "Action":      "🔫", "Adventure":  "🗺️",  "Animation":   "🎨",
    "Biography":   "📖", "Comedy":     "😂",  "Crime":       "🚨",
    "Documentary": "🎥", "Drama":      "🎭",  "Family":      "👨‍👩‍👧",
    "Fantasy":     "🧙", "Film-Noir":  "🕵️",  "History":     "🏛️",
    "Horror":      "👻", "Music":      "🎵",  "Musical":     "🎶",
    "Mystery":     "🔍", "Romance":    "💕",  "Sci-Fi":      "🚀",
    "Sport":       "🏆", "Thriller":   "😱",  "War":         "⚔️",
    "Western":     "🤠",
}
BG = {
    "Action":      "#1a0505", "Adventure":  "#0a1205", "Animation":   "#0a0a1a",
    "Biography":   "#0d0a0a", "Comedy":     "#1a1205", "Crime":       "#0a0505",
    "Documentary": "#050a0a", "Drama":      "#0d0805", "Family":      "#061206",
    "Fantasy":     "#100516", "Film-Noir":  "#0a0a0a", "History":     "#100a00",
    "Horror":      "#1a0000", "Music":      "#060a18", "Musical":     "#060a18",
    "Mystery":     "#08060a", "Romance":    "#18080a", "Sci-Fi":      "#050d1a",
    "Sport":       "#050d05", "Thriller":   "#0a0808", "War":         "#0a0a06",
    "Western":     "#1a0d00",
}

# ════════════════════════════════════════════════════════════════
#  DOWNLOAD HELPERS
# ════════════════════════════════════════════════════════════════
os.makedirs(CACHE_DIR, exist_ok=True)


def _progress(count, block_size, total_size):
    """Simple download progress hook for urllib."""
    if total_size <= 0:
        mb = count * block_size // 1024 // 1024
        sys.stdout.write(f"\r    Downloading… {mb} MB")
    else:
        pct = min(count * block_size / total_size * 100, 100)
        mb  = total_size // 1024 // 1024
        sys.stdout.write(f"\r    {pct:5.1f}%  of {mb} MB")
    sys.stdout.flush()


def fetch(name: str) -> pd.DataFrame:
    """
    Download (or load from cache) an IMDb .tsv.gz dataset.
    Files are cached locally for 24 hours before re-downloading.
    Retries up to 3 times on network errors.
    """
    url  = f"https://datasets.imdbws.com/{name}.tsv.gz"
    path = os.path.join(CACHE_DIR, f"{name}.tsv.gz")

    if os.path.exists(path):
        age_h = (time.time() - os.path.getmtime(path)) / 3600
        if age_h < 24:
            print(f"  ✓  {name}  (cached {age_h:.0f}h ago)")
        else:
            print(f"  ↻  {name}  (cache expired, re-downloading…)")
            for attempt in range(3):
                try:
                    urllib.request.urlretrieve(url, path, _progress)
                    print()
                    break
                except Exception as e:
                    if attempt < 2:
                        print(f"\n  ⚠  Download failed, retrying… ({attempt + 1}/3)")
                        time.sleep(2)
                    else:
                        print(f"\n  ⚠  Download failed after 3 attempts, using cache")
                        break
    else:
        print(f"  ⬇  {name}  (first download…)")
        for attempt in range(3):
            try:
                urllib.request.urlretrieve(url, path, _progress)
                print()
                break
            except Exception as e:
                if attempt < 2:
                    print(f"\n  ⚠  Download failed, retrying… ({attempt + 1}/3)")
                    time.sleep(2)
                else:
                    raise

    with gzip.open(path, "rt", encoding="utf-8") as f:
        return pd.read_csv(f, sep="\t", low_memory=False, na_values=r"\N")


# ════════════════════════════════════════════════════════════════
#  DOWNLOAD DATASETS
# ════════════════════════════════════════════════════════════════
print("\n" + "═" * 60)
print("  CineRoll Builder")
print("═" * 60)
print("\n📦  Downloading IMDb datasets…\n")

basics  = fetch("title.basics")   # title, year, genres, runtime
ratings = fetch("title.ratings")  # averageRating, numVotes

# title.akas — localized titles.  Used to show Russian movie names in RU mode.
print("\n    Fetching Russian titles (title.akas)…", flush=True)
try:
    akas = fetch("title.akas")
    print(f"    Loaded {len(akas):,} rows from title.akas", flush=True)
    ru_titles = (
        akas[akas["region"] == "RU"][["titleId", "title"]]
        .drop_duplicates("titleId")
        .rename(columns={"titleId": "tconst", "title": "title_ru"})
    )
    has_ru = True
    print(f"    Found {len(ru_titles):,} Russian titles.", flush=True)
except Exception as exc:
    has_ru = False
    print(f"    Could not load akas ({exc}). Falling back to English titles.", flush=True)

# ════════════════════════════════════════════════════════════════
#  BASE FILTERING
# ════════════════════════════════════════════════════════════════
print("\n⚙️   Processing…")

# Start with non-adult movies only
base = basics[
    (basics["titleType"] == "movie") &
    (basics["isAdult"]   == 0)
][["tconst", "primaryTitle", "startYear", "genres", "runtimeMinutes"]].copy()

# Join ratings
base = base.merge(ratings, on="tconst", how="inner")

# Apply minimum quality thresholds
base = base[
    (base["numVotes"]      >= MIN_VOTES) &
    (base["averageRating"] >= MIN_RATING) &
    base["startYear"].notna() &
    base["genres"].notna()
].copy()

# Fix types
base["startYear"]     = base["startYear"].astype(int)
base["averageRating"] = base["averageRating"].astype(float)
base["numVotes"]      = base["numVotes"].astype(int)

# Bayesian score: rating × log10(votes)
# Balances quality with popularity — avoids obscure 9.9-rated films with 200 votes.
base["score"] = base["averageRating"] * base["numVotes"].apply(
    lambda v: math.log10(max(v, 1))
)

print(f"  ✓  Movies passing quality filter:  {len(base):,}")

# ════════════════════════════════════════════════════════════════
#  EXPAND BY ALL GENRES  (each movie can appear in multiple genres)
# ════════════════════════════════════════════════════════════════
# IMDb stores genres as comma-separated, e.g. "Drama,Crime,Thriller".
# We split them so one movie row becomes multiple rows, one per genre.
# This lets us rank each genre independently.

expanded = base.copy()
expanded["genre_list"] = expanded["genres"].str.split(",")
expanded = expanded.explode("genre_list")
expanded["genre_list"] = expanded["genre_list"].str.strip()

# Keep only known genres (skip edge cases like "News", "Talk-Show", etc.)
expanded = expanded[expanded["genre_list"].isin(EMOJI.keys())].copy()

print(f"  ✓  Unique genres found:            {expanded['genre_list'].nunique()}")

# ════════════════════════════════════════════════════════════════
#  PER-GENRE TOP-N SELECTION
# ════════════════════════════════════════════════════════════════
# For every genre, independently rank by score and take the top PER_GENRE_LIMIT.
# Then union all genre selections and dedup by tconst.
# A movie's displayed genre is its PRIMARY genre (first in IMDb's list).

selected_ids: set = set()
genre_stats: dict = {}

all_genres = sorted(expanded["genre_list"].unique())
for genre in all_genres:
    genre_df = (
        expanded[expanded["genre_list"] == genre]
        .nlargest(PER_GENRE_LIMIT, "score")
    )
    new_ids = set(genre_df["tconst"].tolist()) - selected_ids
    selected_ids.update(new_ids)
    genre_stats[genre] = len(genre_df)
    print(f"    {genre:<16}  {len(genre_df):>5} selected  ({len(new_ids):>5} new after dedup)")

# ════════════════════════════════════════════════════════════════
#  ADD BONUS HIGH-RATED MOVIES (9+ rating)
# ════════════════════════════════════════════════════════════════
print("\n  Adding bonus high-rated movies (9+)…")
HIGH_RATED_LIMIT = 100  # Add up to 100 per genre with 9+ rating

high_rated = base[base["averageRating"] >= 9.0].copy()
print(f"  Found {len(high_rated):,} movies with rating 9+")

expanded_high = high_rated.copy()
expanded_high["genre_list"] = expanded_high["genres"].str.split(",")
expanded_high = expanded_high.explode("genre_list")
expanded_high["genre_list"] = expanded_high["genre_list"].str.strip()
expanded_high = expanded_high[expanded_high["genre_list"].isin(EMOJI.keys())].copy()

for genre in all_genres:
    if genre in expanded_high["genre_list"].values:
        genre_high = (
            expanded_high[expanded_high["genre_list"] == genre]
            .nlargest(HIGH_RATED_LIMIT, "averageRating")
        )
        new_high_ids = set(genre_high["tconst"].tolist()) - selected_ids
        selected_ids.update(new_high_ids)
        genre_stats[genre] += len(new_high_ids)
        if len(new_high_ids) > 0:
            print(f"    {genre:<16}  +{len(new_high_ids):>3} high-rated (9+)")

# Build final dataframe from selected IDs, using original base rows
final = base[base["tconst"].isin(selected_ids)].copy()

# Assign PRIMARY genre (first genre in the comma-separated list that's in our known set)
def primary_genre(genre_str: str) -> str:
    """Return the first known genre from the movie's genre list."""
    for g in str(genre_str).split(","):
        g = g.strip()
        if g in EMOJI:
            return g
    return "Drama"

final["genre"] = final["genres"].apply(primary_genre)

# Merge Russian titles
if has_ru:
    final = final.merge(ru_titles, on="tconst", how="left")
else:
    final["title_ru"] = None

print(f"\n  ✓  Total movies after dedup:       {len(final):,}")
print(f"  ✓  Year range:                     {final['startYear'].min()} – {final['startYear'].max()}")
print(f"  ✓  Rating range:                   {final['averageRating'].min():.1f} – {final['averageRating'].max():.1f}")

# ════════════════════════════════════════════════════════════════
#  BUILD JSON RECORDS
# ════════════════════════════════════════════════════════════════
records = []
for _, row in final.iterrows():
    genre    = row["genre"]
    emoji    = EMOJI.get(genre, "🎬")
    bg       = BG.get(genre, "#0a0a0a")
    title    = str(row["primaryTitle"])
    title_ru = str(row["title_ru"]) if pd.notna(row["title_ru"]) else title
    year     = int(row["startYear"])
    imdb     = round(float(row["averageRating"]), 1)
    votes    = int(row["numVotes"])
    runtime  = int(row["runtimeMinutes"]) if pd.notna(row.get("runtimeMinutes")) else 0
    tid      = str(row["tconst"])

    # Two separate HDRezka search URLs so the "Watch" button always searches
    # in the right language — EN search when UI is EN, RU search when UI is RU.
    # Include year and title for more accurate results
    rezka_en = HDREZKA + f"{title}+{year}".replace(" ", "+")
    rezka_ru = HDREZKA + f"{title_ru}+{year}".replace(" ", "+")

    records.append({
        "id":       tid,
        "title":    title,       # English (original) title — shown in EN mode
        "title_ru": title_ru,    # Russian title — shown in RU mode
        "year":     year,
        "genre":    genre,
        "emoji":    emoji,
        "bg":       bg,
        "imdb":     imdb,
        "votes":    votes,
        "runtime":  runtime,
        "imdbUrl":  f"https://www.imdb.com/title/{tid}/",
        "rezka_en": rezka_en,
        "rezka_ru": rezka_ru,
    })

genre_count = len(set(r["genre"] for r in records))

# ════════════════════════════════════════════════════════════════
#  INJECT INTO HTML TEMPLATE
# ════════════════════════════════════════════════════════════════
print(f"\n📄  Building {OUT_FILE}…")

if not os.path.exists(TEMPLATE):
    print(f"\n  ERROR: '{TEMPLATE}' not found.")
    print("  Place cineroll_template.html in the same folder as this script.")
    sys.exit(1)

with open(TEMPLATE, "r", encoding="utf-8") as f:
    html = f.read()

# Inject movie data array
movies_json = json.dumps(records, ensure_ascii=False, separators=(",", ":"))
html = html.replace("/*MOVIES_DATA*/[]", movies_json)

# Patch hero tag count strings
html = html.replace(
    "tag:  'Real IMDb data'",
    f"tag:  'Real IMDb data · {len(records):,} movies'"
)
html = html.replace(
    "tag:  'IMDb Data'",
    f"tag:  'IMDb Data · {len(records):,} movies'"
)

# Patch page <title>
html = html.replace(
    "<title>CineRoll</title>",
    f"<title>CineRoll — {len(records):,} IMDb Movies</title>"
)

with open(OUT_FILE, "w", encoding="utf-8") as f:
    f.write(html)

size_kb  = os.path.getsize(OUT_FILE) / 1024
size_mb  = size_kb / 1024

# ════════════════════════════════════════════════════════════════
#  SUMMARY
# ════════════════════════════════════════════════════════════════
print("\n" + "═" * 60)
print("  ✅  Done!")
print("═" * 60)
print(f"  Output file : {os.path.abspath(OUT_FILE)}")
print(f"  File size   : {size_mb:.1f} MB  ({size_kb:.0f} KB)")
print(f"  Total movies: {len(records):,}")
print(f"  Genres      : {genre_count}")
print()
print("  Per-genre breakdown:")
for genre, count in sorted(genre_stats.items(), key=lambda x: -x[1]):
    bar = "█" * (count // 50)
    print(f"    {genre:<16} {count:>5}  {bar}")
print()
print(f"  👉  Open {OUT_FILE} in your browser — no server needed!")
print("═" * 60 + "\n")
