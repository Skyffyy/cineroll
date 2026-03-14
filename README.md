# CineRoll

Movie discovery site built on real IMDb data. Pick filters or roll 🎲 — get a movie with a direct watch link.

## Setup

```bash
pip install pandas
python build_cineroll.py
```

Open `cineroll.html` in your browser. No server needed.

## Files

| File | Purpose |
|---|---|
| `build_cineroll.py` | Downloads IMDb datasets, generates the site |
| `cineroll_template.html` | Website template (HTML/CSS/JS) |
| `cineroll.html` | ✅ Output — open this |

## What it does

- 🎲 Random movie picker with genre / year / rating filters
- 10 000+ movies from IMDb (up to 1 000 per genre)
- RU / EN language toggle — titles and watch links switch language
- ▶ Watch button → HDRezka search, IMDb → button → IMDb page
- Watchlist saved in `localStorage`

## Config

Edit the top of `build_cineroll.py`:

```python
MIN_VOTES       = 5_000   # raise for more popular films only
MIN_RATING      = 5.5     # minimum IMDb score
PER_GENRE_LIMIT = 1_000   # movies per genre
```

## Data

IMDb Non-Commercial Datasets — personal use only.
Cached in `./imdb_cache/`, refreshed every 24h on re-run.
