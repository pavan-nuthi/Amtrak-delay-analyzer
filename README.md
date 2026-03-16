# Amtrak Delay Patterns — Web Scraping & Data Analysis

**STA 220 — Data & Web Technologies for Data Analysis**

Analyzes real-time Amtrak train delay patterns across the United States using web scraping, SQL databases, and statistical visualization.

---

## Data Sources

| Source | Method | Data |
|--------|--------|------|
| [Amtraker API](https://api-v3.amtraker.com/v3/trains) | `requests` (JSON) | Real-time train positions, scheduled vs. actual times, delays, routes |
| [Wikipedia](https://en.wikipedia.org/wiki/List_of_Amtrak_stations) | `pd.read_html` + `lxml` | Station metadata (ridership, state, coordinates) |
| [Open-Meteo API](https://api.open-meteo.com) | `requests` (JSON) | Historical weather data (temperature, precipitation, wind) |

---

## Project Structure

```
Project/
├── amtrak_delay_analysis.ipynb   # Main notebook (scraping, SQL, viz, stats)
├── amtrak_scraper.py             # Standalone scraper for cron scheduling
├── amtrak_delays.db              # SQLite database (auto-created)
├── scraper.log                   # Cron job log file
└── README.md
```

---

## How It Works

### 1. Data Collection (`amtrak_scraper.py`)
- Fetches all active Amtrak trains from the Amtraker API every 15 minutes via cron
- Parses train routes, station stops, scheduled/actual times, GPS positions
- Calculates delay in minutes: `delay = actual_time - scheduled_time`
- Stores everything in a normalized SQLite database (`amtrak_delays.db`)

### 2. Database Schema (`amtrak_delays.db`)
- **`routes`** — Route name, origin, destination
- **`stations`** — Station code, name, timezone, coordinates
- **`delay_logs`** — Per-station delay records with timestamps (main table)
- **`weather`** — Daily weather data for 10 hub stations

### 3. Analysis Notebook (`amtrak_delay_analysis.ipynb`)

| Part | Description |
|------|-------------|
| **Part 1 — Web Scraping** | API scraping, Wikipedia HTML tables, weather API |
| **Part 2 — SQL Database** | Schema creation, data insertion, complex queries (JOINs, window functions, aggregations) |
| **Part 3 — Visualizations** | Interactive map (Folium), bar charts, heatmaps, box plots (Plotly/Matplotlib) |
| **Part 4 — Statistics** | Descriptive stats, ANOVA, Pearson/Spearman correlation, linear regression |

---

## Setup & Run

### 1. Install Dependencies

```bash
cd /path/to/Project
python -m venv .venv
source .venv/bin/activate
pip install requests lxml pandas matplotlib plotly folium scipy numpy requests-cache nbformat scikit-learn
```

### 2. Run the Scraper (one-time)

```bash
python amtrak_scraper.py
```

This creates `amtrak_delays.db` and populates it with a snapshot of all active trains.

### 3. Set Up Automated Collection (optional)

```bash
crontab -e
```

Add this line to scrape every 15 minutes:

```
*/15 * * * * /full/path/to/.venv/bin/python /full/path/to/amtrak_scraper.py >> /full/path/to/scraper.log 2>&1
```

> **Note:** Use absolute paths — cron doesn't load your shell environment.

Check logs: `tail -f scraper.log`

### 4. Open the Notebook

```bash
jupyter notebook amtrak_delay_analysis.ipynb
```

Run all cells top-to-bottom. The notebook reads from the same `amtrak_delays.db` that the cron populates, so the more snapshots collected, the richer the analysis.

### 5. Stop the Cron (when done)

```bash
crontab -r
```

---

## Key Findings

- **ANOVA** confirms statistically significant delay differences across routes (p < 0.001)
- Long-distance routes (e.g., Southwest Chief, Empire Builder) tend to accumulate more delay
- Delays compound along a route — trains that start late get progressively later
- Weather correlations (wind, precipitation) are analyzed at 10 major hub stations
