# Google Maps Data Scraper (Streamlit + Playwright)

This app scrapes business listings from Google Maps and saves the data into a local SQLite database.

You can run **single searches** or create **keyword batches (campaigns)** where multiple workers scrape keywords one-by-one from the database.

## What you can do (Features)

- **Single Search**
  - Search one keyword (example: `Dentist in Goa`)
  - Scrape name, address, website, phone, reviews, rating, latitude, longitude

- **Multi Keyword Campaigns (Batch System)**
  - Upload keywords file:
    - `.txt` (1 keyword per line)
    - `.csv` / `.xlsx` (keyword column)
  - Enter a **Batch Name** (must be unique)
  - Save to DB and choose:
    - **Run now**
    - **Run later**
  - Workers pull keywords from DB automatically (no manual distribution)
  - Keyword status tracking:
    - `NULL/undone` -> `in_progress` -> `done`

- **Batch Management**
  - View keyword preview
  - Download keywords (TXT / Excel)
  - Start campaign later
  - Delete batch

- **Saved Data**
  - Preview saved businesses
  - Download all data or download by batch

## Technology Used

- **Python**
- **Playwright (Chromium)**: automated browsing for Google Maps
- **Streamlit**: web UI
- **SQLite**: local database storage
- **Pandas / OpenPyXL / XlsxWriter**: file parsing and export

## Quick Start (Windows)

1) Install Python (3.10+ recommended)

2) Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

3) Run the web app

```bash
streamlit run streamlit_app.py
```

Open:

- `http://localhost:8501`

## How to use (Simple)

### Run a Campaign

1) Go to **Multi Keyword Search** tab
2) Upload your keywords file (`.txt/.csv/.xlsx`)
3) Enter a **Batch Name**
4) Click **Save Batch** or **Save + Run Campaign Now**
5) To start later:
   - Find the batch in the list
   - Click **Campaign**
   - Set **Workers** and **Limit per keyword**
   - Click **Launch Campaign**

### Where is data saved?

- Database file: `scraper_data.sqlite`
- You can export results from the **Saved Data** tab

## Note

This project is for educational purposes only.

---

## 🖥️ Terminal Monitor (Live Stats)

Run this in a **separate terminal** while scraping to see live progress:

```bash
python monitor.py
```

**What it shows:**
- Total records in database
- Records per batch
- Keyword progress (Total | Done | In-Progress | Pending)
- Last keyword processed per batch
- Currently active workers

Press `Ctrl+C` to stop monitor (scraper continues running).

---

## 🚀 How to Run

### Option 1: Web UI (Streamlit) - Recommended
```bash
streamlit run streamlit_app.py
```
Open browser: `http://localhost:8501`

### Option 2: Single Search (Terminal - Headless)
```bash
python main.py -s "restaurants in bhubaneswar" -t 20 -b "my_batch"
```
- `-s`: Search keyword
- `-t`: Max results per keyword
- `-b`: Batch name (for tracking)

### Option 3: Debug Mode (See Browser)
```bash
python main.py -d -s "restaurants in bhubaneswar" -t 10 -b "debug_batch"
```
- `-d`: Opens visible browser for debugging
- Use when Google Maps UI changes and scraper stops working

### Option 4: Multiple Keywords from File
```bash
python main.py -i keywords.txt -t 20 -b "my_campaign"
```
- `-i`: Input file with keywords (one per line)

---

## 📤 Push to GitHub

### First Time Setup
```bash
# Check current status
git status

# Stage your changes
git add streamlit_app.py main.py monitor.py README.md

# Commit with message
git commit -m "Add Google Maps scraper with batch tracking and monitor"

# Add remote (replace with your repo URL)
git remote add origin https://github.com/YOUR_USERNAME/google_maps_scraper.git

# Push to GitHub
git push -u origin main
# OR if your branch is master:
git push -u origin master
```

### Update Existing Repo
```bash
git add .
git commit -m "Update: fix cookie consent and add batch tracking"
git push origin main
```

---

## 🛠️ Troubleshooting

| Problem | Solution |
|---------|----------|
| Playwright Inspector keeps opening | Clear env: `Remove-Item Env:\PWDEBUG` or use fresh terminal |
| German/Spanish consent page blocks scraper | Code auto-handles multiple languages. If stuck, run with `-d` flag and check screenshots |
| Stuck on "about:blank" | In Playwright Inspector, click **Resume (▶)** button |
| Search box not found | Google Maps UI changed. Run with `-d` flag to see what's on page |
| Database locked | Stop Streamlit, run: `python -c "import sqlite3; sqlite3.connect('scraper_data.sqlite').execute('PRAGMA journal_mode=DELETE')"` |

---

## 📁 Project Structure

```
google_maps_data_scraper/
├── streamlit_app.py     # Web UI and worker management
├── main.py              # Scraper logic (Playwright)
├── monitor.py           # Terminal live stats
├── scraper_data.sqlite  # Database (auto-created)
├── requirements.txt     # Dependencies
└── README.md           # This file
```
