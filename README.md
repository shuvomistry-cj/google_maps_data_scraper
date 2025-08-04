# Google Maps Data Scraper

This project is a Python-based scraper that uses Playwright to extract business data from Google Maps search results. The script can process single or multiple search queries and outputs results in Excel format.

## Features
- Automates Google Maps searches using Playwright (headless Chromium)
- Extracts business name, address, website, phone, reviews, rating, and coordinates
- Supports both single search (via command line) and batch search (via input.txt)
- Outputs results in Excel (.xlsx) format
- **NEW**: Beautiful Streamlit web interface with progress tracking
- **NEW**: Multi-search with bulk ZIP download
- **NEW**: Search history tracking

## Requirements
- Python 3.8+
- See `requirements.txt` for Python dependencies
- Chromium browser (installed via Playwright)

## Setup
1. (Recommended) Create and activate a virtual environment:
   ```bash
   python -m venv venv
   # On Windows:
   venv\Scripts\activate
   # On Linux/macOS:
   source venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

## Usage

### Streamlit Web Interface (Recommended)
Run the beautiful web interface:
```bash
streamlit run streamlit_app.py
```
Then open your browser to `http://localhost:8501`

**Features:**
- **Single Search Tab**: Enter search query and limit, track progress, download Excel
- **Multi Search Tab**: Upload input.txt file, download all results as ZIP
- **History Tab**: View all your previous searches

### Command Line Interface

#### Single Search
Run:
```bash
python main.py -s="<what & where to search for>" -t=<how many>
```
Example:
```bash
python main.py -s="Boston dentist" -t=50
```

#### Batch Search
1. Add each search query on a new line in `input.txt` (see the provided example file).
2. Run:
```bash
python main.py
```
3. Optionally, add `-t=<how many>` to limit results for all searches.

### Output
- Results are saved as Excel files (`output/google_maps_data_<search>.xlsx`).
- Each row contains: name, address, website, phone, reviews count, rating, latitude, longitude.

## Tips
- To get more than the Google Maps search limit (120 results), use more specific queries in `input.txt` (e.g., by city or neighborhood).

#### Example Queries
```
United States Boston dentist
United States New York dentist
United States Texas dentist
```

## License
This project is for educational purposes only.
