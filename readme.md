# 🎵 Billboard Scraper

This project is an upgraded version of a course project developed at **ISEP**. It was originally designed to explore correlations between music features (via Spotify) and their chart success. Today, it has evolved into an **automated scraper** that collects Billboard chart data weekly and shares it with the community through Kaggle.

> 🚀 **Live dataset updated weekly on [Kaggle](https://www.kaggle.com/datasets/ludmin/billboard)**

---

## 📊 Project Overview

Initially, we scraped Billboard & Spotify data to feed an Elasticsearch index and visualize it using **Kibana**. As expected (or unfortunately?), no magic formula for a hit song emerged — but the pipeline worked beautifully.

Due to the lack of up-to-date public datasets, we built our **own automated Billboard chart scraper**, which now:
- Runs weekly via Airflow
- Pushes structured chart data directly to Kaggle

---

## 📁 Current Charts Scraped

Each chart is stored as a CSV file:

| Chart Name        | File                     |
|-------------------|--------------------------|
| Billboard 200     | `billboard200.csv`       |
| Hot 100           | `hot100.csv`             |
| Radio             | `radio.csv`              |
| Streaming Songs   | `streaming_songs.csv`    |
| Digital Song Sales| `digital_songs.csv`      |

### 📦 Each entry contains:
- 🗓️ Date  
- 🎵 Song title  
- 🎤 Artist  
- 🔢 Current rank  
- ⬆️ Last week’s rank  
- 🏆 Peak position  
- ⌛ Weeks on chart  
- 🖼️ Image URL  

---

## 📚 Project Structure

### `dags/` folder:
- `billboard.py` – Airflow DAG that scrapes and uploads data
- `utils.py` – Helper functions (scraping, uploading, etc.)

### `scrapper/` folder:
- `scrap_full.py` – One-time historical scrapping tool
- `weekly_scrap.py` – Main script used by Airflow (manual fallback)
- `utils.py` – Utility functions for scraping
- `config.ini` – Scraper settings (URLs, date ranges)
- `requirements.txt` – Required dependencies

---

## ⚙️ How to Run the Scraper Manually

1. 📦 Install dependencies  
   ```bash
   pip install -r requirements.txt
    ```
2. ⚙️ Edit `config.ini` with the desired Billboard chart and date range

3. 📁 Set your output destination (local path, S3, or Minio)

4. ▶️ Run the scraper:
    ```bash
    python scrapper/weekly_scrap.py --config config.ini
    ```
💡 You can also use scrap_full.py for full-history scrapes.

---

## 🤝 Contributions Welcome

Feel free to open issues, suggest improvements, or contribute directly to the scrapers.  
You can also help by expanding the project to include additional Billboard charts or enhance the Airflow pipeline.

---

## 📬 Contact

Maintained by [@godefroylmb](https://github.com/godefroylmb)  
📊 Weekly dataset updates on [Kaggle](https://www.kaggle.com/datasets/ludmin/billboard)