# Billboard scrapper
This project is an iteration over a project originally created for a big data course at ISEP. 

The base project was a billboard scrapper combined with the spotify API to extract data over the top 100 songs. This data was then used to feed elasticsearch and visualize it with Kibana. As expected, we didn't see any evident correlation between details about the songs and their popularity (or it would means that we found the magic recipe to produce hits).

When doing our project, we had to scrap data by ourselve from the billboard website because no dataset were up to date. This project is a scrapper that run once a week and is pushing the scrapped data from the billboard to kaggle so everybody can use it.

## Data

The data is updated and pushed to Kaggle every week and can be found [here](https://www.kaggle.com/datasets/ludmin/billboard).

#### Current charts being scrapped:
- `billboard200.csv` for the Billboard 200 chart
- `hot100.csv` for the Hot 100 chart
- `radio.csv` for the Radio chart
- `streaming_songs.csv` for the Streaming songs chart
- `digital_songs.csv` for the Digital song sales chart

#### Contains details about :
- Date
- Song's title
- Artist's name
- Rank
- Last week rank
- Peak position
- Weeks in charts
- Image Url

## Files explanation
#### Dags folder:
- `billboard.py` is the dags file that is run by airflow to scrap the data from the billboard website and push it to kaggle.
- `utils.py` contains the functions used by the dags file to scrap the data and push it to kaggle.
#### Scrapper folder:
- `scrap_full.py` is a script that scrapes the designated billboard page from a given start date to a given end date, saving the results to a CSV file. This one was used to scrap the data for the first time and is not used anymore.
- `weekly_scrap.py` is a script that scrapes the designated billboard page from a given start date to a given end date, saving the results to a CSV file. This one is a backup script that is designed to be manually used only when the airflow DAG is not working (and was used to make a save for each week for a potential API usage).
- `utils.py` contains utility functions used by the scrapping scripts.
- `config.ini` is a configuration file that contains the settings for the scrapper, such as the URL to scrap, the start date, and the end date.
- `requirements.txt` contains the dependencies required to run the scrapper.

## How to run the scrapper (in case you want to run it manually):
1. Install the dependencies by running `pip install -r requirements.txt`.
2. Create/edit the `config.ini` file, editing the settings.
3. Check/edit the output folder from the script, for my project I used Minio (a S3 compatible storage) so I set the output folder to Minio, but you might want to set it to a local folder or another S3 compatible storage.
4. Run the scrapper by executing `python weekly_scrap.py --config ../hot100/config.ini` or `python scrap_full.py --config ../hot100/config.ini` (the first one is the one used by airflow, the second one is a backup script that can be used to scrap the data manually).

The scrapper is probably not perfect and could probably be sped up by parallelizing the requests.

