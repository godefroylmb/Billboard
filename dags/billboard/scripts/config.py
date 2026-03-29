import configparser
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "billboard.ini"


def load_charts():

    parser = configparser.ConfigParser()

    if not CONFIG_PATH.exists():
        print("CONFIG NOT FOUND:", CONFIG_PATH)
        return {}

    parser.read(CONFIG_PATH)

    charts = {}

    for section in parser.sections():
        charts[section] = {
            "url_path": parser[section]["url_path"],
            "table": parser[section]["table"],
            "start_date": parser[section]["start_date"],
        }

    print("CHARTS LOADED:", charts)

    return charts