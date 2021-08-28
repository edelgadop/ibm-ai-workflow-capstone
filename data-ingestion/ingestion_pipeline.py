import logging
from logging import config
config.fileConfig("../log_config.conf")
import os
import requests

SOURCE_URL = 'https://github.com/aavail/ai-workflow-capstone/archive/refs/heads/master.zip'


def get_dataset_from_source(source_url, target_file="target.zip"):
    filename = os.path.join(os.getcwd(), target_file)
    logging.info("Downloading source files from {} ...".format(SOURCE_URL))
    r = requests.get(source_url)
    logging.info("Download complete")
    with open(filename, 'wb') as f:
        f.write(r.content)
    logging.info("Saved contents to {}".format(filename))


if __name__ == "__main__":
    get_dataset_from_source(SOURCE_URL, "data_source_raw.zip")



