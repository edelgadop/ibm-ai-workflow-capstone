import logging
from logging import config
config.fileConfig("../log_config.conf")
import os
import requests
import zipfile

SOURCE_URL = "https://github.com/aavail/ai-workflow-capstone/archive/refs/heads/master.zip"
DATA_DIR = "../data"


def get_dataset_from_source(source_url, target_file="target.zip"):
    filename = os.path.join(os.getcwd(), target_file)
    logging.info("Downloading source files from {} ...".format(SOURCE_URL))
    r = requests.get(source_url)
    logging.info("Download complete")
    with open(filename, 'wb') as f:
        f.write(r.content)
    logging.info("Saved contents to {}".format(filename))


def extract_zipfiles(zip_path, target_directory):
    if not os.path.exists(target_directory):
        os.mkdir(target_directory)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(target_directory)
    logging.info("Extracted contents of the zip file to /data")


if __name__ == "__main__":
    zip_filename = "data_source_raw.zip"
    get_dataset_from_source(source_url=SOURCE_URL, target_file=zip_filename)
    extract_zipfiles(zip_filename, DATA_DIR)






