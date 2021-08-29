import logging
from logging import config
import os
import shutil
import requests
import zipfile
import pandas as pd

config.fileConfig("../log_config.conf")

SOURCE_URL = "https://github.com/aavail/ai-workflow-capstone/archive/refs/heads/master.zip"
DATA_DIR = r"../data"


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
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.extractall(target_directory)
    os.remove(zip_path)

    base_dir = "{}/ai-workflow-capstone-master".format(target_directory)
    for file in os.listdir(base_dir):
        json_path = "{}/ai-workflow-capstone-master/{}".format(target_directory, file)
        os.chmod(json_path, 0o666)
        shutil.move(json_path, target_directory)
        logging.info("Extracted contents of the zip file to {}".format(DATA_DIR))


def load_dataset(target_directory):
    base_dir = "{}/{}/".format(DATA_DIR, target_directory)
    raw_files = os.listdir(base_dir)
    logging.info("Creating pandas dataframes from files in {}".format(base_dir))
    raw_dataframes = []
    for json_file in raw_files:
        raw_dataframes.append(pd.read_json("{}/{}".format(base_dir, json_file)))
        logging.debug("Successfully transformed {} to a pandas dataframe.".format(json_file))
    logging.info("Finished processing input data ({} files). "
                 .format(len(raw_dataframes)))
    return raw_dataframes


if __name__ == "__main__":
    zip_filename = "data_source_raw.zip"
    get_dataset_from_source(source_url=SOURCE_URL, target_file=zip_filename)
    extract_zipfiles(zip_filename, DATA_DIR)
    dataframes = load_dataset("cs-train")






