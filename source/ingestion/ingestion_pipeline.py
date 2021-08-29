"""
ingestion_pipeline
===============================================================================

Module used to gather and load the source datasets used in the Capstone Project
for the AI Workflow specialization (IBM, Coursera)

Author: Emilio Delgado Pascual
"""

import logging
import os
import shutil
import zipfile
from logging import config

import pandas as pd
import requests

config.fileConfig("../log_config.conf")

SOURCE_URL = "https://github.com/aavail/ai-workflow-capstone/archive/refs/heads/master.zip"
DATA_DIR = r"../data"
ZIP_FILENAME = "data_source_raw.zip"


def get_dataset_from_source(source_url, target_file="target.zip"):
    """
    Downloads the AAVAIL dataset from the GitHub repository
    :param source_url:
    :param target_file:
    :return:
    """
    filename = os.path.join(os.getcwd(), target_file)
    logging.info("Downloading source files from %s ...", SOURCE_URL)
    request = requests.get(source_url)
    logging.info("Download complete")
    with open(filename, 'wb') as file:
        file.write(request.content)
    logging.info("Saved contents to %s", filename)


def extract_zipfiles(zip_path, target_directory):
    """
    Extracts the json files from the zipped repo
    :param zip_path:
    :param target_directory:
    :return:
    """
    if not os.path.exists(target_directory):
        os.mkdir(target_directory)
    with zipfile.ZipFile(zip_path, "w") as zip_file:
        zip_file.extractall(target_directory)
    os.remove(zip_path)

    base_dir = "{}/ai-workflow-capstone-master".format(target_directory)
    for file in os.listdir(base_dir):
        json_path = "{}/ai-workflow-capstone-master/{}".format(target_directory, file)
        os.chmod(json_path, 0o666)
        shutil.move(json_path, target_directory)
        logging.info("Extracted contents of the zip file to %s", DATA_DIR)


def load_dataset(target_directory):
    """
    Transforms json files to pandas dataframes
    :param target_directory:
    :return:
    """
    base_dir = "{}/{}/".format(DATA_DIR, target_directory)
    raw_files = os.listdir(base_dir)
    logging.info("Creating pandas dataframes from source files ...")
    raw_dataframes = []
    for json_file in raw_files:
        raw_dataframes.append(pd.read_json("{}/{}".format(base_dir, json_file)))
        logging.debug("Successfully transformed %s to a pandas dataframe.", json_file)
    logging.info("Finished processing input data (% files). ", len(raw_dataframes))
    return raw_dataframes


if __name__ == "__main__":
    get_dataset_from_source(source_url=SOURCE_URL, target_file=ZIP_FILENAME)
    extract_zipfiles(ZIP_FILENAME, DATA_DIR)
    dataframes = load_dataset("cs-train")
