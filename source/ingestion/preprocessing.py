"""
preprocessing
===============================================================================

Transformations for the input data

Author: Emilio Delgado Pascual
"""
import logging
import re

import pandas as pd

DATA_DIR = "../capstone/data/cs-train/"


def describe_dataframes(dataframes):
    """
    Prints basic info of the input dataframes
    :param dataframes:
    :return:
    """
    df_count = 0
    for df in dataframes:
        print("""
        DataFrame {}
            - N rows: {}
            - N cols: {}
            - Column names: {}
            - Missing values: {}
        """.format(df_count, df.shape[0], df.shape[1], ",".join(df.columns.values), df.isnull().sum().sum()))
        df_count += 1


def to_snake_case(string):
    """
    Converts a two-word camel case to snake case
    :param string:
    :return:
    """
    pattern_camel_case = r"(\w+)+([A-Z]+\w+)"
    groups_camel = re.findall(pattern_camel_case, string)
    if len(groups_camel) > 0:
        return groups_camel[0][0].lower() + "_" + groups_camel[0][1].lower()
    else:
        return string


def process_dataframes(dataframes):
    """
    Merges incoming dataframes into a single dataframe. Renames columns that are the same but
    have different identifiers
    :param dataframes:
    :return:
    """
    logging.info("Processing raw dataframes ... ")
    processed = []
    for df in dataframes:
        df_new = df.copy()
        df_new.columns = [to_snake_case(column) for column in df.columns.values]
        if "total_price" in df_new.columns:
            df_new.rename(columns={"total_price": "price"}, inplace=True)
        processed.append(df_new)
    df_target = pd.concat(processed)
    logging.info("Finished processing. Target dataframe size: %d x %d." % (df_target.shape[0], df_target.shape[1]))
    return df_target
