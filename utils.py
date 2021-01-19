import pandas as pd
from typing import Dict
import os
from datetime import datetime


def save_df(df: pd.DataFrame, title: str):
    timestamp = datetime.now().strftime("%d-%m-%Y_%H.%M")
    filename = f'{title}_{timestamp}.feather'
    df.reset_index(drop=True).to_feather(os.path.join('data', filename))
    return filename


def load_df(dataset_name: str, full_name=False) -> pd.DataFrame:
    # Load from file.
    if not full_name:
        dataset_name = f'{dataset_name}.feather'

    df = pd.read_feather(
        os.path.join('data', dataset_name)
    )
    return df


def append_to_df(input_dict: Dict, filename: str) -> pd.DataFrame:
    as_series = pd.Series(input_dict)

    filepath = os.path.join('data', f'{filename}.feather')

    # Load from file.
    if os.path.isfile(filepath):
        print('appending')
        df = pd.read_feather(filepath).reset_index(drop=True)
        df = df.append(input_dict, ignore_index=True)
    else:
        df = as_series.to_frame(0).T

    df = df.convert_dtypes(convert_boolean=True)
    df.reset_index(drop=True).to_feather(filepath)

    return df



def serialize_dict(input: dict):
    result = {}
    for key, value in input.items():
        if value is None:
            value = ''
        elif isinstance(value, datetime):
            value = value.strftime("%Y-%m-%dT%H:%M:%SZ")
        result[key] = value
    return result