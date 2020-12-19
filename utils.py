import pandas as pd
import os
from datetime import datetime


def save_df(df: pd.DataFrame, title: str):
    timestamp = datetime.now().strftime("%d-%m-%Y_%H.%M")
    filename = f'{title}_{timestamp}.feather'
    df.reset_index().to_feather(os.path.join('data', filename))
    return filename


def load_df(dataset_name: str) -> pd.DataFrame:
    # Load from file.
    df = pd.read_feather(
        os.path.join('data', f'{dataset_name}.feather')
    )
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