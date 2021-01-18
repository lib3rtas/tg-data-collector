import os
from typing import List, Dict
from datetime import datetime
import time
import re
import pandas as pd
import networkx as nx
from telethon.errors import FloodWaitError, RPCError
import traceback
import logging
import Constants as const
from os import listdir
from os.path import isfile, join

from telethon.sync import TelegramClient
from utils import serialize_dict, save_df, append_to_df, load_df




# Delete corresponding files.
df = load_df('groups_info')
#df_2 = load_df('groups_info_2')

df_nomsgs = df[df['messages_count'] == 0]

all_files = [f for f in listdir('data') if isfile(join('data', f))]
#files_to_delete = []

files_to_keep = []
def add_files_to_delete(row: pd.Series):
    for f in all_files:
        if str(row['group_id']) in f:
            files_to_keep.append(f)

df.apply(add_files_to_delete, axis=1)

files_to_delete = [
    f for f in all_files
    if
    (f not in files_to_keep)
    and 'groups_info' not in f
]

for f in files_to_delete:
    os.remove(os.path.join('data', f))

# Delete from group_info dataset.
df = load_df('groups_info')
df_onlymsgs = df[df['messages_count'] > 0]
df_onlymsgs = df[df['messages_count'] > 0]
save_df(df_onlymsgs, 'groups_info')
