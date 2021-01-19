
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


df = load_df('groups_info')

all_files = [f for f in listdir('data') if isfile(join('data', f))]
msg_files = [f for f in all_files if 'messages' in f]
usr_files = [f for f in all_files if 'user' in f]

def combine_and_save(filename_list: List[str]):
    msg_df = None
    for msg_fn in filename_list:
        new_msg_df = load_df(msg_fn, full_name=True)
        if msg_df is None:
            msg_df = new_msg_df
        else:
            msg_df = pd.concat((msg_df, new_msg_df), axis=0)
    return msg_df


msg_df = combine_and_save(msg_files)
msg_df = msg_df.drop_duplicates(
    subset=['message_id', 'group_id'],
)
save_df(msg_df, 'messages')


usr_df_raw = combine_and_save(usr_files)
usr_df_deduped = usr_df_raw.drop_duplicates(
    subset=['user_id', 'group_id'],
)
save_df(usr_df_deduped, 'users')
