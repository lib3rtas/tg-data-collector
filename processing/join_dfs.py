
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

def combine_and_save(filename_list: List[str], save_name: str):
    msg_df = None
    for msg_fn in filename_list:
        new_msg_df = load_df(msg_fn, full_name=True)
        if msg_df is None:
            msg_df = new_msg_df
        else:
            msg_df = pd.concat((msg_df, new_msg_df), axis=0)
    save_df(msg_df, save_name)


combine_and_save(msg_files, 'messages')
combine_and_save(usr_files, 'users')


#msg_df = None
#for msg_fn in usr_files:
#    new_msg_df = load_df(msg_fn, full_name=True)
#    if msg_df is None:
#        msg_df = new_msg_df
#    else:
#        msg_df = pd.concat((msg_df, new_msg_df), axis=0)
#save_df(df_onlymsgs, 'groups_info')


test_df = load_df('groups_info_example')

test_series = test_df.iloc[0]

test_dict = test_series.to_dict()
test_dict['title'] = 'test'
del test_dict['level_0']
del test_dict['index']

append_to_df(test_dict, 'groups_info')

print('blah')
