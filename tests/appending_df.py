
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



df_onlymsgs = df[df['messages_count'] > 0]
df_onlymsgs = df[df['messages_count'] > 0]

save_df(df_onlymsgs, 'groups_info')


test_df = load_df('groups_info_example')

test_series = test_df.iloc[0]

test_dict = test_series.to_dict()
test_dict['title'] = 'test'
del test_dict['level_0']
del test_dict['index']

append_to_df(test_dict, 'groups_info')

print('blah')