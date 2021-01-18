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
msgs_df = load_df('messages')


df_2 = load_df('groups_info_2')



all_files = [f for f in listdir('data') if isfile(join('data', f))]


message_files = [f for f in all_files if 'messages' in f]

channel_fn = [f for f in all_files if '1218998207_messages' in f][0]

df_channel_msg = pd.read_feather(
    os.path.join('data', channel_fn)
)


print('blah[')
