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
import seaborn as sns

from os import listdir
from os.path import isfile, join

from telethon.sync import TelegramClient
from utils import serialize_dict, save_df, append_to_df, load_df


df = load_df('messages')
df_groups = load_df('groups_info')
df_users = load_df('users')

df_users_agg = df_users.groupby('user_id').agg({
    'group_id': 'nunique',
    'username': 'first',
    'username': 'first',
})

usernr = len(df_users['user_id'].unique())

df_specuser = df[df['user_id'] == 207058986]

df_agg = df.groupby('user_id').agg({
    'user_id': 'first',
    'group_id': 'nunique',
    'timestamp': 'count',
    'message_text': 'first',
})
avg_msg_per_active_user = df_agg[df_agg['timestamp'] > 0]['timestamp'].mean()

avg_groups_per_active_user = df_agg['group_id'].mean()




df_agg = df_agg.sort_values('group_id', ascending=False)

print('blah')


df_agg = df_agg.sort_values('message_text', ascending=False)

print('blah')
