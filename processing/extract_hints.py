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
from scraping.process_group import get_groups_data



df = load_df('groups_info')


msgs_df = load_df('messages')


at_links = []
me_links = []


def check_group_name(row: pd.Series):
    global at_links
    global me_links
    if row['message_text'] is not None:
        words = row['message_text'].split()
        for word in words:
            if (word[0] == '@'):
                at_links.append({'link': word,
                                 'group': row['group_id']})
            if ('t.me/' in word):
                final_word = word
                if not word.startswith('https://'):
                    final_word = 'https://' + word
                me_links.append({'link': final_word,
                                 'group': row['group_id']})


msgs_df.apply(check_group_name, axis=1)
me_links_df = pd.DataFrame(me_links)
me_links_df = me_links_df.drop_duplicates()
link_df_agg = me_links_df.groupby('link').agg({
    'link': 'first',
    'group': 'count'
})
link_df_agg.sort_values('group', ascending=False, inplace=True)

hint_list = link_df_agg['link'].tolist()
link_df_agg.reset_index(drop=True).to_csv('nice2')

get_groups_data(hint_list)
