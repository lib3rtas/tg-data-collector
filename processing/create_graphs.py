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

from telethon.sync import TelegramClient
from utils import serialize_dict, save_df, append_to_df


def assemble_users_group_graph(group_info, users_list):
    group_users_graph = nx.Graph()

    group_users_graph.add_node('group_' + str(group_info['group_id']),
                               **{
                                   'type': 'group',
                                   'subtype': 'chat',
                                   **serialize_dict(group_info),
                               })

    group_users_graph.add_nodes_from([('user_' + str(u['user_id']),
                                       {
                                           'type': 'user',
                                           **serialize_dict(u),
                                       }) for u in users_list])

    for user in users_list:
        group_users_graph.add_weighted_edges_from(
            [('user_' + str(user['user_id']),
              'group_' + str(group_info['group_id']),
              user['nr_of_messages']
              )], group_title=group_info['title'], weight='nr_of_messages'
        )

    print('writing standard graph to file')
    timestamp = datetime.now().strftime("%Y-%m-%d-%Y%H%M%S")
    filename = 'STANDARD_GRAPH__groupid_' + \
               str(group_info['group_id']) + '__' + timestamp
    gephi_filename = filename + '__.gexf'
    nx.write_gexf(group_users_graph, os.path.join('data', gephi_filename))
    print('finished writing standard graph to file')


def assemble_users_messages_group_multi_graph(group_info, users_list, messages_df):
    group_users_messages_graph = nx.MultiGraph()

    group_users_messages_graph.add_node('group_' + str(group_info['group_id']),
                                        **{
                                            'type': 'group',
                                            'subtype': 'chat',
                                            **serialize_dict(group_info),
                                        })

    group_users_messages_graph.add_nodes_from([('user_' + str(u['user_id']),
                                                {
                                                    'type': 'user',
                                                    **serialize_dict(u),
                                                }) for u in users_list])

    for index, message in messages_df.iterrows():
        if message['message_text'] is not None:
            message_text = message['message_text']
        else:
            message_text = ''
        if (message['fwd_from_chat_id'] is not None) and (message['fwd_from_chat_id'] == message['fwd_from_chat_id']):
            fwd_from_chat_id = str(int(message['fwd_from_chat_id']))
        else:
            fwd_from_chat_id = 0

        # check for hyperlinks in message regex from https://stackoverflow.com/a/50790119
        regex_all_urls = r'\b((?:https?://)?(?:(?:www\.)?(?:[\da-z\.-]+)\.(?:[a-z]{2,6})|(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|(?:(?:[0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:(?:(?::[0-9a-fA-F]{1,4}){1,6})|:(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(?::[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(?:ffff(?::0{1,4}){0,1}:){0,1}(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])|(?:[0-9a-fA-F]{1,4}:){1,4}:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))(?::[0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])?(?:/[\w\.-]*)*/?)\b'
        extract_all_urls = re.findall(regex_all_urls, message_text)

        regex_telegram_url = r'(?:https?://)?t\.me/+\S+'
        extract_telegram_urls = re.findall(regex_telegram_url, message_text)

        group_users_messages_graph.add_edges_from(
            [(
                'user_' + str(message['user_id']),
                'group_' + str(group_info['group_id']),
            )], group_title=group_info['title'], message_id='message_' + str(message['message_id']),
            message_text=message_text, timestamp=str(message['timestamp']), fwd_from_chat_id=fwd_from_chat_id,
            nr_all_urls=len(extract_all_urls), all_urls=str(extract_all_urls),
            nr_telegram_urls=len(extract_telegram_urls), telegram_urls=str(extract_telegram_urls)
        )

    print('writing to file')
    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")

    filename = 'MULTI_GRAPH__groupid_' + \
               str(group_info['group_id']) + '__' + timestamp + '__.gexf'

    nx.write_gexf(group_users_messages_graph, os.path.join('data', filename))
    return
