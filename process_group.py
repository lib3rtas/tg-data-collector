import os
from typing import List
from datetime import datetime
import time
import re
import pandas as pd
import networkx as nx
from telethon.errors import FloodWaitError, RPCError
from telethon.sync import TelegramClient

from utils import serialize_dict, save_df


def calc_nr_of_messages_per_user(user_id, messages_user_count):
    try:
        nr_of_messages = messages_user_count[user_id]
        return nr_of_messages
    except:
        return 0


async def process_group(link_hint: str, client: TelegramClient):
    # group info processing
    group = await client.get_entity(link_hint)
    group_info = {
        'group_id': group.id,
        'title': group.title,
        'participants_count': group.participants_count,
        'date_created': group.date,
        'broadcast': group.broadcast,
        'megagroup': group.megagroup
    }
    print(f'Processing group {group_info["title"]}', flush=True)
    print("sleeping for 30 seconds...", flush=True)
    time.sleep(30)

    # exit here if it is a channel
    if group_info['broadcast']:
        return group_info, None, None, None

    # message processing
    messages_dict = []
    messages = await client.get_messages(
        group,
        3000,
        reverse=False,
    )
    for m in messages:
        if m.from_id is not None:
            user_id = None
            if hasattr(m.from_id, 'user_id'):
                user_id = m.from_id.user_id
            else:
                continue
            fwd_from_id = None
            if m.fwd_from is not None:
                if hasattr(m.fwd_from.from_id, 'user_id'):
                    fwd_from_id = m.fwd_from.from_id.user_id
                elif hasattr(m.fwd_from.from_id, 'channel_id'):
                    fwd_from_id = m.fwd_from.from_id.channel_id
            messages_dict.append({
                'message_id': m.id,
                'user_id': user_id,
                'group_id': group.id,
                'timestamp': m.date,
                'message_text': m.message,
                'fwd_from_chat_id': fwd_from_id,
            })

    messages_df = pd.DataFrame(messages_dict)
    save_df(messages_df, 'group_' + str(group_info['group_id']))

    print("sleeping for 30 seconds...", flush=True)
    time.sleep(30)
    # user processing
    messages_user_count = messages_df['user_id'].value_counts()
    users = await client.get_participants(group)
    users_list = [{
        'user_id': u.id,
        'bot': u.bot,
        'first_name': u.first_name,
        'last_name': u.last_name,
        'username': u.username,
        'deleted': u.deleted,
        'verified': u.verified,
        'nr_of_messages': calc_nr_of_messages_per_user(u.id, messages_user_count),
    } for u in users]

    return group_info, users_list, messages_df, messages_dict


async def process_channel(client: TelegramClient, link_hint: str):
    # channel info processing
    channel = await client.get_entity(link_hint)
    channel_info = {
        'channel_id': channel.id,
        'title': channel.title,
        'participants_count': channel.participants_count,
        'date_created': channel.date,
        'broadcast': channel.broadcast,
        'megagroup': channel.megagroup
    }
    print(f'Processing channel {channel_info["title"]}', flush=True)
    print("sleeping for 30 seconds...", flush=True)
    time.sleep(30)

    # message processing
    messages_dict = []
    messages = await client.get_messages(
        channel,
        3000,
        reverse=False,
    )
    for m in messages:
        fwd_from_id = None
        if m.fwd_from is not None:
            if hasattr(m.fwd_from.from_id, 'channel_id'):
                fwd_from_id = m.fwd_from.from_id.channel_id
            messages_dict.append({
                'message_id': m.id,
                'channel_id': channel.id,
                'timestamp': m.date,
                'message_text': m.message,
                'fwd_from_chat_id': fwd_from_id,
    })
    print("sleeping for 30 seconds...", flush=True)
    time.sleep(30)

    return messages_dict


async def get_groups_data(client: TelegramClient, group_hints: List[str]):
    await client.start()  # creates .session file for login in future

    all_channel_messages = []
    all_group_messages = []

    for group in group_hints:
        not_finished_group = True
        timer = 300
        while (not_finished_group):
            not_finished_group = True
            try:
                print('started data scraping for ' + group, flush=True)
                group_info, users_list, messages_df, messages_dict = await process_group(group, client)
                print('finished data scraping for ' + group, flush=True)

                if group_info['megagroup']:
                    print('starting normal graph processing for ' + group)
                    assemble_users_group_graph(group_info, users_list)

                    print('starting multi graph processing for ' + group, flush=True)
                    assemble_users_messages_group_multi_graph(group_info, users_list, messages_df)

                    all_group_messages += messages_dict

                elif group_info['broadcast']:
                    print('extracting channel ' + group, flush=True)
                    all_channel_messages += await process_channel(client, group)
                    not_finished_group = False
                    continue

                print('\033[92m' + 'finished group ' + group + '\033[0m', flush=True)
                print("sleeping for 60 seconds...", flush=True)
                not_finished_group = False
                timer = 300
                time.sleep(60)
            except (FloodWaitError, RPCError) as e:
                print(e, flush=True)
                print('\033[91m' + 'data scraping for ' +
                      group + ' failed because of FloodWait or RPC Error' + '\033[0m', flush=True)
                print(f"sleeping for {timer} seconds...", flush=True)
                time.sleep(timer)
                timer *= 3
            except Exception as e:
                print(e, flush=True)
                print('\033[91m' + 'data scraping for ' +
                      group + ' failed due to unknown Error' + '\033[0m', flush=True)
                print("\033[91m aborting group...\033[0m", flush=True)
                not_finished_group = False

    print('saving dataframes ', flush=True)
    all_channel_messages_df = pd.DataFrame(all_channel_messages)
    save_df(all_channel_messages_df, 'all_channel_messages')

    all_group_messages_df = pd.DataFrame(all_group_messages)
    save_df(all_group_messages_df, 'all_group_messages')

    print('processed all groups')
    print('finishing....', flush=True)
    return


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
