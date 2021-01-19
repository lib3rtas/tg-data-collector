import os
from typing import List, Dict, Optional
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
from utils import serialize_dict, save_df, append_to_df, load_df


block_errors = True


def calc_nr_of_messages_per_user(user_id, messages_user_count):
    try:
        nr_of_messages = messages_user_count[user_id]
        return nr_of_messages
    except:
        return 0


async def process_group(
        link_hint: str,
        client: TelegramClient,
        processed_groups_ids: List[str]
) -> Optional[Dict]:
    # group info processing
    try:
        group = await client.get_entity(link_hint)
    except Exception as e:
        print(f'Exception {str(e)} when getting group info of {link_hint},'
              f'skipping.')
        return

    if group.id in processed_groups_ids:
        print(f'Skipping {link_hint} as it was already processed.')
        return

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

    # message processing
    messages_dict = []
    messages = await client.get_messages(
        group,
        3000,
        reverse=False,
    )
    for m in messages:
        user_id = None
        if m.from_id is not None and hasattr(m.from_id, 'user_id'):
            user_id = m.from_id.user_id
        fwd_from_id = None
        if m.fwd_from is not None and hasattr(m.fwd_from, 'from_id'):
            if hasattr(m.fwd_from.from_id, 'user_id'):
                fwd_from_id = m.fwd_from.from_id.user_id
            elif hasattr(m.fwd_from.from_id, 'channel_id'):
                fwd_from_id = m.fwd_from.from_id.channel_id
        messages_dict.append({
            'message_id': m.id,
            'timestamp': m.date,
            'message_text': m.message,
            'user_id': user_id,
            'fwd_from_chat_id': fwd_from_id,
            'group_id': group.id,
        })
    messages_df = pd.DataFrame(messages_dict)
    save_df(messages_df, f'{group.id}_messages')
    group_info['messages_count'] = messages_df.shape[0]
    print("sleeping for 30 seconds...", flush=True)
    time.sleep(30)

    # user processing
    if not group.broadcast:
        messages_user_count = messages_df['user_id'].value_counts()
        users = await client.get_participants(group)
        users_df = pd.DataFrame([{
            'user_id': u.id,
            'group_id': group.id,
            'bot': u.bot,
            'first_name': u.first_name,
            'last_name': u.last_name,
            'username': u.username,
            'deleted': u.deleted,
            'verified': u.verified,
            'nr_of_messages': calc_nr_of_messages_per_user(
                    u.id, messages_user_count),
        } for u in users])
        if group_info['participants_count'] is None:
            group_info['participants_count'] = users_df.shape[0]
        save_df(users_df, f'{group.id}_users')

    append_to_df(group_info, 'groups_info')

    return group_info


def get_groups_data(group_hints: List):
    # Clean group hints

    logging.basicConfig(
        format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
        level=logging.INFO
    )

    client = TelegramClient(
        'anon',
        const.credentials['api_id'],
        const.credentials['api_hash']
    )
    with client:
        client.loop.run_until_complete(
            get_groups_data_async(client, group_hints))


async def get_groups_data_async(client: TelegramClient, group_hints: List[str]):
    await client.start()  # creates .session file for login in future

    # Get already processed groups once.
    processed_group_ids = []
    if os.path.exists(os.path.join('../data', 'groups_info')):
        df_processed = load_df('groups_info')
        processed_group_ids = df_processed['group_id'].tolist()


    for group in group_hints:
        timer = 300
        try:
            print(f'Started data scraping for {group}')
            await process_group(group, client, processed_group_ids)
            print(f'Finished data scraping for {group}')

            print('Sleeping for 60 seconds...')
            timer = 300
            time.sleep(60)
        except (FloodWaitError, RPCError) as e:
            print(f'API error {str(e)} in group {group}')
            print(f"sleeping for {timer} seconds...", flush=True)
            time.sleep(timer)
            timer *= 3
        except Exception as e:
            print(f'Error {str(e)} in group {group}')
            traceback.print_exc()
            if block_errors:
                raise

    print('processed all groups')
