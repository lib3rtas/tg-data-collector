import os
from typing import List

import pandas as pd
import networkx as nx
from telethon.sync import TelegramClient

from utils import save_df, load_df


async def process_group(link_hint: str, client: TelegramClient):
    await client.start()  # creates .session file for future usage
    group = await client.get_entity(link_hint)

    group_info = {
        'group_id': group .id,
        'title': group .title,
        'participants_count': group .participants_count,
        'date_created': group .date,
    }

    messages = await client.get_messages(group , None, reverse=True)
    messages_df = pd.DataFrame([{
        'message_id': m.id,
        'user_id': m.from_id.user_id,
        'group_id': m.peer_id.chat_id,
        'timestamp': m.date,
        'message_text': m.message,
        'fwd_from_chat_id': (
            m.fwd_from.from_id.channel_id
            if m.fwd_from is not None else None
        ),
    } for m in messages])

    users = await client.get_participants(group )
    users_list = [{
        'user_id': u.id,
        'bot': u.bot,
        'first_name': u.first_name,
        'last_name': u.last_name,
        'username': u.username,
        'deleted': u.deleted,
        'verified': u.verified,
    } for u in users]

    return group_info, users_list, messages_df


async def get_groups_data(client: TelegramClient, group_hints: List[str]):
    all_messages_df = None
    groups_users_graph = nx.Graph()

    for group in group_hints:
        group_info, users_list, messages_df = await process_group(
            group,
            client
        )
        if all_messages_df is None:
            all_messages_df = messages_df
        else:
            all_messages_df = pd.concat((all_messages_df, messages_df), axis=0)

        groups_users_graph.add_node('group_' + str(group_info['group_id']),
                                    **{
                                        'type': 'group',
                                        'subtype': 'chat',
                                        **group_info,
                                    })

        groups_users_graph.add_nodes_from([('user_' + str(u['user_id']),
                                            {
                                                'type': 'user',
                                                **u,
                                            }) for u in users_list])

        groups_users_graph.add_edges_from([(
            'user_' + str(u['user_id']),
            'group_' + str(group_info['group_id'])
        ) for u in users_list])

    save_df(all_messages_df, 'group_messages')
    nx.write_gexf(groups_users_graph, os.path.join('data', 'groups_users_graph'))

    return groups_users_graph, all_messages_df
