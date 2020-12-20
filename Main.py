from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.types import InputPeerEmpty
import logging
import networkx as nx
import os
import networkx as nx
from networkx.algorithms import bipartite
import matplotlib.pyplot as plt

from typing import List

import Constants as const
from process_group import get_groups_data
logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
                    level=logging.INFO)

client = TelegramClient(
            'anon', 
            const.credentials['api_id'], 
            const.credentials['api_hash']
        )


async def main():
    hints = [
        "@donald_j_trump_q_family_germany",
        "@defender_shaef_2q2q",
        "@qanons_deutschland",
        "@qanon_world",
        "@kaisertreu_ewig",
        "@deutschlandtreff",
        "@digitalerchronist_allgemeinchat",
        "@betfury_de",
        "@cryptomondayde",
        "@gaynudesger",
        "@pokemongoger",
        "@germanfurrygroup",
        "@binancegerman",
        "@technicall_crypto",
    ]
    print('downloading from telegram API')
    groups_users_graph, all_messages_df = await get_groups_data(client, hints)
    #print('loading from file')
    #groups_users_graph = nx.read_gexf(os.path.join('data', 'groups_users_graph.gexf'))

    user_count = groups_users_graph.number_of_nodes()
    print('user nodes: ' + str(user_count))
    print('transforming network')
    group_nodes = [
        x for x, y
        in groups_users_graph.nodes(data=True)
        if y['type'] == 'group'
    ]
    groups_graph = bipartite.weighted_projected_graph(
        groups_users_graph, group_nodes)

    print('drawing plot')
    plt.subplot(121)
    nx.draw(groups_graph, with_labels=True, font_weight='bold')
    plt.show()


with client:
    client.loop.run_until_complete(main())