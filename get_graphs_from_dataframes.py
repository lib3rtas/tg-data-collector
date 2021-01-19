from os import listdir, path
from fnmatch import fnmatch
from tqdm import tqdm
from datetime import datetime
import networkx as nx
from utils import load_df, save_df, serialize_dict


def export_bipartite_graph(graph):
    timestamp = datetime.now().strftime("%Y-%m-%d-%Y%H%M%S")
    filename = 'BIPARTITE_GRAPH__' + timestamp + '__.gexf'
    nx.write_gexf(graph, path.join('output', filename))


def export_one_mode_graph(graph):
    timestamp = datetime.now().strftime("%Y-%m-%d-%Y%H%M%S")
    filename = 'ONE_MODE_GRAPH__' + timestamp + '__.gexf'
    nx.write_gexf(graph, path.join('output', filename))


def import_dataframes():
    users_df = load_df('users')
    messages_df = load_df('messages')
    groups_info_df = load_df('groups_info')

    return groups_info_df, users_df, messages_df


def calc_nr_of_messages_per_user(user_id, messages_user_count):
    try:
        nr_of_messages = messages_user_count[user_id]
        return nr_of_messages
    except:
        return 0


def get_group_title(group_id, groups_info_df):
    for index, group in groups_info_df.iterrows():
        if group_id == group['group_id']:
            return group['title']


def generate_graph(groups_info_df, users_df, messages_df):
    result = nx.Graph()
    messages_user_count = messages_df['user_id'].value_counts()

    for index, group in groups_info_df.iterrows():
        result.add_node(
            'group_' + str(group['group_id']),
            **{
                'type': 'group',
                'subtype': 'chat',
                **serialize_dict(group),
            },
        )

    for index, user in users_df.iterrows():
        for result_node in list(result.nodes.data()):
            if user['user_id'] == result_node[1]['user_id']:
                result_node[1]['nr_of_messages'] += user['nr_of_messages']
                break
            else:
                result.add_node(
                    'user_' + str(user['user_id']),
                    **{
                        'type': 'user',
                        'nr_of_message': calc_nr_of_messages_per_user(user['user_id'], messages_user_count),
                        **serialize_dict(user),
                    },
                )
        result.add_weighted_edges_from(
            [(
                'user_' + str(user['user_id']),
                'group_' + str(user['group_id']),
                calc_nr_of_messages_per_user(user['user_id'], messages_user_count)
            )],
            group_title=get_group_title(user['group_id'], groups_info_df),
            weight='nr_of_messages',
        )

    return result


def transform_bipartite_to_one_mode(graph):



if __name__ == '__main__':
    groups_info_df, users_df, messages_df = import_dataframes()
    bipartite_graph = generate_graph(groups_info_df, users_df, messages_df)
    export_bipartite_graph(bipartite_graph)
    one_mode_graph = transform_bipartite_to_one_mode(bipartite_graph)
