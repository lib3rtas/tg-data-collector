from os import listdir, path
from fnmatch import fnmatch
from tqdm import tqdm
from datetime import datetime
from networkx.algorithms import bipartite
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
        nr_of_messages = messages_user_count[user_id] + 1
        return nr_of_messages
    except:
        # was 0 before, but gephi does not accept edges with zero weight
        return 1


def get_group_title(group_id, groups_info_df):
    for index, group in groups_info_df.iterrows():
        if group_id == group['group_id']:
            return group['title']


def generate_bipartite_graph(groups_info_df, users_df, messages_df):
    result = nx.Graph()
    messages_user_count = messages_df['user_id'].value_counts()

    print('generating bipartite graph')

    for index, group in groups_info_df.iterrows():
        result.add_node(
            'group_' + str(group['group_id']),
            **{
                'type': 'group',
                'subtype': 'chat',
                **serialize_dict(group),
            },
            bipartite=0
        )

    for index, user in tqdm(users_df.iterrows()):
        result.add_node(
            'user_' + str(user['user_id']),
            **{
                'type': 'user',
                'nr_of_message': calc_nr_of_messages_per_user(user['user_id'], messages_user_count),
                **serialize_dict(user),
            },
            bipartite=1
        )
        result.add_weighted_edges_from(
            [(
                'user_' + str(user['user_id']),
                'group_' + str(user['group_id']),
                calc_nr_of_messages_per_user(user['user_id'], messages_user_count)
            )],
            group_title=get_group_title(user['group_id'], groups_info_df),
        )

    return result


def generate_one_mode_groups_graph(groups_info_df, users_df, messages_df):
    print('generating group one mode graph')

    tmp_graph = nx.Graph()
    messages_user_count = messages_df['user_id'].value_counts()

    for index, group in groups_info_df.iterrows():
        tmp_graph.add_node(
            'group_' + str(group['group_id']),
            **{
                'type': 'group',
                'subtype': 'chat',
                **serialize_dict(group),
            },
            bipartite=0
        )
    bottom_nodes = []
    for node in tmp_graph.nodes:
        bottom_nodes.append(node)

    for index, user in tqdm(users_df.iterrows()):
        tmp_graph.add_node(
            'user_' + str(user['user_id']),
            **{
                'type': 'user',
                'nr_of_message': calc_nr_of_messages_per_user(user['user_id'], messages_user_count),
                **serialize_dict(user),
            },
            bipartite=1
        )
        tmp_graph.add_weighted_edges_from(
            [(
                'user_' + str(user['user_id']),
                'group_' + str(user['group_id']),
                calc_nr_of_messages_per_user(user['user_id'], messages_user_count)
            )],
            group_title=get_group_title(user['group_id'], groups_info_df),
        )

    result = bipartite.weighted_projected_graph(tmp_graph, bottom_nodes)

    return result


def generate_one_mode_users_graph(groups_info_df, users_df, messages_df):
    print('generating user one mode graph')

    tmp_graph = nx.Graph()
    messages_user_count = messages_df['user_id'].value_counts()

    for index, group in groups_info_df.iterrows():
        if group['megagroup']:
            tmp_graph.add_node(
                'group_' + str(group['group_id']),
                **{
                    'type': 'group',
                    'subtype': 'chat',
                    **serialize_dict(group),
                },
                bipartite=0
            )

    for index, user in tqdm(users_df.iterrows()):
        if (users_df['user_id'] == user['user_id']).sum() > 3:
            tmp_graph.add_node(
                'user_' + str(user['user_id']),
                **{
                    'type': 'user',
                    'nr_of_message': calc_nr_of_messages_per_user(user['user_id'], messages_user_count),
                    **serialize_dict(user),
                },
                bipartite=1
            )
            tmp_graph.add_weighted_edges_from(
                [(
                    'user_' + str(user['user_id']),
                    'group_' + str(user['group_id']),
                    calc_nr_of_messages_per_user(user['user_id'], messages_user_count)
                )],
                group_title=get_group_title(user['group_id'], groups_info_df),
            )

    nodes = []
    for node in tmp_graph.nodes.data():
        if node[1]['type'] == 'user': #and tmp_graph.degree(node[0]) > 1:
            nodes.append(node[0])

    print(len(nodes))

    result = bipartite.weighted_projected_graph(tmp_graph, nodes)

    return result


if __name__ == '__main__':
    groups_info_df, users_df, messages_df = import_dataframes()

    print(groups_info_df.shape)
    print(users_df.shape)
    print(messages_df.shape)

    print('filtering raw feather files')

    # drop columns that are not useful, duplicates or just implicitly in the network
    groups_info_df.drop(['participants_count'], axis=1, inplace=True)
    messages_df.drop(['fwd_from_chat_id'], axis=1, inplace=True)
    users_df.drop(['level_0'], axis=1, inplace=True)

    # fill all NA fields with empty string
    users_df['first_name'] = users_df['first_name'].fillna('')
    users_df['last_name'] = users_df['last_name'].fillna('')
    users_df['username'] = users_df['username'].fillna('')
    messages_df['message_text'] = messages_df['message_text'].fillna('')

    # finally drop message rows that still contain NA values (they cant be used here anyway)
    messages_df.dropna(axis=0, inplace=True)

    if groups_info_df.isnull().sum().sum() > 0 or users_df.isnull().sum().sum() > 0 or messages_df.isnull().sum().sum() > 0:
        raise Exception("There are still Pandas NA values which cant be processed by networkx - ABORTED")

    # TODO more info about nr of lines changed

    print(groups_info_df.shape)
    print(users_df.shape)
    print(messages_df.shape)

    # bipartite_graph = generate_bipartite_graph(groups_info_df, users_df, messages_df)
    # export_bipartite_graph(bipartite_graph)
    # one_mode_groups_graph = generate_one_mode_groups_graph(groups_info_df, users_df, messages_df)
    # export_one_mode_graph(one_mode_groups_graph)
    one_mode_groups_graph = generate_one_mode_users_graph(groups_info_df, users_df, messages_df)
    export_one_mode_graph(one_mode_groups_graph)
