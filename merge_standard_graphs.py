# simple script to merge seperate networkx graphs with correct attributes
from os import listdir, path
from fnmatch import fnmatch
from tqdm import tqdm
from datetime import datetime
import networkx as nx


def import_graphs():
    src_dir = 'input'
    graphs_list = []
    print('reading graphs from ' + path.abspath(src_dir))

    # iterate over gexf files and import graphs
    for file in tqdm(listdir(src_dir)):
        if fnmatch(file, '*.gexf'):
            graphs_list.append(
                nx.read_gexf(path.join('input', file))
            )

    print(f'read in {len(graphs_list)} graphs')
    return graphs_list


# FIXME abysmal efficiency O(n^3) and style, but plain and easy
def merge_graphs(graphs_list):
    result = nx.Graph()

    for graph in tqdm(graphs_list):
        # get necessary data
        result_nodes = list(result.nodes.data())
        nodes = list(graph.nodes.data())

        # add edges
        for edge in graph.edges.data():
            result.add_edge(edge[0], edge[1], group_title=edge[2]['group_title'], nr_of_messages=edge[2]['nr_of_messages'])

        # merge all nodes into result graph
        for node in nodes:
            # handle user node
            if node[1]['type'] == 'user':
                duplicate = False
                for result_node in result_nodes:
                    duplicate = False
                    if node[1] == result_node[1]:
                        result_node[1]['nr_of_messages'] += node[1]['nr_of_messages']
                        duplicate = True
                        break
                if not duplicate:
                    result.add_node(node[0], **node[1])

            # handle group node
            elif node[1]['type'] == 'group':
                duplicate = False
                for result_node in result_nodes:
                    duplicate = False
                    if node[1] == result_node[1]:
                        duplicate = True
                        break
                if not duplicate:
                    result.add_node(node[0], **node[1])

    return result


def export_graph(graph):
    timestamp = datetime.now().strftime("%Y-%m-%d-%Y%H%M%S")
    filename = 'MERGED_GRAPH__' + timestamp + '__.gexf'
    nx.write_gexf(graph, path.join('output', filename))


if __name__ == '__main__':
    graphs_list = import_graphs()
    merged_graph = merge_graphs(graphs_list)
    export_graph(merged_graph)
