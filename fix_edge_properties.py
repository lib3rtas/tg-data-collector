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


def export_graph(graph):
    timestamp = datetime.now().strftime("%Y-%m-%d-%Y%H%M%S")
    filename = 'FIXED_GRAPH__' + timestamp + '__.gexf'
    nx.write_gexf(graph, path.join('output', filename))


def fix_edge_weight(graph):
    for edge in graph.edges.data():
        edge[2]['weight'] += 1
    return graph


if __name__ == '__main__':
    graphs_list = import_graphs()
    for graph in graphs_list:
        new_graph = fix_edge_weight(graph)
        export_graph(new_graph)
