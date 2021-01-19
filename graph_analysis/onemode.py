from typing import Dict
import networkx as nx
import copy


G = nx.read_gexf('ONE_MODE_GRAPH__2021-01-19-2021194124__.gexf')

nodes = copy.deepcopy(G.nodes(data=True))

for id, n in nodes:
    if not n['megagroup']:
        G.remove_node(id)

nr = nx.algorithms.components.number_connected_components(G)

comps = [
    c for c in
    nx.algorithms.components.connected_components(G)
]

biggest_comp = G.subgraph(comps[0])


# General graph metrics.

nr_nodes = biggest_comp.number_of_nodes()
nr_edges = biggest_comp.number_of_edges()

density = nr_edges / (nr_nodes * (nr_nodes - 1))
print(f'Graph density: {density}')

#avg_conn = nx.algorithms.connectivity.connectivity.average_node_connectivity(biggest_comp)
#print(f'Average connectivity of graph: {avg_conn}')

diam = nx.networkx.algorithms.distance_measures.diameter(biggest_comp)
print(f'Diameter of graph: {diam}')

def apply_metric(result: Dict, name: str):
    best_result = list(result.values())[0]
    metric_winner = G.nodes[list(result.keys())[0]]
    print(f'{name} winner: {metric_winner["group_id"]} '
          f'{metric_winner["title"]}'
          f'  value: {best_result}')


# How efficiently can all nodes be reached from a node?
apply_metric(
    nx.algorithms.closeness_centrality(biggest_comp),
    'Closeness centrality'
)

# How much flow goes through a node?
apply_metric(
    nx.algorithms.centrality.current_flow_betweenness_centrality(
        biggest_comp, weight='weight'
    ),
    'Betweenness flow'
)
