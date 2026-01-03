"""
Convert SUMO network into proper format for METIS input, partition with METIS,
and write in one file per partition the SUMO network edges of that partition.
"""

import os
import sys
import argparse
import xml.etree.ElementTree as ET

from collections import defaultdict
from pymetis import Options, part_graph
from log.logger import logger

NO_WEIGHT = "none"

WEIGHT_ROUTE_NUM = "route-num"
WEIGHT_OSM = "osm"
WEIGHT_RANDOM = "random"
WEIGHT_FROMEDGE = "fromedge"
WEIGHT_VEHICLE_LOAD = "vehicle-load"

NODE_WEIGHT_CONNECTIONS = "connections"
NODE_WEIGHT_CONNECTIONS_EXP = "connexp"

weight_funs = [
    WEIGHT_ROUTE_NUM,
    WEIGHT_OSM,
    WEIGHT_RANDOM,
    WEIGHT_FROMEDGE,
]

node_weight_funs = [
    NODE_WEIGHT_CONNECTIONS,
    NODE_WEIGHT_CONNECTIONS_EXP,
]

parser = argparse.ArgumentParser()
parser.add_argument('netfile', help="SUMO network file to partition (in .net.xml format)")
parser.add_argument('numparts', type=int, help="Amount of partitions to create. Might end up being lower in the output in small graphs.")
parser.add_argument('--check-connection', action='store_true', help="Check if the graph is connected")

if 'SUMO_HOME' in os.environ:
    SUMO_HOME = os.environ['SUMO_HOME']
else:
    SUMO_HOME = "/usr/share/sumo"
    # sys.exit("please declare environment variable 'SUMO_HOME'")

tools = os.path.join(SUMO_HOME, 'tools')
logger.info("SUMO_HOME: {}".format(SUMO_HOME))
sys.path.append(os.path.join(tools))
from sumolib.net import readNet
from sumolib.net.edge import Edge
from sumolib.net.node import Node
import sumolib

def remove_empty_parts(partitions: list[int], warn: bool = True):
    """From a list assigning a partition to each index, remove 
    missing indices (meaning, shift later ones back so they are a
    continuous range).
    METIS might sometimes return empty partitions in small graphs.
    """

    part_values = set(partitions)
    min_val = 0
    max_val = max(part_values)
    missing_values = set(range(min_val, max_val + 1)) - part_values
    if warn and len(missing_values) > 0:
        print(f"[WARN] Empty partitions: <{', '.join([str(x) for x in missing_values])}>", file=sys.stderr)
    offsets = {v: len([x for x in missing_values if x < v]) for v in part_values}

    return [x - offsets[x] for x in partitions]

def partition_network(
    netfile: str, numparts: int,
    weight_functions: list[str]|str = WEIGHT_ROUTE_NUM,
    node_weight_functions: list[str]|str = None,
    routefile: str = None,
    output_weights_file: str = None,
    check_connection: bool = False,
    management_folder: str = None,
    lambda_weight = None,
    lambda_node_weight = None,
):
    """Partition a SUMO network using METIS

    Args:
        netfile (str): Path to the SUMO network
        numparts (int): Target amount of parts (might have less in output)
        weight_functions (list[str] | str, optional): Zero or more of various weightings to use
            for edges, current options: osm, route-num.
            route-num requires a routefile to be set.
            
            The weight of an edge is used to calculcate the edge cuts optimization target: 
            the algorithm strives to minimize the edge cut, calculated as the sum of the weights
            of edges connecting nodes of different partitions.
        node_weight_functions (list[str] | str, optional): One or more weightings to use for nodes, 
            by default uses None. 
            
            The weight of a node is used as a criteria for partitioning: the algorithm tries to
            create partitions with the same total weight of contained nodes.
        routefile (str, optional): path to route file to use for weighting.
        output_weights_file (str, optional): if set, path to write a file with the weight of each node, in a json-dict format.
        check_connection (bool, optional): check if the graph is connected
        management_folder (str, optional): path to the management folder, by default None
    """
    if weight_functions is None:
        weight_functions = []
    elif type(weight_functions) is str:
        weight_functions = [weight_functions]
    weight_functions = set(weight_functions)
    if node_weight_functions is None:
        node_weight_functions = []
    elif type(node_weight_functions) is str:
        node_weight_functions = [node_weight_functions]
    node_weight_functions = set(node_weight_functions)
    
    print(f"Weight functions used: {', '.join(weight_functions)}")
    print(f"Node weight functions used: {', '.join(node_weight_functions)}")
    print(f"Lambda weight: {lambda_weight}")

    if routefile is None and WEIGHT_ROUTE_NUM in weight_functions:
        weight_functions.remove(WEIGHT_ROUTE_NUM)
        print(f"[WARN] Needs a routefile specified to use weight option {WEIGHT_ROUTE_NUM}", file=sys.stderr)

    net: sumolib.net.Net = readNet(netfile)
    nodes: list[Node] = net.getNodes()


    edge_weights = {}
    numNodes = len(nodes)

    # logger.info("Nodes: {}".format(nodes))
    # logger.info("Number of nodes: {}".format(numNodes))

    if routefile is not None:
        routes_by_edge = _count_routes_in_edges(routefile)

    def get_edge_weight(edge: Edge) -> int:
        nonlocal routes_by_edge, weight_functions

        wgt = 1
        if lambda_weight is not None:
            wgt += lambda_weight(edge)
        else:
            if WEIGHT_OSM in weight_functions:
                wgt += _get_edge_weight_osm(edge)
            if WEIGHT_ROUTE_NUM in weight_functions:
                wgt += _get_edge_weight_routecount(edge, routes_by_edge)
            if WEIGHT_RANDOM in weight_functions:
                wgt += _get_random_weight(edge)
            if WEIGHT_FROMEDGE in weight_functions:
                wgt += _get_weight_from_edge(edge)
            if WEIGHT_VEHICLE_LOAD in weight_functions:
                wgt += _get_vehicle_load_for_edge(edge)
        return int(wgt * 100)
    
    def get_node_weight(node: Node) -> int:
        nonlocal routes_by_edge, node_weight_functions
        
        wgt = 1
        if lambda_node_weight is not None:
            wgt += lambda_node_weight(node)
        else:
            if NODE_WEIGHT_CONNECTIONS in node_weight_functions:
                wgt += len(node.getConnections())
            if NODE_WEIGHT_CONNECTIONS_EXP in node_weight_functions:
                wgt += len(node.getConnections()) ** 2
        return int(wgt * 100)
    
        # return 1
    
    def get_node_data(node: Node) -> tuple[list[int],list[int]]:
        """For every node in the graph, return 
        its neighbor nodes, the weight of the edges
        to every neighbor node, and its weight
        """
        nonlocal edge_weights

        # Consider both outgoing and ingoing edges for every node, 
        # to simplify the METIS algorithm
        # in case of both directions edge, average the node weights
        weights = {}

        incoming: list[Edge] = node.getIncoming()
        for edge in incoming:
            other_node = _get_other_node(edge, node)
            wgt = get_edge_weight(edge)
            weights[other_node] = wgt

        outgoing: list[Edge] = node.getOutgoing()
        for edge in outgoing:
            other_node = _get_other_node(edge, node)
            wgt = get_edge_weight(edge)
            edge_weights[edge.getID()] = wgt
            if other_node in weights:
                weights[other_node] = int((weights[other_node] + wgt) * 0.5)
            else:
                weights[other_node] = wgt

        nodes = list(weights.keys())
        # Make sure to keep the same order
        return nodes, [weights[node] for node in nodes], get_node_weight(node)

    # for every node i, list of its neighbors indices
    neighbors = [None] * numNodes
    neighbor_edge_wgts = [None] * numNodes
    node_weights = [None] * numNodes
    num_neighs_total = 0
    nodes_dict = {node: i for i, node in enumerate(nodes)}
    
    for i, node in enumerate(nodes):
        neighs, eweights, weight = get_node_data(node)
        neighbor_edge_wgts[i] = eweights
        neighbors[i] = [nodes_dict[nnode] for nnode in neighs]
        node_weights[i] = weight
        num_neighs_total += len(neighbors[i])

    if check_connection:
        try:
            is_connected = _is_connected(neighbors)
        except RecursionError:
            print("Graph too big for connection check, will assume it is", file=sys.stderr)
            is_connected = True
        print(f"Graph connected: {is_connected}")
    else:
        print("Check connection disabled")
        is_connected = True

    # logger.info("Neighbors: {}".format(neighbors))
    # logger.info("Neighbor edge weights: {}".format(neighbor_edge_wgts))
    # logger.info("Num_neighs_total: {}".format(num_neighs_total))

    xadj, adjncy, eweights = _neighbors_to_xadj(neighbors, num_neighs_total, neighbor_edge_wgts)

    # logger.info("xadj {}".format(xadj))
    # logger.info("adjncy {}".format(adjncy))

    # execute metis
    # See metis docs [here](https://github.com/KarypisLab/METIS/blob/master/manual/manual.pdf)
    # (page 13 at time of writing) for parameter explanations
    
    # In particular, weights (with ncon=1, aka one weight per vertex):
    # The weights of the vertices (if any) are stored in an additional array called vwgt. [...]. Note that if
    # each vertex has only a single weight, then vwgt will contain n elements, and vwgt[i] will store the weight of the
    # ith vertex. The vertex-weights must be integers greater or equal to zero. [...]
    # The weights of the edges (if any) are stored in an additional array called adjwgt. This array contains 2m elements,
    # and the weight of edge adjncy[j] is stored at location adjwgt[j]. The edge-weights must be integers greater
    # than zero.
    
    metis_opts = Options()
    metis_opts.contig = True
    try:
        # edgecuts: amount of edges lying between partitions, that were cut
        edgecuts, parts = part_graph(
            numparts, xadj=xadj, adjncy=adjncy,
            eweights=eweights,
            vweights=node_weights,
            recursive=False,
            options=metis_opts,
        )
    except:
        print("[ERR] Metis error: tried to partition non-connected graph as contiguous, will return non contiguous partitions", file=sys.stderr)
        metis_opts.contig = False
        edgecuts, parts = part_graph(
            numparts, xadj=xadj, adjncy=adjncy,
            eweights=eweights,
            vweights=node_weights,
            recursive=False,
            options=metis_opts,
        )

    # logger.info("Edgecuts: {}".format(edgecuts))
    # logger.info("Parts: {}".format(parts))

    # parts is a list like this: parts[nodeid] = partid
    # might have empty partitions with metis in small graphs, so remove them
    parts = remove_empty_parts(parts)
    actual_numparts = len(set(parts))

    # get edges corresponding to partitions
    edges = [set() for _ in range(actual_numparts)]
    for node, partition in zip(nodes, parts):
        nodeEdges = node.getIncoming() + node.getOutgoing()
        # logger.info("Analyzing {}, adding incoming {} and outcoming{}".format(node.getID(), [e.getID() for e in node.getIncoming()], [e.getID() for e in node.getOutgoing()]))
        for e in nodeEdges:
            if e.getID() not in edges[partition]:
                edges[partition].add(e.getID())

    # print("edges", edges)

    with open(os.path.join(management_folder, "numParts.txt"), 'w', encoding='utf-8') as f:
        f.write(f"{actual_numparts}")

    # write edges of partitions in separate files
    for i, edge_set in enumerate(edges):
        with open(os.path.join(management_folder, f"edgesPart{i}.txt"), 'w', encoding='utf-8') as f:
            for eID in edge_set:
                print(eID, file=f)
    
    if output_weights_file is not None:
        with open(output_weights_file, 'w', encoding='utf-8') as f:
            print('{', file=f)
            for i, eid in enumerate(edge_weights):
                wgt = edge_weights[eid]
                print(f'    "{eid}": {wgt}', end="", file=f)
                if i < len(edge_weights.keys()) - 1:
                    print(',', file=f)
                else:
                    print('', file=f)
            print('}', file=f)

    return (actual_numparts, edges, edgecuts)

def _is_connected(neighbors):
    visited = defaultdict(bool)

    def dfs(id):
        visited[id] = True
        for neighbor in neighbors[id]:
            if not visited[neighbor]:
                dfs(neighbor)

    if not neighbors or len(neighbors) == 0:
        return True
    
    dfs(0)

    return all(visited.values())

def _get_other_node(edge, node) -> Node:
    to_node: Node = edge.getToNode()
    from_node: Node = edge.getFromNode()
    return to_node if to_node.getID() != node.getID() else from_node

OSM_HIGHWAY_WEIGHTS = {
    'motorway': 15,
    'trunk': 15,
    'primary': 10,
    'secondary': 5,
    'tertiary': 1.5,
    'unclassified': 1.5,
    'residential': 1,
}

def _get_edge_weight_osm(edge: Edge):
    edge_type = edge.getType()
    split = edge_type.split('.')
    if len(split) > 1 and split[0] == 'highway' and split[1] in OSM_HIGHWAY_WEIGHTS:
        return OSM_HIGHWAY_WEIGHTS[split[1]]
    return 1

def _count_routes_in_edges(routefile: str) -> dict[str: int]:
    tree = ET.parse(routefile)
    root = tree.getroot()
    #<route edges="201998907#0 30618699 26256819#0 -82715344 -23276777#4 -23276777#1 65292474#0 65292474#2 232512842#0 325434952 -217845955#1 />"
    routes = root.findall("route")
    out = {}
    for route in routes:
        edges = [x for x in route.attrib["edges"].split(" ") if x != ""]
        for edge in edges:
            out[edge] = out.get(edge, 0) + 1
    return out


def _get_edge_weight_routecount(edge: Edge, routes_by_edge: dict):
    edge_id = edge.getID()
    return (routes_by_edge.get(edge_id, -5) + 5) * 10

def _get_weight_from_edge(edge: Edge):
    return int(edge.getID()[-1]) * int(edge.getID()[3])

def _get_random_weight(edge: Edge):
    import random
    return random.randint(1, 10 ** 3)

def _get_vehicle_load_for_edge(edge: Edge):

    return 0

# convert from neighbor list to C-like metis format
def _neighbors_to_xadj(neighbors: list, num_edges: int, *other_lists: list) -> tuple[list, list]:
    # The adjacency structure of the graph is stored as follows: The
    # adjacency list of vertex *i* is stored in array *adjncy* starting at
    # index ``xadj[i]`` and ending at (but not including) index ``xadj[i +
    # 1]``. That is, for each vertex i, its adjacency list is stored in
    # consecutive locations in the array *adjncy*, and the array *xadj* is
    # used to point to where it begins and where it ends.

    xadj = [None] * (len(neighbors) + 1)
    adjncy = [None] * num_edges
    other_lists_out = [[None] * num_edges] * len(other_lists)

    adj_idx = 0
    for id, neigh_elements in enumerate(zip(neighbors, *other_lists)):
        xadj[id] = adj_idx
        neighs = neigh_elements[0]
        for i, neigh in enumerate(neighs):
            adjncy[adj_idx] = neigh
            if len(neigh_elements) > 1:
                for j, neigh_list in enumerate(neigh_elements[1:]):
                    other_lists_out[j][adj_idx] = neigh_list[i]
            adj_idx += 1
        xadj[id+1] = adj_idx # mostly redundant, but covers last one

    return xadj, adjncy, *other_lists_out