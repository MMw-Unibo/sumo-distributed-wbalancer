import os
import json
import pickle
import shutil
import socket
import random
import sumolib

from pathlib import Path
from log.logger import logger
from multiprocessing import Pipe
from partitioning.partition import SUMOPartition
from partitioning.metis_partitioning import partition_network


from utils import send_big_data_to_socket, receive_msgs


class PartitionManager():
    '''
    This class is responsible for the initial partitioning operations of the simulation.
    It:
        * splits the network into logical partitions using metis algorithm.
        * identifies shared edges between partitions.
        * detected connections between partitions.
        * instantiate the SUMOPartition objects
    '''

    MANAGEMENT_FOLDER = os.path.join(Path(os.path.abspath(__file__)).parent.parent.parent, "management_folder")   
    # Information for viewer
    DEBUG = os.getenv("DEBUG", False)
    PIPE_PATH = os.getenv("NAMED_PIPE", '/tmp/parallelumo')
    WEIGHT_FUNCTION = os.getenv("WEIGHT_FUNCTION", "osm")
    PREFIX = os.getenv("PREFIX", "osm")

    def __init__(self, network_file, route_file, debug_socket = None, spawners = []):
        self.network_file = network_file
        self.route_file = route_file
        self.simulation_folder = os.path.dirname(network_file)

        # Contains information about location of the partitions (communication endpoints (?))
        self.partitions_info = {}
        self.status_channels = {}

        self.current_partitioning = {}
        self.current_partitions = []

        self.state_comm_map = None
        self._state_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.__client_socket = debug_socket
        self.__spawners = spawners

        logger.info("Partition Manager initialized with the following parameters:")
        logger.info("\tNetwork file: {}".format(self.network_file))
        logger.info("\tRoute file: {}".format(self.route_file))
        logger.info("\tManagement folder: {}".format(self.MANAGEMENT_FOLDER))

        # Clean management folder
        self.__clean_management_folder()


    def inject_status(self, part_idx, state):
        logger.info("Injecting status for partition {} to {}: {}".format(part_idx, (self.state_comm_map[part_idx]["address"], self.state_comm_map[part_idx]["port"]), state))
        # self.status_channels[part_idx][1].send(state)
        # self._state_socket.sendto(pickle.dumps(state), (self.state_comm_map[part_idx]["address"], self.state_comm_map[part_idx]["port"]))
        send_big_data_to_socket(self._state_socket, (self.state_comm_map[part_idx]["address"], self.state_comm_map[part_idx]["port"]), state, "man")


    def get_current_partitions(self):
        return self.current_partitions


    def trigger_balancing(self, num_partitions, partitions, method = "random", check = True, simtime = -1, lambda_weight = None, lambda_node_weight = None):
        logger.info("Triggering balancing...")

        # Tell partitions to stop at a specific time
        for partition_id in self.current_partitioning.keys():
            # self.status_channels[partition_id][1].send({"type" : "stop_simtime", "stop_simtime": simtime + 20})
            # self._state_socket.sendto(pickle.dumps({"type" : "stop_simtime", "stop_simtime": simtime + 20}), (self.state_comm_map[partition_id]["address"], self.state_comm_map[partition_id]["port"]))
            send_big_data_to_socket(self._state_socket, (self.state_comm_map[partition_id]["address"], self.state_comm_map[partition_id]["port"]), {"type" : "stop_simtime", "stop_simtime": simtime + 20}, id="man")

        old_partitioning = self.current_partitioning.copy()
        _, _, _, _, partition_info, communication_info, net = self.partition_network(num_partitions, method, check, lambda_weight, lambda_node_weight)

        # Create new partitions as modification of the previous one. So, we minimize the amount of data to be transferred to the partitions.
        partitions_mapping_table = {}
        to_discard = []
        for new_p_idx in partition_info.keys():
            new_partitions_edges = partition_info[new_p_idx]["local_edges"]
            max_similarity_score = -1
            max_similarity_score_idx = -1
            log = []
            for old_p_idx in old_partitioning.keys():
                if old_p_idx in to_discard:
                    continue
                logger.info("Comparing partition {} with partition {}".format(new_p_idx, old_p_idx))
                old_partitions_edges = old_partitioning[old_p_idx]["local_edges"]
                similarity_score = len(set(new_partitions_edges).intersection(set(old_partitions_edges))) / len(new_partitions_edges)
                log.append((new_p_idx, old_p_idx, similarity_score))
                if similarity_score > max_similarity_score:
                    max_similarity_score = similarity_score
                    max_similarity_score_idx = old_p_idx
            
            self.current_partitioning[max_similarity_score_idx] = partition_info[new_p_idx]
            to_discard.append(max_similarity_score_idx)
            partitions_mapping_table[new_p_idx] = max_similarity_score_idx

        # for partition_id in partition_info.keys():
        #     partitions_mapping_table[partition_id] = partition_id

        logger.info("Partitions mapping table: {}".format(partitions_mapping_table))
        # logger.info("Conflict check: {}".format(not len(partitions_mapping_table.values()) == len(partition_info.keys())))

        ## Check which partitions has the old edges
        transfer_structure = {}
        # {
        #     "partition_id": {
        #         "to_p1": [],
        #         "to_p2": [],
        #       ... 
        #     }
        # }
        for partition_id in old_partitioning.keys():
            mapping = partitions_mapping_table[partition_id]
            transfer_structure[partition_id] = {}
            for local_edge in old_partitioning[partition_id]["local_edges"]:
                if local_edge not in self.current_partitioning[partition_id]["local_edges"]:
                    # logger.info("Edge {} not in partition {}".format(local_edge, partition_id))
                    for new_partition_id in self.current_partitioning.keys():
                        if new_partition_id not in transfer_structure[partition_id].keys() and new_partition_id != partition_id:
                            transfer_structure[partition_id][new_partition_id] = []
                        if new_partition_id != partition_id and local_edge in self.current_partitioning[new_partition_id]["local_edges"]:
                            # logger.info("Edge {} is now in partition {}".format(local_edge, new_partition_id))
                            transfer_structure[partition_id][new_partition_id].append(local_edge)


        # logger.info("Transfer structure: {}".format(transfer_structure))
        # logger.info("Old partitioning: {}".format(old_partitioning))

        if self.DEBUG:
            partitions_waliases = {}
            for partition_id in partition_info.keys():
                partitions_waliases[partitions_mapping_table[partition_id]] = partition_info[partition_id]

            logger.info("Sending partition information to server...")
            data = {}
            data["type"] = "partitions_def"
            data["content"] = json.dumps(partitions_waliases)
            # logger.info(partition_info)
            payload = json.dumps(data)
            payload_len = len(bytes(payload, 'utf-8'))
            self.__client_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))
            # logger.info("-- {}".format(partition_info[0]["local_edges"][0]))
            # logger.info("- {}".format(partitions_waliases[partitions_mapping_table[0]]["local_edges"][0]))

        # import time
        # time.sleep(1)
        for partition_id in partition_info.keys():
            mapping = partitions_mapping_table[partition_id]
            partition = partition_info[partition_id]
            new_state = {}
            # new_state["test"] = "inject status test Part. {}".format(partition_id)
            new_state["type"] = "inject_state"
            new_state["local_edges"] = partition["local_edges"]
            new_state["border_edges"] = list(partition["border_edges"].keys())
            new_state["communication_info"] = communication_info
            new_state["border_info"] = {edge: partitions_mapping_table[part] for edge, part in partition["border_edges"].items()}
            new_state["transfer_structure"] = transfer_structure[partition_id]


            # logger.info("Sending new state to partition {} alias of {}".format(mapping, partition_id))
            self.inject_status(mapping, new_state)

    
    def partition_network(self, num_partitions, weight_function, check = True, lambda_weight = None, lambda_node_weight = None):
        '''
        This method creates the partitions of the network and returns the information about the partitions.
        For each partition:
            * Local edges: edges that belong exclusively to the partition
            * Border edges: edges that lead with adjacent partitions
            * Neighbours: info on adjacent partitions

        Each shared edge is assigned to only one partitions among the ones who share it.
        For one partition, the shared edge is considered as local, for the other partition, the shared edge is considered as border edge.
        The partition is chosen based on the destination of its outgoing connections with the adjacent partitions.
        The most connected partition among the sharing ones is chosen.
        Additionally each outgoing edge which lead to the other partition is, additionally, considered as border edge.

        Inputs:
            * num_partitions: number of partitions to create

        Returns:
            * tuple: 
                * partitions: list of SUMOPartition objects
                * actual_numparts: actual number of partitions created
                * edge_sets: list of partitions
               
        '''

        partitions = []
        discarded_edges = []

        # logger.info("Starting partitioning network in {} partitions...".format(num_partitions))
        # logger.info("Executing function with {} {} {} {}".format(os.path.join(self.simulation_folder, "{}.net.xml".format(self.PREFIX)), num_partitions, True, self.MANAGEMENT_FOLDER))
        actual_numparts, edge_sets, edgecuts = partition_network(os.path.join(self.simulation_folder, "{}.net.xml".format(self.PREFIX)), num_partitions, check_connection=True, management_folder=self.MANAGEMENT_FOLDER, weight_functions=weight_function, lambda_weight=lambda_weight, lambda_node_weight=lambda_node_weight)
        # logger.info("Network partitioned successfully...Results:")
        # logger.info("Actual number of partitions: {}".format(actual_numparts))
        # logger.info("Edgecuts: {}".format(edgecuts))
        # logger.info("Edge set: {}".format(edge_sets))

        net = sumolib.net.readNet(os.path.join(self.simulation_folder, "{}.net.xml".format(self.PREFIX)))
        part_shared_edges, flat_shared_edges, edge_based, neighbours = self.__check_shared_edges(edge_sets)

        # Each shared edge will belong local to only one partition.
        # For the other partition, the shared edge will be considered as a border edge (vehicles need to leave).
        partition_info = {}
        communication_info = [[]] * actual_numparts
        for partition_id in range(actual_numparts):
            partition_skeleton = {}
            partition_skeleton["local_edges"] = list(filter(lambda edge: edge not in part_shared_edges[partition_id], edge_sets[partition_id]))
            partition_skeleton["border_edges"] =  {}
            partition_info[partition_id] = partition_skeleton
            communication_info[partition_id] = [None] * actual_numparts

        def assign_edge(edge, explored, partition_info, assigned_edge, edge_based, flat_shared_edges, net):
            adjacent_partitions = []
            destination_partitions = {}
            assigned = []
            edge = net.getEdge(edge)
            outgoing_edges = edge.getOutgoing()
            explored.add(edge.getID())
            assigned.append(edge.getID())
            # logger.info("Exploring edge: {}".format(edge.getID()))
            # logger.info("Starting assigned are {}".format(assigned_edge))

            for outgoing_edge in outgoing_edges:
                # logger.info("Outgoing edge: {}".format(outgoing_edge.getID()))
                if outgoing_edge.getID() in explored:
                    continue

                if outgoing_edge.getID() in flat_shared_edges:
                    if outgoing_edge.getID() not in assigned_edge:
                        # logger.info("Assigning edge to {}".format(outgoing_edge.getID()))
                        assigned_rec = assign_edge(outgoing_edge.getID(), explored, partition_info, assigned_edge, edge_based, flat_shared_edges, net)
                        assigned = assigned + assigned_rec

                    adjacent_partitions.append(assigned_edge[outgoing_edge.getID()])
                    destination_partitions[outgoing_edge.getID()] = assigned_edge[outgoing_edge.getID()]

                else:
                    for partition_id, partition_edges in enumerate(edge_sets):
                        if outgoing_edge.getID() in partition_edges:
                            adjacent_partitions.append(partition_id)
                            destination_partitions[outgoing_edge.getID()] = partition_id

            # logger.info("Adjacent partitions for edge {}: {}".format(edge.getID(), adjacent_partitions))
            sharing_partitions = edge_based[edge.getID()]
            # logger.info("Sharing partitions for edge {}: {}".format(edge.getID(), sharing_partitions))
            adjacent_partitions = list(filter(lambda p: p in sharing_partitions, adjacent_partitions))
            most_connected = self.__get_most_connected_partition(adjacent_partitions) if len(adjacent_partitions) > 0 else sharing_partitions[0]
            # logger.info("Edge: {}, Sharing partition: {}, Most connected: {}".format(edge.getID(), sharing_partitions, most_connected))
            sharing_partitions.remove(most_connected)
            partition_info[most_connected]["local_edges"].append(edge.getID())
            assigned_edge[edge.getID()] = most_connected
            for outgoing_edge in destination_partitions.keys():
                if destination_partitions[outgoing_edge] != most_connected:
                    partition_info[most_connected]["border_edges"][outgoing_edge] = destination_partitions[outgoing_edge]
            for remaining_partition in sharing_partitions:
                partition_info[remaining_partition]["border_edges"][edge.getID()] = most_connected

            # logger.info("Assigned edge: {}".format(assigned_edge))
            # logger.info("Destination partitions: {}".format(destination_partitions))
            # logger.info("---")

            return assigned

        # flat_shared_edges_t = list(filter(lambda s: s in ["1194069485"], flat_shared_edges))
        to_be_explored = list(flat_shared_edges.copy())
        assigned_edge = {}
        i = 0
        # logger.info("To be explored #{}: {}".format(i, to_be_explored))
        while len(to_be_explored) > 0:
            assigned = assign_edge(to_be_explored[0], set(), partition_info, assigned_edge, edge_based, flat_shared_edges, net)
            # logger.info("Final assignment: {}".format(assigned))
            to_be_explored = list(filter(lambda e: e not in assigned, to_be_explored))
            i += 1
            # logger.info("To be explored #{}: {}".format(i, to_be_explored))

        neighbours = {}
        for partition_id in range(actual_numparts):
            neighbours[partition_id] = set()
            neighbours[partition_id].update(set(partition_info[partition_id]["border_edges"].values()))

         # Create a map for the communication pipe to be used by the partitions
        # [[A->A, A->B, A->C, ...], [B->A, B->B, B->C, ...], ...]
        for neighbour_start in neighbours.keys():
            for neighbour_end in neighbours[neighbour_start]:
                if communication_info[neighbour_start][neighbour_end] is None:
                    end_a, end_b = Pipe()

                    communication_info[neighbour_start][neighbour_end] = end_a
                    communication_info[neighbour_end][neighbour_start] = end_b

        neighbour_comm_map = {}
        state_comm_map = {}
        port_start = random.randint(10000, 20000)
        for neighbour in neighbours.keys():
            spawner = self.__spawners[neighbour % len(self.__spawners)]
            neighbour_entry = {}
            neighbour_entry["address"]          = spawner[0]
            neighbour_entry["port"]             = port_start + neighbour
            neighbour_entry["control_port"]     = port_start + neighbour + 1000
            neighbour_entry["state_port"]       = port_start + neighbour + 2000

            state_entry = {}
            state_entry["address"]              = spawner[0]
            state_entry["port"]                 = port_start + neighbour + 2000

            neighbour_comm_map[neighbour] = neighbour_entry
            state_comm_map[neighbour] = state_entry

        if self.state_comm_map is None:
            self.state_comm_map = state_comm_map

        logger.info(neighbour_comm_map) 
        logger.info(state_comm_map)


        # for partition_id in range(actual_numparts):
        #     logger.info("-------- Partition {} information --------".format(partition_id))
        #     logger.info("Edges: {}".format(set(partition_info[partition_id]["local_edges"])))
        #     logger.info("Border edges: {}".format(partition_info[partition_id]["border_edges"]))

        if check:
            checks = []
            checks.append(self.__check_1(partition_info, flat_shared_edges, edge_based, neighbours, discarded_edges))
            checks.append(self.__check_2(partition_info, flat_shared_edges, edge_based, neighbours, discarded_edges))
            checks.append(self.__check_3(partition_info, flat_shared_edges, edge_based, neighbours, discarded_edges))
            checks.append(self.__check_4(partition_info, flat_shared_edges, edge_based, neighbours, discarded_edges))

            logger.info("Discarded edges: {}".format(discarded_edges))     
            if not all(checks):
                raise Exception("Partitioning checks failed")


        # Upadte current partitioning
        for partition_id in range(actual_numparts):
            self.current_partitioning[partition_id] = partition_info[partition_id]
            
        # if self.DEBUG:
        #     logger.info("Sending partition information to server...")
        #     data = {}
        #     data["type"] = "partitions_def"
        #     data["content"] = json.dumps(partition_info)
        #     # logger.info(partition_info)
        #     payload = json.dumps(data)
        #     payload_len = len(bytes(payload, 'utf-8'))
        #     self.__client_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))


        return partitions, actual_numparts, edge_sets, discarded_edges, partition_info, neighbour_comm_map, net


    def create_partitions(self, num_partitions, check = True, lambda_weight = None):
        partitions = []
        partitions, actual_numparts, edge_sets, discarded_edges, partition_info, communication_info, net = self.partition_network(num_partitions, self.WEIGHT_FUNCTION, check = check, lambda_weight=lambda_weight)

        for partition_id in partition_info.keys():
            partition = partition_info[partition_id]
            # print(partition["border_edges"])
            self.status_channels[partition_id] = Pipe()
            # sumopartition = SUMOPartition(partition_id, partition["local_edges"], list(partition["border_edges"].keys()), communication_info, partition["border_edges"], self.simulation_folder, net, self.route_file, self.status_channels[partition_id][0], ("0.0.0.0", 2233))
            # partitions.append(sumopartition)

        self.current_partitions = partitions

        if self.DEBUG:
            logger.info("Sending partition information to server...")
            data = {}
            data["type"] = "partitions_def"
            data["content"] = json.dumps(partition_info)
            # logger.info(partition_info)
            payload = json.dumps(data)
            payload_len = len(bytes(payload, 'utf-8'))
            self.__client_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))

        return partition_info, communication_info, actual_numparts, edge_sets, discarded_edges


    def __check_1(self, partition_info, flat_shared_edges, edge_based, neighbours, discarded_edges):
        '''
        Check if all the shared edges are managed by the partitions.
        '''
        flat_border_edges = set()
        for partition in partition_info.values():
            for border_edge in partition["border_edges"].keys():
                flat_border_edges.add(border_edge)

        check_1 = all(item in flat_border_edges or item in discarded_edges for item in flat_shared_edges)
        if check_1:
            logger.info("Check #1 (coverage): OK")
        else:
            logger.info("Check #1 (coverage): FAILED")
            logger.info("Not covered edges: {}".format(set(flat_shared_edges) - set(flat_border_edges)))

        return check_1


    def __check_2(self, partition_info, flat_shared_edges, edge_based, neighbours, discarded_edges):
        '''
        Check if each shared edge is local by only one partition and border to the other.
        '''
        check_2 = True
        for shared_edge in flat_shared_edges:
            if not check_2:
                break

            partition_involved_wlocal = []
            partition_involved_wborder = []
            for partition in partition_info.values():
                if shared_edge in partition["local_edges"]:
                    partition_involved_wlocal.append(partition)
                if shared_edge in partition["border_edges"].keys():
                    partition_involved_wborder.append(partition)

            ground_truth = edge_based[shared_edge]
            if (
                len(partition_involved_wlocal) == 1 and
                len(partition_involved_wborder) == 1 and
                len(partition_involved_wlocal) + len(partition_involved_wborder) == len(ground_truth) and
                all(partition_id in partition_involved_wlocal or partition_id in partition_involved_wborder for partition_id in ground_truth)
                ):
                check_2 = False
            
        if check_2:
            logger.info("Check #2 (partition correctness): OK")
        else:
            logger.info("Check #2 (partition correctness): FAILED")

        return check_2

    
    def __check_3(self, partition_info, flat_shared_edges, edge_based, neighbours, discarded_edges):
        '''
        Check if the neighbours are correct for each partition.
        '''
        check_3 = True
        errors = []
        for partition_id in partition_info.keys():
            partition = partition_info[partition_id]
            neighbours_check = set()
            for border in partition["border_edges"].keys():
                neighbours_check.add(partition["border_edges"][border])

            # Some neighbours could be not preserved even if correct.
            # This is beacause initial neighbours simply give information about shared edges without considering the road topology.
            # This is not the case for neighbouts_check, which is the actual neighbours reacheable from the partition. 
            if not neighbours_check.issubset(neighbours[partition_id]):
                check_3 = False
                errors.append([partition_id, neighbours_check, neighbours[partition_id]])

        if check_3:
            logger.info("Check #3 (neighbours): OK")
        else:
            logger.info("Check #3 (neighbours): FAILED")
            logger.info("Errors: {}".format(errors))

        return check_3
    

    def __check_4(self, partition_info, flat_shared_edges, edge_based, neighbours, discarded_edges):
        '''
        Check if there are no overlapping edges between local and border edges.
        '''
        check_4 = True
        errors = []
        for partition_id in partition_info.keys():
            if set(partition_info[partition_id]["local_edges"]).intersection(partition_info[partition_id]["border_edges"].keys()):
                check_4 = False
                errors.append({"partition_id": partition_id, "overlap": set(partition_info[partition_id]["local_edges"]).intersection(partition_info[partition_id]["border_edges"].keys())})

        if check_4:
            logger.info("Check #4 (overlapping): OK")
        else:
            logger.info("Check #4 (overlapping): FAILED")
            logger.info("Errors: {}".format(errors))

        return check_4


    def __get_most_connected_partition(self, adjacent_partitions):
        '''
        This method returns the most connected partition among the adjacent ones.
        Inputs:
            * adjacent_partitions: list of adjacent partitions ids - [1,2,2,3]

        Returns:
            * int: most connected partition id
        '''
        return max(adjacent_partitions, key = adjacent_partitions.count)

    
    def __get_adjacent_partitions(self, shared_edge, edge_sets, flat_shared_edges, net, explored = set()):
        '''
        This method find the list of adjacent partitions for a specific shared edge.
        Inputs:
            * shared_edge: edge id
            * edge_sets: list of partitions
            * flat_shared_edges: set of shared edges
            * net: sumolib net object
            * explored: set of explored edges

        Returns:
            * tuple:
                * adjacent_partitions: list of adjacent partitions ids
                * destination_partitions: dictionary with the destination partition for each outgoing edge
        '''

        adjacent_partitions = []
        destination_partitions = {}
        edge = net.getEdge(shared_edge)
        outgoing_edges = edge.getOutgoing()
        explored.add(shared_edge)

        for outgoing_edge in outgoing_edges:
            logger.info("Outgoing edge: {}".format(outgoing_edge.getID()))
            if outgoing_edge.getID() in flat_shared_edges:
                logger.info("here1")
                if outgoing_edge.getID() not in explored:
                    adj, _ = self.__get_adjacent_partitions(outgoing_edge.getID(), edge_sets, flat_shared_edges, net, explored)
                    most_connected = self.__get_most_connected_partition(adj)
                    adjacent_partitions.append(most_connected)
                    destination_partitions[outgoing_edge.getID()] = most_connected

            else:
                logger.info("here2")
                for partition_id, partition_edges in enumerate(edge_sets):
                    if outgoing_edge.getID() in partition_edges:
                        adjacent_partitions.append(partition_id)
                        destination_partitions[outgoing_edge.getID()] = partition_id

        return adjacent_partitions, destination_partitions


    def __clean_management_folder(self):
        logger.info("Cleaning management folder ...")
        for file in os.listdir(self.MANAGEMENT_FOLDER):
            if os.path.isdir(os.path.join(self.MANAGEMENT_FOLDER, file)):
                shutil.rmtree(os.path.join(self.MANAGEMENT_FOLDER, file))
            else:
                os.remove(os.path.join(self.MANAGEMENT_FOLDER, file))


    def __check_shared_edges(self, partitions):
        '''
        Check the edges shared among partitions, these are the border edges to consider in the partitioning.
        Args:
            partitions: list of lists, each list contains the edges of a partition.

        Returns:
            shared_edges: Dictionary with the shared edges for each partition
            {
                "partition1": [edge1, edge2, ...],
                "partition2": [edge3, edge4, ...],
                ...
            }
            raw_shared_edges: Set of edges shared among partitions
        '''

        shared_edges = {}
        edge_based_sharing = {}
        raw_shared_edges = set()
        neighbours = {}

        for i in range(len(partitions)):
            for element in partitions[i]:
                for j in range(len(partitions)):
                    if j > i and element in partitions[j]:
                        if i not in shared_edges:
                            shared_edges[i] = []
                        if j not in shared_edges:
                            shared_edges[j] = []
                        if i not in neighbours:
                            neighbours[i] = set()
                        if j not in neighbours:
                            neighbours[j] = set()

                        edge_based_sharing[element] = [i, j]
                        raw_shared_edges.add(element)
                        shared_edges[i].append(element)
                        shared_edges[j].append(element)
                        neighbours[i].add(j)
                        neighbours[j].add(i)


        # logger.info("Shared edges: {}".format(raw_shared_edges))
        # logger.info("Shared edges per partitions: {}".format(shared_edges))
        # logger.info("Edge-based sharing: {}".format(edge_based_sharing))
        # logger.info("Neighbours: {}".format(neighbours))

        return shared_edges, raw_shared_edges, edge_based_sharing, neighbours