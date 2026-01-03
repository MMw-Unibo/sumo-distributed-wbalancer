import os
import json
import pickle
import select
import socket
import itertools

from log.logger import logger
from multiprocessing import Queue

from utils import send_big_data_to_socket, receive_msgs

CHECK = os.getenv("CHECK", True)

class LoadBalancer():

    def __init__(self, partition_manager, num_partitions, threshold, simulation_window, socket_info, debug_socket = None, stats_socket = None):
        self.partition_manager = partition_manager
        self.threshold = threshold
        self.simulation_window = simulation_window

        self.num_partitions = num_partitions
        self.monitoring_structure = {}
        for i in range(num_partitions):
            self.monitoring_structure[i] = {
                "step" : -1,
                "load" : -1,
                "edges_load" : {}    
            }

        self.queue = Queue(100)
        self.trigger_enabled = True
        self.last_trigger = 0
        self.edges_load = {}

        ## Setup socket to receiveload info from partitions
        socket_address = socket_info[0]
        socket_port = socket_info[1]
        logger.info("Configuring UDP server socket on port {}".format(socket_port))
        self.__server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__server_socket.bind((socket_address, socket_port))
        logger.info("Server UDP socket binded on port {}".format(socket_port))
        self.debug_socket = debug_socket
        self.stats_socket = stats_socket

        self.max_repartitioning = 3
        self.n_repartitioning = 0
        self.max_skip = 1
        self.n_skip = 0
        
        self.monitor_window = 0.05

        self.stats = {}
        self.stats["monitoring_steps"] = []
        self.stats["triggered_steps"] = []

    def change_threshold(self, new_threshold):
        self.threshold = new_threshold


    def get_queue(self):
        return self.queue
    

    def get_stats(self):
        return self.stats
    

    def start_monitoring(self):
        self.n_repartitioning = 0
        while True:
            load_msgs = receive_msgs(self.__server_socket, 1)
            if load_msgs != []:
                partition_data = load_msgs[0]

            # readable, _, _ = select.select([self.__server_socket], [], [], 0)
            # if self.__server_socket in readable:
                # partition_data = self.__server_socket.recv(1024 * 60)
                # partition_data = pickle.loads(partition_data)
                _type = partition_data["type"]
                _data = partition_data["data"]

                if _data == "stop":
                    self.stats_socket.send(self.stats)
                    break

                self.edges_load.update(_data["edges_load"])

                # logger.info(f"Received partition data:\n\tTYPE: {_type}\n\tDATA: {_data}")

                if _type == "update":
                    self.monitoring_structure[_data["id"]]["load"] = _data["load"]
                    self.monitoring_structure[_data["id"]]["step"] = _data["step"]
                    self.monitoring_structure[_data["id"]]["edges_load"] = _data["edges_load"]

                self.__check_trigger()

    def __check_trigger(self):
        # Check if all partition are in the same step and have sent their load
        load_sum = 0
        triggered = False

        for i in range(self.num_partitions - 1):
            if self.monitoring_structure[i]["step"] == -1 or self.monitoring_structure[i]["load"] == -1 or self.monitoring_structure[i]["step"] != self.monitoring_structure[i + 1]["step"]:
                return
            
        # Check if the current simulation time is within the monitor window
        logger.info(f"Checking {self.monitoring_structure[0]['step']} - {self.simulation_window * self.monitor_window} - {self.simulation_window * (1 - self.monitor_window)}")
        if self.monitoring_structure[0]["step"] <= self.simulation_window * self.monitor_window or self.monitoring_structure[0]["step"] >= self.simulation_window * (1 - self.monitor_window):
            return
        
        self.stats["monitoring_steps"].append(self.monitoring_structure[0]["step"])
            
        if self.n_repartitioning < self.max_repartitioning:
            if self.n_skip == 0:
                self.trigger_enabled = True
            elif self.trigger_enabled == False:
                self.n_skip = (self.n_skip + 1) % self.max_skip


        for i in range(self.num_partitions):
            load = self.monitoring_structure[i]["load"]
            load_sum += load

        for i in range(self.num_partitions):
            load_contrib = self.monitoring_structure[i]["load"] / load_sum
            self.monitoring_structure[i]["load_contrib"] = load_contrib

        if load_sum == 0:
            return

        # Create each possibile combination between partitions
        distinct_partitions = list(self.monitoring_structure.keys())
        combinations = list(itertools.combinations(distinct_partitions, 2))

        # Check the contribution of each node towards the total load
        for i in combinations:
            first = i[0]
            second = i[1]
            diff = abs(self.monitoring_structure[first]["load_contrib"] - self.monitoring_structure[second]["load_contrib"])
            if diff >= self.threshold and self.trigger_enabled:
                # logger.info(f"MONITORING REPORT: Triggering balancing at {self.monitoring_structure[first]['step']}")
                # logger.info(f"edges_load: {self.edges_load}")
                triggered = True
                self.trigger_enabled = False
                self.last_trigger = self.monitoring_structure[first]["step"]
                self.partition_manager.trigger_balancing(self.num_partitions, self.partition_manager.get_current_partitions(), check=CHECK, simtime = self.monitoring_structure[0]["step"], lambda_weight=self.assign_weight_to_edge, lambda_node_weight=self.assign_weight_to_node)
                self.n_repartitioning += 1
                self.stats["triggered_steps"].append(self.monitoring_structure[first]["step"])
                break

        debug_msg = "MONITORING REPORT:\n\tTHRESHOLD: {}\n\t".format(self.threshold)
        for i in range(self.num_partitions):
            debug_msg += f"Partition {i}: {self.monitoring_structure[i]['load_contrib']:.2f}\n\t"
        for i in combinations:
            first = i[0]
            second = i[1]
            diff = abs(self.monitoring_structure[first]["load_contrib"] - self.monitoring_structure[second]["load_contrib"])
            debug_msg += f"{first} - {second}): {diff:.2f} - Step: {self.monitoring_structure[first]['step']}\n\t"
        debug_msg += f"Max repartitioning: {self.max_repartitioning} - Current repartitioning: {self.n_repartitioning} - Max skip: {self.max_skip} - Current skip: {self.n_skip} - Trigger enabled: {self.trigger_enabled}\n\t"
        debug_msg += f"Migration triggered: {triggered}\n\t"
        logger.info(debug_msg)

        # Send data to debugger
        if self.debug_socket:
            data = {}
            data_content = {}
            data_content["message"] = debug_msg
            data_content["trigger"] = triggered
            
            data["type"] = "load_balancer"
            data["content"] = data_content
            payload = json.dumps(data)
            payload_len = len(bytes(payload, 'utf-8'))
            self.debug_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))

        

    def assign_weight_to_edge(self, edge):
        val = self.edges_load[edge.getID()] if edge.getID() in self.edges_load else 0

        return val + 5 * 10
    

    def assign_weight_to_node(self, node):
        incoming = node.getIncoming()
        outgoing = node.getOutgoing()

        incoming_load = sum([self.edges_load[edge.getID()] for edge in incoming if edge.getID() in self.edges_load]) if len(incoming) > 0 else 0
        outgoing_load = sum([self.edges_load[edge.getID()] for edge in outgoing if edge.getID() in self.edges_load]) if len(outgoing) > 0 else 0
        
        return (incoming_load + outgoing_load) * 10