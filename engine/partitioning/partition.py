import os
import ast
import json
import time
import traci
import socket
import random
import select
import pickle
import sumolib
import logging
import multiprocessing as mp
import traci.constants as tc

from pathlib import Path
from log.logger import partition_logger
from decorator.timing_decorator import timed, timed_block, get_thread_stats

from utils import send_big_data_to_socket, receive_msgs, cause_accident


####################################################################################################
# SUMO binaries
NETCONVERT = sumolib.checkBinary('netconvert')
SUMO = sumolib.checkBinary('sumo')
SUMO_GUI = sumolib.checkBinary('sumo-gui')

MONITORING = 1
TRIP_LENGTH_INFO = 2
LOAD_INFO = 4

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError

class SUMOPartition(mp.Process):

    DEBUG = os.getenv("DEBUG", False)
    DELAY_SIM = float(os.getenv("DELAY_SIM", "0"))
    PIPE_PATH = os.getenv("NAMED_PIPE", '/tmp/parallelumo')
    BALANCER_UPDATE_PACE = int(os.getenv("BALANCER_UPDATE_PACE", 10000000)) # Send update to balancer every 100 simulation steps


    def __init__(self, partition_id, edges_set, border_edges_set, neighbour_info, border_info, simulation_folder, simulation_net, route_file, state_channel, main_info, debug = False, balancer_update_pace = None):
        """
        Constructor for the SUMOPartition class.
        Args:
            partition_id: the ID of the partition.
            edges_set: the set of edges composing the partition.
            border_edges_set: the set of border edges of the partition. They are the edges shared with other partitions.
            neighbour_info: the information of the connected neighbours of the partition. Necessary for communication part
            border_info: the information about the connection of the outgoing border edges. For each edge, it contains the partition to which it is connected and the outgoing destination edge
            simulation_folder: the path to the simulation folder to get the simulation info from (e.g. routes).
            simulation_net: the sumolib object of the simulation road network
        """
        # COMMENT: If the simulation folder is used only for the route, it could be better to precompute the initial routes for each partition and then pass them to the class

        self.partition_id = partition_id
        self.logger = logging.LoggerAdapter(partition_logger, {"partition_id": self.partition_id})
        self.edges_set = edges_set
        self.border_edges_set = border_edges_set
        self.neighbour_info = neighbour_info
        # self.neighbour_info = { idx:val for (idx,val) in enumerate(neighbour_info) }
        # self.neighbour_info = {idx:val for (idx,val) in self.neighbour_info.items() if val is not None}
        # self.logger.info("Neighbour info: {}".format(self.neighbour_info.keys()))
        # self.logger.info("Partition ID: {}".format(self.partition_id))
        self.neighbours = list(self.neighbour_info.keys())
        self.neighbours.remove(self.partition_id)
        self.border_info = border_info
        self.simulation_folder = simulation_folder
        self.net = simulation_net
        self.internal_connections = {}
        self.stats_collector = None
        self.state_channel = state_channel
        self.balancer_queue = None
        self.history = []
        self.current_step = 0
        self.main_info = main_info
        self.balancer_update_pace = balancer_update_pace


        ## Start data socket to receive data
        self.logger.info("Configuring UDP server socket on port {}".format(neighbour_info[self.partition_id]["port"]))
        self.__data_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__data_socket.bind((neighbour_info[self.partition_id]["address"], neighbour_info[self.partition_id]["port"]))
        self.logger.info("Server UDP socket binded on port {}".format(neighbour_info[self.partition_id]["port"]))

        ## Start control socket to receive data
        self.logger.info("Configuring UDP control socket on port {}".format(neighbour_info[self.partition_id]["control_port"]))
        self.__control_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__control_socket.bind((neighbour_info[self.partition_id]["address"], neighbour_info[self.partition_id]["control_port"]))
        self.logger.info("Control UDP socket binded on port {}".format(neighbour_info[self.partition_id]["control_port"]))

        ## Start server socket to receive state data
        self.logger.info("Configuring UDP server socket on port {}".format(neighbour_info[self.partition_id]["state_port"]))
        self.__state_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__state_socket.bind((neighbour_info[self.partition_id]["address"], neighbour_info[self.partition_id]["state_port"]))
        self.logger.info("Server UDP socket binded on port {}".format(neighbour_info[self.partition_id]["state_port"]))

        ## Configuring client socket
        self.__client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.simulated_vehicles_ids = set()
        self.simulated_vehicles_p_triggering = {}

        self._configuration_file = os.path.join(simulation_folder, "osm.sumocfg")
        self._routes_file = route_file

        self.running = False

        self.stop_simtime = -1


        # Add also incoming internal edges to border, to avoid short border to be missed
        additional_edges = {}
        for border_edge in self.border_info.keys():
            edge = self.net.getEdge(border_edge)
            incoming_edges = edge.getIncoming()
            for incoming_edge in incoming_edges:
                for incoming_conn in incoming_edges[incoming_edge]:
                    conn_id = incoming_conn.getViaLaneID()
                    additional_edges[conn_id] = self.border_info[border_edge]
                    self.internal_connections[conn_id] = border_edge

        self.border_info.update(additional_edges)

        self.current_invalid_edges = set()

        self.__debug_socket = None
        if debug:
            try:
                self.logger.info("Connecting to debug server...")
                self.__debug_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.__debug_socket.connect(("192.168.17.10", 6666))
                self.logger.info("Connected to debug server...")
            except FileNotFoundError:
                self.logger.info(f"Error: Named pipe {self.PIPE_PATH} not found")
            except Exception as e:
                self.logger.info(f"An error occurred: {e}")

        # if self.DEBUG:
        #     self.logger.info("Sending partition information to server...")
        #     data = {}
        #     data["type"] = "partitions_def"
        #     data["content"] = json.dumps({self.partition_id:{"local_edges": edges_set, "border_edges": border_edges_set}})
        #     payload = json.dumps(data)
        #     payload_len = len(bytes(payload, 'utf-8'))
        #     self.__debug_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))

        # # self.logger.info(self)


    def __str__(self):
        message = "\n"
        message += "-------- Partition {} information --------\n".format(self.partition_id)
        message += "Edges: {}\n".format(self.edges_set)
        message += "Border edges: {}\n".format(self.border_edges_set)
        message += "Neighbour info: {}\n".format(self.neighbour_info)
        message += "Neighbours: {}\n".format(self.neighbours)
        message += "Border info: {}\n".format(self.border_info)
        message += "Simulation folder: {}\n".format(self.simulation_folder)
        message += "Route file: {}\n".format(self._routes_file)

        return message
    
    
    def inject_spawner_info(self, spawn_info):
        """
        Inject the spawn info to the partition.
        """
        self.spawn_info = spawn_info
    

    def inject_balancer_info(self, balancer_info):
        """
        Create the UDP socket to communicate with the balancer
        """
        self.logger.info("Configuring UDP client socket to balancer on address {}:{}".format(balancer_info[0], balancer_info[1]))
        self.__balancer_info = balancer_info
        self.__balancer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def inject_stats_collector(self, stats_collector):
        """
        Inject the stats collector to the partition.
        """
        self.stats_collector = stats_collector
        

    def run(self, simulation_time=3600, gui = False, flags = 0):
        # ## Init sockets and connect to neighbours
        # self.neighbour_sockets = {}
        # for neighbour in self.neighbours:
        #     if neighbour != self.partition_id:
        #         self.logger.info("Connecting to neighbour {} {}".format(neighbour, self.neighbour_info[neighbour]))
        #         connected = False
        #         self.neighbour_sockets[neighbour] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #         while(not connected):
        #             try:
        #                 self.neighbour_sockets[neighbour].connect((self.neighbour_info[neighbour]["address"], self.neighbour_info[neighbour]["port"]))
        #                 self.logger.info("Connected to neighbour {}".format(neighbour))
        #                 connected = True
        #             except Exception as e:
        #                 self.logger.info("Error connecting to neighbour {}: {}".format(neighbour, e))
        #                 time.sleep(1)


        self.monitoring = (flags & MONITORING) == MONITORING
        self.logger.info("monitoring flag: {}".format(self.monitoring))
        self.logger.info("Simulation delay: {}".format(self.DELAY_SIM))

        # Stats
        removed_vehicles = []
        teleported_vehicles = set()
        vehicles_load = []

        # Control/Monitoring
        route_ids_cache = set()
        traffic_monitoring_pos = None
        if self.monitoring:
            traffic_monitoring_pos = set()

        self.running = True


        sumo_cmd = [SUMO_GUI if gui else SUMO, "-c", self._configuration_file, "--start", "--no-warnings"]
        sumo_cmd += ["--quit-on-end"]
        sumo_cmd += ["--time-to-teleport.remove"] # Remove vehicles instead of teleporting them
        if (flags & LOAD_INFO) == LOAD_INFO:
            sumo_cmd += ["--summary", "{}".format(os.path.join(Path(self.simulation_folder).parent, "simulation_results", "summary_{}.out".format(self.partition_id)))]
            sumo_cmd += ["--fcd-output", "{}".format(os.path.join(Path(self.simulation_folder).parent, "simulation_results", "dump_{}.out".format(self.partition_id)))]
        if (flags & TRIP_LENGTH_INFO) == TRIP_LENGTH_INFO:
            sumo_cmd += ["--vehroute-output.route-length", "--vehroute-output", "{}".format(str(os.path.join(Path(self.simulation_folder).parent, "simulation_results", "tripinfo_{}.out".format(self.partition_id))))]

        traci.start(sumo_cmd)


        # Subscription to border edges
        self.__subscribe_to_border_edges()
        traci.simulation.subscribe([tc.VAR_DEPARTED_VEHICLES_IDS, tc.VAR_TELEPORT_ENDING_VEHICLES_IDS ])

        # Initialization route for single partition
        route_ids = self.__initialize_routes() if "routes" in self._routes_file else self.__initialize_trips()

        balancing = False
        balancing_step = -1
        inject_msg = None
        all_departs = []
        all_delays = []
        all_injections = []
        state = None
        with timed_block("simulation_time"):
            while self.current_step < simulation_time:
                
                if self.current_step >= self.stop_simtime:
                    # Balancer notification check
                    state_msgs = receive_msgs(self.__state_socket, 1, 0)
                    if state_msgs != []:
                        state = state_msgs[0]
                        # readable, _, _ = select.select([self.__state_socket], [], [], 0)
                        # if self.__state_socket in readable:
                        #     with timed_block("balancer_check_times", mode="append"):
                        #         balancing = True
                        #         state = self.__state_socket.recv(1024 * 60)
                        #         state = pickle.loads(state)
                        # self.logger.info("Message received at timestep {}: {}".format(self.current_step, state))
                        # state = self.state_channel.recv()
                        # self.logger.info("New state: {}".format(state))

                        if state["type"] == "stop_simtime":
                            self.logger.info("Stopping simulation at timestep {}".format(state["stop_simtime"]))
                            self.stop_simtime = state["stop_simtime"]
                            
                        elif state["type"] == "inject_state":                            
                            with timed_block("state_injection_times", mode="append"):
                                ## State injection
                                all_injections.append(self.current_step)
                                self.logger.info("New state received at timestep {}".format(self.current_step))
                                self.stop_simtime = -1
                                balancing_step = self.current_step
                                current_invalid_edges, to_transfer = self.__inject_state(state)

                                # Check if vehicles are preserved during the state injection
                                self.simulated_vehicles_p_triggering[self.current_step] = {}
                                self.simulated_vehicles_p_triggering[self.current_step]["before"] = [int(x) for x in traci.vehicle.getIDList()]

                                self.current_invalid_edges = self.current_invalid_edges.union(current_invalid_edges)

                                # self.logger.info("invalid vehicles: {}".format(self.current_invalid_edges))

                                # Send vehicles to other partitions
                                for neighbour in self.neighbours:
                                    # self.logger.info("Sending vehicles to partition {}: {}".format(neighbour, "empty" if neighbour not in to_transfer else "not empty"))
                                    self.__send_data_to_partition(to_transfer[neighbour] if neighbour in to_transfer else [], neighbour, True)

                                # Receive vehicles from other partitions
                                all_vehicle_to_insert = []
                                all_vehicle_to_insert = [x for xs in receive_msgs(self.__data_socket, len(self.neighbours)) for x in xs]
                                self.logger.info("STATE VEHICLES RECEIVED : {}".format(all_vehicle_to_insert))
                                # for neighbour in self.neighbours:
                                #     self.logger.info("Receiving vehicles from partition {}".format(neighbour))
                                #     received_vehicles = self.__receive_data_from_partition(neighbour)
                                    
                                #     all_vehicle_to_insert += received_vehicles

                                if len(all_vehicle_to_insert) > 0:
                                    for vehicle_to_insert in all_vehicle_to_insert:
                                        # self.logger.info("[inj] Adding vehicle {} to partition {} wroute {} at timestep {} - test presence {}".format(vehicle_to_insert["vehicle_id"], self.partition_id, vehicle_to_insert["route_id"],  self.current_step, vehicle_to_insert["route_id"] in route_ids_cache))
                                        new_route_id = "{}_{}".format(vehicle_to_insert["route_id"], self.partition_id)
                                        traci.route.add(new_route_id, vehicle_to_insert["route"])
                                        route_ids_cache.add(new_route_id)
                                        traci.vehicle.add(vehicle_to_insert["vehicle_id"], new_route_id, departSpeed="desired", departPos="last", departLane="best")

                                self.logger.info("State injected at timestep {}".format(self.current_step))


                            # Information for the viewver
                            if self.__debug_socket is not None:
                                data = {}
                                data_content = {}
                                data_content["timestep"] = self.current_step
                                data_content["partition"] = self.partition_id
                                data_content["vehicles"] = self.__getSimulationData()
                                
                                data["type"] = "state_injection"
                                data["content"] = data_content
                                payload = json.dumps(data)
                                payload_len = len(bytes(payload, 'utf-8'))
                                self.__debug_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))
                        else:
                            self.logger.info("Unknown message")

                ## End state injection

                if self.current_step == self.stop_simtime:
                    # self.logger.info("Stopping simulation at timestep {}".format(self.current_step))
                    continue


                with timed_block("step_times", mode = "append"):
                    scheduled_removal_vehicles = set()

                    with timed_block("sumo_step_times", mode = "append"):
                        try:
                            self.current_step += 1
                            traci.simulationStep()
                            # self.logger.info("Simulation step {}".format(self.current_step))
                        except traci.exceptions.FatalTraCIError as e:
                            self.logger.info("Exiting simulation...")
                            break

                    # if self.current_step == 200:
                    #     if self.partition_id == 1:
                    #         # random_edges = random.sample(self.border_edges_set, 15)
                    #         random_edges =  ['127693383#3', '1021481768#0', '5670254#3', '1088321124#0', '5671774#4', '1088320654#0', '32936741', '456308943#0', 
                    #                          '954178925#0', '1266830893#0', '438381568#2', '989267825#0', '1046750793#0', '847952489#0', '986615540#0']
                    #         print("Causing accidents on edges: {}".format(random_edges))
                    #         for random_edge in random_edges:
                    #             self.logger.info("Causing accident on edge {}".format(random_edge))
                    #             for inc_edge in self.net.getEdge(random_edge).getIncoming():
                    #                 for lane in inc_edge.getLanes():
                    #                     self.logger.info("Causing accident on lane {}".format(lane.getID()))
                    #                     # self.logger.info("Lane ID: {}".format(lane.getID()))
                    #                     cause_accident(traci, self.current_step, inc_edge.getID(), lane.getID().split("_")[1], duration = 3000.0, debug_socket = self.__debug_socket)

                    ## Send update to balancer
                    if self.__balancer_socket is not None and self.current_step % self.balancer_update_pace == 0:
                        with timed_block("balancer_update_times", mode = "append"):
                            msg = {}
                            msg["type"] = "update"
                            msg["data"] = {}
                            msg["data"]["id"] =  self.partition_id
                            msg["data"]["load"] = traci.vehicle.getIDCount()
                            msg["data"]["step"] =  self.current_step
                            msg["data"]["edges_load"] = {}
                            for edge in self.edges_set:
                                msg["data"]["edges_load"][edge] = traci.edge.getLastStepVehicleNumber(edge)

                            # self.logger.info("Sendin data to balancer {}".format(self.__balancer_info))
                            # self.__balancer_socket.sendto(pickle.dumps(msg), (self.__balancer_info[0], self.__balancer_info[1]))
                            send_big_data_to_socket(self.__balancer_socket, (self.__balancer_info[0], self.__balancer_info[1]), msg, self.partition_id)
                    

                    if (flags & LOAD_INFO) == LOAD_INFO:
                        vehicles_load.append(traci.vehicle.getIDCount())

                    departed_events = self.__get_departed_data()

                    # self.logger.info("Departed vehicles at timestep {}: {}".format(self.current_step, departed_events))
                    border_events = self.__get_border_data()
                    teleport_events = self.__get_teleport_data()
                    # self.logger.info("Border events: {}".format([item for tup in [el[tc.LAST_STEP_VEHICLE_ID_LIST] for el in border_events.values()]for item in tup]))
                    # self.logger.info("Teleport events: {}".format(teleport_events))
                    # self.logger.info("Concat: {}".format([el for el in departed_events if el in border_events.keys()]))
                    # self.logger.info("Concat 2: {}".format([el for el in departed_events if el in teleport_events]))
                    # self.logger.info("Departed vehicles: {} at {}".format(departed_events, self.current_step))
                    
                    # if balancing_step != -1 and self.current_step == balancing_step + 1:
                    #     # Check if vehicles are preserved during the state injection
                    #     current_vehicles = traci.vehicle.getIDList()
                    #     current_vehicles = [int(veh) for veh in current_vehicles if veh not in departed_events]
                    #     self.simulated_vehicles_p_triggering[self.current_step - 1]["after"] = current_vehicles

                    current_vehicles_on_border = self.__manage_vehicles_on_border(border_events)
                    current_teleported_vehicles = self.__manage_teleported_vehicles(teleport_events)

                    scheduled_removal_vehicles.update({el[0] for el in current_vehicles_on_border})
                    scheduled_removal_vehicles.update(current_teleported_vehicles)
                    
                    teleported_vehicles.update(current_teleported_vehicles)
                    route_ids_cache.update(self.__manage_departed_vehicles(departed_events))

                    if len(self.current_invalid_edges) > 0:
                        to_remove_this_step = {veh for veh in departed_events if veh in self.current_invalid_edges}
                        departed_events = [veh for veh in departed_events if veh not in to_remove_this_step]
                        self.current_invalid_edges -= to_remove_this_step

                        # self.logger.info("Remaining to remove: {}".format(self.current_invalid_edges))
                        for veh in to_remove_this_step:
                            # self.logger.info("Removing previously scheduled vehicle {} at timestep {}".format(veh, self.current_step))
                            traci.vehicle.remove(veh)

                    # self.logger.info("Scheduled removal vehicles: {}".format( scheduled_removal_vehicles))

                    # Prepare data structure for communication
                    vehicles_transfer_info = {}
                    # self.logger.info("Communication info: {}".format(self.neighbour_info))
                    # self.logger.info("Temp: {}".format(self.temp))
                    for neighbour in self.neighbours:
                        vehicles_transfer_info[neighbour] = []

                    with timed_block("vehicle_info_times", mode = "append"):
                        for vehicle_info in current_vehicles_on_border:
                            vehicle_info_id, vehicle_info_border = vehicle_info
                            vehicle_transfer_structure = self.__get_vehicle_info(vehicle_info_id, vehicle_info_border, self.current_step)
                            # self.logger.info("Vehicles_transfer_info: {} {}".format(vehicles_transfer_info, vehicle_transfer_structure))
                            vehicles_transfer_info[vehicle_transfer_structure["to"]].append(vehicle_transfer_structure)

                    ## COMMUNICATION PART
                    with timed_block("comm_times", mode = "append"):
                        # Send cars to other partitions
                        with timed_block("send_times", mode = "append"):
                            for neighbour in self.neighbours:
                                self.__send_data_to_partition(vehicles_transfer_info[neighbour], neighbour)

                        # # # Wait for cars from other partitions
                        # for neighbour in range(len(self.neighbours)):
                        #     # self.logger.info("Waiting for cars from partition {}".format( neighbour))
                        #     with timed_block("receive_times", mode = "append"):
                        #         received_vehicles = self.__receive_data_from_partition(neighbour)

                        with timed_block("receive_times", mode = "append"):
                            received_vehicles = [x for xs in receive_msgs(self.__data_socket, len(self.neighbours)) for x in xs]

                            # ACK TO TELL THE OTHER PARTITIONS TO GO AHEAD
                            for neighbour in self.neighbours:
                                self.__send_control_to_partition({}, neighbour)

                            receive_msgs(self.__control_socket, len(self.neighbours))

                        if received_vehicles != []:
                            # route_ids_cache = traci.route.getIDList()
                            # self.logger.info("Route IDs cache: {}".format( route_ids_cache))
                            with timed_block("insertion_times", mode = "append"):
                                for received_vehicle in received_vehicles:
                                    # COMMENT: What if a vehicle is inserted in the same partition (due to a repartition)?
                                    if received_vehicle["vehicle_id"] in self.current_invalid_edges:
                                        # self.logger.info("Vehicle {} already scheduled for removal".format(received_vehicle["vehicle_id"]))
                                        traci.vehicle.remove(received_vehicle["vehicle_id"])

                                    new_route_id = "{}_{}".format(received_vehicle["route_id"], self.partition_id)
                                    # self.logger.info("Adding vehicle {} wroute {} at timestep {} - test presence {}".format(received_vehicle["vehicle_id"], new_route_id, self.current_step, received_vehicle["vehicle_id"] in self.current_invalid_edges))

                                    traci.route.add(new_route_id, received_vehicle["route"])
                                    traci.vehicle.add(received_vehicle["vehicle_id"], new_route_id, departPos="last")

                    ## CONTROL PART
                    if self.monitoring:
                        traffic_monitoring_pos.update(self.__monitor_partition_step(departed_events, scheduled_removal_vehicles, self.current_step))

                    # Information for the viewver
                    if self.__debug_socket is not None:
                        # self.logger.info("Sending vehicles information to server...")
                        data = {}
                        data_content = {}
                        data_content["timestep"] = self.current_step
                        data_content["partition"] = self.partition_id
                        data_content["vehicles"] = self.__getSimulationData()
                        
                        data["type"] = "vehicles"
                        data["content"] = data_content
                        payload = json.dumps(data)
                        payload_len = len(bytes(payload, 'utf-8'))
                        self.__debug_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))

                    # Finally, remove vehicles scheduled for this step
                    with timed_block("removal_times", mode = "append"):
                        for vehicle in scheduled_removal_vehicles:
                            # self.logger.info("Removing vehicle: {}".format( vehicle))
                            removed_vehicles.append(vehicle)
                            # traci.vehicle.unsubscribe(vehicle)
                            traci.vehicle.remove(vehicle)
                    
                    # Simulation delay
                    if self.DELAY_SIM > 0:
                        time.sleep(self.DELAY_SIM)

        self.running = False

        traci.close()

        self.logger.info("Balancing: {}".format(balancing))
        self.logger.info("Balancing step: {}".format(balancing_step))

        if self.__client_socket is not None:
            stats = get_thread_stats().copy()
            # self.__log_stats(stats, simulation_time, flags)
            stats["id"] = self.partition_id
            stats["last_sim_step"] = self.current_step
            stats["removed_vehicles"] = removed_vehicles
            stats["teleported_vehicles"] = teleported_vehicles
            stats["vehicles_outside_partition"] = list(filter(lambda x: x[0] not in teleported_vehicles and x[1] not in self.border_edges_set, traffic_monitoring_pos)) if self.monitoring else []
            stats["step_times_avg"] = sum(stats["step_times"]) / simulation_time
            stats["comm_times_avg"] = sum(stats["comm_times"]) / simulation_time
            stats["border_data_times_avg"] = sum(stats["border_data_time"]) / simulation_time
            stats["teleport_data_times_avg"] = sum(stats["teleport_data_time"]) / simulation_time
            stats["manage_vehicles_times_avg"] = sum(stats["manage_vehicles_time"]) / simulation_time
            stats["manage_teleported_vehicles_times_avg"] = sum(stats["manage_teleported_vehicles_time"]) / simulation_time
            stats["vehicle_info_times_avg"] = sum(stats["vehicle_info_times"]) / simulation_time
            stats["departed_data_times_avg"] = sum(stats["departed_data_time"]) / simulation_time
            stats["manage_departed_times_avg"] = sum(stats["manage_departed_time"]) / simulation_time
            stats["monitor_partition_step_times_avg"] = sum(stats["monitor_partition_step_time"]) / simulation_time if self.monitoring else 0
            stats["removal_times_avg"] = sum(stats["removal_times"]) / simulation_time
            stats["sumo_step_times_avg"] = sum(stats["sumo_step_times"]) / simulation_time
            stats["send_times_avg"] = sum(stats["send_times"]) / simulation_time
            stats["receive_times_avg"] = sum(stats["receive_times"]) / simulation_time
            stats["insertion_times_avg"] = sum(stats["insertion_times"]) / simulation_time
            # stats["simulated_vehicles_ids"] = self.simulated_vehicles_ids
            # stats["simulated_vehicles_p_triggering"] = self.simulated_vehicles_p_triggering
            # stats["all_departs"] = all_departs
            if (flags & LOAD_INFO) == LOAD_INFO:
                stats["vehicles_load"] = vehicles_load
                stats["vehicles_delay"] = all_delays
            if self.__balancer_socket is not None:
                print(stats["state_injection_times"]) if "state_injection_times" in stats else print("No state injection times")
                stats["balancer_check_times_avg"] = sum(stats["balancer_check_times"]) / simulation_time if "balancer_check_times" in stats else 0
                stats["inject_state_times_avg"] = sum(stats["state_injection_times"]) / simulation_time if "state_injection_times" in stats else 0
                stats["balancer_update_times_avg"] = sum(stats["balancer_update_times"]) / simulation_time if "balancer_update_times" in stats else 0
                stats["balancer_update_times_total"] = sum(stats["balancer_update_times"]) if "balancer_update_times" in stats else 0
                stats["injections"] = all_injections

            # stats["simulation_time"] = end_time - start_time
            # stats["step_times"] = step_times
            # stats["comm_times"] = comm_times

            del stats["step_times"]
            del stats["comm_times"]
            del stats["border_data_time"]
            del stats["teleport_data_time"]
            del stats["manage_vehicles_time"]
            del stats["manage_teleported_vehicles_time"]
            del stats["vehicle_info_times"]
            del stats["removal_times"]
            del stats["sumo_step_times"]
            del stats["send_times"]
            del stats["receive_times"]
            del stats["insertion_times"]
            del stats["departed_data_time"]
            del stats["manage_departed_time"]
            if self.monitoring:
                del stats["monitor_partition_step_time"]
            # if self.balancer_queue is not None and "balancer_check_times" in stats:
            #     del stats["balancer_check_times"]
            # if self.balancer_queue is not None and "state_injection_times" in stats:
            #     del stats["state_injection_times"]
            # if self.balancer_queue is not None and "balancer_update_times" in stats:
            #     del stats["balancer_update_times"]

            message = 'A' * 70000
            send_big_data_to_socket(self.__client_socket, self.main_info, stats, self.partition_id)

            time.sleep(10)

        self.logger.info("Simulation ended at timestep {}".format(self.current_step))
        self.__client_socket.sendto(pickle.dumps(os. getpid()), self.spawn_info)
            

    ## STRUCTURE METHODS
    @timed("subscriptions_time")
    def __subscribe_to_border_edges(self):
        """
        Subscribe to the border edges of the partition to detect when vehicle is crossing the border.
        In addition, the subscription happens also for the outgoing lane of the border edge. This to avoid the case in which the border edges is too small and the passage is not detected
        """

        for border_edge in self.border_info.keys():
            if border_edge.startswith(":"):
                # self.logger.info("Subscribing to edge: {}".format( border_edge))
                traci.lane.subscribe(border_edge, [ tc.LAST_STEP_VEHICLE_NUMBER, tc.LAST_STEP_VEHICLE_ID_LIST ])
            else:
                # self.logger.info("Subscribing to lane: {}".format( border_edge))
                traci.edge.subscribe(border_edge, [ tc.LAST_STEP_VEHICLE_NUMBER, tc.LAST_STEP_VEHICLE_ID_LIST ])

    
    def __unsubscribe_to_border_edges(self):
        """
        Subscribe to the border edges of the partition to detect when vehicle is crossing the border.
        In addition, the subscription happens also for the outgoing lane of the border edge. This to avoid the case in which the border edges is too small and the passage is not detected
        """

        for border_edge in self.border_info.keys():
            if border_edge.startswith(":"):
                # self.logger.info("Unsubscribing to edge: {}".format( border_edge))
                traci.lane.unsubscribe(border_edge)
            else:
                # self.logger.info("Unsubscribing to lane: {}".format( border_edge))
                traci.edge.unsubscribe(border_edge)


    def __initialize_trips(self, depart_time = 0):
        """
        Get the subset of the trips which starts in this partition. If a vehicle start on an outgoing border edge it is not added.
        """
        self.logger.info("Initializing trips...")
        route_ids = []

        for trip in sumolib.xml.parse_fast(self._routes_file, "trip", ["id", "type", "depart", "departLane", "departSpeed", "fromTaz", "toTaz", "from", "to"], optional=True):
            from_edge = trip.attr_from
            if float(trip.depart) >= depart_time and from_edge in self.edges_set and from_edge not in self.border_info.keys():
                # self.logger.info("Adding vehicle {} in local partition [{}] starting from {} at timestep {}".format(trip.id, self.partition_id, from_edge, self.current_step))
                # new_route_id = "{}_{}".format(trip.id, self.partition_id)
                new_route_id = trip.id
                traci.route.add(new_route_id, [from_edge, trip.to])
                traci.vehicle.add(trip.id, new_route_id, depart=trip.depart)
                route_ids.append(new_route_id)

        # self.logger.info("Route IDs: {}".format( route_ids))
        return route_ids
    
    
    def __initialize_routes(self, depart_time = 0):
        """
        Get the subset of the routes which starts in this partition. If a vehicle start on an outgoing border edge it is not added.
        """
        self.logger.info("Initializing routes...")
        route_ids = []

        for trip in sumolib.xml.parse(self._routes_file, "vehicle"):
            route = trip.route[0]
            edges = route.edges.split()
            from_edge = edges[0]
            if float(trip.depart) >= depart_time and from_edge in self.edges_set and from_edge not in self.border_info.keys():
                # new_route_id = "{}_{}".format(trip.id, self.partition_id)
                new_route_id = trip.id
                traci.route.add(new_route_id, edges)
                traci.vehicle.add(trip.id, new_route_id, depart=trip.depart)
                route_ids.append(new_route_id)
                # self.logger.info("Added vehicle {} with route {} in partition {}".format(trip.id, edges, self.partition_id))

        # self.logger.info("Route IDs: {}".format( route_ids))
        return route_ids
    

    def __get_simulation_data(self):
        return traci.simulation.getAllSubscriptionResults()[""]
    

    @timed("departed_data_time", mode = "append")
    def __get_departed_data(self):
        """
        Get the data from the departed vehicles.
        """
        departed_vehicles = traci.simulation.getAllSubscriptionResults()[""][tc.VAR_DEPARTED_VEHICLES_IDS]
        # self.logger.info("Departed vehicles: {} {}".format(departed_vehicles, type(departed_vehicles)))

        return departed_vehicles
    

    @timed("border_data_time", mode = "append")
    def __get_border_data(self):
        """
        Get the data from the border edges and lane subscriptions to check if some vehicle has crossed the border.
        Data has this form:
        {'43205393#0': {16: 0, 18: ()}, ':253995679_0_0': {16: 0, 18: ()}}
        """
        edge_subscriptions_data = traci.edge.getAllSubscriptionResults()
        lane_subscriptions_data = traci.lane.getAllSubscriptionResults()
        edge_subscriptions_data.update(lane_subscriptions_data)

        # self.logger.info("Edge subscriptions data: {}".format( edge_subscriptions_data))

        return edge_subscriptions_data
    
    @timed("teleport_data_time", mode = "append")
    def __get_teleport_data(self):
        """
        Get the data from the teleport subscriptions to check if some vehicle has teleported outside the partition.
        """
        teleport_subscriptions_data = traci.simulation.getAllSubscriptionResults()[""][tc.VAR_TELEPORT_ENDING_VEHICLES_IDS ]
        # self.logger.info("Teleport subscriptions data: {}".format( teleport_subscriptions_data))

        return teleport_subscriptions_data
    
    @timed("manage_departed_time", mode = "append")
    def __manage_departed_vehicles(self, departed_events):
        t = set()
        for departed_vehicle in departed_events:
            # self.logger.info("Departing vehicle: {}".format( departed_vehicle))
            t.add(traci.vehicle.getRouteID(departed_vehicle))
            self.simulated_vehicles_ids.add(int(departed_vehicle))
            # traci.vehicle.subscribe(departed_vehicle, [tc.VAR_ROUTE_ID, tc.VAR_LANEPOSITION, tc.VAR_SPEED, tc.VAR_EDGES])

        return t
    
    @timed("manage_vehicles_time", mode = "append")
    def __manage_vehicles_on_border(self, border_events):
        """
        Analyze the border events to detect if some vehicle has crossed the border.
        Returns the list of vehicles that crossed the border for the current step
        """

        vehicles_on_border = set()
        for subscription_key in border_events.keys():
                subscription = border_events[subscription_key]
                if subscription[tc.LAST_STEP_VEHICLE_NUMBER] > 0:
                    for vehicle_id in subscription[tc.LAST_STEP_VEHICLE_ID_LIST]:
                        if vehicle_id not in self.current_invalid_edges:
                            # self.logger.info("Vehicle {} reached the border on edge {}".format(vehicle_id, subscription_key))
                            vehicles_on_border.add((vehicle_id, subscription_key))
        
        return vehicles_on_border
    
    @timed("manage_teleported_vehicles_time", mode = "append")
    def __manage_teleported_vehicles(self, teleport_events):
        """
        Analyze the teleport events to detect if some vehicle has teleported outside the partition.
        Returns the list of teleported vehicles for the current step
        """

        teleported_vehicles = set()
        for subscription in teleport_events:
            if traci.lane.getEdgeID(traci.vehicle.getLaneID(subscription)) not in self.edges_set and subscription not in self.current_invalid_edges:
                # self.logger.info("Vehicle {} teleported outside partition {}".format(subscription, traci.lane.getEdgeID(traci.vehicle.getLaneID(subscription))))
                teleported_vehicles.add(subscription)
                # COMMENT: How do i know which partition manage the edge on which the vehicle has teleported?
    
        return teleported_vehicles
    
    # @timed("vehicle_info_times", mode = "append")
    def __get_vehicle_info(self, vehicle_id, edge_id, current_step, _to = None):
        """
        Get the information about the vehicle and the edge it is on.
        """
        # with timed_block("vehicle_info_times", mode = "append"):
        # data = traci.vehicle.getSubscriptionResults(vehicle_id)
        vehicle_transfer_structure = {}
        route = traci.vehicle.getRoute(vehicle_id)
        route_id = traci.vehicle.getRouteID(vehicle_id)
        offset = traci.vehicle.getLanePosition(vehicle_id)
        # speed = traci.vehicle.getSpeed(vehicle_id)
        
        whole_route = route

        # self.logger.info("Vehicle {} on edge {}".format( vehicle_id, edge_id))
        to = self.border_info[edge_id] if _to is None else _to
        ref_border = self.internal_connections[edge_id] if edge_id.startswith(":") else edge_id
        remaining_route = whole_route[whole_route.index(ref_border):]
            
        vehicle_transfer_structure["vehicle_id"] = vehicle_id
        vehicle_transfer_structure["edge"] = edge_id
        vehicle_transfer_structure["route"] = remaining_route
        vehicle_transfer_structure["route_id"] = route_id
        vehicle_transfer_structure["offset"] = offset
        # vehicle_transfer_structure["speed"] = speed
        vehicle_transfer_structure["simulation_step"] = current_step
        vehicle_transfer_structure["to"] = to
        vehicle_transfer_structure["from"] = self.partition_id

        return vehicle_transfer_structure


    def __send_data_to_partition(self, data, target_partition, big=False):
        """
        Send the data to the target_partition.
        """
        # print("Sending data to partition {}".format(target_partition))
        # self.__send_big_data_to_socket(self.__client_socket,  (self.neighbour_info[target_partition]["address"], self.neighbour_info[target_partition]["port"]), data)
        send_big_data_to_socket(self.__client_socket, (self.neighbour_info[target_partition]["address"], self.neighbour_info[target_partition]["port"]), data, self.partition_id)



    def __send_control_to_partition(self, data, target_partition, big=False):
        """
        Send the control info to the target_partition.
        """
        # self.__send_big_data_to_socket(self.__client_socket, (self.neighbour_info[target_partition]["address"], self.neighbour_info[target_partition]["control_port"]), data)
        send_big_data_to_socket(self.__client_socket, (self.neighbour_info[target_partition]["address"], self.neighbour_info[target_partition]["control_port"]), data, self.partition_id)


    def __receive_data_from_partition(self, source_partition):
        """
        Receive the data from the target_partition.
        """
        # received_vehicles = self.neighbour_info[source_partition].recv()
        received_vehicles = self.__data_socket.recv(1024 * 60)
        received_vehicles = pickle.loads(received_vehicles)
        # if(received_vehicles != []):
        #     self.logger.info("Received vehicles: {} from partition {}".format( received_vehicles, source_partition))

        return received_vehicles
    
    # def __send_big_data_to_socket(self, socket, target, data, threshold = 40000):
    #     byte_data = (json.dumps(data, default=set_default)).encode('utf-8')
    #     data_size = len(byte_data)
    #     import inspect
    #     stack = inspect.stack()
    #     calling_function = stack[2].function
    #     # self.logger.info("--- {}".format(data_size))
    #     # self.logger.info("{}.{} - Sending data to partition {} in ts {}".format(stack[2].function, stack[2].lineno, target, self.current_step))
    #     if data_size > threshold:
    #         chunks = [byte_data[i:i + threshold] for i in range(0, data_size, threshold)]
    #         for idx, chunk in enumerate(chunks):
    #             decoded_chunk = chunk.decode('utf-8')
    #             more = 1 if (idx < len(chunks) - 1) else 0
    #             final_msg = "{}%{}%{}%{}".format(self.partition_id, more, decoded_chunk, self.current_step)
    #             # self.logger.info(final_msg)
    #             # self.logger.info("Sending chunk to partition {} in ts {}".format(target, self.current_step))
    #             socket.sendto(pickle.dumps(final_msg), target)
    #     else:
    #         # self.logger.info("Sending whole data to partition {} in ts {}".format(target, self.current_step))
    #         socket.sendto(pickle.dumps("{}%{}%{}%{}".format(self.partition_id, 0, data, self.current_step)), target)

    # def __receive_msgs_frompartitions(self, partitions, socket):
    #     # self.logger.info("Listening from socket {}".format(socket))
    #     msg_completed_received = 0
    #     message_to_be_processed = [""] * (max(partitions) + 1)
    #     final_messages = []
    #     while msg_completed_received < len(partitions):
    #         readable, _, _ = select.select([socket], [], [])
    #         if socket in readable:
    #             partition_stat = pickle.loads(socket.recv(1024*60))
    #             # self.logger.info("Received msg {}".format(partition_stat))
    #             partition = int(partition_stat.split("%")[0])
    #             more = int(partition_stat.split("%")[1])
    #             data = partition_stat.split("%")[2]
    #             message_to_be_processed[partition] += data

    #             if partition not in partitions:
    #                 self.logger.info("NOT EXPECTED MESSAGE!")

    #             # self.logger.info("Received data from {} and more {} in ts {}".format(partition, more, self.current_step))

    #             if more == 0:
    #                 # self.logger.info("Partitions: {} {} {}".format(partitions, max(partitions) + 1, message_to_be_processed))
    #                 # self.logger.info("Completed stats from partition 33 {}: {} {} {} {} {} {}".format(partition, message_to_be_processed[partition], type(message_to_be_processed[partition]), partition_stat, partition, more, data))
    #                 final_value = ast.literal_eval(message_to_be_processed[partition].replace("'", "\""))
    #                 final_messages.append(final_value)
    #                 # self.logger.info("Final value: {}".format(final_value))
    #                 # self.logger.info("Type message {}".format(type(message_to_be_processed[partition])))
    #                 # self.logger.info("id" in message_to_be_processed[partition])
    #                 msg_completed_received += 1

    #     return final_messages

    @timed("monitor_partition_step_time", mode = "append")
    def __monitor_partition_step(self, departed_vehicles_list, scheduled_removal_vehicles, current_step):
        """
        Monitor the whole network to detect vehicles that run outside the partition.
        It subscribe to all departing vehicles and monitor them along their whole route.
        """

        vehicles_outside_partition = set()

        for removed_vehicle in scheduled_removal_vehicles:
            try:
                # self.logger.info("Unsubscribed to vehicle {}".format(removed_vehicle))
                traci.vehicle.unsubscribe(removed_vehicle)
            except Exception as e:
                # self.logger.info("Unable to unsubscribe vehicle {}".format(removed_vehicle))
                pass

        # self.logger.info("Departed vehicles: {}".format(departed_vehicles_list))
        departed_vehicles_list = list(set(list(departed_vehicles_list) + list(traci.simulation.getLoadedIDList())))
        for departing_vehicle in departed_vehicles_list:
            # self.logger.info("Subscribed to vehicle {}".format(departing_vehicle))
            traci.vehicle.subscribe(departing_vehicle, [tc.VAR_ROAD_ID])

        
        position_list = traci.vehicle.getAllSubscriptionResults()
        position_list = {key:val for key,val in position_list.items() if key not in scheduled_removal_vehicles}
        # self.logger.info("Position list: {}".format(position_list.keys()))
        for vehicle_id in position_list.keys():
            # Position can be empty if vehicle is teleporting
            if position_list[vehicle_id][tc.VAR_ROAD_ID] != "" and position_list[vehicle_id][tc.VAR_ROAD_ID] not in self.edges_set and not position_list[vehicle_id][tc.VAR_ROAD_ID].startswith(":"):
                # self.logger.info("Vehicle {} outside partition -- Position: {}".format( vehicle_id, position_list[vehicle_id][tc.VAR_ROAD_ID]))
                vehicles_outside_partition.add((vehicle_id, position_list[vehicle_id][tc.VAR_ROAD_ID], self.current_step))

        return vehicles_outside_partition
    

    def __getSimulationData(self):
        vehicle_data = []
        vehicle_ids = traci.vehicle.getIDList()
        for vehicle_id in vehicle_ids:
            x, y = traci.vehicle.getPosition(vehicle_id)
            lon, lat = traci.simulation.convertGeo(x, y)
            edge_id = traci.vehicle.getRoadID(vehicle_id)
            road_id = traci.vehicle.getRouteID(vehicle_id)
            vehicle_data.append({"vehicle_id": vehicle_id, "lat": lat, "lon": lon, "edge_id": edge_id, "route_id": road_id})
        return vehicle_data
    

    def __inject_state(self, state):
        """
        Inject the state to the partition.
        """

        # Manage old statte
        self.__unsubscribe_to_border_edges()

        local_edges = state["local_edges"]
        border_edges = state["border_edges"]
        neighbour_info = state["communication_info"]
        border_info = state["border_info"]
        transfer_structure = state["transfer_structure"]

        old_local_edges = self.edges_set
        self.history.append(old_local_edges)
        old_border_edges = self.border_edges_set
        self.edges_set = local_edges
        self.border_edges_set = border_edges
        # self.neighbour_info = neighbour_info
        # self.neighbour_info = { idx:val for (idx,val) in enumerate(neighbour_info) }
        # self.neighbour_info = {idx:val for (idx,val) in self.neighbour_info.items() if val is not None}
        self.neighbours = list(self.neighbour_info.keys())
        self.neighbours.remove(self.partition_id)
        self.border_info = border_info
        self.internal_connections = {}

        # Add also incoming internal edges to border, to avoid short border to be missed
        additional_edges = {}
        for border_edge in self.border_info.keys():
            edge = self.net.getEdge(border_edge)
            incoming_edges = edge.getIncoming()
            for incoming_edge in incoming_edges:
                for incoming_conn in incoming_edges[incoming_edge]:
                    conn_id = incoming_conn.getViaLaneID()
                    additional_edges[conn_id] = self.border_info[border_edge]
                    self.internal_connections[conn_id] = border_edge

        self.border_info.update(additional_edges)

        self.logger.info("New border info: {}".format(self.border_info))

        # Manage new state
        # Modify subscriptions
        self.__subscribe_to_border_edges()

        # Manage already running vehicles
        # FIXME: Running vehicles which are in a wrong edge do not have to be removed but sent to the correct partition
        to_transfer = {}
        current_running_vehicles = traci.vehicle.getIDList()
        for vehicle_id in current_running_vehicles:
            vehicle_road = traci.vehicle.getRoadID(vehicle_id)
            if vehicle_road not in self.edges_set:
                # Check where it has to be sent
                for partition_id in transfer_structure.keys():
                    if partition_id not in to_transfer:
                        to_transfer[partition_id] = []
                    if vehicle_road in transfer_structure[partition_id]:
                        # Send vehicle to partition
                        to_transfer[partition_id].append(self.__get_vehicle_info(vehicle_id, vehicle_road, self.current_step, partition_id))

                if self.monitoring:
                    try:
                        traci.vehicle.unsubscribe(vehicle_id)
                    except Exception as e:
                        pass

                traci.vehicle.remove(vehicle_id)


        to_remove = set()
        if "routes" in self._routes_file:
            for trip in sumolib.xml.parse(self._routes_file, "vehicle"):
                route = trip.route[0]
                edges = route.edges.split()
                from_edge = edges[0]

                have_been_in_previous_local_part = any([from_edge in x for x in self.history])
                if float(trip.depart) > self.current_step and from_edge in self.edges_set and not have_been_in_previous_local_part and from_edge not in self.border_info.keys():
                    new_route_id = "{}_{}".format(trip.id, self.partition_id)
                    traci.route.add(new_route_id, edges)
                    traci.vehicle.add(trip.id, new_route_id, depart=trip.depart)
                elif float(trip.depart) > self.current_step and from_edge not in self.edges_set and have_been_in_previous_local_part:
                    if traci.vehicle.getRoadID(trip.id) != "":
                        traci.vehicle.remove(trip.id)
                    else:
                        to_remove = trip.id
        else:
            for trip in sumolib.xml.parse_fast(self._routes_file, "trip", ["id", "type", "depart", "departLane", "departSpeed", "fromTaz", "toTaz", "from", "to"], optional=True):
                from_edge = trip.attr_from
                have_been_in_previous_local_part = any([from_edge in x for x in self.history])
                # self.logger.info("{} have been in previous local part: {} {}".format(trip.id, have_been_in_previous_local_part, [from_edge in x for x in self.history]))
                if float(trip.depart) > self.current_step and from_edge in self.edges_set and not have_been_in_previous_local_part and from_edge not in self.border_info.keys():
                    # self.logger.info("[inj] Adding route {} with edges {} in partition {} at timestep {}".format(trip.id, [from_edge, trip.to], self.partition_id, self.current_step))
                    new_route_id = "{}_{}".format(trip.id, self.partition_id)
                    traci.route.add(new_route_id, [from_edge, trip.to])
                    traci.vehicle.add(trip.id, new_route_id, depart=trip.depart)
                elif float(trip.depart) > self.current_step and from_edge not in self.edges_set and have_been_in_previous_local_part:
                    if traci.vehicle.getRoadID(trip.id) != "":
                        # self.logger.info("[State injection]: Removing vehicle {} at timestep {}".format(trip.id, self.current_step))
                        traci.vehicle.remove(trip.id)
                    else:
                        # self.logger.info("[State injection]: Saving vehicle {} for later removal at timestep {}".format(trip.id, self.current_step))
                        # traci.vehicle.subscribe(trip.id, [tc.VAR_ROAD_ID])
                        to_remove.add(trip.id)

        # self.logger.info("[State injection]: Vehicles to transfer: {}".format(to_transfer))
        # self.logger.info("[State injection]: Vehicles to remove: {}".format(to_remove))

        return to_remove, to_transfer



        # self.logger.info("Initializing routes...")
        # route_ids = []

        # for trip in sumolib.xml.parse(self._routes_file, "vehicle"):
        #     route = trip.route[0]
        #     edges = route.edges.split()
        #     from_edge = edges[0]
        #     if float(trip.depart) >= depart_time and from_edge in self.edges_set and from_edge not in self.border_info.keys():
        #         # new_route_id = "{}_{}".format(trip.id, self.partition_id)
        #         new_route_id = trip.id
        #         traci.route.add(new_route_id, edges)
        #         traci.vehicle.add(trip.id, new_route_id, depart=trip.depart)
        #         route_ids.append(new_route_id)
        #         # self.logger.info("Added vehicle {} with route {} in partition {}".format(trip.id, edges, self.partition_id))

        # # self.logger.info("Route IDs: {}".format( route_ids))
        # return route_ids
    

    def __log_stats(self, stats, simulation_time, flags):
        msg = ""
        msg += "Length of step times: {}\n".format(simulation_time)
        msg += "Length of comm times: {}\n".format(simulation_time)
        msg += "Length of border data times: {}\n".format(simulation_time)
        msg += "Length of teleport data times: {}\n".format(simulation_time)
        msg += "Length of manage vehicles times: {}\n".format(simulation_time)
        msg += "Length of manage teleported vehicles times: {}\n".format(simulation_time)
        msg += "Length of vehicle info times: {}\n".format(simulation_time)
        msg += "Length of departed data times: {}\n".format(simulation_time)
        msg += "Length of manage departed times: {}\n".format(simulation_time)
        msg += "Length of removal times: {}\n".format(simulation_time)
        msg += "Length of sumo step times: {}\n".format(simulation_time)
        msg += "Length of send times: {}\n".format(simulation_time)
        msg += "Length of receive times: {}\n".format(simulation_time)
        msg += "Length of insertion times: {}\n".format(simulation_time)
        
        if (flags & LOAD_INFO) == LOAD_INFO:
            msg += "Length of vehicles load: {}\n".format(simulation_time)
        if self.__balancer_socket is not None:
            msg += "Length of balancer check times: {}\n".format(simulation_time)
            msg += "Length of state injection times: {}\n".format(simulation_time)
            msg += "Length of balancer update times: {}\n".format(simulation_time)

        self.logger.info(msg)
