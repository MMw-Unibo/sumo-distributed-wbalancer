import os
import ast
import uuid
import json
import socket
import pickle
import select
import shutil
import sumolib
import subprocess
import multiprocessing as mp

from pathlib import Path
from log.logger import logger

from partitioning.partition_manager import PartitionManager
from balancing.load_balancer import LoadBalancer
from utils import send_big_data_to_socket, receive_msgs

####################################################################################################
MANAGEMENT_FOLDER = os.path.join(Path(os.path.abspath(__file__)).parent.parent, "management_folder")
SIMULATION_FOLDER = os.getenv("SIM_FOLD", os.path.join(Path(os.path.abspath(__file__)).parent.parent, "simulations", "traci_test_sim"))
CONFIGURATION_FILE = os.getenv("CONF_FILE", os.path.join(SIMULATION_FOLDER, "osm.sumocfg"))
ROUTE_FILE = os.getenv("ROUTE_FILE", os.path.join(SIMULATION_FOLDER, "fixed_routes.rou.xml"))
PREFIX = os.getenv("PREFIX", "osm")

# SUMO commands
SUMO_HOME = os.environ.get("SUMO_HOME")
NETCONVERT = sumolib.checkBinary('netconvert')
SUMO = sumolib.checkBinary('sumo')
SUMO_GUI = sumolib.checkBinary('sumo-gui')

# Partitioning parameters
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", 2))
SIMULATION_TIME = int(os.getenv("SIM_TIME", 3600))

# Partition check
CHECK = os.getenv("CHECK", True)

HOST_IP = os.getenv("HOST_IP", "0.0.0.0")

# Load-balancing parameters
BALANCING = os.getenv("BALANCING", False)
BALANCING_THRESHOLD = float(os.getenv("BALANCING_THRESHOLD", 0.25))
BALANCER_UPDATE_PACE = int(os.getenv("BALANCER_UPDATE_PACE", 10000000))

# Enable visualization
MONITORING = os.getenv("MONITORING", False)
VIS = os.getenv("VIS", False)
if VIS:
    from viz.partition_chart import generate_partition_map

# Information for viewer
DEBUG = os.getenv("DEBUG", False)
DEBUG_IP = os.getenv("DEBUG_IP", HOST_IP)
DEBUG_PORT = os.getenv("DEBUG_PORT", 6666)
PIPE_PATH = os.getenv("NAMED_PIPE", '/tmp/parallelumo')

# Registry information
REGISTRY_IP = os.getenv("REGISTRYIP", "192.168.17.60")
REGISTRY_PORT = int(os.getenv("REGISTRYPORT", 1111))
####################################################################################################

handled_vehicles = []

def clean_management_folder():
    logger.info("Cleaning management folder ...")
    for file in os.listdir(MANAGEMENT_FOLDER):
        if os.path.isdir(os.path.join(MANAGEMENT_FOLDER, file)):
            shutil.rmtree(os.path.join(MANAGEMENT_FOLDER, file))
        else:
            os.remove(os.path.join(MANAGEMENT_FOLDER, file))

def print_spawners(spawners):
    msg = "\nSpawners:\n--------------------------------\n"
    for spawner in spawners:
        msg += "{}:{}\n--------------------------------\n".format(spawner[0], spawner[1])
    
    logger.info(msg)


def run_simulation(simulation_folder, num_partitions, route_file, simulation_time = 3600, visualization = False, partition_check = False, flags = 0, balancer_update_pace = BALANCER_UPDATE_PACE): 

    logger.info("Starting Simulation...")

    client_socket = None
    if DEBUG:
        NET2GEOJSON = os.path.join(SUMO_HOME, "tools", "net/net2geojson.py")
        try:
            logger.info("Connecting to debug server...")
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((DEBUG_IP, DEBUG_PORT))
            logger.info("Connected to debug server at {}:{}".format(DEBUG_IP, DEBUG_PORT))
        except FileNotFoundError:
            logger.info(f"Error: Named pipe {PIPE_PATH} not found")
        except Exception as e:
            logger.info(f"An error occurred: {e}")

        # Send the geojson of the whole network
        net2geo_options = ["python", NET2GEOJSON]
        net2geo_options += ["-n", os.path.join(simulation_folder, "{}.net.xml".format(PREFIX))]
        net2geo_options += ["-o", os.path.join(simulation_folder, "net.geojson")]
        # net2geo_options += ["--lanes"]
        net2geo_options += ["--internal"]

        try:
            logger.info("Calling net2geojson.py with options: {}".format(net2geo_options))
            subprocess.run(net2geo_options, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            logger.error("Error while running net2geojson.py: {}".format(e.stderr))

        with open(os.path.join(simulation_folder, "net.geojson"), "r") as f:
            geojson = f.read()
            data = {}
            data["type"] = "network"
            data["content"] = json.loads(geojson)
            payload = json.dumps(data)
            payload_len = len(bytes(payload, 'utf-8'))
            client_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))

    run_id = uuid.uuid4()
    
    # Set up Pipe for stats collection
    rcv, snd = mp.Pipe(duplex = False)
    global_stats = {}

    # Set up UDP client socket for balancer
    balancer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    balancer_socket.bind((HOST_IP, 1222))

    balancer_socket.sendto(pickle.dumps({"type": "get", "data": {}}), (REGISTRY_IP, REGISTRY_PORT))
    spawners = pickle.loads(balancer_socket.recv(1024 * 60))
    # logger.info(res)

    # Set up server UDP socket for partition stats
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((HOST_IP, 2233))

    ## Send information to each spawner process
    print_spawners(spawners)
    for idx, spawner in enumerate(spawners):
        logger.info("Sending information to spawner at {}:{}".format(spawner[0], spawner[1]))
        # balancer_socket.sendto(pickle.dumps({"type": "control_info", "data": {"main_endpoint": (HOST_IP, 2233), "balancer_endpoint": (HOST_IP, 2222), "partition_manager_endpoint": (HOST_IP, 2233), "debugger_endpoint": ("192.168.17.60", 6666) if DEBUG else None}}), spawner)
        send_big_data_to_socket(balancer_socket, spawner, {"type": "control_info", "data": {"main_endpoint": (HOST_IP, 2233), "balancer_endpoint": (HOST_IP, 2222), "partition_manager_endpoint": (HOST_IP, 2233), "debugger_endpoint": ("192.168.17.60", 6666) if DEBUG else None}}, idx)


    partition_manager = PartitionManager(os.path.join(simulation_folder, "{}.net.xml".format(PREFIX)), route_file, client_socket, spawners)
    partitions, communication_info, _, edge_sets, notmanaged_edges = partition_manager.create_partitions(num_partitions, check=partition_check)

    ## How to distribute the partitions? Round-robin for now
    # communication_info = {}
    # for partition_id in partitions.keys():
    #     spawner = spawners[partition_id % len(spawners)]
    #     communication_info[partition_id] = {}
    #     communication_info[partition_id]["address"] = spawner[0]
    #     communication_info[partition_id]["port"] = spawner[1]
    #     communication_info[partition_id]["state_port"] = spawner[1] + 1000

    logger.info(communication_info.keys())
    for partition_id in partitions.keys():
        spawner = spawners[partition_id % len(spawners)]
        spawn_data = {}
        spawn_data["id"] = partition_id
        spawn_data["partition_structure"] = partitions[partition_id]
        spawn_data["communication_info"] = communication_info
        spawn_data["simulation_folder"] = simulation_folder
        spawn_data["route_file"] = route_file
        # spawn_data["debug"] = "False" if not DEBUG else "True"
        spawn_data["simulation_time"] = SIMULATION_TIME
        spawn_data["flags"] = flags
        spawn_data["balancer_update_pace"] = balancer_update_pace

        logger.info("Sending spawn request to spawner at {}:{}".format(spawner[0], spawner[1]))
        # balancer_socket.sendto(pickle.dumps({"type": "spawn", "data": spawn_data}), spawner)
        send_big_data_to_socket(balancer_socket, spawner, {"type": "spawn", "data": spawn_data}, partition_id)

        logger.info(communication_info)

    balancer_queue = None
    if BALANCING:
        logger.info("Starting load balancer...")
        balancer = LoadBalancer(partition_manager, num_partitions, BALANCING_THRESHOLD, SIMULATION_TIME, (HOST_IP, 2222), client_socket, snd)
        balancer_queue = balancer.get_queue()
        balancer_process = mp.Process(target = balancer.start_monitoring)
        balancer_process.start()
    
    # for partition in partitions:
    #     logger.info(partition)

    # Generate partition map
    if visualization:
        logger.info("Generating partition chart...")
        chart_process = mp.Process(target = generate_partition_map, args = (os.path.join(simulation_folder, "{}.net.xml".format(PREFIX)), edge_sets))
        chart_process.start()


    stats_msgs = receive_msgs(server_socket, num_partitions)
    idx = 0
    for stat in stats_msgs:
        if "id" not in stat:
            raise Exception("Stats must have an 'id' field")
        idx = idx + 1
        logger.info("Received stats from partition {} -- {}/{}".format(stat["id"], idx, num_partitions))
        stat["vehicles_outside_partition"] = list(filter(lambda v: v[1] not in notmanaged_edges, stat["vehicles_outside_partition"]))
        if stat["id"] in global_stats:
            global_stats[stat["id"]].update(stat)
        else:
            global_stats[stat["id"]] = stat

        global_stats[stat["id"]]["run_id"] = run_id
        global_stats[stat["id"]]["num_part"] = num_partitions


    if BALANCING:
        # balancer_queue.put({"type": "control", "data": "stop"})
        # balancer_process.join()
        # balancer_socket.sendto(pickle.dumps({"type": "control", "data": "stop"}), (HOST_IP, 2222))
        send_big_data_to_socket(balancer_socket, (HOST_IP, 2222), {"type": "control", "data": "stop"}, "main")
        balancer_process.join()

        balancer_stats = rcv.recv()
        # balancer_stats = balancer_socket.recv(1024 * 60)
        # balancer_stats = pickle.loads(balancer_stats)

    if visualization:
        chart_process.join()


    # Get all the files from the nodes
    logger.info("Retrieving files from nodes...")
    get_data_script = os.path.join(os.getenv("PROJECT_FOLDER"), "get_data.sh")
    hosts_list = os.path.join(os.getenv("PROJECT_FOLDER"), "hosts_list")
    file_folder = os.path.join(SIMULATION_FOLDER, "simulation_results")

    try:
        subprocess.run([get_data_script, file_folder, hosts_list, file_folder], check=True)
        logger.info("Files retrieved successfully.")
    except subprocess.CalledProcessError as e:
        logger.error("Error while retrieving files: {}".format(e.stderr))

    return global_stats.values(), notmanaged_edges, balancer_stats if BALANCING else None


if __name__ == "__main__":
    flags = 0
    stats, _, _ = run_simulation(SIMULATION_FOLDER, NUM_PARTITIONS, ROUTE_FILE, simulation_time = SIMULATION_TIME, visualization = VIS, partition_check=CHECK, flags = flags)

    for stat in stats:
        if flags == 1:
            logger.info("\nVehicles outside {}: {}".format(stat["id"], sorted(stat["vehicles_outside_partition"], key=lambda x: x[2])))

    # logger.info(stats)
    
    import time
    import pandas as pd
    from pathlib import Path
    LOG_FOLDER = os.getenv("LOG_FOLDER", os.path.join(Path(os.path.abspath(__file__)).parent.parent, "logs"))
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    stats_filename = f"stats_{NUM_PARTITIONS}part_{timestamp}.csv"
    df = pd.DataFrame(stats)
    df.to_csv(os.path.join(LOG_FOLDER, stats_filename), index=False)