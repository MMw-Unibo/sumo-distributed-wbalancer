import os
import sys
import json
import ast
import socket
import pickle
import select
import signal
import sumolib
import multiprocessing as mp

from pathlib import Path
from log.logger import logger
from partitioning.partition import SUMOPartition
from utils import send_big_data_to_socket


IP = os.getenv("SPAWNERIP", "0.0.0.0")
SPAWN_PORT = int(os.getenv("SPAWNERPORT", 3232))
CONTROL_PORT = int(os.getenv("CONTROLPORT", 3233))
REGISTRY_IP = os.getenv("REGISTRYIP", "0.0.0.0")
REGISTRY_PORT = int(os.getenv("REGISTRYPORT", 1111))
SIMULATION_FOLDER = os.getenv("SIM_FOLD", os.path.join(Path(os.path.abspath(__file__)).parent.parent, "simulations", "/home/"))


server_socket = None
partition_runs = []

def signal_handler(sig, frame):
    logger.info("Unregistering...")
    server_socket.sendto(pickle.dumps({"type": "unregister", "data": {"ip": IP, "port": SPAWN_PORT}}), (REGISTRY_IP, REGISTRY_PORT))
    for partition in partition_runs:
        partition.terminate()
    sys.exit(0)

if __name__ == "__main__":

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((IP, SPAWN_PORT))
    logger.info("Server UDP socket binded on {}:{}".format(IP, SPAWN_PORT))

    control_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    control_socket.bind((IP, CONTROL_PORT))
    logger.info("Control UDP socket binded on {}:{}".format(IP, CONTROL_PORT))

    server_socket.sendto(pickle.dumps({"type": "register", "data": {"ip": IP, "port": SPAWN_PORT}}), (REGISTRY_IP, REGISTRY_PORT))
    logger.info("Sent register request to registry")

    signal.signal(signal.SIGINT, signal_handler)

    main_endpoint = None
    balancer_endpoint = None
    partition_manager_endpoint = None
    debugger_endpoint = None

    setup = False


    current_data = ""
    buffer = {}
    
    for filename in os.listdir(SIMULATION_FOLDER):
        file_path = os.path.join(SIMULATION_FOLDER, filename)
        if os.path.isfile(file_path):  # Only delete files, not subfolders
            os.remove(file_path)


    while True:
        if not setup:
            logger.info("Waiting for information from main...")
        readable, _, _ = select.select([server_socket, control_socket], [], [])
        if server_socket in readable:
            spawn_request_frag = server_socket.recv(1024 * 60)
            # logger.info("Got raw request from main 1:\n{}".format(spawn_request_frag.split(b'%', 2)))
            
            if not spawn_request_frag:
                break
            
            message_id, more, chunk = spawn_request_frag.split(b'%', 2)
            # logger.info("Got raw request from main 2:\n{}".format((message_id, more, chunk)))
            # logger.info(more == b'0')

            if message_id not in buffer:
                buffer[message_id] = b''
            buffer[message_id] += chunk

            try:            
                if more == b'0':
                    spawn_request = pickle.loads(buffer.pop(message_id))
                    logger.info("Got raw request from main:\n{}".format(spawn_request))

                    request_type = spawn_request["type"]
                    request_data = spawn_request["data"]

                    if request_type == "control_info":
                        logger.info("Received control info from main:\n{}".format(request_data))
                        main_endpoint = request_data["main_endpoint"]
                        balancer_endpoint = request_data["balancer_endpoint"]
                        partition_manager_endpoint = request_data["partition_manager_endpoint"]
                        debugger_endpoint = request_data["debugger_endpoint"]
                        setup = True
                    elif request_type == "spawn":
                        # logger.info("Received spawn request from main:\n{}".format(request_data))
                        partition_id = request_data["id"]
                        partition_structure = request_data["partition_structure"]
                        communication_info = request_data["communication_info"]
                        simulation_folder = os.path.join(SIMULATION_FOLDER, request_data["simulation_folder"].split("/")[-1])
                        route_file = os.path.join(simulation_folder, request_data["route_file"].split("/")[-1])
                        simulation_time = request_data["simulation_time"]
                        flags = request_data["flags"]
                        balancer_update_pace = request_data["balancer_update_pace"]
                        net = sumolib.net.readNet(os.path.join(simulation_folder, "osm.net.xml"))
                        sumopartition = SUMOPartition(partition_id, partition_structure["local_edges"], list(partition_structure["border_edges"].keys()), communication_info, partition_structure["border_edges"], simulation_folder, net, route_file, None, main_endpoint, True if debugger_endpoint is not None else False, balancer_update_pace)
                        sumopartition.inject_balancer_info(balancer_endpoint)
                        sumopartition.inject_spawner_info((IP, CONTROL_PORT))

                        partition_process = mp.Process(target = sumopartition.run, args = (simulation_time, False, flags))
                        partition_runs.append(partition_process)
                        partition_process.start()
            except Exception as e:
                logger.error("Error while processing spawn request from main:\n{}".format(e))
                raise e

        if control_socket in readable:
            control_request = pickle.loads(control_socket.recv(1024 * 60))
            logger.info("Partition PID={} ended".format(control_request))
            
            to_remove = []
            for partition in partition_runs:
                if partition.pid == control_request:
                    partition.join()
                    partition_runs.remove(partition)