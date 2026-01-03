import os
import json
import time
import traci
import shutil
import socket
import sumolib
import random
import subprocess
import traci.constants as tc

from pathlib import Path
from log.logger import logger

from utils import cause_accident
from decorator.timing_decorator import timed, timed_block, get_thread_stats

####################################################################################################
MANAGEMENT_FOLDER = os.path.join(Path(os.path.abspath(__file__)).parent.parent, "management_folder")
SIMULATION_FOLDER = os.getenv("SIM_FOLD", os.path.join(Path(os.path.abspath(__file__)).parent.parent, "simulations", "traci_test_sim"))
CONFIGURATION_FILE = os.getenv("CONF_FILE", os.path.join(SIMULATION_FOLDER, "osm.sumocfg"))
ROUTE_FILE = os.getenv("ROUTE_FILE", os.path.join(SIMULATION_FOLDER, "fixed_routes.rou.xml"))
SIMULATION_TIME = int(os.getenv("SIM_TIME", 3600))
PREFIX = os.getenv("PREFIX", "osm")

# SUMO binaries
NETCONVERT = sumolib.checkBinary('netconvert')
SUMO = sumolib.checkBinary('sumo')
SUMO_GUI = sumolib.checkBinary('sumo-gui')
SUMO_HOME = os.environ.get("SUMO_HOME")
NET2GEOJSON = os.path.join(SUMO_HOME, "tools", "net/net2geojson.py")


# Flags
TRIP_LENGTH_INFO = 2
LOAD_INFO = 4

HOST_IP = os.getenv("HOST_IP", "0.0.0.0")
DEBUG = os.getenv("DEBUG", False)
DEBUG_IP = os.getenv("DEBUG_IP", HOST_IP)
DEBUG_PORT = os.getenv("DEBUG_PORT", 6666)
PIPE_PATH = os.getenv("NAMED_PIPE", '/tmp/parallelumo')

####################################################################################################

current_step = None

def convert_wsl_to_windows_path(wsl_path):
    # Replace WSL-specific path prefix
    wsl_path = wsl_path.replace('/mnt/', '').replace('/', '\\')

    # Add the Windows drive letter prefix
    windows_path = f'{wsl_path[0].upper()}:{wsl_path[1:]}'

    return windows_path

@timed("clean_management_folder")
def clean_management_folder():
    logger.info("Cleaning management folder ...")
    for file in os.listdir(MANAGEMENT_FOLDER):
        if os.path.isdir(os.path.join(MANAGEMENT_FOLDER, file)):
            shutil.rmtree(os.path.join(MANAGEMENT_FOLDER, file))
        else:
            os.remove(os.path.join(MANAGEMENT_FOLDER, file))


def getSimulationData():
    vehicle_data = []
    vehicle_ids = traci.vehicle.getIDList()
    for vehicle_id in vehicle_ids:
        # if vehicle_id == "56":
        #     logger.info("Vehicle 56 is on edge {} at ts {}".format(traci.vehicle.getRoadID(vehicle_id), current_step))
        x, y = traci.vehicle.getPosition(vehicle_id)
        lon, lat = traci.simulation.convertGeo(x, y)
        edge_id = traci.vehicle.getRoadID(vehicle_id)
        road_id = traci.vehicle.getRouteID(vehicle_id)
        vehicle_data.append({"vehicle_id": vehicle_id, "lat": lat, "lon": lon, "edge_id": edge_id, "route_id": road_id})
    return vehicle_data


def run_simulation(configuration_file, route_file, simulation_time=3600, flags=0):
    global current_step
    # Clean management folder
    clean_management_folder()

    __debug_socket = None
    if DEBUG:
        try:
            logger.info("Connecting to debug server...")
            __debug_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            __debug_socket.connect((DEBUG_IP, DEBUG_PORT))
            logger.info("Connected to debug server at {}:{}".format(DEBUG_IP, DEBUG_PORT))
        except FileNotFoundError:
            logger.info(f"Error: Named pipe {PIPE_PATH} not found")
        except Exception as e:
            logger.info(f"An error occurred: {e}")

        # Send the geojson of the whole network
        net2geo_options = ["python", NET2GEOJSON]
        net2geo_options += ["-n", os.path.join(Path(SIMULATION_FOLDER), "{}.net.xml".format(PREFIX))]
        net2geo_options += ["-o", os.path.join(Path(SIMULATION_FOLDER), "net.geojson")]
        # net2geo_options += ["--lanes"]
        net2geo_options += ["--internal"]

        try:
            logger.info("Calling net2geojson.py with options: {}".format(net2geo_options))
            subprocess.run(net2geo_options, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            logger.error("Error while running net2geojson.py: {}".format(e.stderr))

        with open(os.path.join(Path(SIMULATION_FOLDER), "net.geojson"), "r") as f:
            geojson = f.read()
            data = {}
            data["type"] = "network"
            data["content"] = json.loads(geojson)
            payload = json.dumps(data)
            payload_len = len(bytes(payload, 'utf-8'))
            __debug_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))

    # Stats
    step_times = []

    # Read whole network
    net = sumolib.net.readNet(os.path.join(Path(SIMULATION_FOLDER), "{}.net.xml".format(PREFIX)))
    edges = net.getEdges(False)
    
    res_folder = os.path.join(SIMULATION_FOLDER, "simulation_results")
            
    logger.info("Starting normal SUMO simulation")
    sumo_cmd = [SUMO, "-c", configuration_file, "-r", route_file, "--start", "--no-warnings", "--lanedata-output", os.path.join(Path(res_folder), "lanedata.out")]
    sumo_cmd += ["--time-to-teleport.remove"]
    if (flags & TRIP_LENGTH_INFO) == TRIP_LENGTH_INFO:
        sumo_cmd += ["--vehroute-output.route-length", "--vehroute-output", os.path.join(Path(res_folder), "tripinfo.out")]
    if (flags & LOAD_INFO) == LOAD_INFO:
        sumo_cmd += ["--summary", os.path.join(Path(res_folder), "summary.out")]
        sumo_cmd += ["--fcd-output", os.path.join(Path(res_folder), "dump.out")]

    traci.start(sumo_cmd)

    current_step = 0
    vehicles_load = []
    vehicles_delay = []
    departs = []
    
    accidents_edges =  ['127693383#3', '1021481768#0', '5670254#3', '1088321124#0', '5671774#4', '1088320654#0', '32936741', '456308943#0', '954178925#0', 
                        '1266830893#0', '438381568#2', '989267825#0', '1046750793#0', '847952489#0', '986615540#0']
    
    traci.simulation.subscribe([tc.VAR_DEPARTED_VEHICLES_IDS])
    with timed_block("simulation_time"):
        while current_step < simulation_time:
            with timed_block("step_times", mode = "append"):
                current_step += 1
                try:
                    traci.simulationStep()
                    # logger.info("Simulation step {}".format(current_step))
                except traci.exceptions.FatalTraCIError as e:
                    logger.info("Exiting simulation...")
                    break

                ## Simulate Accident
                # if current_step == 200:
                #     # random_edge = edges[random.randint(0, len(edges) - 1)].getID()
                #     for random_edge in accidents_edges:
                #         logger.info("Causing accident on edge {}".format(random_edge))
                #         for inc_edge in net.getEdge(random_edge).getIncoming():
                #             for lane in inc_edge.getLanes():
                #                 logger.info("Causing accident on lane {}".format(lane.getID()))
                #                 # self.logger.info("Lane ID: {}".format(lane.getID()))
                #                 cause_accident(traci, current_step, inc_edge.getID(), lane.getID().split("_")[1], duration = 3000.0, debug_socket = __debug_socket)

                if (flags & LOAD_INFO) == LOAD_INFO:
                    vehicles_load.append(traci.vehicle.getIDCount())

                departed = traci.simulation.getAllSubscriptionResults()[""][tc.VAR_DEPARTED_VEHICLES_IDS]
                for vehicle_id in departed:
                    departs.append((vehicle_id, current_step, -1))

                if __debug_socket is not None:
                    # self.logger.info("Sending vehicles information to server...")
                    data = {}
                    data_content = {}
                    data_content["timestep"] = current_step
                    data_content["vehicles"] = getSimulationData()
                    
                    data["type"] = "vehicles_single"
                    data["content"] = data_content
                    payload = json.dumps(data)
                    payload_len = len(bytes(payload, 'utf-8'))
                    __debug_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))
    
    traci.close()

    stats = get_thread_stats().copy()
    stats["id"] = "no-partitioned-sumo"
    # stats["simulation_time"] = dec_stats["simulation_time"]
    # stats["step_times"] = step_times
    stats["step_times_avg"] = sum(stats["step_times"]) / len(stats["step_times"])
    stats["num_part"] = 1
    stats["last_sim_step"] = current_step
    if (flags & LOAD_INFO) == LOAD_INFO:
        stats["vehicles_load"] = vehicles_load
    logger.info("Simulation time: {}".format(stats["simulation_time"]))

    _max = max(stats["step_times"])
    idx_max = stats["step_times"].index(_max)
    logger.info("AVG: {:.03f}".format(stats["step_times_avg"]))
    logger.info("MAX: {}".format(_max))
    logger.info("MAX VEH: {}".format(stats["vehicles_load"][idx_max] if (flags & LOAD_INFO) == LOAD_INFO else "N/A"))

    # logger.info(departs)

    return stats


if __name__ == "__main__":
    stats = run_simulation(CONFIGURATION_FILE, ROUTE_FILE, simulation_time = SIMULATION_TIME, flags = LOAD_INFO)

    # logger.info("####### STATS #######")
    # logger.info("\tID: {}".format(stats["id"]))
    # logger.info("\tSimulation time (s): {:.03f}".format(stats["simulation_time"]))
    # logger.info("\tAverage step time (s): {:.03f}".format(stats["step_times_avg"]))
    # logger.info("#####################")
    
    import time
    import pandas as pd
    from pathlib import Path
    LOG_FOLDER = os.getenv("LOG_FOLDER", os.path.join(Path(os.path.abspath(__file__)).parent.parent, "logs"))
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    stats_filename = f"stats_1part_{timestamp}.csv"
    df = pd.DataFrame(stats)
    df.to_csv(os.path.join(LOG_FOLDER, stats_filename), index=False)