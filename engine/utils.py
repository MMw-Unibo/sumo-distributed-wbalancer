import ast
import json
import pickle
import select
from log.logger import logger

def check_shared_edges(partitions):
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
    raw_shared_edges = set()
    for i in range(len(partitions)):
        for element in partitions[i]:
            for j in range(len(partitions)):
                if j > i and element in partitions[j]:
                    raw_shared_edges.add(element)
                    if i not in shared_edges:
                        shared_edges[i] = []
                    if j not in shared_edges:
                        shared_edges[j] = []

                    shared_edges[i].append(element)
                    shared_edges[j].append(element)


    logger.info("Shared edges: {}".format(raw_shared_edges))
    logger.info("Shared edges per partitions: {}".format(shared_edges))

    return shared_edges, raw_shared_edges


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    if obj is None:
        return ""
    raise TypeError

# def send_big_data_to_socket(socket, target, data, id="none", threshold = 40000):
#         byte_data = (json.dumps(data, default=set_default)).encode('utf-8')
#         data_size = len(byte_data)
#         # self.logger.info("--- {}".format(data_size))
#         # self.logger.info("{}.{} - Sending data to partition {} in ts {}".format(stack[2].function, stack[2].lineno, target, self.current_step))
#         if data_size > threshold:
#             chunks = [byte_data[i:i + threshold] for i in range(0, data_size, threshold)]
#             for idx, chunk in enumerate(chunks):
#                 decoded_chunk = chunk.decode('utf-8')
#                 more = 1 if (idx < len(chunks) - 1) else 0
#                 final_msg = "{}%{}%{}".format(id, more, decoded_chunk)
#                 # self.logger.info(final_msg)
#                 # self.logger.info("Sending chunk to partition {} in ts {}".format(target, self.current_step))
#                 socket.sendto(pickle.dumps(final_msg), target)
#         else:
#             logger.info("Sending whole data to partition {}".format(target))
#             socket.sendto(pickle.dumps("{}%{}%{}".format(id, 0, data)), target)

def send_big_data_to_socket(sock, target, data, id, threshold = 1024 * 40):
    """
    Splits a dictionary into chunks if it exceeds the threshold and sends it over a socket.
    Each message follows the format: <id>%<flag>%<chunk>
    
    Args:
        sock: socket object
        target: tuple (ip, port)
        data_dict: dictionary to be sent
        id: message id
        threshold: maximum size of each message
    """
    serialized_data = pickle.dumps(data)  # Serialize dictionary
    data_size = len(serialized_data)
    
    if data_size <= threshold:
        message_id = id
        message = f"{message_id}%0%".encode() + serialized_data
        sock.sendto(message, target)
    else:
        message_id = id
        chunk_size = threshold - 50  # Adjusting for metadata overhead
        for i in range(0, data_size, chunk_size):
            # logger.info("Sending chunk to {}".format(target))
            chunk = serialized_data[i:i + chunk_size]
            flag = 0 if i + chunk_size >= data_size else 1
            message = f"{message_id}%{flag}%".encode() + chunk
            sock.sendto(message, target)


def receive_msgs(socket, num_msg, timeout = None):
    buffer = {}
    msgs = []
    num_msg_received = 0
    already_received = set()

    # logger.info("Waiting for {} messages...".format(num_msg))
    while num_msg_received < num_msg:
        readable, _, _ = select.select([socket], [], [], timeout)

        if not readable and timeout == 0:
            return []

        if socket in readable:
            data = socket.recv(1024 * 60)
            header_end = data.index(b'%', 0)  # Find first '%'
            second_percent = data.index(b'%', header_end + 1)  # Find second '%'
            
            header = data[:second_percent].decode()
            chunk = data[second_percent + 1:]
            message_id, more = header.split('%')
            if message_id not in buffer:
                buffer[message_id] = b''
            buffer[message_id] += chunk

            if message_id in already_received:
                logger.info("Message from {} already received".format(message_id))

            if more == '0':
                # logger.info("Received message with id: {}".format(message_id))
                try:
                    final_message = pickle.loads(buffer.pop(message_id))
                    num_msg_received += 1
                    # yield final_message   # This is a generator
                    msgs.append(final_message)
                    already_received.add(message_id)
                except Exception as e:
                    logger.error(f"Error while unpickling message {message_id}: {e}\n")

    return msgs


accidents_idx = 0
def cause_accident(traci, step, edgeID, laneID, duration = 1000.0, debug_socket = None):
    global accidents_idx
    crashed_vehid = "111999111{}".format(accidents_idx)
    crashed_road = edgeID
    crashed_lane = laneID
    route_id = "crashed_route_{}".format(accidents_idx)
    length = traci.lane.getLength("{}_{}".format(crashed_road, crashed_lane))
    traci.route.add(route_id, [crashed_road])
    logger.info("Vehicle {} is going to crash on edge {} lane {} at pos {} at ts {}".format(crashed_vehid, crashed_road, crashed_lane, length/2, step + 1))
    traci.vehicle.add(crashed_vehid, route_id, depart=step + 1, departLane=crashed_lane, departSpeed=0, departPos=length/2)
    traci.vehicle.setStop(crashed_vehid, crashed_road, pos=length/2, duration=duration, laneIndex = crashed_lane)

    accidents_idx += 1

    if debug_socket:
        shape = traci.lane.getShape(crashed_road + "_" + crashed_lane)
        lon, lat = traci.simulation.convertGeo(*shape[int(len(shape)/2)])
        data = {}
        data_content = {}
        data_content["timestep"] = step
        data_content["edgeID"] = edgeID
        data_content["laneID"] = laneID
        data_content["lat"] = lat
        data_content["lon"] = lon
        data_content["duration"] = duration
        
        data["type"] = "accident"
        data["content"] = data_content
        payload = json.dumps(data)
        payload_len = len(bytes(payload, 'utf-8'))
        debug_socket.sendall(("{}%{}".format(payload_len, payload)).encode('utf-8'))