import os
import socket
import pickle
import select

from log.logger import logger


IP = os.getenv("REGISTRYIP", "0.0.0.0")
REGISTRY_PORT = int(os.getenv("REGISTRYPORT", 1111))


if __name__ == "__main__":

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind((IP, REGISTRY_PORT))
    logger.info("Server UDP socket binded on {}:{}".format(IP, REGISTRY_PORT))

    spawners = []

    while True:
        readable, _, _ = select.select([server_socket], [], [])
        if server_socket in readable:
            msg, add = server_socket.recvfrom(1024 * 60)
            spawn_request = pickle.loads(msg)
            request_type = spawn_request["type"]
            request_data = spawn_request["data"]

            if request_type == "register":
                logger.info("Received register request: {}".format(request_data))
                spawners.append((request_data["ip"], request_data["port"]))
            elif request_type == "unregister":
                logger.info("Received unregister request: {}".format(request_data))
                spawners = [spawner for spawner in spawners if spawner != (request_data["ip"], request_data["port"])]
                logger.info("Remaining spawners: {}".format(spawners))
            elif request_type == "get":
                logger.info("Received get request: {}\nResponding with {} to {}".format(request_data, spawners, add))
                server_socket.sendto(pickle.dumps(spawners), add)