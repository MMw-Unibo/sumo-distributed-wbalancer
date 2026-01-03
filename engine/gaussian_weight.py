import os
import math
import sumolib
import argparse
import xml.etree.ElementTree as ET

from pathlib import Path


SIM_FOLDER = os.getenv("SIM_FOLD", os.path.join(Path(os.path.abspath(__file__)).parent.parent, "simulations", "traci_test_sim"))

# Arguments definition
parser = argparse.ArgumentParser(description='Generate weight file with gaussian distribution')
parser.add_argument('--sim', type=str, default=os.path.join(Path(os.path.abspath(__file__)).parent.parent, "simulations", "traci_test_sim"), help='Path to simulation folder')
parser.add_argument('--edge', type=str, action="append", help='Edge at the center of the gaussian distribution')
parser.add_argument('--mu', type=float, default=0, help='Mean value for the gaussian distribution')
parser.add_argument('--sigma', type=float, default=200, help='Standard deviation for the gaussian distribution')
parser.add_argument('--density', type=int, default=20, help='Insertion density to generate traffic')
parser.add_argument('--wduarouter', action="store_true", default=False, help='Use Durarouter to remove invalid trips')
parser.add_argument('--output', type=str, default="random_trips_3600_id20_weighted.rou.xml", help='Output file name')


def getDistance(node1, node2):
    return math.sqrt((node1[0] - node2[0])**2 + (node1[1] - node2[1])**2)


def getLaneCenter(lane):
    lane_nodes = lane.getShape()
    lane_nsegments = len(lane_nodes) - 1
    lane_length = lane.getLength()

    cum_distance = 0
    prev_cum_distance = 0
    for segment in range(lane_nsegments):
        start_node = lane_nodes[segment]
        end_node = lane_nodes[segment + 1]
        distance = getDistance(start_node, end_node)
        cum_distance += distance
        if cum_distance > lane_length / 2:
            relative_distance = (lane_length / 2) - prev_cum_distance
            interpolation_coeff = relative_distance / distance
            interpolated_node = (
                start_node[0] + interpolation_coeff * (end_node[0] - start_node[0]),
                start_node[1] + interpolation_coeff * (end_node[1] - start_node[1])
            )
            return interpolated_node
        
    return lane_nodes[-1]
        
def gaussian_weight(x, mu, sigma):
    return math.exp(-((x - mu) ** 2) / (2 * sigma ** 2))

def generatePoiFile(filename, net, pois):
    additional = ET.Element('additional', {
        'xmlns:xsi': "http://www.w3.org/2001/XMLSchema-instance",
        'xsi:noNamespaceSchemaLocation': "http://sumo.dlr.de/xsd/additional_file.xsd"
    })

    for i, mid_point in enumerate(pois):
        poi = ET.SubElement(additional, 'poi', {
            'id': str(mid_point[0]),
            'x': str(mid_point[1][0]),
            'y': str(mid_point[1][1]),
            'color': "255,0,0"
        })

    tree = ET.ElementTree(additional)
    tree.write(os.path.join(SIM_FOLDER, filename), encoding='UTF-8', xml_declaration=True)

    print(f"Generated file {filename} with {len(mid_points)} POIs")


def generateWeightFile(filename, net, weights):
    edgedata = ET.Element('edgedata')
    interval = ET.SubElement(edgedata, 'interval', {'id': 'all', 'begin': '0', 'end': '3600'})
    for edge_id in [edge.getID() for edge in net.getEdges()]:
        edge = ET.SubElement(interval, 'edge', {
            'id': edge_id
        })

        for lane_id in [lane.getID() for lane in net.getEdge(edge_id).getLanes()]:
            lane = ET.SubElement(edge, 'lane', {
                'id': lane_id,
                'traveltime': str(weights[lane_id]),
            })

    tree = ET.ElementTree(edgedata)
    ET.indent(tree, space="\t", level=0)
    tree.write(os.path.join(SIM_FOLDER, filename), encoding='UTF-8', xml_declaration=True, method="xml")

    print(f"Generated file {filename} with {len(weights)} weights")


def generateWeightFileWEdgeProb(filename, net, weights, simfolder):
    edgedata = ET.Element('edgedata')
    interval = ET.SubElement(edgedata, 'interval', {'id': 'all', 'begin': '0', 'end': '3600'})
    for edge_id in [edge.getID() for edge in net.getEdges()]:
        edge = ET.SubElement(interval, 'edge', {
            'id': edge_id,
            'value': str(weights[edge_id]),
        })

    tree = ET.ElementTree(edgedata)
    ET.indent(tree, space="\t", level=0)
    tree.write(os.path.join(simfolder, filename), encoding='UTF-8', xml_declaration=True, method="xml")

    print(f"Generated file {filename} with {len(weights)} weights")

def generateWeightFileWEdgeTravelTime(filename, net, weights, simfolder):
    edgedata = ET.Element('edgedata')
    interval = ET.SubElement(edgedata, 'interval', {'id': 'all', 'begin': '0', 'end': '3600'})
    for edge_id in [edge.getID() for edge in net.getEdges()]:
        edge = ET.SubElement(interval, 'edge', {
            'id': edge_id,
            'traveltime': str(weights[edge_id] * 100),
        })

    tree = ET.ElementTree(edgedata)
    ET.indent(tree, space="\t", level=0)
    tree.write(os.path.join(simfolder, filename), encoding='UTF-8', xml_declaration=True, method="xml")

    print(f"Generated file {filename} with {len(weights)} weights")

def generateRouteFileWWeights(route_filename, weight_filename, simfolder, density):
    command = f"cd {simfolder} && python {os.getenv('SUMO_HOME')}/tools/randomTrips.py -n {os.path.join(simfolder, 'osm.net.xml')} -e 3600 -o {route_filename} --random --insertion-density {density} --weights-prefix {weight_filename.split('.')[0]}"
    os.system(command)

def clean_routes(route_filename, simfolder):
    print(f"Cleaning routes with Duarouter ...")
    command = f"cd {simfolder} && duarouter --ignore-errors -n {os.path.join(simfolder, 'osm.net.xml')} --route-files {route_filename} -o {route_filename.split('.')[0]}_cleaned.rou.xml --write-trips"
    os.system(command)


if __name__ == "__main__":
    args = parser.parse_args()
    print(args)

    net = sumolib.net.readNet(os.path.join(args.sim, "osm.net.xml"))
    filename_poi = "gaussian_weight.poi.xml"
    filename_edgeprob = "gaussian_weight.src.xml"
    filename_edgetraveltime = "gaussian_weight.add.xml"
    filename_route = args.output


    mid_points = []
    weights = {}
    for edge in net.getEdges():
        lane = edge.getLanes()[0]
        lane_center = getLaneCenter(lane)

        weight = 0
        for target_edge in args.edge:
            target_lane = net.getEdge(target_edge).getLanes()[0]
            target_center = getLaneCenter(target_lane)
            distance = getDistance(target_center, lane_center)

            weight += gaussian_weight(distance, args.mu, args.sigma)
        weights[edge.getID()] = weight

        # print(f"Edge {edge.getID()} Lane {lane.getID()} Center {lane_center} Distance {distance} Weight {weight}")

    generateWeightFileWEdgeProb(filename_edgeprob, net, weights, args.sim)
    generateWeightFileWEdgeTravelTime(filename_edgetraveltime, net, weights, args.sim)

    generateRouteFileWWeights(filename_route, filename_edgetraveltime, args.sim, args.density)
    if args.wduarouter:
        clean_routes(filename_route, args.sim)

