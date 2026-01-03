import os
import sys
import sumolib
import subprocess
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import contextily as cx


from pathlib import Path
from log.logger import logger
from utils import check_shared_edges

SUMO_HOME = os.environ.get("SUMO_HOME")
TOOLS_FOLDER = os.path.join(SUMO_HOME, "tools")
MANAGEMENT_FOLDER = os.path.join(Path(os.path.abspath(__file__)).parent.parent.parent, "management_folder")

NETCONVERT = sumolib.checkBinary('netconvert')

CONTEXTILY_PROVIDER = cx.providers.OpenStreetMap.Mapnik


def generate_partition_map(net, partitions):
    # Check shared edges
    _, shared_edges = check_shared_edges(partitions)
    for edge in shared_edges:
        logger.info("Shared edge: {}".format(edge))

    partitions_data = []

    for part_idx, net_part in enumerate(partitions):
        logger.info("Creating net file for partition {}...".format(part_idx))
        net_part = os.path.abspath(os.path.join(Path(net).parent, "part{}.net.xml".format(part_idx)))
        netconvert_options = [NETCONVERT]
        netconvert_options += ("--keep-edges.input-file", os.path.join(MANAGEMENT_FOLDER, "edgesPart{}.txt".format(part_idx)))
        netconvert_options += ("-s", net)
        netconvert_options += ("-o", net_part)

        # Create partitioned network file
        try:
            logger.info("Calling NETCONVERT with options: {}".format(netconvert_options))
            subprocess.run(netconvert_options, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            logger.error("Error while running NETCONVERT: {}".format(e.stderr))
            sys.exit()

        # Create GeoJSON of the whole network
        net2geo_options = ["python", os.path.join(TOOLS_FOLDER, "net", "net2geojson.py")]
        net2geo_options += ["-n", net_part]
        net2geo_options += ["-o", os.path.join(Path(net).parent, "net{}.geojson".format(part_idx))]
        # net2geo_options += ["--lanes"]
        net2geo_options += ["--internal"]

        try:
            logger.info("Calling net2geojson.py with options: {}".format(net2geo_options))
            subprocess.run(net2geo_options, check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            logger.error("Error while running net2geojson.py: {}".format(e.stderr))
            sys.exit()

        # Read GeoJSON with geopandas
        net_gdf = gpd.read_file(os.path.join(Path(net).parent, "net{}.geojson".format(part_idx)), geometry="geometry", epsg=4326)
        net_gdf["partition"] = "Partition {}".format(part_idx)
        partitions_data.append(net_gdf)


    merged_gdf = gpd.GeoDataFrame(pd.concat(partitions_data, ignore_index=True))
    cut_edges_gdf = merged_gdf.loc[merged_gdf["id"].isin(shared_edges)]

    logger.info("\n{}".format(merged_gdf.head()))
    logger.info("\n{}".format(merged_gdf.dtypes))
    logger.info("\n{}".format(cut_edges_gdf.head()))
    logger.info("\n{}".format(cut_edges_gdf.dtypes))

    # Generate chart
    merged_gdf = merged_gdf.to_crs(epsg=3857)
    cut_edges_gdf = cut_edges_gdf.to_crs(epsg=3857)
    plt.figure(figsize=(15, 15))
    ax = merged_gdf.plot(column="partition", legend=True, cmap="tab10", linewidth=1.0, legend_kwds={"loc": "upper center", "bbox_to_anchor": (0.5, 1.10), "ncols": 5})
    cut_edges_gdf.plot(ax=ax, color="black", label="Shared edges")
    cx.add_basemap(ax, attribution_size=0, source=CONTEXTILY_PROVIDER, alpha=0.7)
    ax.set_axis_off()

    plt.tight_layout()
    plt.savefig(os.path.join(Path(net).parent, "partition_map.pdf"), dpi=300, bbox_inches='tight')