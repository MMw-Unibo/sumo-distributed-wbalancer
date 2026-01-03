import os
import logging
from pathlib import Path

logging.basicConfig(
    level=os.getenv("LOGGING_LEVEL", "INFO"),
    format='%(asctime)s [%(levelname)s] [%(module)s]: %(message)s'
)

# Root logger
logger = logging.getLogger('')

# Partition logger
partition_logger = logging.getLogger('partition_logger')
partition_logger.propagate = False
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(module)s - %(partition_id)s]: %(message)s'))
file_handler = logging.FileHandler(os.path.join(Path(os.path.abspath(__file__)).parent, "partition.log"), 'w+')
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(module)s - %(partition_id)s]: %(message)s'))
partition_logger.addHandler(console_handler)
partition_logger.addHandler(file_handler)

logger.info("-----------------------------------")
logger.info("Logger initialized with level {}".format(os.getenv("LOGGING_LEVEL", "INFO")))