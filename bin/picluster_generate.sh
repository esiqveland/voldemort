#!/bin/bash

PROJECT_DIR="${HOME}/src/masteroppgave/voldemort"
CONFIG_DIR="${PROJECT_DIR}/config/picluster"

mkdir -p ${CONFIG_DIR}

# number of nodes in cluster
NUM_NODES=2

# number of partitions
PARTITIONS=4

python2.7 generate_cluster_xml.py  \
    --file picluster.hostnames.txt --name picluster  \
    --nodes ${NUM_NODES} --partitions ${PARTITIONS}  \
    --socket-port 6666 --http-port 8081  \
    --output-dir ${CONFIG_DIR}  \
    --current-stores ${CONFIG_DIR}/config/stores.xml

