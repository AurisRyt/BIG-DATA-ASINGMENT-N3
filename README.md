# BIG-DATA-ASINGMENT-N3
MongoDB Sharded Cluster
Overview
This project implements a fault-tolerant MongoDB sharded cluster using Docker containers. The system processes vessel tracking data, filters noise, and analyzes time differences between data points.
Architecture

2 Shards: Each shard consists of 3 replica nodes for high availability
Config Servers: 3-node replica set for cluster metadata
Mongos Router: Entry point for client connections
Python Worker: Container for running data processing scripts

Components

docker-compose.yml: Defines the complete cluster infrastructure
parallel_insert.py: Imports CSV data into MongoDB using parallel processing
full_filter.py: Filters noise and validates vessel data points
improved_delta_t_vis.py: Analyzes time differences between vessel data points
fault_tolerance_demo.py: Demonstrates cluster resilience during node failures

Usage

Start the cluster: docker-compose up -d
Import data: docker exec -it python-worker python /app/parallel_insert.py
Filter data: docker exec -it python-worker python /app/full_filter.py
Run analysis: docker exec -it python-worker python /app/improved_delta_t_vis.py
Test fault tolerance: docker exec -it python-worker python /app/fault_tolerance_demo.py
