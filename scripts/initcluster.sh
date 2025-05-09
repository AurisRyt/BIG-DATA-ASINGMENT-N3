#!/bin/bash

# Wait for all containers to be ready
echo "Waiting for MongoDB containers to start..."
sleep 30

# Initialize config server replica set
echo "Initiating config server replica set..."
mongosh --host config-server-1 --port 27017 --eval '
rs.initiate({
  _id: "configserver",
  configsvr: true,
  members: [
    { _id: 0, host: "config-server-1:27017" },
    { _id: 1, host: "config-server-2:27017" },
    { _id: 2, host: "config-server-3:27017" }
  ]
})'

sleep 20

# Initialize shard1 replica set
echo "Initiating shard1 replica set..."
mongosh --host shard1-1 --port 27017 --eval '
rs.initiate({
  _id: "shard1",
  members: [
    { _id: 0, host: "shard1-1:27017" },
    { _id: 1, host: "shard1-2:27017" },
    { _id: 2, host: "shard1-3:27017" }
  ]
})'

# Initialize shard2 replica set
echo "Initiating shard2 replica set..."
mongosh --host shard2-1 --port 27017 --eval '
rs.initiate({
  _id: "shard2",
  members: [
    { _id: 0, host: "shard2-1:27017" },
    { _id: 1, host: "shard2-2:27017" },
    { _id: 2, host: "shard2-3:27017" }
  ]
})'

sleep 60

# Add shards to the mongos router
echo "Adding shards to mongos..."
mongosh --host mongos --port 27017 --eval 'sh.addShard("shard1/shard1-1:27017,shard1-2:27017,shard1-3:27017")'
mongosh --host mongos --port 27017 --eval 'sh.addShard("shard2/shard2-1:27017,shard2-2:27017,shard2-3:27017")'

# Enable sharding on database and collections
echo "Enabling sharding on vesselDB..."
mongosh --host mongos --port 27017 --eval 'sh.enableSharding("vesselDB")'
mongosh --host mongos --port 27017 --eval 'db = db.getSiblingDB("vesselDB"); db.createCollection("raw_data"); db.createCollection("filtered_data");'
mongosh --host mongos --port 27017 --eval 'sh.shardCollection("vesselDB.raw_data", {MMSI: "hashed"})'
mongosh --host mongos --port 27017 --eval 'sh.shardCollection("vesselDB.filtered_data", {MMSI: "hashed"})'

echo "MongoDB Sharded Cluster Setup Complete"