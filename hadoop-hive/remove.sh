#!/bin/bash

echo "Removing Hadoop-Hive cluster..."

docker compose down -v

echo "Cluster removed successfully!"
