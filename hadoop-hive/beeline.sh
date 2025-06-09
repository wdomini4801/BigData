#!/bin/bash

echo "Connecting to beeline..."

docker exec -it hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000
