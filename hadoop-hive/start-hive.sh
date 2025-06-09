#!/bin/bash

# Initialize HDFS directories for Hive
echo "Initializing HDFS directories for Hive..."
docker exec -it $(docker-compose ps -q namenode) bash -c "
    hdfs dfs -mkdir -p /tmp
    hdfs dfs -mkdir -p /user/hive/warehouse
    hdfs dfs -chmod g+w /tmp
    hdfs dfs -chmod g+w /user/hive/warehouse
"

# Initialize Hive schema
echo "Initializing Hive metastore schema..."
docker exec -it $(docker-compose ps -q hive-metastore) bash -c "
    schematool -dbType postgres -initSchema
"

echo "Cluster initialization complete!"
echo ""
echo "Web UIs available at:"
echo "- Hadoop NameNode: http://localhost:9870"
echo "- YARN ResourceManager: http://localhost:8088" 
echo "- DataNode 1: http://localhost:9864"
echo "- DataNode 2: http://localhost:9865"
echo "- DataNode 3: http://localhost:9866"
echo "- YARN NodeManager: http://localhost:8042"
echo "- Hive Server2: http://localhost:10002"
echo ""
echo "Service endpoints:"
echo "- HDFS NameNode: hdfs://localhost:8020"
echo "- Hive Metastore: thrift://localhost:9083"
echo "- HiveServer2: jdbc:hive2://localhost:10000"
echo ""
echo "To connect to Hive CLI:"
echo "docker exec -it hive-server /opt/hive/bin/hive"
echo ""
echo "To connect to Beeline:"
echo "docker exec -it hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000"
