# Sprawdzona konfiguracja hadoopa ze sparkiem

z tym wywołaniem kontenery dostają miejsce na dysku a nie w pamięci. Ja tu dodaję miejsce na dysku D.

``` bash
# locally

./compose-up.sh 3.3.0 3 \
  /host_mnt/d/hadoop-cluster/hadoop-data \
  /host_mnt/d/hadoop-cluster/hadoop-logs \
  /host_mnt/d/hadoop-cluster/hbase-logs \
  /host_mnt/d/hadoop-cluster/hive-logs \
  /host_mnt/d/hadoop-cluster/sqoop-logs \
  mariadb \
  /host_mnt/d/hadoop-cluster/mariadb-data

./hadoop-start.sh start

# on master container
echo "export SPARK_HOME=/usr/local/spark" >> ~/.bashrc
echo "export PATH=$PATH:$SPARK_HOME/bin" >> ~/.bashrc
echo "export LD_LIBRARY_PATH=$PATH:$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH" >> ~/.bashrc
cd /usr/local/

rm -rf spark

wget https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz

tar -xzf spark-3.5.6-bin-hadoop3.tgz
mv spark-3.5.6-bin-hadoop3 spark

cp /usr/local/spark/conf/spark-defaults.conf.template /usr/local/spark/conf/spark-defaults.conf
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
export LD_LIBRARY_PATH=$PATH:$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH
# (optional) test run
$SPARK_HOME/bin/spark-shell --master yarn --deploy-mode=client
```