# Sprawdzona konfiguracja hadoopa ze sparkiem
Obraz na podstawie konfiguracji z ```hjben/hadoop-eco/docker-scripts/compose-up.sh```. W celu uruchomienia sparka dodano odpowiednie porty. Cała konfiguracja dostępne w załączonym pliku ```compose-up.sh```. Z tym wywołaniem kontenery dostają miejsce na dysku a nie w pamięci. Ja tu dodaję miejsce na dysku D.

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

# Uruchomienie procesów sparka

1. Zbudować jar'a - ```mvn clean package```
2. Wysłać jar'a na kontener - ```docker cp .\target\spark-aggregate-1.0-SNAPSHOT.jar master:/tmp/```
3. Na kontenerze uruchomić kolejno procesy:
```bash
spark-submit --class org.example.AggregateMetadataJob --master yarn --deploy-mode client /tmp/spark-aggregate-1.0-SNAPSHOT.jar
```
```bash
spark-submit --class org.example.WeatherJoinJob --master yarn --deploy-mode client /tmp/spark-aggregate-1.0-SNAPSHOT.jar
```
bez PATH'a dodać ewentualnie ```$SPARK_HOME/bin/spark-shell```


