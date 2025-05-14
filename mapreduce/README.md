# MapReduce

1. Postaw i uruchom kontenery Hadoopa (z instrukcji).


2. Uruchom skrypt akwizycji danych (z brancha ``local`` -> ``main.py``). Potrzebny będzie plik z kluczem do API Kaggle, który musisz sobie wygenerować i zapisać w ``~/.kaggle/kaggle.json``.


3. To sobie pomuli trochę (w zależności od internetu). Potem (albo równolegle) stwórz plik .jar dla zadania MapReduce (w katalogu ``mapreduce`` wykonaj komendę ``mvn clean package``).


4. Utworzony plik .jar prześlij na kontener ``master``:

   ```docker cp [NAZWA_PLIKU_JAR] master:/[KATALOG_DOCELOWY_DOWOLNY]```


5. Jak już będziesz miał pliki z danymi w HDFS, to możesz uruchomić zadanie:

   ```docker exec master hadoop jar [KATALOG_Z_JAR] /data/kaggle/joint_data_2017-2023 /data/kaggle/stations_metadata.csv /data/output/```

   Po wykonaniu logi pojawią się w terminalu. Rezultaty będą widoczne w HDFS w katalogu ``data/output`` - 3 pliki, z racji na dodane 3 Reducery (można to modyfikować w klasie ``PollutantJoiningDriver.java``).