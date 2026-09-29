# Top K Hosts Airbnb - Hadoop MapReduce

Per eseguire l'applicazione è necessario utilizzare il cluster Docker utilizzato durante il corso. 
Copiare i file contenuti in /hddata nella cartella /hddata sul proprio PC.

### Prerequisiti
1. Docker Desktop avviato
2. Cluster hadoop-new configurato
3. File airbnb1.csv e TopHost.jar in /hddata/airbnb

### Esecuzione

1. **Avviare Docker Desktop**

2. **Avviare i container di hadoop-new**
   ```bash
   docker-compose up -d
   # oppure
   docker container start master slave1 slave2 slave3
   ```

3. **Entrare nel container master**
   ```bash
   docker exec -it master bash
   ```

4. **Avviare HDFS**
   ```bash
   $HADOOP_HOME/sbin/start-dfs.sh
   ```

5. **Caricare il dataset in HDFS**
   ```bash
   $HADOOP_HOME/bin/hdfs dfs -put /data/airbnb/airbnb1.csv /airbnb1.csv
   ```

6. **Avviare YARN**
   ```bash
   $HADOOP_HOME/sbin/start-yarn.sh
   ```

7. **Eseguire l'analisi MapReduce**
   ```bash
   $HADOOP_HOME/bin/hadoop jar /data/airbnb/TopHost.jar it.unisa.hpc.hadoop.tophost.DriverTopHost /input/airbnb1.csv /intermediate /output 10
   ```

8. **Leggere i risultati del job1**
   ```bash
  $HADOOP_HOME/bin/hdfs dfs -cat /intermediate/part-r-00000

   ```

9. **Leggere i risultati del job2**
   ```bash
  $HADOOP_HOME/bin/hdfs dfs -cat /output/part-r-00000

   ```
### Output Atteso
```
I primi 10 host con più appartamenti
Pos.    ID Host         Numero Appartamenti
1       7503643         34
2       417504          28
3       1475015         20
4       13347167                15
5       6885157         11
6       7245581         10
7       1177497         8
8       3038687         7
9       2015914         6
10      2027013         6