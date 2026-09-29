# Big Data Analysis: Airbnb NYC with Hadoop MapReduce and Apache Spark
This repository contains a distributed processing project focused on analyzing the Airbnb dataset for New York City. The goal is to extract advanced statistical insights while addressing challenges related to handling malformed data, such as the presence of commas within text fields in CSV format. The project leverages two of the main frameworks in the Big Data ecosystem: **Apache Hadoop (MapReduce)** and **Apache Spark**.

## 🧠 Project Architecture

The project is divided into two independent analytical tasks:

1. **Top-K Hosts (Hadoop MapReduce):** Identifies the K hosts with the highest number of listings leveraging the *Job Chaining* metapattern. 
   * The first job performs the aggregated count (*Numerical Summarization* pattern).
   * The second job executes the global ranking (*Top K* pattern).
2. **Neighborhood Statistics (Apache Spark):** Calculates the number of listings, the average price, and identifies the cheapest and most expensive listings for each borough (e.g., Manhattan, Brooklyn). It utilizes a pipeline of transformations on `JavaPairRDD` (such as `filter`, `mapToPair`, `groupByKey`, `mapValues`, and `sortByKey`).

---

## 🛠 Prerequisites

**For the MapReduce environment:**
* Docker Desktop running.
* `hadoop-new` cluster configured.
* The `airbnb1.csv` and `TopHost.jar` files placed in the local `/hddata/airbnb` folder (to be copied depending on the PC configuration).

**For the Spark environment:**
* Docker Desktop running.
* The `launch_single.sh` and `AirbnbAnalysis.jar` files placed in the `/testfiles` folder.
* The `airbnb1.csv` dataset placed in `/testfiles/input`.

---

## 🚀 Execution: Hadoop MapReduce (Top-K Hosts)

1. Start Docker Desktop and the `hadoop-new` cluster containers:
   ```bash
   docker-compose up -d
   # or
   docker container start master slave1 slave2 slave3
   ```
2. Enter the master container:
   ```bash
   docker exec -it master bash
   ```
3. Start HDFS and load the dataset:
   ```bash
   $HADOOP_HOME/sbin/start-dfs.sh
   $HADOOP_HOME/bin/hdfs dfs -put /data/airbnb/airbnb1.csv /airbnb1.csv
   ```
4. Start YARN and run the MapReduce analysis (in the example, K=10 is requested):
   ```bash
   $HADOOP_HOME/sbin/start-yarn.sh
   $HADOOP_HOME/bin/hadoop jar /data/airbnb/TopHost.jar it.unisa.hpc.hadoop.tophost.DriverTopHost /input/airbnb1.csv /intermediate /output 10
   ```
5. Read the intermediate (Job 1) and final (Job 2) results:
   ```bash
   $HADOOP_HOME/bin/hdfs dfs -cat /intermediate/part-r-00000
   $HADOOP_HOME/bin/hdfs dfs -cat /output/part-r-00000
   ```

---

## 🚀 Execution: Apache Spark (Neighborhood Statistics)

1. Start Docker:
   ```bash
   docker compose up -d
   ```
2. Access the Spark container:
   ```bash
   docker container exec -ti spark-master bash
   ```
3. Navigate to the test directory and run the analysis using the dedicated script:
   ```bash
   cd /testfiles/
   ./launch_single.sh
   ```
4. Read the final results:
   ```bash
   cd output
   cat part-00000
   ```

---

## 📊 Expected Output

*(Note: The expected output is presented in Italian, reflecting the actual execution of the code).*

### MapReduce Output (Top 10 Hosts)
```text
I primi 10 host con più appartamenti
Pos.    ID Host         Numero Appartamenti
1       7503643         34
2       417504          28
3       1475015         20
4       13347167        15
5       6885157         11
6       7245581         10
7       1177497         8
8       3038687         7
9       2015914         6
10      2027013         6
```

### Spark Output (Neighborhood Statistics)
```text
Analisi dei prezzi per quartiere:

Quartiere: Bronx
Numero annunci: 60
Prezzo medio: 73.98$
Annuncio più economico: "Very Large Private Room on quiet st"
Costo: 30.00$
Annuncio più costoso: "Yankee Nest"
Costo: 250.00$
---------------------------------------------------------------------------------------------------
Quartiere: Brooklyn
Numero annunci: 2232
Prezzo medio: 154.59$
Annuncio più economico: "$455 Cozy 1bd, BKLYN Sublet March"
Costo: 18.00$
Annuncio più costoso: "Film Location"
Costo: 8000.00$
---------------------------------------------------------------------------------------------------
Quartiere: Manhattan
Numero annunci: 2338
Prezzo medio: 209.47$
Annuncio più economico: "Large furnished 2 bedrooms- - 30 days Minimum"
Costo: 10.00$
Annuncio più costoso: "UWS 1BR w/backyard + block from CP"
Costo: 6000.00$
---------------------------------------------------------------------------------------------------
Quartiere: Queens
Numero annunci: 329
Prezzo medio: 100.72$
Annuncio più economico: "Small Cozy Room Wifi & AC near JFK"
Costo: 29.00$
Annuncio più costoso: "Big Queens NY Apt. Clean & safe."
Costo: 600.00$
---------------------------------------------------------------------------------------------------
Quartiere: Staten Island
Numero annunci: 30
Prezzo medio: 140.47$
Annuncio più economico: "Enjoy Staten Island Hospitality"
Costo: 20.00$
Annuncio più costoso: "Spacious center hall colonial"
Costo: 700.00$
---------------------------------------------------------------------------------------------------
```
