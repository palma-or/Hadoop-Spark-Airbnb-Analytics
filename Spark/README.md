# Airbnb Analysis - Spark

Per eseguire l'applicazione è necessario utilizzare il cluster Docker utilizzato durante il corso. 

### Prerequisiti
1. Docker Desktop avviato
2. File launch_single.sh e AirbnbAnalysis.jar in /testfiles
3. Dataset airbnb1.csv in /testfiles/input

### Esecuzione

1. **Avviare Docker**
docker compose up -d

2. **Accedere al container**
docker container exec -ti spark-master bash

3. **Spostarsi nella directory testfiles**
cd /testfiles/

4. **Eseguire l'analisi**
./launch_single.sh

5. **Leggere i risultati**
cd output
cat part-00000

### Output Atteso
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