package it.unisa.hpc.spark.airbnbAnalysis;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import scala.Tuple2;

public class AirbnbAnalysis {
    
    private static String[] parseCSVLine(String line) {
        java.util.List<String> result = new java.util.ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean insideQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                // Gestione virgolette: cambia lo stato inside/outside
                insideQuotes = !insideQuotes;
            } else if (c == ',' && !insideQuotes) {
                // Virgola fuori dalle virgolette: fine campo
                result.add(currentField.toString());
                currentField = new StringBuilder();
            } else {
                // Carattere normale: aggiungilo al campo corrente
                currentField.append(c);
            }
        }
        // Aggiungi l'ultimo campo
        result.add(currentField.toString());
        
        return result.toArray(new String[0]);
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Atteso: AirbnbAnalysis <input-file> <output-path>");
            System.exit(1);
        }
        
        String inputFile = args[0];
        String outputPath = args[1];

        // Configurazione Spark e del contesto Spark
        SparkConf conf = new SparkConf().setAppName("Airbnb analisi dei prezzi");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Leggo il file di input
        JavaRDD<String>  lines = sc.textFile(inputFile);
        
        // Rimuovo l'header
        String header = lines.first();
        JavaRDD<String> airbnbRDD = lines.filter(line -> !line.equals(header));

        // TRASFORMAZIONE 1: filter
        // Applica una trasformazione di filtro per selezionare solo le righe 
        // con un prezzo valido (prezzo > 0) e struttura corretta
        JavaRDD<String> validListingsRDD = airbnbRDD.filter(listing -> {
            String[] fields = parseCSVLine(listing);
            // Controllo che ci siano abbastanza campi e che il prezzo sia numerico
            if (fields.length <= 9) return false;
            try {
                double price = Double.valueOf(fields[9]);
                return price > 0;
            } catch (NumberFormatException e) {
                return false;
            }
        });

        // TRASFORMAZIONE 2: mapToPair
        // Crea un PairRDD dove ogni coppia contiene come chiave neighbourhood_group e come valore, l'intera riga dell'annuncio
        JavaPairRDD<String, String> neighbourhoodListingsRDD = validListingsRDD.mapToPair(listing -> {
            String neighbourhoodGroup;
            Tuple2<String, String> pair;
            // Separo la riga in campi, estraggo il neighbourhood_group dal campo in posizione 4 e se non esiste uso "Unknown" 
            // come default
            String[] fields = parseCSVLine(listing);
            neighbourhoodGroup = (fields.length > 4) ? fields[4] : "Unknown";
            pair = new Tuple2<String, String>(neighbourhoodGroup, listing);
            return pair;
        });

        // TRASFORMAZIONE 3: groupByKey
        // Crea una coppia (neighbourhood_group, lista di tutti gli annunci associati a quel quartiere) e lo fa per ogni quartiere
        // Raggruppa i dati per chiave
        JavaPairRDD<String, Iterable<String>> groupedByNeighbourhoodRDD = neighbourhoodListingsRDD.groupByKey();

        // TRASFORMAZIONE 4: mapValues
        // Per ogni quartiere, fa il conteggio totale degli annunci e calcola sia il prezzo medio che quello minimo e 
        // per entrambi fornisce il nome dell'annuncio corrispondente
        JavaPairRDD<String, String> neighbourhoodStatsRDD = groupedByNeighbourhoodRDD.mapValues(listings -> {
            int count = 0;
            double totalPrice = 0.0;
            double minPrice = Double.MAX_VALUE;
            double maxPrice = Double.MIN_VALUE;
            String minPriceListing = "";
            String maxPriceListing = "";

            for (String listing : listings) {
                String[] fields = parseCSVLine(listing);
                if (fields.length <= 9) continue; // ignora righe malformate
                double price;
                try {
                    price = Double.valueOf(fields[9]);
                } catch (NumberFormatException e) {
                    continue; // Salto gli annunci con prezzo non valido
                }
                
                count++;
                totalPrice += price;
                // minimo
                if (price < minPrice) {
                    minPrice = price;
                    minPriceListing = fields[1]; // nome annuncio
                }
                // massimo
                if (price > maxPrice) {
                    maxPrice = price;
                    maxPriceListing = fields[1]; 
                }
            }
            // media
            double avgPrice = totalPrice / count;

            // Formatto la stampa dell'output
            StringBuilder stats = new StringBuilder();
            stats.append("Numero annunci: ").append(count).append("\n");
            stats.append("Prezzo medio: ").append(String.format("%.2f", avgPrice)).append("$\n\n");
            stats.append("Annuncio più economico: \"").append(minPriceListing).append("\"\n");
            stats.append("Costo: ").append(String.format("%.2f", minPrice)).append("$\n");
            stats.append("Annuncio più costoso: \"").append(maxPriceListing).append("\"\n");
            stats.append("Costo: ").append(String.format("%.2f", maxPrice)).append("$\n");
            stats.append("---------------------------------------------------------------------------------------------------");

            return stats.toString();
        });

        // TRASFORMAZIONE 5: sortByKey
        // Ordina i risultati in ordine alfabetico per nome del quartiere
        JavaPairRDD<String, String> sortedNeighbourhoodStatsRDD = neighbourhoodStatsRDD.sortByKey();

        // TRASFORMAZIONE 6: map
        // Formatta ogni entry aggiungendo un'intestazione con il nome del quartiere prima delle statistiche
        JavaRDD<String> formattedOutputRDD = sortedNeighbourhoodStatsRDD.map(entry -> {
            StringBuilder output = new StringBuilder();
            output.append("Quartiere: ").append(entry._1).append("\n");
            output.append(entry._2);
            return output.toString();
        });

        // Creo un RDD con l'intestazione generale del report
        JavaRDD<String> headerRDD = sc.parallelize(java.util.Arrays.asList("Analisi dei prezzi per quartiere:\n"));
        
        // Combino l'intestazione con i risultati. coalesce(1) mi fa avere un solo file di output
        JavaRDD<String> finalOutputRDD = headerRDD.union(formattedOutputRDD).coalesce(1);

        // AZIONE: salva il risultato nella cartella di output
        finalOutputRDD.saveAsTextFile(outputPath);

        // Chiudo lo Spark context
        sc.close();
    }
        
    }
    
    