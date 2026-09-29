package it.unisa.hpc.hadoop.tophost;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *Orlando Palma 0622702433 IZ p.orlando8@studenti.unisa.it
 * D'Aniello Giuseppe gidaniello@unisa.it
 * Esercizio 1 – Map Reduce
 * Trovare i k host con il maggior numero di appartamenti; il parametro k deve essere passato dall’utente.
 */

/**
 * Mapper per il Job 1: Conta gli appartamenti per ogni host
 * 
 * Input: Linea del CSV (LongWritable offset, Text linea)
 * Output: (host_id, 1) per ogni appartamento
 */
public class MapperCount extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private Text hostId = new Text();
    private final static IntWritable one = new IntWritable(1);
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        // Salto l'header 
        if (key.get() == 0) {
            return;
        }
        
        // Parsing 
        String line = value.toString();
        String[] fields = parseCSVLine(line);
        
        // Verifico che ci siano abbastanza campi
        if (fields.length < 4) {
            return; 
        }
        
        // Estraggo host_id (indice 2)
        String host = fields[2].trim();
        
        // Emetto (host_id, 1)
        hostId.set(host);
        context.write(hostId, one);
    }
    
    /**
     * Parser CSV che gestisce le virgolette 
     * Logica:
     * Traccia se sono dentro o fuori dalle virgolette
     * Divide sulla virgola solo se sono fuori dalle virgolette
     * Le virgolette sono solo delimitatori, non fanno parte del valore
     */
    private String[] parseCSVLine(String line) {
        java.util.List<String> result = new java.util.ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean insideQuotes = false;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (c == '"') {
                // Gestione virgolette -> cambia lo stato inside/outside
                insideQuotes = !insideQuotes;
            } else if (c == ',' && !insideQuotes) {
                // Virgola fuori dalle virgolette -> fine campo
                result.add(currentField.toString());
                currentField = new StringBuilder();
            } else {
                // Carattere normale -> lo aggiungo al campo corrente
                currentField.append(c);
            }
        }
        // Aggiungo l'ultimo campo
        result.add(currentField.toString());
        
        return result.toArray(new String[0]);
    }
}