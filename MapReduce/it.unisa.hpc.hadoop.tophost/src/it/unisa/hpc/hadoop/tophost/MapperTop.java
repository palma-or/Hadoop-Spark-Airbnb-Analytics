package it.unisa.hpc.hadoop.tophost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *Orlando Palma 0622702433 IZ p.orlando8@studenti.unisa.it
 * D'Aniello Giuseppe gidaniello@unisa.it
 * Esercizio 1 – Map Reduce
 * Trovare i k host con il maggior numero di appartamenti; il parametro k deve essere passato dall’utente.
 */

/**
 * Mapper per il Job 2: Trova i top K host locali per ogni mapper
 * 
 * Legge l'output del Job 1 (host_id\tcount) e mantiene
 * solo i top K host per il subset di dati che elabora
 *
 */
public class MapperTop extends Mapper<Text, Text, NullWritable, HostCount> {
    
    private Map<String, Integer> hostCountMap;
    private int k;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // HashMap per memorizzare tutti gli host_id e i loro count
        hostCountMap = new HashMap<>();
        
        // Leggo il parametro K dalla configurazione
        k = context.getConfiguration().getInt("top.k", 10); // default 10
    }
    
    /**
     * Map: Legge ogni coppia (host_id, count) e la memorizza nella HashMap
     * 
     * Input: KeyValueTextInputFormat legge file nel formato "key\tvalue" 
     * dove key: host_id e value: count
     */
    @Override
    protected void map(Text key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String hostId = key.toString();
        int count = Integer.parseInt(value.toString());
        
        hostCountMap.put(hostId, count);
    }
    
    /**
     * Cleanup: Dopo aver processato tutti i record, trova i top K locali
     * e li emette verso il reducer 
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        
        List<HostCount> hostList = new ArrayList<>();
        
        for (Map.Entry<String, Integer> entry : hostCountMap.entrySet()) {
            hostList.add(new HostCount(entry.getKey(), entry.getValue()));
        }
        
        hostList.sort(new HostCountComparator());
        
        int limit = Math.min(k, hostList.size());
        
        // Key = NullWritable perchè voglio un solo reducer che aggreghi tutto
        for (int i = 0; i < limit; i++) {
            context.write(NullWritable.get(), hostList.get(i));
        }
    }
}