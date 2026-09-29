package it.unisa.hpc.hadoop.tophost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *Orlando Palma 0622702433 IZ p.orlando8@studenti.unisa.it
 * D'Aniello Giuseppe gidaniello@unisa.it
 * Esercizio 1 – Map Reduce
 * Trovare i k host con il maggior numero di appartamenti; il parametro k deve essere passato dall’utente.
 */

/**
 * Reducer per il Job 2: Trova i top K host globali
 * 
 * Riceve tutti i top K locali da ogni mapper e trova i top K globali
 * Viene chiamato una sola volta perchè tutti i mapper
 * emettono con la stessa chiave (NullWritable.get())
 */
public class ReducerTop extends Reducer<NullWritable, HostCount, NullWritable, Text> {
    
    private int k;
    private Text outputValue = new Text();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = context.getConfiguration().getInt("top.k", 10); // default 10
    }
    
    /**
     * Reduce: Aggrega tutti i top K locali e trova i top K globali
     */
    @Override
    protected void reduce(NullWritable key, Iterable<HostCount> values, Context context) 
            throws IOException, InterruptedException {
        
        Map<String, Integer> globalHostMap = new HashMap<>();
        
        for (HostCount value : values) {
            String hostId = value.getHostId();
            int count = value.getCount();
            
            globalHostMap.put(hostId, 
                Math.max(globalHostMap.getOrDefault(hostId, 0), count));
        }
        
        List<HostCount> globalHostList = new ArrayList<>();
        
        for (Map.Entry<String, Integer> entry : globalHostMap.entrySet()) {
            globalHostList.add(new HostCount(entry.getKey(), entry.getValue()));
        }
        
        globalHostList.sort(new HostCountComparator());
        
        int limit = Math.min(k, globalHostList.size());
        
        // Formato stampa output
        outputValue.set("I primi " + limit + " host con più appartamenti");
        context.write(NullWritable.get(), outputValue);
        outputValue.set("Pos.\tID Host\t\tNumero Appartamenti");
        context.write(NullWritable.get(), outputValue);
        
        for (int i = 0; i < limit; i++) {
            HostCount host = globalHostList.get(i);
            outputValue.set((i+1) + "\t" + host.getHostId() + "\t\t" + host.getCount());
            context.write(NullWritable.get(), outputValue);
        }
    }
}