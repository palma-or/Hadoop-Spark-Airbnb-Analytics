package it.unisa.hpc.hadoop.tophost;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *Orlando Palma 0622702433 IZ p.orlando8@studenti.unisa.it
 * D'Aniello Giuseppe gidaniello@unisa.it
 * Esercizio 1 – Map Reduce
 * Trovare i k host con il maggior numero di appartamenti; il parametro k deve essere passato dall’utente.
 */

/**
 * Reducer per il Job 1: Somma il conteggio degli appartamenti per ogni host
 * 
 * Input: (host_id, [1, 1, 1, ...])
 * Output: (host_id, totale_appartamenti)
 */
public class ReducerCount extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable totalCount = new IntWritable();
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        int sum = 0;
        
        // Sommo tutti gli 1 per questo host_id
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        // Emetto (host_id, totale_appartamenti)
        totalCount.set(sum);
        context.write(key, totalCount);
    }
}