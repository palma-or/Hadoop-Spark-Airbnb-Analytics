package it.unisa.hpc.hadoop.tophost;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *Orlando Palma 0622702433 IZ p.orlando8@studenti.unisa.it
 * D'Aniello Giuseppe gidaniello@unisa.it
 * Esercizio 1 – Map Reduce
 * Trovare i k host con il maggior numero di appartamenti; il parametro k deve essere passato dall’utente.
 */

/**
 * Driver, implementa il Job Chaining
 * 
 * Esegue due job MapReduce in sequenza:
 * Job 1: Conta il numero di appartamenti per ogni host
 * Job 2: Trova i top K host con più appartamenti
 * 
 */
public class DriverTopHost {
    
    public static void main(String[] args) throws Exception {
        
        // Verifico il numero di parametri
        if (args.length != 4) {
            System.err.println("Uso: DriverTopHost <input_path> <temp_output> <final_output> <k>");
            System.err.println("  input_path: Path del dataset CSV di input");
            System.err.println("  temp_output: Path temporaneo per output del Job 1");
            System.err.println("  final_output: Path finale per output del Job 2");
            System.err.println("  k: Numero di top host da trovare");
            System.exit(-1);
        }
        
        // Parsing parametri
        Path inputPath = new Path(args[0]);
        Path tempOutputPath = new Path(args[1]);
        Path finalOutputPath = new Path(args[2]);
        int k = Integer.parseInt(args[3]);
        
        if (k <= 0) {
            System.err.println("Errore: K deve essere > 0");
            System.exit(1);
        }
        
        Configuration conf = new Configuration();
        conf.setInt("top.k", k); // Salvo K 
        
        // JOB 1: Conteggio appartamenti per host
        Job job1 = Job.getInstance(conf, "Job 1 - Count Apartments per Host");
        job1.setJarByClass(DriverTopHost.class);
        
        // paths
        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tempOutputPath);
        
        // Formato Input e Output
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        // Mapper
        job1.setMapperClass(MapperCount.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        
        // Reducer
        job1.setReducerClass(ReducerCount.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        // Eseguo Job 1 e aspetto il completamento
        boolean job1Success = job1.waitForCompletion(true);
        
        if (!job1Success) {
            System.err.println("Job 1 fallito!");
            System.exit(1);
        }
        
        System.out.println("Job 1 completato con successo!");
        
        // Analisi rapida risultati Job 1
        FileSystem fs = FileSystem.get(conf);
        int totalHosts = countLinesInOutput(fs, tempOutputPath);

        // Notifica solo se K > totale
        if (k > totalHosts) {
            System.out.println("\nNota: Richiesti K=" + k + " host, disponibili " + 
                              totalHosts + ". Restituiti tutti i " + totalHosts + " host.\n");
        }
        
        // JOB 2: Top K Host (Pattern Top K)
        Job job2 = Job.getInstance(conf, "Job 2 - Top K Hosts");
        job2.setJarByClass(DriverTopHost.class);
        
        // paths -> Input del Job 2 è l'output del Job 1
        FileInputFormat.addInputPath(job2, tempOutputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);
        
        // Formato Input e Output -> KeyValueTextInputFormat legge file nel formato "key\tvalue"
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        // Mapper
        job2.setMapperClass(MapperTop.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(HostCount.class);
        
        // Reducer
        job2.setReducerClass(ReducerTop.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        
        // Voglio un solo reducer per aggregare tutti i top K locali
        job2.setNumReduceTasks(1);
        
        // Eseguio Job 2 e aspetto il completamento
        boolean job2Success = job2.waitForCompletion(true);
        
        if (!job2Success) {
            System.err.println("Job 2 fallito!");
            System.exit(1);
        }
        
        System.out.println("Job 2 completato con successo!");
        System.out.println("Top " + k + " host trovati e salvati in: " + finalOutputPath);
        
        System.exit(0);
    }
    
/**
* Conta righe nell'output (numero di host univoci)
*/
    private static int countLinesInOutput(FileSystem fs, Path outputPath) 
            throws IOException {
        int count = 0;
        FileStatus[] files = fs.listStatus(outputPath);

        for (FileStatus file : files) {
            if (file.getPath().getName().startsWith("part-r-")) {
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(file.getPath()))
                );
                while (reader.readLine() != null) count++;
                reader.close();
            }
        }
        return count;
    }
}