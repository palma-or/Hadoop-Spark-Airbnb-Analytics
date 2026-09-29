package it.unisa.hpc.hadoop.tophost;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *Orlando Palma 0622702433 IZ p.orlando8@studenti.unisa.it
 * D'Aniello Giuseppe gidaniello@unisa.it
 * Esercizio 1 – Map Reduce
 * Trovare i k host con il maggior numero di appartamenti; il parametro k deve essere passato dall’utente.
 */

/**
 * Classe custom per rappresentare una coppia (host_id, count)
 */

public class HostCount implements Writable {
    
    private String hostId;
    private Integer count;
    
    public HostCount() {
        this.hostId = "";
        this.count = 0;
    }
    
    public HostCount(String hostId, Integer count) {
        this.hostId = hostId;
        this.count = count;
    }
    
    public String getHostId() {
        return hostId;
    }
    
    public Integer getCount() {
        return count;
    }

    public void setHostId(String hostId) {
        this.hostId = hostId;
    }
    
    public void setCount(Integer count) {
        this.count = count;
    }
    

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);      
        out.writeUTF(hostId);     
    }
    
    
    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();     
        hostId = in.readUTF();    
    }
    
    @Override
    public String toString() {
        return hostId + "\t" + count;
    }
}