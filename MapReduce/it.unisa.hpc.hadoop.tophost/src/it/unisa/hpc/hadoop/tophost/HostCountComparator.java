package it.unisa.hpc.hadoop.tophost;

import java.util.Comparator;

/**
 *Orlando Palma 0622702433 IZ p.orlando8@studenti.unisa.it
 * D'Aniello Giuseppe gidaniello@unisa.it
 * Esercizio 1 – Map Reduce
 * Trovare i k host con il maggior numero di appartamenti; il parametro k deve essere passato dall’utente.
 */

/**
 * Comparatore per ordinare gli HostCount in ordine decrescente
 * 
 * Ordina per:
 * 1. Count (decrescente) - vengono prima gli host con più appartamenti 
 * 2. HostId (crescente) - in caso di parità ordina per Host_id alfabeticamente
 */
public class HostCountComparator implements Comparator<HostCount> {
    
    @Override
    public int compare(HostCount o1, HostCount o2) {
        // Confronta per count in ordine decrescente: o2.count - o1.count 
        int countComparison = o2.getCount().compareTo(o1.getCount());
        
        if (countComparison != 0) {
            return countComparison; // Se i count sono diversi, restituisci il risultato
        }
        
        // Se i count sono uguali, ordina per hostId in ordine crescente
        return o1.getHostId().compareToIgnoreCase(o2.getHostId());
    }
}