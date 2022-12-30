/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.parallelism.reconfiguration;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.parallelism.ParallelMapping;


/**
 *
 * @author alex
 */
public class AgressivePolicy implements PSMRReconfigurationPolicy {

    int conflict = 0;
    int nconflict = 0;

    //intervalo de verificação(amostra)
    private final int interval = 10000;

    @Override
    public int checkReconfiguration(TOMMessage request, int activeThreads, int numMaxThreads) {

        if (request.getGroupId() == ParallelMapping.CONFLICT_ALL) {
            conflict++;
        } else if (request.getGroupId() == ParallelMapping.CONFLICT_NONE) {
            nconflict++;
        }

        if (conflict + nconflict == interval) {

            int cp = (conflict * 100) / interval;

           /* System.out.println(" ******************* ");
            System.out.println(" ************************* ");
            System.out.println(" *****************************      Porcentagem = " + cp);
            System.out.println(" ************************* ");
            System.out.println(" ******************* ");*/

            this.conflict = 0;
            this.nconflict = 0;
            
            if (cp < 18) {
            //FULL POWER
                return numMaxThreads - activeThreads;

            } else if (cp > 18 && cp <= 22) {
                //50% of threads
                return ((numMaxThreads * 50) / 100) - activeThreads;
            } else 
            //sequencial mode
            if (activeThreads == 1) {
                return 0;
            } else {
                return (activeThreads - 1) * (-1);
            }
        }

        return 0;
    }

}
