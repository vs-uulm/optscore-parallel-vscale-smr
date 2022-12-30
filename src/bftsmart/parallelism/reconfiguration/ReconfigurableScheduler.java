/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.parallelism.reconfiguration;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import java.util.concurrent.LinkedBlockingQueue;
import bftsmart.parallelism.MessageContextPair;
import bftsmart.parallelism.ParallelMapping;
import bftsmart.parallelism.scheduler.DefaultScheduler;

/**
 *
 * @author eduardo
 */
public class ReconfigurableScheduler extends DefaultScheduler {

    protected PSMRReconfigurationPolicy reconf;

     public ReconfigurableScheduler(int initialWorkers){
         super(initialWorkers);
     }
    
    public ReconfigurableScheduler(int minWorkers, int initialWorkers, int maxWorkers, PSMRReconfigurationPolicy reconf) {
        super(minWorkers, initialWorkers, maxWorkers);
        if(reconf == null){
            this.reconf = new DefaultPSMRReconfigurationPolicy();
        }else{
            this.reconf = reconf;
        }
        
    }
    
   
    
    
    
    
    @Override
    public void schedule(MessageContextPair request) {
        
        int ntReconfiguration = this.reconf.checkReconfiguration(request.message, 
                this.mapping.getNumThreadsAC(), this.mapping.getNumMaxOfThreads());
        //examina se é possível reconfigurar ntReconfiguration threads
        ntReconfiguration = this.mapping.checkNumReconfigurationThreads(ntReconfiguration);

        if (ntReconfiguration < 0) {
            this.nextThread = 0;
            mapping.setNumThreadsAC(mapping.getNumThreadsAC() + ntReconfiguration);
            //COLOCAR NA FILA DE TODAS AS THREADS UMA REQUEST DO TIPO THREADS_RECONFIGURATION
            TOMMessage reconf = new TOMMessage(0, 0, 0, 0, null, 0, TOMMessageType.ORDERED_REQUEST,
                    ParallelMapping.THREADS_RECONFIGURATION);
            MessageContextPair mRec = new MessageContextPair(reconf, null);
            LinkedBlockingQueue[] q = mapping.getQueues();
            try {
                for (LinkedBlockingQueue q1 : q) {
                    q1.put(mRec);
                }
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }

        } else if (ntReconfiguration > 0) {
            this.nextThread = 0;
            mapping.setNumThreadsAC(mapping.getNumThreadsAC() + ntReconfiguration);
            //COLOCAR NA FILA DE TODAS AS THREADS UMA REQUEST DO TIPO THREADS_RECONFIGURATION
            TOMMessage reconf = new TOMMessage(0, 0, 0, 0, null, 0, TOMMessageType.ORDERED_REQUEST,
                    ParallelMapping.THREADS_RECONFIGURATION);
            MessageContextPair mRec = new MessageContextPair(reconf, null);
            LinkedBlockingQueue[] q = mapping.getQueues();
            try {
                for (LinkedBlockingQueue q1 : q) {
                    q1.put(mRec);
                }
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }

        }
        

        super.schedule(request);
    }

}
