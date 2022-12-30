/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.parallelism.scheduler;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import bftsmart.parallelism.MessageContextPair;
import bftsmart.parallelism.ParallelMapping;


/**
 *
 * @author eduardo
 */
public class DefaultScheduler implements Scheduler {

    protected ParallelMapping mapping;
    protected int nextThread = 0;

   
 
    public DefaultScheduler(int numberWorkers) {
         this(numberWorkers, numberWorkers, numberWorkers);
    }

    public DefaultScheduler(int minWorkers, int initialWorkers, int maxWorkers){
        if(minWorkers <= initialWorkers && initialWorkers <= maxWorkers){
            this.mapping = new ParallelMapping(minWorkers, initialWorkers, maxWorkers);
        }else{
            this.mapping = new ParallelMapping(initialWorkers, initialWorkers, initialWorkers);
        }
    }

    
    @Override
    public ParallelMapping getMapping() {
        return mapping;
    }

   
    @Override
    public void schedule(MessageContextPair request) {
        try {
            if (request.message.groupId == ParallelMapping.CONFLICT_NONE) {
                mapping.getThreadQueue(nextThread).put(request);
                nextThread = (nextThread + 1) % mapping.getNumThreadsAC();
            } else if (request.message.groupId == ParallelMapping.CONFLICT_ALL) {
                LinkedBlockingQueue[] q = mapping.getQueuesActive();
                for (LinkedBlockingQueue q1 : q) {
                    q1.put(request);
                }
            } else {
                if (request.message.groupId < mapping.getNumThreadsAC()) {

                    mapping.getThreadQueue(request.message.groupId).put(request);

                } else {//MULTIGROUP
                    LinkedBlockingQueue[] q = mapping.getQueues(request.message.groupId);

                    if (q != null) {
                        for (LinkedBlockingQueue q1 : q) {

                            q1.put(request);
                        }
                    } else {
                        //TRATAR COMO CONFLICT ALL
                        request.message.groupId = ParallelMapping.CONFLICT_ALL;
                        q = mapping.getQueuesActive();
                        for (LinkedBlockingQueue q1 : q) {
                            q1.put(request);
                        }
                    }
                }
            }
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            Logger.getLogger(DefaultScheduler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
