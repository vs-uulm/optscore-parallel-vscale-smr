/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.parallelism.scheduler;

import bftsmart.parallelism.MessageContextPair;
import bftsmart.parallelism.ParallelMapping;


/**
 *
 * @author eduardo
 */
public interface Scheduler {
    public void schedule(MessageContextPair request);
    public ParallelMapping getMapping();
}
