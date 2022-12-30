/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.parallelism.reconfiguration;

import bftsmart.tom.core.messages.TOMMessage;


/**
 *
 * @author alex
 */
public class DefaultPSMRReconfigurationPolicy implements PSMRReconfigurationPolicy{

    @Override
    public int checkReconfiguration(TOMMessage request, int activeThreads, int numMaxThreads) {
        return 0;
    }
    
    
    
}
