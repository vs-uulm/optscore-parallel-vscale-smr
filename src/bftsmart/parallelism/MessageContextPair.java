/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.parallelism;

import bftsmart.tom.MessageContext;
import bftsmart.tom.core.messages.TOMMessage;

/**
 *
 * @author eduardo
 */
public class MessageContextPair {
    public TOMMessage message;
        public MessageContext msgCtx;

        public MessageContextPair(TOMMessage message, MessageContext msgCtx) {
            this.message = message;
            this.msgCtx = msgCtx;
        }

}
