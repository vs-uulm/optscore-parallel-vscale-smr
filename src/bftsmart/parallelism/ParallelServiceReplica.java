/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.parallelism;

import bftsmart.consensus.messages.MessageFactory;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.consensus.roles.Proposer;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.ExecutionManager;
import bftsmart.tom.core.ParallelTOMLayer;
import bftsmart.tom.core.messages.TOMMessage;


import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.util.ShutdownHookThread;
import bftsmart.tom.util.TOMUtil;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CyclicBarrier;
import bftsmart.parallelism.reconfiguration.PSMRReconfigurationPolicy;
import bftsmart.parallelism.reconfiguration.ReconfigurableScheduler;
import bftsmart.parallelism.scheduler.DefaultScheduler;
import bftsmart.parallelism.scheduler.Scheduler;

/**
 *
 * @author alchieri
 */
public class ParallelServiceReplica extends ServiceReplica {

    
    protected Scheduler scheduler;

    
    public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, int initialWorkers) {
        this(id, executor, recoverer, new DefaultScheduler(initialWorkers));
    }
   
    public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, int minWorkers, int initialWorkers, int maxWorkers) {
        this(id, executor, recoverer, new ReconfigurableScheduler(minWorkers, initialWorkers, maxWorkers, null));
    }

    public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, 
            int minWorkers, int initialWorkers, int maxWorkers, PSMRReconfigurationPolicy rec) {
        this(id, executor, recoverer, new ReconfigurableScheduler(minWorkers, initialWorkers, maxWorkers, rec));
    }
    
    
    
    public ParallelServiceReplica(int id, Executable executor, Recoverable recoverer, Scheduler s) {
        super(id, executor, recoverer);
        //this.mapping = m;
        if(s == null){
            this.scheduler = new DefaultScheduler(1);
        }else{
            this.scheduler = s;
        }
        initWorkers();
    }

    private void initWorkers() {
        int tid = 0;
        for (int i = 0; i < this.scheduler.getMapping().getNumMaxOfThreads(); i++) {
            new ServiceReplicaWorker(this.scheduler.getMapping().getThreadQueue(i), tid).start();
            tid++;
        }
    }
    
   /* public ParallelServiceReplica(int id, boolean isToJoin, Executable executor, Recoverable recoverer, ParallelMapping m, PSMRReconfigurationPolicy rp) {
        super(id, "", isToJoin, executor, recoverer);
        this.mapping = m;
        //alexend
        if (rp == null) {
            this.reconf = new DefaultPSMRReconfigurationPolicy();
        } else {
            this.reconf = rp;
        }
        //alexend
        initMapping();
    }*/

    public int getNumActiveThreads() {
        return this.scheduler.getMapping().getNumThreadsAC();
    }

    public boolean addExecutionConflictGroup(int groupId, int[] threadsId) {
        return this.scheduler.getMapping().addMultiGroup(groupId, threadsId);
    }
    
    


   

    /**
     * Barrier used to reconfigure the number of replicas in the system
     * @return 
     */
    public CyclicBarrier getReconfBarrier() {
        return this.scheduler.getMapping().getReconfBarrier();
    }

    @Override
    public void receiveMessages(int consId[], int regencies[], int leaders[], CertifiedDecision[] cDecs, TOMMessage[][] requests) {
        //int numRequests = 0;
        int consensusCount = 0;
        boolean noop = true;




        for (TOMMessage[] requestsFromConsensus : requests) {
            TOMMessage firstRequest = requestsFromConsensus[0];
            int requestCount = 0;
            noop = true;
            for (TOMMessage request : requestsFromConsensus) {
                
                bftsmart.tom.util.Logger.println("(ServiceReplica.receiveMessages) Processing TOMMessage from client " + request.getSender() + " with sequence number " + request.getSequence() + " for session " + request.getSession() + " decided in consensus " + consId[consensusCount]);
                
                if (request.getViewID() == SVController.getCurrentViewId()) {
                    if (request.getReqType() == TOMMessageType.ORDERED_REQUEST) {
                        noop = false;
                        //numRequests++;
                        MessageContext msgCtx = new MessageContext(request.getSender(), request.getViewID(),
                                request.getReqType(), request.getSession(), request.getSequence(), request.getOperationId(),
                                request.getReplyServer(), request.serializedMessageSignature, firstRequest.timestamp,
                                request.numOfNonces, request.seed, regencies[consensusCount], leaders[consensusCount],
                                consId[consensusCount], cDecs[consensusCount].getConsMessages(), firstRequest, false);
                        
                        if (requestCount + 1 == requestsFromConsensus.length) {

                            msgCtx.setLastInBatch();
                        }
                        request.deliveryTime = System.nanoTime();

                        
                                            
                        this.scheduler.schedule(new MessageContextPair(request, msgCtx));
                            
                    } else if (request.getReqType() == TOMMessageType.RECONFIG) {

                        SVController.enqueueUpdate(request);
                    } else {
                        throw new RuntimeException("Should never reach here!");
                    }

                } else if (request.getViewID() < SVController.getCurrentViewId()) {
                    // message sender had an old view, resend the message to
                    // him (but only if it came from consensus an not state transfer)
                    tomLayer.getCommunication().send(new int[]{request.getSender()}, new TOMMessage(SVController.getStaticConf().getProcessId(), 
                            request.getSession(), request.getSequence(), TOMUtil.getBytes(SVController.getCurrentView()), SVController.getCurrentViewId()));
                
                }
                requestCount++;
            }
            
             // This happens when a consensus finishes but there are no requests to deliver
            // to the application. This can happen if a reconfiguration is issued and is the only
            // operation contained in the batch. The recoverer must be notified about this,
            // hence the invocation of "noop"
            if (noop && this.recoverer != null) {
                
                bftsmart.tom.util.Logger.println("(ServiceReplica.receiveMessages) Delivering a no-op to the recoverer");

                System.out.println(" --- A consensus instance finished, but there were no commands to deliver to the application.");
                System.out.println(" --- Notifying recoverable about a blank consensus.");

                byte[][] batch = null;
                MessageContext[] msgCtx = null;
                if (requestsFromConsensus.length > 0) {
                    //Make new batch to deliver
                    batch = new byte[requestsFromConsensus.length][];
                    msgCtx = new MessageContext[requestsFromConsensus.length];

                    //Put messages in the batch
                    int line = 0;
                    for (TOMMessage m : requestsFromConsensus) {
                        batch[line] = m.getContent();

                        msgCtx[line] = new MessageContext(m.getSender(), m.getViewID(),
                            m.getReqType(), m.getSession(), m.getSequence(), m.getOperationId(),
                            m.getReplyServer(), m.serializedMessageSignature, firstRequest.timestamp,
                            m.numOfNonces, m.seed, regencies[consensusCount], leaders[consensusCount],
                            consId[consensusCount], cDecs[consensusCount].getConsMessages(), firstRequest, true);
                        msgCtx[line].setLastInBatch();
                        
                        line++;
                    }
                }

                this.recoverer.noOp(consId[consensusCount], batch, msgCtx);
                
                //MessageContext msgCtx = new MessageContext(-1, -1, null, -1, -1, -1, -1, null, // Since it is a noop, there is no need to pass info about the client...
                //        -1, 0, 0, regencies[consensusCount], leaders[consensusCount], consId[consensusCount], cDecs[consensusCount].getConsMessages(), //... but there is still need to pass info about the consensus
                //        null, true); // there is no command that is the first of the batch, since it is a noop
                //msgCtx.setLastInBatch();
                
                //this.recoverer.noOp(msgCtx.getConsensusId(), msgCtx);
            }
            
            consensusCount++;
        }
        if (SVController.hasUpdates()) {
            TOMMessage reconf = new TOMMessage(0, 0, 0,0, null, 0, TOMMessageType.ORDERED_REQUEST, ParallelMapping.CONFLICT_RECONFIGURATION);
            MessageContextPair m = new MessageContextPair(reconf, null);
            LinkedBlockingQueue[] q = this.scheduler.getMapping().getQueues();
            try {
                for (LinkedBlockingQueue q1 : q) {
                    q1.put(m);
                }
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }

        }
    }

    
     
    
    
    /**
     * This method initializes the object
     *
     * @param cs Server side communication System
     * @param conf Total order messaging configuration
     */
    private void initTOMLayer() {
        if (tomStackCreated) { // if this object was already initialized, don't do it again
            return;
        }

        if (!SVController.isInCurrentView()) {
            throw new RuntimeException("I'm not an acceptor!");
        }

        // Assemble the total order messaging layer
        MessageFactory messageFactory = new MessageFactory(id);

        Acceptor acceptor = new Acceptor(cs, messageFactory, SVController);
        cs.setAcceptor(acceptor);

        Proposer proposer = new Proposer(cs, messageFactory, SVController);

        ExecutionManager executionManager = new ExecutionManager(SVController, acceptor, proposer, id);

        acceptor.setExecutionManager(executionManager);

        tomLayer = new ParallelTOMLayer(executionManager, this, recoverer, acceptor, cs, SVController, verifier);

        executionManager.setTOMLayer(tomLayer);

        SVController.setTomLayer(tomLayer);

        cs.setTOMLayer(tomLayer);
        cs.setRequestReceiver(tomLayer);

        acceptor.setTOMLayer(tomLayer);

        if (SVController.getStaticConf().isShutdownHookEnabled()) {
            Runtime.getRuntime().addShutdownHook(new ShutdownHookThread(tomLayer));
        }
        tomLayer.start(); // start the layer execution
        tomStackCreated = true;

        replicaCtx = new ReplicaContext(cs, SVController);
    }
    
    private class ServiceReplicaWorker extends Thread {

        private LinkedBlockingQueue<MessageContextPair> requests;
        private int thread_id;

        public ServiceReplicaWorker(LinkedBlockingQueue<MessageContextPair> requests, int id) {
            this.thread_id = id;
            this.requests = requests;
            //System.out.println("Criou um thread: " + id);

        }

        public void run() {
            MessageContextPair msg = null;

            while (true) {

                try {

                    msg = requests.take();

                    //System.out.println(">>>Thread " + thread_id + " PEGOU REQUEST");
                    //Thread.sleep(400);
                    if (msg.message.groupId == ParallelMapping.CONFLICT_NONE || msg.message.groupId == thread_id) {
                        //System.out.println("Thread " + thread_id + " vai executar uma operacao conflict none! " + msg.message.toString());
                        //sleep(5000);
                        //System.exit(0);
                        byte[] response = ((SingleExecutable) executor).executeOrdered(msg.message.getContent(), msg.msgCtx);
                        msg.message.reply = new TOMMessage(id, msg.message.getSession(),
                                msg.message.getSequence(), response, SVController.getCurrentViewId());
                        bftsmart.tom.util.Logger.println("(ParallelServiceReplica.receiveMessages) sending reply to "
                                + msg.message.getSender());
                        replier.manageReply(msg.message, msg.msgCtx);

                    } else if (msg.message.groupId == ParallelMapping.CONFLICT_RECONFIGURATION) {
                        scheduler.getMapping().getReconfBarrier().await();
                        //System.out.println(">>>Thread " + thread_id + " vai aguardar uma reconfiguração!");
                        scheduler.getMapping().getReconfBarrier().await();

                    } else if (msg.message.groupId == ParallelMapping.THREADS_RECONFIGURATION) {
                        if (thread_id == 0) {
                            scheduler.getMapping().getReconfThreadBarrier().await();
                            //ATUALIZAR AS BARREIRAS CONFLIC_ALL
                            scheduler.getMapping().reconfigureBarrier();
                            //Thread.sleep(1000);
                            scheduler.getMapping().getReconfThreadBarrier().await();
                            //System.out.println(">>>Thread " + thread_id + " SAIU DA BARREIRA");
                        } else {
                            scheduler.getMapping().getReconfThreadBarrier().await();
                            //System.out.println(">>>Thread " + thread_id + " ESPERANDO RECONFIGURACAO DE NUM T");
                            //Thread.sleep(1000);
                            scheduler.getMapping().getReconfThreadBarrier().await();
                            //System.out.println(">>>Thread " + thread_id + "SAIU DA BARREIRA");
                        }
                    } else if (thread_id == scheduler.getMapping().getExecutorThread(msg.message.groupId)) {//CONFLIC_ALL ou MULTIGROUP

                        scheduler.getMapping().getBarrier(msg.message.groupId).await();
                        // System.out.println("Thread " + thread_id + " );

                        byte[] response = ((SingleExecutable) executor).executeOrdered(msg.message.getContent(), msg.msgCtx);

                        msg.message.reply = new TOMMessage(id, msg.message.getSession(),
                                msg.message.getSequence(), response, SVController.getCurrentViewId());
                        bftsmart.tom.util.Logger.println("(ParallelServiceReplica.receiveMessages) sending reply to "
                                + msg.message.getSender());
                        replier.manageReply(msg.message, msg.msgCtx);

                        //System.out.println("Thread " + thread_id + " executou uma operacao!");
                        scheduler.getMapping().getBarrier(msg.message.groupId).await();
                    } else {
                        //System.out.println(">>>Thread vai liberar execução!"+thread_id);
                        //if(thread_id == 4){

                        //  Thread.sleep(1000);
                        //}
                        scheduler.getMapping().getBarrier(msg.message.groupId).await();
                        //System.out.println(">>>Thread " + thread_id + " vai aguardar a execucao de uma operacao conflict: " + msg.message.groupId);
                        scheduler.getMapping().getBarrier(msg.message.groupId).await();
                    }
                } catch (Exception ie) {
                    ie.printStackTrace();
                    continue;
                }
            }

        }

    }

}
