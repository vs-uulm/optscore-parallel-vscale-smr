package de.optscore.vscale.server;

import bftsmart.parallelism.MessageContextPair;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.leaderchange.CertifiedDecision;
import bftsmart.tom.server.Executable;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.util.TOMUtil;
import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.client.EvalClientRunner;
import de.uniulm.vs.art.uds.UDScheduler;

import java.util.logging.Logger;

/**
 * Main Server class of our system evaluation efforts. Starts a replica when used as main class,
 * and listens for EvalRequests. Performs actions specified in EvalRequests.
 */
public class UDSServiceReplica extends ServiceReplica {


    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(UDSServiceReplica.class.getName());

    private EvalServer evalServer;

    public UDSServiceReplica(int id, Executable executor, Recoverable recoverer) {
        super(id, executor, recoverer);

        // only possible because UDSServiceReplica is for benchmarking and should at the moment only be instantiated
        // by EvalServer
        try {
            this.evalServer = (EvalServer) executor;
        } catch(ClassCastException e) {
            e.printStackTrace();
            logger.severe("Could not cast executor to EvalServer (for stats logging). UDSServiceReplica is only for " +
                    "benchmarking and should not be used/instantiated by classes other than EvalServer");
            System.exit(3);
        }

        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);
    }

    @Override
    public void receiveMessages(int[] consId, int[] regencies, int[] leaders, CertifiedDecision[] cDecs, TOMMessage[][] requests) {
        int consensusCount = 0;
        int requestCount;
        boolean noop;

        // loop through array of delivered message batches
        for (TOMMessage[] requestsFromConsensus : requests) {
            logger.finest("Received decision with " + requestsFromConsensus.length +" request(s) from DeliveryThread");
            TOMMessage firstRequest = requestsFromConsensus[0];
            requestCount = 0;
            noop = true;

            // loop through a batch of messages
            for (TOMMessage request : requestsFromConsensus) {

                // bftsmart.tom.util.Logger.println("(ServiceReplica.receiveMessages) Processing TOMMessage from
                // client " + request.getSender() + " with sequence number " + request.getSequence() + " for session " + request.getSession() + " decided in consensus " + consId[consensusCount]);


                // if the request has the correct view ID and is to be handled as an ordered request, proceed
                // normally, which is in this case means: Schedule the request as a thread with UDS
                if (request.getViewID() == SVController.getCurrentViewId() &&
                        request.getReqType() == TOMMessageType.ORDERED_REQUEST) {

                    noop = false;

                    MessageContext msgCtx = new MessageContext(request.getSender(), request.getViewID(),
                    request.getReqType(), request.getSession(), request.getSequence(), request.getOperationId(),
                    request.getReplyServer(), request.serializedMessageSignature, firstRequest.timestamp,
                    request.numOfNonces, request.seed, regencies[consensusCount], leaders[consensusCount],
                    consId[consensusCount], cDecs[consensusCount].getConsMessages(), firstRequest, false);

                    // statslogging: initial request received time after GCS
                    if(evalServer.runStats != null && msgCtx.getSender() != 5000) {
                        evalServer.runStats[msgCtx.getSender()%1000][msgCtx.getOperationId()]
                                .setReqReceivedInServiceReplica(EvalServer.BENCHMARK_BEGINTIME_SERVER + System
                                        .nanoTime() - EvalServer.NANOTIME_OFFSET_SERVER);
                    }

                    if (requestCount + 1 == requestsFromConsensus.length) {
                        msgCtx.setLastInBatch();
                    }
                    request.deliveryTime = System.currentTimeMillis();

                    // the runnable responsible for fulfilling the client request
                    Runnable standardRunnable = () -> {
                        MessageContextPair messageContextPair = new MessageContextPair(request, msgCtx);

                        // execute the business logic and get the response bytes
                        byte[] response = ((SingleExecutable) executor).
                                executeOrdered(messageContextPair.message.getContent(), messageContextPair.msgCtx);

                        // create the TOMMessage reply from the response bytes
                        messageContextPair.message.reply = new TOMMessage(id, messageContextPair.message.getSession(),
                                messageContextPair.message.getSequence(), messageContextPair.message.getOperationId(),
                                response, SVController.getCurrentViewId(), TOMMessageType.ORDERED_REQUEST);

                        // send out the reply
                        replier.manageReply(messageContextPair.message, messageContextPair.msgCtx);

                        if(evalServer.runStats != null && msgCtx.getSender() != 5000) {
                            evalServer.runStats[msgCtx.getSender()%1000][msgCtx.getOperationId()]
                                    .setReqFullyCompletedAndSentBackReply(EvalServer.BENCHMARK_BEGINTIME_SERVER + System
                                            .nanoTime() - EvalServer.NANOTIME_OFFSET_SERVER);
                        }
                    };
                    // blocking call, give request to scheduler as soon as it accepts new Runnables
                    UDScheduler.getInstance().addRequest(standardRunnable);

                    // statslogging: time after successfully submitting request to UDS
                    if(evalServer.runStats != null && msgCtx.getSender() != 5000) {
                        evalServer.runStats[msgCtx.getSender()%1000][msgCtx.getOperationId()]
                                .setReqSubmittedtoUDS(EvalServer.BENCHMARK_BEGINTIME_SERVER + System
                                        .nanoTime() - EvalServer.NANOTIME_OFFSET_SERVER);
                    }

                // If the message was a reconfig-message, let SVController know and don't set noop to false
                } else if (request.getViewID() == SVController.getCurrentViewId() &&
                        request.getReqType() == TOMMessageType.RECONFIG) {
                    SVController.enqueueUpdate(request);

                // message sender had an old view; resend the message to him (but only if it came from consensus
                // and not state transfer)
                } else if (request.getViewID() < SVController.getCurrentViewId()) {
                    tomLayer.getCommunication().send(new int[]{request.getSender()}, new TOMMessage(SVController.getStaticConf().getProcessId(),
                    request.getSession(), request.getSequence(), TOMUtil.getBytes(SVController.getCurrentView()), SVController.getCurrentViewId()));

                } else {
                    throw new RuntimeException("Should never reach here!");
                }
                requestCount++;
            }

            // This happens when a consensus finishes but there are no requests to deliver
            // to the application. This can happen if a reconfiguration is issued and is the only
            // operation contained in the batch. The recoverer must be notified about this,
            // hence the invocation of "noop"
            if (noop && this.recoverer != null) {

                //bftsmart.tom.util.Logger.println("(ServiceReplica.receiveMessages) Delivering a no-op to the " +
                //        "recoverer");

                System.out.println(" --- A consensus instance finished, but there were no commands to deliver to the application.");
                System.out.println(" --- Notifying recoverable about a blank consensus.");

                byte[][] batch;
                MessageContext[] msgCtx;

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

                this.recoverer.noOp(consId[consensusCount], batch, msgCtx);

                //MessageContext msgCtx = new MessageContext(-1, -1, null, -1, -1, -1, -1, null, // Since it is a noop, there is no need to pass info about the client...
                //        -1, 0, 0, regencies[consensusCount], leaders[consensusCount], consId[consensusCount], cDecs[consensusCount].getConsMessages(), //... but there is still need to pass info about the consensus
                //        null, true); // there is no command that is the first of the batch, since it is a noop
                //msgCtx.setLastInBatch();

                //this.recoverer.noOp(msgCtx.getConsensusId(), msgCtx);
            }

            consensusCount++;
        }

        //
        if (SVController.hasUpdates()) {
            TOMMessage reconf = new TOMMessage(0, 0, 0,0, null, 0, TOMMessageType.ORDERED_REQUEST);
            MessageContextPair m = new MessageContextPair(reconf, null);

            // TODO deal with view updates
            /* LinkedBlockingQueue[] q = this.scheduler.getMapping().getQueues();
            try {
                for (LinkedBlockingQueue q1 : q) {
                    q1.put(m);
                }
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
            */

        }
    }
}
