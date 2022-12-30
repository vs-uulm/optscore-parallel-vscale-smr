package de.optscore.vscale.client;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.RequestContext;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import de.optscore.vscale.EvalRequest;
import de.optscore.vscale.util.EvalReqStatsClient;
import de.optscore.vscale.util.MeanVarianceSampler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class AsyncEvalClient implements ReplyListener, EvalClient {

    ServiceProxy proxy;

    private int requestsPerClientPerRun;
    private long[] sendingDuration;
    private long[] sentRequestTimes;
    private long[][] receivedRequestTimes;
    private TOMMessage[][] replicaReplies;
    private int receivedRequestCount;
    private long firstRequestSentTime;
    private long lastRequestSentTime;
    private long firstRequestReceivedTime;
    private long lastRequestReceivedTime;

    private int operationId;

    private boolean testRunComplete;

    private double[] endResults;

    private static final Logger logger = Logger.getLogger(AsyncEvalClient.class.getName());

    public AsyncEvalClient(int clientId, int requestsPerClientPerRun) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        this.requestsPerClientPerRun = requestsPerClientPerRun;
        this.sendingDuration = new long[requestsPerClientPerRun+1];
        this.sentRequestTimes = new long[requestsPerClientPerRun+1];
        this.receivedRequestTimes = new long[requestsPerClientPerRun+1][];
        this.replicaReplies = new TOMMessage[requestsPerClientPerRun+1][];
        this.receivedRequestCount = 0;
        this.firstRequestSentTime = -1;
        this.lastRequestReceivedTime = -1;
        this.lastRequestSentTime = -1;
        this.firstRequestReceivedTime = -1;
        this.testRunComplete = false;
        this.proxy = new AsynchServiceProxy(clientId);
    }

    public void sendRequest(EvalRequest request) {
        sendRequestAsync(request);
    }

    private void sendRequestAsync(EvalRequest request) {
        // remember time now to measure how long the client actually takes to send a request out
        long beforeSend = System.currentTimeMillis();

        // send the request to all replicas
        operationId = ((AsynchServiceProxy) this.proxy).invokeAsynchRequest(EvalRequest.serializeEvalRequest(request)
                , this,
                TOMMessageType.ORDERED_REQUEST);
        // remember time now, after request has been sent off
        sentRequestTimes[operationId] = System.currentTimeMillis();

        // remember the start/end of the test run (i.e. the time of the very first/last request this client sent)
        if(operationId == 0) {
            firstRequestSentTime = sentRequestTimes[operationId];
        }
        if(operationId == requestsPerClientPerRun - 1) {
            lastRequestSentTime = sentRequestTimes[operationId];
        }

        // initialize the arrays for gathering all replies (and receivedTimes) from the replicas. The length of
        // the arrays is the number of replicas currently in the system (in the current view)
        replicaReplies[operationId] = new TOMMessage[proxy.getViewManager().getCurrentViewN()];
        receivedRequestTimes[operationId] = new long[proxy.getViewManager().getCurrentViewN()];

        // remember the time it took to send the request out
        long afterSend = System.currentTimeMillis();
        sendingDuration[operationId] = afterSend - beforeSend;
    }

    @Override
    public void replyReceived(RequestContext context, TOMMessage reply) {
        // remember we got a reply back
        replicaReplies[reply.getOperationId()][reply.getSender()] = reply;

        String replicaRepliesStringified = Arrays.stream(replicaReplies[reply.getOperationId()]).
                map(tomMessage -> (tomMessage == null) ? "empty".getBytes() : tomMessage.getContent()).
                map(String::new).collect(Collectors.joining(", ", "[", "]"));
        logger.finest("Client# " + proxy.getProcessId() + ": Received reply for req with opId "
                + reply.getOperationId() + " from replica " + reply.getSender() + ", sent at " +
                context.getSendingTime() + ": " + replicaRepliesStringified);

        // remember receive-time for each request
        receivedRequestTimes[reply.getOperationId()][reply.getSender()] = System.currentTimeMillis();
        if(reply.getOperationId() == 0) {
            firstRequestReceivedTime = receivedRequestTimes[reply.getOperationId()][reply.getSender()];
        }

        // enough replies for the request responsible for this reply have been received
        if(replicaReplyReceivedSatisfiesQuorum(context, reply)) {
            receivedRequestCount++;
        }

        // all requests have been fully answered by enough replicas (quorum size reached for every request)
        if(receivedRequestCount == requestsPerClientPerRun) {
            logger.info("Received all " + requestsPerClientPerRun +
                            " replies for this client (" + proxy.getProcessId() + "). Calculating stats");

            // calculate overall
            lastRequestReceivedTime = System.currentTimeMillis();
            long endToEndTestTime = lastRequestReceivedTime - firstRequestSentTime;
            double throughput = requestsPerClientPerRun / (endToEndTestTime / 1000d);
            double clientReqPerSec = requestsPerClientPerRun /
                    ((lastRequestSentTime - firstRequestSentTime) / 1000d);
            long firstRequestLatency = firstRequestReceivedTime - firstRequestSentTime;
            long lastRequestLatency = lastRequestReceivedTime - lastRequestSentTime;
            logger.info("Client# " + proxy.getProcessId() + ": Overall testing time end to end = " +
                    endToEndTestTime + "ms");
            logger.info("Client# " + proxy.getProcessId() + ": Client req/s sending throughput = " + clientReqPerSec +
                    "req/s");
            logger.info("Client# " + proxy.getProcessId() + ": End to end req/s throughput = " + throughput +
                    "req/s");
            logger.info("Client# " + proxy.getProcessId() + ": first req latency = " + firstRequestLatency + "ms");
            logger.info("Client# " + proxy.getProcessId() + ": last req latency = " + lastRequestLatency + "ms");


            // calculate end-to-end latency and averages for all received replies from each replica
            MeanVarianceSampler requestSampler = new MeanVarianceSampler();
            MeanVarianceSampler testRunSampler = new MeanVarianceSampler();
            long eteLatenciesPerRequest[];
            for(int i = 0; i < requestsPerClientPerRun; i++) {
                eteLatenciesPerRequest = new long[receivedRequestTimes[i].length];
                requestSampler.reset();
                for(int j = 0; j < receivedRequestTimes[i].length; j++) {
                    eteLatenciesPerRequest[j] = receivedRequestTimes[i][j] - sentRequestTimes[i];
                    if(eteLatenciesPerRequest[j] > 0) {
                        requestSampler.add(eteLatenciesPerRequest[j]);
                    }
                }
                // endToEndLatencies[i] = eteLatenciesPerRequest;
                double requestMean = requestSampler.getMean();
                double requestStdDvUnbiased = requestSampler.getStdDevUnbiased();
                testRunSampler.add(requestMean);

                String eteLatenciesPerRequestsStringified = Arrays.stream(eteLatenciesPerRequest).
                        mapToObj(latency -> ((latency < 0) ? "(ignored)" : Long.toString(latency)))
                        .collect(Collectors.joining(",", "[", "]"));
                logger.finest("Client# " + proxy.getProcessId() + ": Latencies (in ms) for request " +
                        i + " = " + eteLatenciesPerRequestsStringified + " with mean " + requestMean +
                        " and stdDev " + requestStdDvUnbiased);
            }
            logger.info("Client# " + proxy.getProcessId() + ": arithmetic mean of arithmetic means of all " +
                    "per-request end-to-end latencies = " + testRunSampler.getMean() +
                    " and stdDev " + testRunSampler.getStdDevUnbiased());

            // create result array: [(# of answered reqs), (avgLatency), (stdDev of avgLatencies),
            // (endToEndTestTime), (throughput), (clientReqPerSec), (firstRequestLatency), (lastRequestLatency)]
            endResults = new double[]{receivedRequestCount, testRunSampler.getMean(), testRunSampler.getStdDevUnbiased(),
                    endToEndTestTime, throughput, clientReqPerSec, firstRequestLatency, lastRequestLatency};

            this.testRunComplete = true;
        }
    }

    @Override
    public void reset() {
        // for now, cancel tests and throw runtime exception for debug purposes
        // TODO properly handle this case
        logger.severe("Proxy re-issued a request, re-initialize Client!");
        throw new RuntimeException("Proxy re-issued a request, re-initialize Client!");
    }

    /**
     * Immediately shuts down this client's ServiceProxy and stops sending requests. Breaks statistics reporting for
     * normal EvalClients. Useful for InteractiveEvalClients.
     */
    @Override
    public void shutDownClient() {
        // close Client connections to replicas and discard proxy
        this.proxy.close();
        this.proxy = null;
    }

    /**
     * Checks whether an incoming reply from a replica is enough to satisfy the quorum.
     * If yes, return true and clear remembered replies, since all the performance data is saved already.
     * If no, add reply to the received replies for the respective request, wait for more replies.
     * @param context Aysnc context of the request that was responsible for this reply
     * @param reply TOMMessage from a replica, reply to a previously issued request
     * @return true if quorum for request is satisfied, false otherwise
     */
    private boolean replicaReplyReceivedSatisfiesQuorum(RequestContext context, TOMMessage reply) {
        // check whether quorum is satisfied already, if so, return true and clean up space in the replicaReplies
        int sameContent = 0;
        TOMMessage[] replies = replicaReplies[reply.getOperationId()];
        for(TOMMessage replicaReply : replies) {
            if(replicaReply != null && Arrays.equals(replicaReply.getContent(), reply.getContent())) {
                sameContent++;
            }
        }

        if(sameContent >= getReplyQuorum()) {
            // forget received replies so they can be GCed
            logger.finest("Client# " + proxy.getProcessId() + ": Received enough replies for request with opId " +
                    reply.getOperationId());
            replicaReplies[reply.getOperationId()] = null;
            ((AsynchServiceProxy) proxy).cleanAsynchRequest(reply.getOperationId());
            return true;
        }

        return false;
    }

    private int getReplyQuorum() {
        if (proxy.getViewManager().getStaticConf().isBFT()) {
            return (int) Math.ceil((proxy.getViewManager().getCurrentViewN()
                    + proxy.getViewManager().getCurrentViewF()) / 2) + 1;
        } else {
            return (int) Math.ceil((proxy.getViewManager().getCurrentViewN()) / 2) + 1;
        }
    }

    @Override
    // FIXME fix
    public ArrayList<EvalReqStatsClient> getStats() {
        return new ArrayList<>(0);
    }

    @Override
    public int getOperationId() {
        return operationId;
    }

    @Override
    public void startLogging() {
        // TODO
    }

    @Override
    public void stopLogging(boolean eraseOldData) {
        // TODO
    }

}
