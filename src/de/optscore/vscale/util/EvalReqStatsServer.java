package de.optscore.vscale.util;

/**
 * Light-weight object for recording statistics of individual EvalRequests on the server, for easier tracking of
 * latencies, etc. Has no ties to actual request objects, so the heavy-weight request object and their payloads can
 * be GCed.
 */
public class EvalReqStatsServer {

    private long reqReceivedInServiceReplica;
    private long reqSubmittedtoUDS;
    private long reqStartedExecution;
    private long reqEndedExecution;
    private long reqFullyCompletedAndSentBackReply;

    public long getReqReceivedInServiceReplica() {
        return reqReceivedInServiceReplica;
    }

    public void setReqReceivedInServiceReplica(long reqReceivedInServiceReplica) {
        this.reqReceivedInServiceReplica = reqReceivedInServiceReplica;
    }

    public long getReqSubmittedtoUDS() {
        return reqSubmittedtoUDS;
    }

    public void setReqSubmittedtoUDS(long reqSubmittedtoUDS) {
        this.reqSubmittedtoUDS = reqSubmittedtoUDS;
    }

    public long getReqStartedExecution() {
        return reqStartedExecution;
    }

    public void setReqStartedExecution(long reqStartedExecution) {
        this.reqStartedExecution = reqStartedExecution;
    }

    public long getReqEndedExecution() {
        return reqEndedExecution;
    }

    public void setReqEndedExecution(long reqEndedExecution) {
        this.reqEndedExecution = reqEndedExecution;
    }

    public long getReqFullyCompletedAndSentBackReply() {
        return reqFullyCompletedAndSentBackReply;
    }

    public void setReqFullyCompletedAndSentBackReply(long reqFullyCompletedAndSentBackReply) {
        this.reqFullyCompletedAndSentBackReply = reqFullyCompletedAndSentBackReply;
    }

    public long[] getStatsAsArray() {
        return new long[]{ reqReceivedInServiceReplica, reqSubmittedtoUDS, reqStartedExecution, reqEndedExecution,
                reqFullyCompletedAndSentBackReply };
    }

    public long[] getStatsAsArrayWithSenderIdAndOpId(int senderId, int opId) {
        return new long[]{ senderId, opId, reqReceivedInServiceReplica, reqSubmittedtoUDS, reqStartedExecution,
                reqEndedExecution,
                reqFullyCompletedAndSentBackReply };
    }
}
