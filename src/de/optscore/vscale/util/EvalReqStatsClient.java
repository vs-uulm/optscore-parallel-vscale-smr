package de.optscore.vscale.util;

/**
 * Light-weight object for recording statistics of individual EvalRequests, for easier tracking of latencies, etc.
 * Has no ties to actual request objects except the operationId, so the heavy-weight request objects and their
 * payloads can be GCed.
 */
public class EvalReqStatsClient {

    /**
     * The operationId of the EvalRequest these stats are collected for
     */
    private final int opId;

    /* Variables to log different times in the life cycle of an EvalRequest */
    private long sentTime;
    private long receivedTime;


    public EvalReqStatsClient(int opId) {
        this.opId = opId;
    }

    public int getOpId() {
        return opId;
    }

    public long getSentTime() {
        return sentTime;
    }

    public void setSentTime(long sentTime) {
        this.sentTime = sentTime;
    }

    public long getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(long receivedTime) {
        this.receivedTime = receivedTime;
    }
}
