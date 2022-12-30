package de.optscore.vscale.client;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import de.optscore.vscale.EvalRequest;
import de.optscore.vscale.util.EvalReqStatsClient;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class InteractiveAsyncEvalClient extends AsyncEvalClient implements ReplyListener {

    private static final Logger logger = Logger.getLogger(AsyncEvalClient.class.getName());

    private List<EvalReqStatsClient> reqStats;
    private List<TOMMessage[]> replicaReplies;

    public InteractiveAsyncEvalClient(int clientId) {
        super(clientId, 0);
        this.reqStats = new ArrayList<>(1000);
        replicaReplies = new ArrayList<>(1000);
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);
    }

    @Override
    public void sendRequest(EvalRequest request) {
        // create byte array for sending to replicas
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        int operationId;
        // sequentially write action-parameter array to byte-array (action|parameter|action|parameter|etc...)
        try {
            int[][] actions = request.getActions();
            for(int[] action : actions) {
                dos.writeInt(action[0]);
                dos.writeInt(action[1]);
            }

            // send the request to all replicas and create stats object for remembering times
            operationId = ((AsynchServiceProxy) this.proxy).invokeAsynchRequest(bos.toByteArray(), this,
                    TOMMessageType.ORDERED_REQUEST);
            EvalReqStatsClient requestStats = new EvalReqStatsClient(operationId);
            requestStats.setSentTime(System.currentTimeMillis());
            this.reqStats.add(operationId, requestStats);

        } catch(IOException e) {
            logger.severe("IOException while building EvalRequest: " + e.getMessage());
        }
    }

    @Override
    public void replyReceived(RequestContext context, TOMMessage reply) {
        // save reply in corresponding TOMMessage reply-array
        replicaReplies.get(reply.getOperationId())[reply.getSender()] = reply;

        if(replicaReplyReceivedSatisfiesQuorum(context, reply)) {
            this.reqStats.get(reply.getOperationId()).setReceivedTime(System.currentTimeMillis());
        }

        logger.finer("Interactive AsyncEvalClient received reply for request #" + context.getOperationId() +
                " from replica " + reply.getSender());

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
        TOMMessage[] replies = replicaReplies.get(reply.getOperationId());
        for(TOMMessage replicaReply : replies) {
            if(replicaReply != null && Arrays.equals(replicaReply.getContent(), reply.getContent())) {
                sameContent++;
            }
        }

        if(sameContent >= getReplyQuorum()) {
            // forget received replies so they can be GCed
            logger.finest("Client# " + proxy.getProcessId() + ": Received enough replies for request with opId " +
                    reply.getOperationId());
            replicaReplies.remove(reply.getOperationId());
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

}
