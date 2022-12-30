package de.optscore.vscale.client;

import bftsmart.tom.ServiceProxy;
import de.optscore.vscale.EvalRequest;
import de.optscore.vscale.util.EvalReqStatsClient;

import java.util.ArrayList;
import java.util.logging.Logger;

public class SyncEvalClient extends ServiceProxy implements EvalClient {

    private static final Logger logger = Logger.getLogger(SyncEvalClient.class.getName());

    private boolean statsLoggingOn;
    private ArrayList<EvalReqStatsClient> stats = null;

    public SyncEvalClient(int procId) {
        super(procId);

        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);
        this.statsLoggingOn = false;
        stats = new ArrayList<>(30000);
    }

    public void sendRequest(EvalRequest request) {
            // tmp save time of before sending request, since opId (= index in array of times) is not yet available
        long beforeSend = ClientWorker.BENCHMARK_BEGIN_CLIENT + System.nanoTime() - ClientWorker.NANOTIME_OFFSET_CLIENT;

        // send the request to all replicas, and we don't actually care about the reply at the moment
        this.invokeOrdered(EvalRequest.serializeEvalRequest(request));

        if(statsLoggingOn) {
            // remember times now, after operationId is available (via ServiceProxy superclass) and the replies have arrived
            long receivedTime = ClientWorker.BENCHMARK_BEGIN_CLIENT + System.nanoTime() - ClientWorker
                    .NANOTIME_OFFSET_CLIENT;
            EvalReqStatsClient requestStats = new EvalReqStatsClient(operationId);
            requestStats.setSentTime(beforeSend);
            requestStats.setReceivedTime(receivedTime);
            stats.add(requestStats);
        }

    }

    @Override
    public ArrayList<EvalReqStatsClient> getStats() {
        return stats;
    }

    @Override
    public void startLogging() {
        this.statsLoggingOn = true;
        logger.finest("Logging was enabled for EvalClient #" + this.getProcessId());
    }

    @Override
    public void stopLogging(boolean eraseOldData) {
        if(eraseOldData) {
            this.stats.clear();
            this.stats = new ArrayList<>(30000);
        }
        this.statsLoggingOn = false;
        logger.finest("Logging was disabled for EvalClient #" + this.getProcessId());
    }

    @Override
    public void shutDownClient() {
        this.close();
    }

    @Override
    public int getOperationId() {
        return operationId;
    }
}
