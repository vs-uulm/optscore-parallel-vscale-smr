package de.optscore.vscale.client;

import de.optscore.vscale.EvalRequest;
import de.optscore.vscale.util.EvalReqStatsClient;

import java.util.ArrayList;

public interface EvalClient {

    void sendRequest(EvalRequest request);

    void shutDownClient();

    ArrayList<EvalReqStatsClient> getStats();

    int getOperationId();

    void startLogging();

    void stopLogging(boolean eraseOldData);

}
