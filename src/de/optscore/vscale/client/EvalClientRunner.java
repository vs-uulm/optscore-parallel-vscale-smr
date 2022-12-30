package de.optscore.vscale.client;


import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.EvalRequest;
import de.optscore.vscale.util.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Threaded test runner, which spawns an EvalClient to fire off requests to all replicas
 */
public class EvalClientRunner implements Runnable {

    private static final Logger logger = Logger.getLogger(ClientWorker.class.getName());

    private int processId;

    EvalClient client;
    List<EvalRequest> requests;

    private TestcaseConfiguration conf;

    private Random random;

    private boolean shutDownReceived;
    private boolean clientActive;

    /**
     * Creates a basic runner without (A)syncEvalClient initialization.
     * Useful for extending this class (e.g. InteractiveEvalClientRunner).
     */
    public EvalClientRunner(int id) {
        this.processId = id;
        initRequests();
    }

    /**
     * Creates a new EvalClientRunner with a given Id.
     * @param id Id of this runner for gathering statistics per runner
     */
    public EvalClientRunner(int id, TestcaseConfiguration conf) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        this.processId = id;

        // create EvalClient running normal testcases; immediately connects to replicas on creation
        // TODO properly pass withStatsLogging, remove/modify AsyncEvalClient
        this.client = conf.asyncMode ? new AsyncEvalClient(this.processId, 4) : new
                SyncEvalClient(this.processId);

        // new Random with processId as seed for determinism
        this.random = new Random(id);

        this.conf = conf;
        this.shutDownReceived = false;
        // don't send requests right away, wait until toggled on
        this.clientActive = false;
        initRequests();
    }

    /**
     * Creates new requests for a test run of this EvalClientRunner
     * // TODO use workloadIDs and pre-specified workloads
     */
    void initRequests() {
        // initialize requests
        this.requests = new ArrayList<>(5);
        EvalRequest req0 = new EvalRequest.EvalRequestBuilder().action(EvalActionType.SIMULATELOAD, 0).build();
        EvalRequest req1 = new EvalRequest.EvalRequestBuilder().action(EvalActionType.SIMULATELOAD, 250000).build();
        EvalRequest req2 = new EvalRequest.EvalRequestBuilder().action(EvalActionType.SIMULATELOAD, 1000000).build();
        EvalRequest req3 = new EvalRequest.EvalRequestBuilder().build(); // dummyReq since it will be overwritten
        EvalRequest req4 = new EvalRequest.EvalRequestBuilder()
                .action(EvalActionType.LOCK, 0)
                .action(EvalActionType.UNLOCK, 0)
                .action(EvalActionType.SIMULATELOAD, 250000)
                .action(EvalActionType.LOCK, 0)
                .action(EvalActionType.UNLOCK, 0)
                .build();
        this.requests.add(req0);
        this.requests.add(req1);
        this.requests.add(req2);
        this.requests.add(req3);
        this.requests.add(req4);
    }

    /**
     * Starts this EvalClientRunner, which means it will fire off requests until it is stopped
     */
    @Override
    public void run() {
        logger.finer("Starting EvalClientRunner #" + this.getProcessId());
        int workloadId = conf.workloadId;
        // create requests for workload3
        EvalRequest[] workload3Reqs = new EvalRequest[32];
        for(int i = 0; i < workload3Reqs.length; i++) {
            workload3Reqs[i] = new EvalRequest.EvalRequestBuilder()
                    .action(EvalActionType.LOCK, i)
                    .action(EvalActionType.SIMULATELOAD, 250000)
                    .action(EvalActionType.UNLOCK, i)
                    .build();
        }
        while(!shutDownReceived) {
            while(clientActive) {
                if(workloadId != 3) {
                    this.client.sendRequest(this.requests.get(workloadId));
                } else {
                    // in case of workload 3, we want to lock one of 32 locks .. so craft the corresponding requests
                    this.client.sendRequest(workload3Reqs[random.nextInt(workload3Reqs.length)]);
                }
            }
            // don't hog the CPU until the actual client/sending of requests has been started
            try {
                Thread.sleep(3);
            } catch(InterruptedException e) {
                // ignore
            }
        }
    }

    public int getProcessId() {
        return processId;
    }

    public EvalClient getClient() {
        return client;
    }

    public boolean isClientActive() {
        return clientActive;
    }

    public void setClientActive(boolean clientActive) {
        this.clientActive = clientActive;
    }

    public void close() {
        this.shutDownReceived = true;
        this.client.shutDownClient();
    }

}
