package de.optscore.vscale.client;

import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.EvalRequest;
import de.optscore.vscale.util.CSVWriter;
import de.optscore.vscale.util.EvalReqStatsClient;
import de.optscore.vscale.util.TestcaseConfiguration;
import de.optscore.vscale.util.TestcaseSyncClient;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientWorker {

    private static final Logger logger = Logger.getLogger(ClientWorker.class.getName());
    public static final Level GLOBAL_LOGGING_LEVEL = Level.WARNING;

    public static final long BENCHMARK_BEGIN_CLIENT = System.currentTimeMillis() * 1000000;
    public static final long NANOTIME_OFFSET_CLIENT = System.nanoTime();

    private final EvalRequest dummyRequest = new EvalRequest.EvalRequestBuilder()
            .action(EvalActionType.READONLY, 0)
            .build();

    private CSVWriter[] statsWriters;

    private int startingProcessId;
    private TestcaseConfiguration testcaseConfiguration;
    private ExecutorService clientThreadPool;
    private TestcaseSyncClient syncClient;

    private EvalClientRunner[] evalClientRunners;

    public ClientWorker(int startingProcessId, String coordinatorIP,
                        int coordinatorPort, TestcaseConfiguration testcaseConfiguration) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);
        logger.info("Starting UDS + BFT Test Engine ...");

        this.startingProcessId = startingProcessId;
        this.testcaseConfiguration = testcaseConfiguration;

        this.clientThreadPool = Executors.newCachedThreadPool();

        // Start syncClient thread for syncing with other clientWorkers
        this.syncClient = new TestcaseSyncClient(coordinatorIP, coordinatorPort);
        clientThreadPool.execute(syncClient);
        logger.finer("SyncClient started and connected to TestCoordinator");
    }

    private void prepareTestcase() {
        // prepare output folders for raw data
        if(new File("eval-output-clients/" + testcaseConfiguration.name + "/").mkdirs()) {
            logger.finest("Created output directory for raw client stats CSV files");
        } else {
            logger.warning("Could not create new directory for stats file dumps. Directory either already " +
                    "existed or permissions are incorrect.");
        }

        // prepare CSV file writers for raw data. One for each runner
        statsWriters = new CSVWriter[testcaseConfiguration.timesToIncrement * testcaseConfiguration
                .clientIncrement];
        for(CSVWriter writer : statsWriters) {
            writer = null;
        }

        // prepare data structures for holding EvalClients and their results
        evalClientRunners = new EvalClientRunner[testcaseConfiguration.timesToIncrement * testcaseConfiguration
                .clientIncrement];
        for(EvalClientRunner runner : evalClientRunners) {
            runner = null;
        }

        // wait here until all clientWorkers are ready
        syncClient.waitForAllClientsReady();

        // return after everything has been set up
    }

    private void doTestcaseCycles() {
        // replicas should have been configured for testcase and all clientWorkers are ready; start testing cycles

        for(int cycle = 0; cycle < testcaseConfiguration.timesToIncrement; cycle++) {
            // start {ci} clients
            logger.info("Starting cycle #" + (cycle + 1) + "/" + testcaseConfiguration.timesToIncrement +
                    " and adding " + testcaseConfiguration.clientIncrement + " additional clients ...");
            int startingOffset = cycle * testcaseConfiguration.clientIncrement;
            for(int j = startingOffset; j < startingOffset +  testcaseConfiguration.clientIncrement; j++) {
                logger.finest("Adding new EvalClientRunner #" + (startingProcessId + j) + " to pool of runners (at " +
                        "index " + j);
                evalClientRunners[j] = new EvalClientRunner(startingProcessId + j, testcaseConfiguration);
                statsWriters[j] = new CSVWriter(new File("eval-output-clients/" + testcaseConfiguration.name + "/" +
                        testcaseConfiguration.getTestCaseName() + "-" + (startingProcessId + j) + ".csv"));
                statsWriters[j].writeLine(Arrays.asList("opId", "sentTimeNano", "recvTimeNano", "cycle"));
                // start the EvalClientRunner threads. They won't send requests until activated
                clientThreadPool.execute(evalClientRunners[j]);
            }
            logger.info("... for a total of " + (startingOffset + testcaseConfiguration.clientIncrement) + " clients");

            // wait until connections have safely been established, then start sending requests
            threadSleep(10000);

            // wait until all clients are created and successfully connected
            syncClient.waitForAllClientsReady();

            // now start sending requests and initiate warmup-phase
            logger.fine("Starting all EvalClientRunners that are not already active ...");
            for(EvalClientRunner runner : evalClientRunners) {
                if(runner != null) {
                    runner.setClientActive(true);
                }
            }

            // wait {wu} seconds
            logger.fine("Waiting " + testcaseConfiguration.warmupPhaseSecs + " seconds (warmup)...");

            threadSleep(testcaseConfiguration.warmupPhaseSecs * 1000);

            logger.info("Warmed up for " + testcaseConfiguration.warmupPhaseSecs + " seconds. Starting logging...");
            for(EvalClientRunner runner : evalClientRunners) {
                if(runner != null) {
                    runner.getClient().startLogging();
                }
            }

            // log for {me} seconds
            threadSleep(testcaseConfiguration.measurePhaseSecs * 1000);

            logger.info("Logged for " + testcaseConfiguration.measurePhaseSecs + " seconds. Stopping logging and " +
                    "dumping stats...");
            for(int j = 0; j < evalClientRunners.length; j++) {
                EvalClientRunner runner = evalClientRunners[j];
                if(runner != null) {
                    // first stop generating new logging results
                    runner.getClient().stopLogging(false);
                    ArrayList<EvalReqStatsClient> runnerStats = runner.getClient().getStats();
                    logger.info("Logged " + runnerStats.size() + " reqStats for runner #" + runner.getProcessId());

                    // statsWriters have the same array index as runners; write stats for this cycle and runner to file
                    if(statsWriters[j] != null) {
                        // logging should be turned off now, if this doesn't work I need to re-do this whole part...
                        try {
                            for(EvalReqStatsClient singleReqStats : runnerStats) {
                                statsWriters[j].writeLine(new long[]{singleReqStats.getOpId(), singleReqStats
                                        .getSentTime(), singleReqStats.getReceivedTime(), cycle });
                            }
                            statsWriters[j].flush();
                        } catch(ConcurrentModificationException e) {
                            // I love concurrency
                            logger.severe("Logging was still enabled while trying to dump stats to disk... Testcase " +
                                    "data now likely corrupt. Please restart testcase.");
                            e.printStackTrace();
                        }
                    }

                    // throw away stats of client, they're now saved to disk
                    runner.getClient().stopLogging(true);
                }
            }

            // wait until all clients have dumped their stats and then continue with the next cycle or end the testcase
            syncClient.waitForAllClientsReady();
        }

        logger.info("Testcase cycles done ... shutting down");

        for(EvalClientRunner runner : evalClientRunners) {
            if(runner != null) {
                runner.setClientActive(false);
            }
        }

        // wait a bit so all requests are likely done (with {bl} even when UDS is waiting) and we don't get exceptions
        threadSleep(5000);

        // shut down runners
        for(EvalClientRunner runner : evalClientRunners) {
            if(runner != null) {
                runner.close();
            }
        }

        for(CSVWriter writer : statsWriters) {
            writer.closeWriter();
        }
        try {
            // this will notify the TestcaseCoordinator that this testcase is finished and the next one can be started
            syncClient.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
        clientThreadPool.shutdown();
    }

    private static void threadSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // parse passed info to create TestcaseConfiguration
        if(args.length < 16) {
            System.out.println("===== Missing arguments");
            System.out.println("Usage: ClientWorker <startingProcessId> " +
                    "<testcaseName> <withUDS> <numOfActiveCores> " +
                    "<UDSprims> <UDSsteps> " +
                    "<numOfReplicas> <numOfClientWorkers> <workloadId> " +
                    "<warmupPhaseSecs> <measurementPhaseSecs> " +
                    "<clientIncrement> <timestoIncrement> <baseLoadInterval>" +
                    "<testcaseCoordinatorIP> <testcaseCoordinatorPort>");
            System.out.println("Example: EvalClientRunner 1000 " +
                    "test_case_412 true 4 " +
                    "8 1 " +
                    "4 4 2 " +
                    "10 10 " +
                    "1 20 250" +
                    "127.0.0.1 13337");
            System.out.println("=====");
            System.exit(16);
        }

        int startingProcessId = Integer.parseInt(args[0]);
        TestcaseConfiguration conf = new TestcaseConfiguration();
        conf.name = args[1];
        conf.withUDS = Boolean.parseBoolean(args[2]);
        conf.numOfActiveCores = Integer.parseInt(args[3]);
        conf.udsConfPrim = Integer.parseInt(args[4]);
        conf.udsConfSteps = Integer.parseInt(args[5]);
        conf.numOfReplicas = Integer.parseInt(args[6]);
        conf.numOfClientWorkers = Integer.parseInt(args[7]);
        conf.workloadId = Integer.parseInt(args[8]);
        conf.warmupPhaseSecs = Integer.parseInt(args[9]);
        conf.measurePhaseSecs = Integer.parseInt(args[10]);
        conf.clientIncrement = Integer.parseInt(args[11]);
        conf.timesToIncrement = Integer.parseInt(args[12]);
        conf.baseLoadInterval = Integer.parseInt(args[13]);
        String testcaseCoordinatorIP = args[14];
        int testcaseCoordinatorPort = Integer.parseInt(args[15]);

        conf.asyncMode = false;
        conf.interactiveMode = false;

        // create clientWorker
        ClientWorker worker = new ClientWorker(startingProcessId, testcaseCoordinatorIP, testcaseCoordinatorPort, conf);

        // prepare testcase
        worker.prepareTestcase();

        // start measurements
        worker.doTestcaseCycles();
    }
}
