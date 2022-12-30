package de.optscore.vscale.util;

import bftsmart.tom.AsynchServiceProxy;
import bftsmart.tom.core.messages.TOMMessageType;
import com.jcabi.ssh.Ssh;
import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.EvalRequest;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Dominik Meißner, Gerhard Habiger
 */
public class TestcaseCoordinator implements Runnable {

    public static final String DEFAULT_TEST_COORDINATOR_HOST = "127.0.0.1";
    public static final int DEFAULT_TEST_COORDINATOR_PORT = 13337;

    private static final String id_rsaKeyfilePath = "/Users/[username]]/.ssh/[key]";
    private static final String sshUsername = "[username]]";
    private static final String outputDir = "eval-output/";
    private static final int sshPort = 22;

    private String testCoordinatorIP;
    private int testCoordinatorPort;

    private String testcaseConffilePath;
    private TestcaseConfiguration[] testcaseConfigurations;

    private Ssh[] replicaSsh;
    private Ssh[] clientSsh;

    private CyclicBarrier barrier;
    private ServerSocket serverSocket;
    private ExecutorService threadPool;

    private ScheduledExecutorService baseLoadGenerator = Executors.newScheduledThreadPool(1);
    private final EvalRequest dummyRequest = new EvalRequest.EvalRequestBuilder()
            .action(EvalActionType.READONLY, 0)
            .build();

    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(TestcaseCoordinator.class.getName());

    public TestcaseCoordinator(String confFilePath) throws IOException {
        this(confFilePath, DEFAULT_TEST_COORDINATOR_HOST, DEFAULT_TEST_COORDINATOR_PORT);
    }

    public TestcaseCoordinator(String confFilePath, String coordinatorIP, int coordinatorPort)
            throws
            IOException {
        logger.setLevel(Level.FINER);

        this.testcaseConffilePath = confFilePath;
        this.testCoordinatorIP = coordinatorIP;
        this.testCoordinatorPort = coordinatorPort;
        serverSocket = new ServerSocket(coordinatorPort);
        serverSocket.setSoTimeout(0);
        threadPool = Executors.newCachedThreadPool();

        logger.finer("TestcaseCoordinator running ...");
    }

    private void loadTestcaseConfigurations() {
        try {
            BufferedReader testcaseReader = new BufferedReader(new FileReader(testcaseConffilePath));
            testcaseConfigurations = testcaseReader.lines().skip(1).map(this::createTestCaseConfFromCSVLine).toArray
                    (TestcaseConfiguration[]::new);
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private TestcaseConfiguration createTestCaseConfFromCSVLine(String csvLine) {
        // Testcase name,UDS on/off,# of active Cores,UDS prims,UDS steps,# of replicas,# of clientWorkers,workloadId,
        // warmup phase,measurement phase,clientIncrement,times to increment,replica IPs,clientworker IPs
        String[] csvElements = csvLine.split(",");

        if(csvElements.length < 16) {
            System.out.println("===== Missing Testcase arguments in supplied .csv!");
            System.out.println("Needed per line in CSV: <Testcase name>," +
                    ",<st/mt>,<numOfActiveCores>,<numOfMaxCores>" +
                    "<UDSprims>,<UDSsteps>," +
                    "<numOfReplicas>,<numOfClientWorkers>,<workloadId> " +
                    "<warmupPhaseSecs>,<measurementPhaseSecs>," +
                    "<clientIncrement>,<timestoIncrement>," +
                    "<replicaIPs>,<clientWorkerIPs>");
            System.out.println("test_case_400,mt,4,32,8,1,4,4,2,10,10,2,6,250," +
                    "127.0.0.1-127.0.0.2-127.0.0.3-127.0.0.4,127.0.0.5-127.0.0.6-127.0.0.7-127.0.0.8");
            System.out.println("=====");
            System.exit(15);
        }

        TestcaseConfiguration conf = new TestcaseConfiguration();
        conf.name = csvElements[0];
        conf.withUDS = csvElements[1].equalsIgnoreCase("mt");
        conf.numOfActiveCores = Integer.parseInt(csvElements[2]);
        conf.numOfMaxCores = Integer.parseInt(csvElements[3]);
        conf.udsConfPrim = conf.withUDS ? Integer.parseInt(csvElements[4]) : 1;
        conf.udsConfSteps = conf.withUDS ? Integer.parseInt(csvElements[5]) : 1;
        conf.numOfReplicas = Integer.parseInt(csvElements[6]);
        conf.numOfClientWorkers = Integer.parseInt(csvElements[7]);
        conf.workloadId = Integer.parseInt(csvElements[8]);
        conf.warmupPhaseSecs = Integer.parseInt(csvElements[9]);
        conf.measurePhaseSecs = Integer.parseInt(csvElements[10]);
        conf.clientIncrement = Integer.parseInt(csvElements[11]);
        conf.timesToIncrement = Integer.parseInt(csvElements[12]);
        conf.baseLoadInterval = Integer.parseInt(csvElements[13]);
        conf.replicaIPs = csvElements[14].split("-");
        conf.clientWorkerIPs = csvElements[15].split("-");

        // initialize barrier for SyncClient waiting stuff
        barrier = new CyclicBarrier(conf.clientWorkerIPs.length);

        if(conf.numOfReplicas != conf.replicaIPs.length || conf.numOfClientWorkers != conf.clientWorkerIPs.length) {
            logger.severe("Mismatch of supplied replicaIPs or clientIPs with number of configured replicas or " +
                    "clientWorkers. Aborting ...");
            System.exit(8);
        }

        // static configuration for current testcases
        conf.asyncMode = false;
        conf.interactiveMode = false;

        return conf;
    }

    public void run() {

        // load test case configurations from testcase config file
        loadTestcaseConfigurations();
        
        // start main testcase loop
        for(int i = 0; i < testcaseConfigurations.length; i++) {
            TestcaseConfiguration currentConf = testcaseConfigurations[i];
            logger.info("##### Starting Testcase " + currentConf.name + " (" + currentConf.getTestCaseName() + ")");

            try {
                // Connect to all replicas and clientWorkers
                logger.info("Connecting to " + currentConf.numOfReplicas + " replicas and " + currentConf
                    .numOfClientWorkers + " clientWorkers...");
                replicaSsh = new Ssh[currentConf.numOfReplicas];
                clientSsh = new Ssh[currentConf.numOfClientWorkers];
                for(int j = 0; j < currentConf.numOfReplicas; j++) {
                    replicaSsh[j] = sshLogin(currentConf.replicaIPs[j], sshPort, sshUsername, id_rsaKeyfilePath);
                }
                for(int j = 0; j < currentConf.numOfClientWorkers; j++) {
                    clientSsh[j] = sshLogin(currentConf.clientWorkerIPs[j], sshPort, sshUsername, id_rsaKeyfilePath);
                }

                // if all connections have been established successfully, continue; also create output streams for
                // debug files for both replicas and clientWorkers
                boolean connectionsSuccessful = true;
                FileOutputStream[] replicaFos = new FileOutputStream[replicaSsh.length];
                FileOutputStream[] clientWorkerFos = new FileOutputStream[clientSsh.length];
                for(int j = 0; j < replicaSsh.length; j++) {
                    connectionsSuccessful &= (replicaSsh[j] != null);
                    replicaFos[j] = new FileOutputStream(new File(outputDir + "replica-debug/" +
                            currentConf.name + "-replica" + j + ".out"));
                }
                for(int j = 0; j < clientSsh.length; j++) {
                    connectionsSuccessful &= (clientSsh[j] != null);
                    clientWorkerFos[j] = new FileOutputStream(new File(outputDir + "clientWorker-debug/" +
                            currentConf.name + "-clientWorker" + j + ".out"));
                }
                if(connectionsSuccessful) {
                    logger.finer("All shells created, connections to servers and clients active");

                    // start BFT-SMaRt replicas
                    logger.finer("Starting BFT-SMaRt replicas ...");
                    IntStream.range(0, currentConf.numOfReplicas).forEach(
                            j -> threadPool.execute(() -> {
                                try {
                                    replicaSsh[j].exec(startBftReplicaCommand(j, currentConf.withUDS),
                                            null,
                                            replicaFos[j],
                                            replicaFos[j]);
                                } catch(IOException e) {
                                    e.printStackTrace();
                                }
                            })
                    );

                    // wait until replicas have connected and are ready for requests
                    try {
                        Thread.sleep(10 * 1000);
                    } catch(InterruptedException e) {
                        // ain't happenin', and if so we're shit out of luck
                        logger.severe("Testcasecoordinator was interrupted while waiting for replicas to connect." +
                                " Test might be corrupt if we continue. Aborting ...");
                        System.exit(10);
                    }

                    // reconfigure replicas for testcase
                    AsynchServiceProxy controlProxy = new AsynchServiceProxy(5000); // orchestration clientId := 5000

                    logger.info("##### Reconfiguring replicas for test case " + currentConf.getTestCaseName());
                    EvalRequest.EvalRequestBuilder reconfigRequestBuilder;

                    // first activate all cores ... //FIXME disabling/re-enabling of cores broken on Epyc machines
                    reconfigRequestBuilder = new EvalRequest.EvalRequestBuilder();
                    /*for(int j = 0; j < currentConf.numOfMaxCores; j++) {
                        reconfigRequestBuilder.action(EvalActionType.ADD_CPU_CORE, j);
                    }*/
                    // ... then deactivate some/none depending on configuration
                    for(int j = currentConf.numOfMaxCores; j > currentConf.numOfActiveCores; j--) {
                        reconfigRequestBuilder.action(EvalActionType.REMOVE_CPU_CORE, j - 1);
                    }
                    // configure UDS primaries // TODO add way to reconfig steps
                    reconfigRequestBuilder.action(EvalActionType.RECONFIG_UDS, currentConf.udsConfPrim);

                    // tell replicas to start logging CPU load
                    reconfigRequestBuilder.action(EvalActionType.STATS_START, currentConf.getTestCaseIdFromName());

                    EvalRequest reconfigRequest = reconfigRequestBuilder.build();

                    // use asynch because UDS could hang, i.e. no replies will be sent if no additional requests come in
                    // we don't actually care about replies though, just want to tell the servers to reconfig
                    controlProxy.invokeAsynchRequest(EvalRequest.serializeEvalRequest(reconfigRequest), null,
                            TOMMessageType.ORDERED_REQUEST);

                    // wait just a little bit so the reconfig happened before clients connect and send requests
                    try {
                        Thread.sleep(5000);
                    } catch(InterruptedException e) {
                        // doesn't really matter if we don't wait the full 5000ms
                    }

                    // start dummyReqSender. This will put a basic load on the server so it completes UDS rounds in
                    // cases where there are less clients than primaries. Can be same for every test case if
                    // configured correctly, so should not be a problem if done right.
                    if(currentConf.baseLoadInterval > 0) {
                        final byte[] serializedDummyReq = EvalRequest.serializeEvalRequest(dummyRequest);
                        logger.info("Starting baseLoadGenerator (1 dummyReq every " + currentConf.baseLoadInterval +
                                "ms)");
                        baseLoadGenerator.scheduleAtFixedRate(() -> {
                            logger.finest("Sending baseload dummyReq ...");
                            controlProxy.invokeAsynchRequest(serializedDummyReq, null,
                                    TOMMessageType.ORDERED_REQUEST);
                        }, 100, currentConf.baseLoadInterval, TimeUnit.MILLISECONDS);
                    }

                    // replicas should be up and reconfigured, so start clients
                    logger.finer("Starting clientWorkers ...");
                    IntStream.range(0, currentConf.numOfClientWorkers).forEach(
                            j -> threadPool.execute(() -> {
                                try {
                                    clientSsh[j].exec(startClientCommand(calcStartingProcessIdForClient(j,
                                            currentConf.timesToIncrement, currentConf.clientIncrement), currentConf),
                                            null,
                                            clientWorkerFos[j],
                                            clientWorkerFos[j]);
                                } catch(IOException e) {
                                    e.printStackTrace();
                                }
                            })
                    );

                    // now we open server sockets and wait for clients to finish their work ...
                    int numOfClientsConnected = 0;
                    CyclicBarrier allClientsDone = new CyclicBarrier(currentConf.numOfClientWorkers + 1);
                    while(numOfClientsConnected < currentConf.numOfClientWorkers) {
                        try {
                            // wait for a syncClient to connect
                            Socket socket = serverSocket.accept();
                            numOfClientsConnected++;
                            // start a new thread to handle the syncClient
                            threadPool.execute(() -> {
                                logger.finer("SyncClient connected " + socket.getInetAddress());
                                try {
                                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                                    PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

                                    for(String line; (line = reader.readLine()) != null; ) {
                                        logger.finest(line);
                                        String[] parts = line.split("-", 2);
                                        try {
                                            if(parts.length == 2 && parts[0].equals("ready")) {
                                                logger.finer("Info: received ready from " + socket.toString());
                                                // wait on the barrier until all other syncClients are ready
                                                barrier.await();
                                                // all syncClients have sent ready-messages, tell every client to proceed
                                                writer.println("go-" + parts[1]);
                                                logger.finer("Info: sending go to " + socket.toString());
                                            } else if(parts.length == 2 && parts[0].equals("close")) {
                                                break;
                                            } else {
                                                logger.warning("Warning: received unknown message type");
                                            }
                                        } catch(InterruptedException | BrokenBarrierException e) {
                                            // in case the barrier breaks, send a reset to the clients
                                            writer.println("reset-" + parts[1]);
                                            logger.warning("Warning: barrier broke, resetting clients");
                                        }
                                    }
                                } catch(IOException e) {
                                    e.printStackTrace();
                                }

                                // wait until all clients are finished with all testruns, then terminate
                                try {
                                    logger.fine("Waiting on all Clients done barrier");
                                    allClientsDone.await();
                                    logger.fine("All Clients done barrier reached");
                                } catch(InterruptedException | BrokenBarrierException e1) {
                                    e1.printStackTrace();
                                }
                            });
                        } catch(IOException e) {
                            e.printStackTrace();
                        }
                    }

                    // wait until testcase is completed
                    try {
                        allClientsDone.await();

                        // tell replicas to dump logged CPU stats
                        controlProxy.invokeAsynchRequest(EvalRequest.serializeEvalRequest(new EvalRequest
                                        .EvalRequestBuilder().action(EvalActionType.STATS_DUMP, currentConf
                                        .getTestCaseIdFromName()).build()),
                                null, TOMMessageType.ORDERED_REQUEST);

                        // wait 10 secs until CPU stats are dumped and hanging requests are done / queues are emptied
                        threadSleep(5 * 1000);

                        logger.info("##### Testcase done, killing all JVMs!");

                        // discard controlProxy
                        controlProxy.close();

                        // kill all JVMs
                        IntStream.range(0, currentConf.numOfClientWorkers).forEach(
                                j -> threadPool.execute(() -> {
                                    try {
                                        clientSsh[j].exec("sudo killall -15 java",
                                                null,
                                                null,
                                                null
                                        );
                                    } catch(IOException e) {
                                        e.printStackTrace();
                                    }
                                })
                        );
                        IntStream.range(0, currentConf.numOfReplicas).forEach(
                                j -> threadPool.execute(() -> {
                                    try {
                                        replicaSsh[j].exec("sudo killall -15 java",
                                                null,
                                                null,
                                                null
                                        );
                                    } catch(IOException e) {
                                        e.printStackTrace();
                                    }
                                })
                        );

                        // transfer all created output files from cluster
                        logger.info("Transferring all created output files to TestCoordinator host...");
                        ProcessBuilder rsyncBuilder;
                        String evalOutputPath = ":~/optscore-parallel-vscale-smr/eval-output-";
                        String user = System.getProperty("user.name");
                        for(int j = 0; j < currentConf.clientWorkerIPs.length; j++) {
                            // transfer client request stats
                            rsyncBuilder = new ProcessBuilder();
                            rsyncBuilder.inheritIO();
                            try {
                                rsyncBuilder.command("rsync",  "-avzh", user + "@" + currentConf.clientWorkerIPs[j] +
                                        evalOutputPath
                                        + "clients/" + currentConf.name + "/*", "eval-output/"+ currentConf.name + "/");
                                rsyncBuilder.start().waitFor();
                            } catch(IOException e) {
                                e.printStackTrace();
                            }
                        }
                        for(int j = 0; j < currentConf.replicaIPs.length; j++) {
                            rsyncBuilder = new ProcessBuilder();
                            rsyncBuilder.inheritIO();
                            try {
                                // transfer CPU logs from replicas
                                rsyncBuilder.command("rsync", "-avzh", user + "@" + currentConf.replicaIPs[j] +
                                        evalOutputPath
                                        + "servers/" + currentConf.name + "/*", "eval-output/" + currentConf.name + "/");
                                rsyncBuilder.start().waitFor();
                            } catch(IOException e) {
                                e.printStackTrace();
                            }
                        }

                        // wait between testcases, just for safety and termination of everything. Probably unnecessary
                        Thread.sleep(2000);
                    } catch(InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }

                } else {
                    logger.severe("Could not create one or more connections to replicas/clientWorkers. Aborting...");
                    System.exit(9);
                }
            } catch(FileNotFoundException e) {
                e.printStackTrace();
            }

        }

        logger.info("All testcases completed. Shutting down TestcaseCoordinator...");
        baseLoadGenerator.shutdown();
        threadPool.shutdown();

    }

    private Ssh sshLogin(String hostname, int port, String username, String keyfilePath) {
        File keyFile = new File(keyfilePath);
        try {
            BufferedReader kfReader = new BufferedReader(new InputStreamReader(new FileInputStream(keyFile)));
            String privKey = kfReader.lines().collect(Collectors.joining("\n"));
            kfReader.close();
            return new Ssh(hostname, port, username, privKey);
        } catch(IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String startBftReplicaCommand(int processId, boolean withUDS) {
        return "cd /home/[username]/optscore-parallel-vscale-smr && sudo java -server -Xms4g -Xmx32g -Djava.util.logging.config" +
                ".file=" +
                "/home/[username]/optscore-parallel-vscale-smr/logging.properties " +
                "-cp lib/*:out/production/optscore-parallel-vscale-smr de.optscore.vscale.server.EvalServer " +
                processId + " " + withUDS;
    }

    private String startClientCommand(int startingProcessId, TestcaseConfiguration conf) {
        return "cd /home/[username]/optscore-parallel-vscale-smr && " +
                "java -Djava.util.logging.config.file=/home/[username]/optscore-parallel-vscale-smr/logging.properties " +
                "-cp lib/*:out/production/optscore-parallel-vscale-smr " +
                "de.optscore.vscale.client.ClientWorker " +
                startingProcessId + " " +
                conf.name + " " + conf.withUDS + " " + conf.numOfActiveCores + " " +
                conf.udsConfPrim + " " + conf.udsConfSteps + " " +
                conf.numOfReplicas + " " + conf.numOfClientWorkers + " " + conf.workloadId + " " +
                conf.warmupPhaseSecs + " " + conf.measurePhaseSecs + " " +
                conf.clientIncrement + " " + conf.timesToIncrement + " " + conf.baseLoadInterval + " " +
                testCoordinatorIP + " " + testCoordinatorPort;
    }

    private int calcStartingProcessIdForClient(int startingProcessId, int timesToIncrement, int clientIncrement) {
        return 1000 + (startingProcessId * timesToIncrement * clientIncrement);
    }

    private static void threadSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Please specify a configuration file path.");
            System.exit(1);
        }
        String confFilePath = args[0];
        TestcaseCoordinator coord = null;
        try {
            if(args.length == 1) {
                coord = new TestcaseCoordinator(confFilePath);
            } else if(args.length == 3) {
                coord = new TestcaseCoordinator(confFilePath, args[1], Integer.parseInt(args[2]));
            } else {
                System.out.println("Please specify a hostname and port number for the TestcaseCoordinator");
                System.exit(2);
            }
            new Thread(coord).start();
        } catch(IOException e) {
            e.printStackTrace();
        }


    }
}
