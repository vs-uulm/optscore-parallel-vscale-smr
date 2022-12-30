package de.optscore.vscale.server;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.SingleExecutable;
import com.sun.management.UnixOperatingSystemMXBean;
import de.optscore.reconfiguration.cpu.CpuReconfigurationException;
import de.optscore.reconfiguration.cpu.CpuReconfigurator;
import de.optscore.reconfiguration.cpu.LinuxCpuReconfigurator;
import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.client.EvalClientRunner;
import de.optscore.vscale.util.CSVWriter;
import de.optscore.vscale.util.EvalReqStatsServer;
import de.uniulm.vs.art.uds.DummySharedState;
import de.uniulm.vs.art.uds.UDSLock;
import de.uniulm.vs.art.uds.UDScheduler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Provides all the code needed to handle client requests, i.e. create, lock and unlock mutexes, or simulate CPU load.
 */
public class EvalServer implements SingleExecutable {

    private ServiceReplica serviceReplica;

    /**
     * Holds DummyUDSharedStates which can be locked, modified and unlocked by EvalClients
     */
    private final List<DummySharedState> sharedStates;
    private final List<Lock> locks;
    private final boolean withUDS;

    public static final long BENCHMARK_BEGINTIME_SERVER = System.currentTimeMillis() * 1000000;
    public static final long NANOTIME_OFFSET_SERVER = System.nanoTime();

    /**
     * For adding and removing CPU cores at runtime
     */
    private CpuReconfigurator cpuReconfigurator;

    /*
     * Stats logging stuff
     */
    public EvalReqStatsServer[][] runStats = null;
    private Map<Long, Double> cpuStats = null;

    private final UnixOperatingSystemMXBean statsBean;
    private final ExecutorService utilityPool = Executors.newCachedThreadPool();

    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(EvalServer.class.getName());

    public EvalServer(int id, boolean withUDS) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        this.withUDS = withUDS;
        try {
            this.cpuReconfigurator = new LinuxCpuReconfigurator();
        } catch(CpuReconfigurationException e) {
            e.printStackTrace();
            logger.severe(e.getMessage());
            logger.severe("Could not load CPU reconfiguration, shutting down server!");
            System.exit(1);
        }

        int sharedStateCount = 32;
        int lockCount = 32;
        // create sharedStates EvalClients can use, which always means: lock the state's inherent UDSLock, add
        // modifying thread's name to state's internal StringList, unlock
        this.sharedStates = new ArrayList<>(sharedStateCount + 1);
        for(int i = 0; i < sharedStateCount; i++) {
            sharedStates.add(new DummySharedState(i, withUDS));
        }

        // create Locks EvalClients can lock/unlock however they want
        this.locks = new ArrayList<>(sharedStateCount + 1);
        if(withUDS) {
            for(int i = 0; i < lockCount; i++) {
                locks.add(new UDSLock());
            }
        } else {
            for(int i = 0; i < lockCount; i++) {
                locks.add(new ReentrantLock());
            }
        }

        this.statsBean = (UnixOperatingSystemMXBean) ManagementFactory
                .getOperatingSystemMXBean();

        // TODO deal with recovery/snapshots, etc
        if(withUDS) {
            this.serviceReplica = new UDSServiceReplica(id, this, null);
        } else {
            this.serviceReplica = new ServiceReplica(id, this, null);
        }
    }


    @Override
    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {
                ByteArrayInputStream bis = new ByteArrayInputStream(command);
        DataInputStream dis = new DataInputStream(bis);

        try {
            // intCount = number of bytes / 4 in byte array
            // intCount / 2 is the number of commands (per command: 1 int for command, 1 int for parameter)
            int cmdCount = dis.available() / 4 / 2;

            // Rebuild and execute actions array from EvalClient
            StringBuilder actionLog = new StringBuilder(Thread.currentThread().getName() + ": Received the following " +
                    "actions from Client " + msgCtx.getSender() + ": [ ");
            int action;
            int parameter;
            boolean badRequest = false;
            byte[] reply = new byte[0];
            for(int i = 0; i < cmdCount; i++) {
                action = dis.readInt();
                parameter = dis.readInt();
                actionLog.append(action).append(',').append(parameter);
                if(i != cmdCount - 1) actionLog.append(" | ");

                switch(action) {
                    case EvalActionType.LOCK:
                        badRequest = lockDummyLock(parameter);
                        break;
                    case EvalActionType.UNLOCK:
                        badRequest = unlockDummyUDSLock(parameter);
                        break;
                    case EvalActionType.ADDTOSHAREDSTATE:
                        badRequest = addToSharedState(parameter);
                        break;
                    case EvalActionType.SIMULATELOAD:
                        if(parameter > 0) {
                            simulateCPULoadNanos(parameter);
                        }
                        break;
                    case EvalActionType.READONLY:
                        // do nothing
                        break;
                    case EvalActionType.STATS_START:
                        // start CPU logging thread
                        cpuStats = new LinkedHashMap<>(500);
                        utilityPool.submit(() -> {
                            logger.finer("CPULogger starting ...");
                            double load;
                            long currentTime;
                            while(cpuStats != null) {
                                load = statsBean.getProcessCpuLoad();
                                currentTime = BENCHMARK_BEGINTIME_SERVER + System.nanoTime() - NANOTIME_OFFSET_SERVER;
                                logger.finest("CPULogger is saving (process-)CPU load of " + load + " at "
                                        + currentTime);
                                try {
                                    cpuStats.put(currentTime, load);
                                    Thread.sleep(250);
                                } catch(InterruptedException e) {
                                    // nothing happens except that we log more CPU load data points than planned
                                } catch(NullPointerException e) {
                                    // theoretically, there is a chance that cpuStats is cleaned inbetween a loop
                                    // start and the actual saving operation. In this case, just ignore the error and
                                    // stop the thread like it was supposed to
                                    logger.fine("CPULogger shutting down because of NullPointerException ...");
                                    break;
                                }
                            }
                            logger.finer("CPULogger shutting down ...");
                        });
                        break;
                    case EvalActionType.STATS_DUMP:
                        // parameter = testcaseId
                        // prepare output folder for raw data
                        if(new File("eval-output-servers/test_case_" + parameter + "/").mkdirs()) {
                            logger.finest("Created output directory for raw client stats CSV files");
                        } else {
                            logger.warning("!!!! Could not create directory for stats file dumps. Please check " +
                                    "whether test case already existed and permissions are correct and maybe restart " +
                                    "testcase.");
                        }

                        logger.finer("Dumping cpuStats for testcase " + parameter);
                        // create new CSVWriter to dump stats to file for this run
                        CSVWriter cpuLogWriter = new CSVWriter(new File("eval-output-servers/test_case_" +
                                parameter + "/test_case_" + parameter + "-replica_" + serviceReplica.getId() +
                                "-cpuLoad.csv"));

                        // write header
                        cpuLogWriter.writeLine(Arrays.asList("nanoTime", "allCoresLoadAvg"));

                        // dump all logged CPU stats
                        for(long time : cpuStats.keySet()) {
                            cpuLogWriter.writeLine(Arrays.asList(Long.toString(time), cpuStats.get(time).toString()));
                        }
                        cpuLogWriter.flush();
                        cpuLogWriter.closeWriter();

                        // clean up all logged stats (timings + CPU load log). Unnecessary, but meh whatever
                        cpuStats.clear();
                        // setting cpuStats to null will stop CPU logging automatically
                        cpuStats = null;

                        break;
                    case EvalActionType.REMOVE_CPU_CORE:
                        logger.warning("Deactivating CPU Core #" + parameter + "!");
                        try {
                            this.cpuReconfigurator.listAvailableCores().get(parameter).setOnline(false);
                        } catch(CpuReconfigurationException e) {
                            e.printStackTrace();
                        }
                        reply = new byte[1];
                        reply[0] = (byte) this.cpuReconfigurator.numberOfActiveCpuCores();
                        break;
                    case EvalActionType.ADD_CPU_CORE:
                        logger.warning("Activating CPU Core #" + parameter + "!");
                        try {
                            this.cpuReconfigurator.listAvailableCores().get(parameter).setOnline(true);
                        } catch(IndexOutOfBoundsException e) {
                            // we tried to activate a core that wasn't present ... which shouldn't matter
                            logger.warning("Got activation request for core #" + parameter + ", while only " +
                                    this.cpuReconfigurator.listAvailableCores().size() + " cores are available. " +
                                    "Ignoring...");
                        }
                        reply = new byte[1];
                        reply[0] = (byte) this.cpuReconfigurator.numberOfActiveCpuCores();
                        break;
                    case EvalActionType.RECONFIG_UDS:
                        logger.info("Reconfiguring UDS...");
                        // TODO add a way to reconfig steps
                        reply = new byte[1];
                        reply[0] = (byte) UDScheduler.getInstance().requestReconfiguration(parameter, 1);
                        break;
                    default:
                        logger.severe(Thread.currentThread().getName() + ": Unrecognized EvalActionType " +
                                "received in request; type was: " + action);
                        return "400 Bad Request".getBytes();
                }
            }
            logger.finer(actionLog.append(" ]").toString());

            if(!badRequest) {
                logger.fine(Thread.currentThread().getName() + ": Successfully executed all actions from " +
                        "EvalClient " + msgCtx.getSender() + " for request with opId " + msgCtx.getOperationId() +
                        ". Returning reply.");
                return reply;
            } else {
                logger.fine(Thread.currentThread().getName() + ": EvalClient " + msgCtx.getSender() +
                        "'s request " + msgCtx.getOperationId() + " was malformed or included bad parameters");
                return reply;
            }
        } catch(IOException e) {
            logger.severe("IOException while reading data from request in replica: " + e.getMessage());
            e.printStackTrace();
        } catch(CpuReconfigurationException e) {
            e.printStackTrace();
            logger.severe("Error while reconfiguring CPUs. Shutting down the server!");
            this.serviceReplica.kill();
            System.exit(1);
        }
        return null;
    }

    private boolean addToSharedState(int sharedStateId) {
        if(sharedStateId >= sharedStates.size()) {
            // bad request
            return true;
        }

        DummySharedState state = sharedStates.get(sharedStateId);
        logger.finer(Thread.currentThread().getName() + " is modifying state in sharedState " + state.getId());
        state.addToSharedState((Thread.currentThread().getName()));
        return false;
    }

    private boolean lockDummyLock(int lockId) {
        if(lockId >= locks.size()) {
            // bad request
            return true;
        }

        logger.finer(Thread.currentThread().getName() + ": Locking (UDS)Lock " + lockId);
        locks.get(lockId).lock();
        return false;
    }

    private boolean unlockDummyUDSLock(int lockId) {
        if(lockId >= locks.size()) {
            // bad request
            return true;
        }

        // try to unlock the UDSLock. If the unlocking thread wasn't the owner of the mutex, a RuntimeException
        // (IllegalMonitorStateException) will be thrown
        logger.finer(Thread.currentThread().getName() + ": Unlocking (UDS)Lock " + lockId);
        locks.get(lockId).unlock();
        return false;
    }

    private void printSharedStates() {
        for(DummySharedState state : sharedStates) {
            String stateState = state.getSharedState().stream().collect(Collectors.joining(", ",
                    "SharedState #" + state.getId() + ": [", "]"));
            stateState = (stateState.length() > 5000) ? stateState.substring(0, 5000) : stateState;
            logger.fine(stateState);
        }
    }

    private void resetStates() {
        int sharedStateCount = sharedStates.size();
        sharedStates.clear();
        for(int i = 0; i < sharedStateCount; i++) {
            sharedStates.add(new DummySharedState(i, withUDS));
        }
    }

    @Override
    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return new byte[0];
    }

    /**
     * Fully occupies a thread for the given duration
     *
     * @param durationInMilliseconds how long the thread should spin for, in ms
     */
    private void simulateCPULoad(int durationInMilliseconds) {
        simulateCPULoad(durationInMilliseconds, 1.0d);
    }

    /**
     * Occupies the thread by simply spinning in a non-optimizable loop.
     * Optional parameter loadFactor specifies how much the thread should occupy a core if it would be scheduled to
     * run on a single core exclusively during the duration of the simulated load (ranging from 0-100%).
     * Adapted from https://caffinc.github.io/2016/03/cpu-load-generator/
     *
     * @param durationInMilliseconds how long the thread should spin for, in ms
     * @param loadFactor             how much the thread should work, ranging from 0 - 1.0 (0-100%); only takes effect when
     *                               duration is a lot longer than 100ms (~ >1000ms)
     */
    private void simulateCPULoad(int durationInMilliseconds, double loadFactor) {
        logger.finest(Thread.currentThread().getName() + ": Simulating load with loadFactor " + loadFactor +
                " for " + durationInMilliseconds + "ms");
        long startTime = System.currentTimeMillis();
        try {
            // spin for given duration
            while(System.currentTimeMillis() - startTime < durationInMilliseconds) {
                // Every 100ms, sleep for percentage of unladen time
                if(System.currentTimeMillis() % 100 == 0) {
                    Thread.sleep((long) Math.floor((1 - loadFactor) * 100));
                }
            }
        } catch(InterruptedException e) {
            logger.finest(Thread.currentThread().getName() + ": Was interrupted " +
                    "while simulating CPU load (in Thread.sleep() to reduce load).");
        }
        logger.finest(Thread.currentThread().getName() + ": Finished simulating load");
    }

    /**
     * Occupies the thread by spinning and counting how often it spun for a given number of nanoseconds (slightly
     * veriable depending on the current systems JVM's accuracy).
     *
     * @param durationInNanoseconds How long the thread should spin for, in ns
     */
    private void simulateCPULoadNanos(int durationInNanoseconds) {
        logger.finest(Thread.currentThread().getName() + ": Simulating load for " + durationInNanoseconds + "ns.");
        long startTime = System.nanoTime();
        while(System.nanoTime() - startTime < durationInNanoseconds) ;
        logger.finest(Thread.currentThread().getName() + ": Finished simulating load (" + durationInNanoseconds +
                "ns)");
    }

    public static void main(String[] args) {
        if(args.length < 2) {
            printUsageAndExit();
        }

        try {
            int id = Integer.parseInt(args[0]);
            boolean withUDS = Boolean.parseBoolean(args[1]);

            new EvalServer(id, withUDS);
        } catch(NumberFormatException e) {
            printUsageAndExit();
        }
    }

    private static void printUsageAndExit() {
        System.out.println("===== Missing arguments");
        System.out.println("Usage: EvalServer <serverId> <withUDS>");
        System.out.println("=====");
        System.exit(1);
    }
}
