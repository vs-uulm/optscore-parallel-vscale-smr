package de.uniulm.vs.art.uds;

import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.client.EvalClientRunner;

import java.util.logging.Logger;

public class UDSTester {

    private static final Logger logger = Logger.getLogger(UDSTester.class.getName());

    public static void main(String[] args) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);
        logger.info("Starting UDSTester ...");

        int numberOfThreadsToCreate = 500000;
        DummySharedState state = new DummySharedState(0, true);

        Runnable sharedStateAdder = () -> {
            logger.fine("Runnable running in thread: " + Thread.currentThread().getName());
            logger.fine(Thread.currentThread().getName() + " is trying to modify state...");
            state.addToSharedState(Thread.currentThread().getName());
            logger.fine(Thread.currentThread().getName() + " modified state.");
            //String sharedStateStatus = state.getSharedState().stream().
            //        collect(Collectors.joining(", ", "==== sharedState: [", "]"));
            //logger.fine(sharedStateStatus.substring(sharedStateStatus.length()-100));
        };
        Runnable sharedStateChecker = () -> {
            logger.fine("Runnable running in checker-thread: " + Thread.currentThread().getName());
            try {
                while(state.getSharedState().size() < numberOfThreadsToCreate) {
                    Thread.sleep(50);
                }
                long start = state.getFirstModified();
                long end = state.getLastModified();
                //String sharedStateStatus = state.getSharedState().stream().map(Thread::getName).
                //        collect(Collectors.joining(", ", "==== sharedState: [", "]"));
                logger.warning("Number of threads in sharedState after test ran: " + state.getSharedState().size());
                logger.warning("Added "+ numberOfThreadsToCreate + " threads to sharedState. Test ran for "
                        + (end - start) + " milliseconds");
                logger.warning("(simple) average throughput (of entire test) was " +
                        (double) numberOfThreadsToCreate / (((double) end - start) / 1000) + " threads/s");
                logger.info("Checker-thread found enough threads in sharedState. Test over. Terminating...");
                System.exit(0);
            } catch(InterruptedException e) {
                logger.info("Checker-thread was interrupted while sleeping");
            }
        };
        Thread checkerThread = new Thread(sharedStateChecker);
        checkerThread.start();

        //String sharedStateStatus = state.getSharedState().stream().map(Thread::getName).
        //        collect(Collectors.joining(", ", "==== sharedState: [", "]"));
        //logger.info(sharedStateStatus);

        // create and add threads trying to add themselves to single DummySharedState
        logger.info("Adding " + numberOfThreadsToCreate + " threads to sharedState...");
        for(int j = 0; j < numberOfThreadsToCreate; j++) {
            UDScheduler.getInstance().addRequest(sharedStateAdder);
        }

    }
}
