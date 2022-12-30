package de.uniulm.vs.art.uds;

import bftsmart.parallelism.MessageContextPair;
import bftsmart.parallelism.ParallelMapping;
import bftsmart.parallelism.scheduler.Scheduler;
import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.client.EvalClientRunner;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * UDS Scheduler implementation as a Singleton.
 */
public class UDScheduler implements Scheduler {

    /**
     * Ordered list of all threads in the application that were or are to be scheduled by UDS
     */
    private final List<UDSThread> threads;

    /**
     * Primary threads in the current round
     */
    private List<UDSThread> primaries;

    /**
     * The single devliery thread that waits for admission;
     */
    private UDSThread admissionThread;
    
    /**
     * Current round, incremented when a new round is started
     */
    private int round;

    /**
     * The number of threads that have already been 'seen' by UDS
     * TODO: deal with wraparounds after MAX_INT threads have been created/seen
     */
    private int highestThreadNo;

    /**
     * The next UDS thread ID
     * TODO: deal with wraparounds after MAX_INT threads have been created
     */
    private AtomicInteger threadID = new AtomicInteger(0);

    /**
     * Indicator whether any progress was made in the current round
     */
    private boolean progress = false;

    /**
     * Thread-local variable referring to current UDS thread
     */
    private static final ThreadLocal<UDSThread> currentUDSThread = new ThreadLocal<>();

    /**
     * Holds all values required to configure UDS; used by UDS methods to read config during rounds, so should never
     * be changed by outside methods, only by UDS' own reconfigure()-method.
     */
    private final UDSConfiguration udsConfiguration = new UDSConfiguration();

    /**
     * A freely modifiable copy of UDS config, which is used for changing the actual UDS config during
     * reconfigurigation (inbetween rounds) by copying its contents.
     */
    private final UDSConfiguration requestedUDSConfiguration = new UDSConfiguration();

    /**
     * Thread pool for request scheduling
     */
    private final ExecutorService udsThreadPool = Executors.newCachedThreadPool();

    /**
     * Use ReentrantLock so we have access
     * to its Condition Objects and can await/signal threads efficiently.
     */
    private final ReentrantLock schedulerLock = new ReentrantLock();

    /*
     * Global wait conditions for UDS
     * Thread-local conditions are managed within UDSThread
     */
    private final Condition isFinished = schedulerLock.newCondition();
    private final Condition threadExists = schedulerLock.newCondition();

    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(UDScheduler.class.getName());

    /**
     * Singleton. Not instantiable.
     */
    private UDScheduler() {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        this.threads = new LinkedList<>();
        this.primaries = new ArrayList<>(4);
        this.admissionThread = null;
        this.round = 0;
        // initial configuration with 1 prims and 1 step
        requestReconfiguration(1, 1);
    }


    /**
     * Singleton pattern
     */
    private static class LazySingletonHolder {
        static final UDScheduler instance = new UDScheduler();
    }

    /**
     * Get the Singleton instance of this UDS Scheduler
     *
     * @return Singleton UDS Scheduler instance
     */
    public static UDScheduler getInstance() {
        return LazySingletonHolder.instance;
    }

    /**
     * Get current UDS Thread
     */
    public static UDSThread getCurrentUDSThread() {
        return currentUDSThread.get();
    }

    /**
     * Gives UDS a thread to schedule. Will be called by BFT-SMaRt's delivery thread (or somesuch),
     * so we should be able to rely on the proper order of insertions of threads to be scheduled.
     * <p>
     * Will create a Future and hand the provided MessageContextPair over to that Future. The Future will then be
     * scheduled with this classes @addRequest-method.
     *
     * @param request
     */
    @Override
    public void schedule(MessageContextPair request) {

    }

    /**
     * Is not needed for UDS, so will be ignored
     *
     * @return always null
     */
    @Override
    public ParallelMapping getMapping() {
        return null;
    }

    /**
     * Adds a request (= Runnable) to the UDS scheduling queue. Blocks(!) until the internal UDS thread list is
     * empty enough so the new thread can be added and started.
     * Threads are started by this method, and then scheduled with UDS.
     *
     * @param r Runnable responsible for fulfilling a client request, added to UDS for scheduling purposes.
     */
    public void addRequest(Runnable r) {
        // Thread can now actually be added, so take schedulerLock to guard against another thread messing with the
        // thread queue while we add and start the new one
        schedulerLock.lock();
        try {
            /*
             * Create the UDS thread and its runnable with boiler plate code for correct termination
             */
            UDSThread thread = new UDSThread();
            thread.initTask(r);

            // Implementation of back-pressure if there are too many pending threads in the system
            // TODO: Maximum size is a good guess but far from being evaluated or tuned
            if(this.threads.size() > ((udsConfiguration.getN() * 2) + 20)) {
                logger.finest(Thread.currentThread().getName() + " blocking thread " + thread.getIdString() + 
                		" due to too many threads");
                admissionThread = thread;
                thread.awaitAdmission();                	
            }

            // First the thread is added to the thread list to preserve order
            logger.finest(Thread.currentThread().getName() + " adding new UDSThread " +
                    thread.getIdString() + " to threadList");
            this.threads.add(thread);

            if(logger.isLoggable(Level.FINER)) {
                String threadListStatus = threads.stream().map(UDSThread::getIdString).
                        collect(Collectors.joining(", ", ".... threadList: [", "]"));
                logger.finer(threadListStatus);
            }

            // start the thread
            logger.finest(Thread.currentThread().getName() + " starting new UDSThread " +
                    thread.getIdString());
            thread.start();

            // signal scheduling thread waiting for threads in startRound().
            // Should be only one thread awaiting this condition, but call signalAll() just to be safe.
            threadExists.signalAll();
        } finally {
            schedulerLock.unlock();
        }
    }

    /******************************************************
     ***************** Scheduling Methods *****************
     ******************************************************/

    /**
     * Start a new scheduling round. Can only be called by scheduler methods, not publically.
     */
    private void startRound() {
        schedulerLock.lock();
        try {
            // increment round counter
            this.round++;

            // empty the set of primaries (just create a new one to solve bootstrap problems, old one will be GCed)
            // initial size of n + 1 to avoid possible weird resizing effects of ArrayList (off-by-one?)
            primaries = new ArrayList<>(udsConfiguration.getN() + 1);

            // reconfigure UDS
            reconfigure(progress);
            String totalOrderStatus = udsConfiguration.getTotalOrder().stream().map(Object::toString).
                    collect(Collectors.joining(", ", " |||| total order: [", "]"));
            logger.info(Thread.currentThread().getName() + " is UDScheduling: Starting round " + this.round +
                    totalOrderStatus + " with " + udsConfiguration.getN() + " primaries by adding threads to " +
                    "primaries and signaling them");

            int i;
            int n;
            for(i = 0, n = 0; true; i++) {
                while(threads.size() <= i || !threads.get(i).isStarted()) {
                    logger.finer(Thread.currentThread().getName() +
                            " is UDScheduling: Waiting for threads on threadExists in startRound() (Round " +
                            this.round + ")");
                    threadExists.await();
                    logger.finer(Thread.currentThread().getName() +
                            " was woken up by signal to threadExists in startRound() (Round " +
                            this.round + ")");
                }
                UDSThread t = threads.get(i);

                // If a thread is not already terminated and the prim()-predicate allows it, add thread to primaries
                if(!t.isTerminated() && prim(t)) {
                    logger.finer(Thread.currentThread().getName() + " is UDScheduling: Adding thread to " +
                            "primaries: " + t.getIdString());
                    t.setPrimary(true);
                    this.primaries.add(t);

                    if(logger.isLoggable(Level.FINER)) {
                        String primariesStatus = primaries.stream().map(UDSThread::getIdString).
                                collect(Collectors.joining(", ", "**** primaries: [", "]"));
                        logger.finer(primariesStatus);
                    }

                    // If we have enough primaries for this round, stop looking for more primaries
                    if(++n >= udsConfiguration.getN())
                    {
                        break;
                    }
                }
            }


            // update the number of seen threads
            if(highestThreadNo < i) {
                logger.finest(Thread.currentThread().getName() + " increasing highestThreadNo to " + i);
                highestThreadNo = i;
            }
            
            // prune thread list by removing all terminated threads
            {
                int removedThreads = threads.size();
                if(threads.removeIf(UDSThread::isTerminated)) {
                    // some space for new threads

                    // compute removed threads and correct number of seen threads
                    removedThreads -= threads.size();
                    highestThreadNo -= removedThreads;
                    logger.finest(Thread.currentThread().getName() + " correcting highestThreadNo to " +
                            highestThreadNo);

                    // admit waiting delivery thread
                    if( admissionThread != null ) {
                    	admissionThread.signalAdmission();
                        logger.finer(Thread.currentThread().getName() + " admitting thread " + admissionThread.getIdString());
                        admissionThread= null;
                    }
                }
            }
        } catch(InterruptedException e) {
            logger.info(Thread.currentThread().getName() + " has been interrupted while waiting on " +
                    "threadExists. Re-waiting ...");
        } finally {
            schedulerLock.unlock();
        }
    }

    /**
     * For explicit thread creation by other UDSThreads only.
     * To be called whenever a new UDSThread is being created by another UDSThread.
     * // TODO unused so far; check whether this method is still usable after optimizations
     */
    public UDSThread createThread(Runnable r) {
        schedulerLock.lock();
        try {
            // Obey total order
            waitForTurn();

            // create new thread
            UDSThread newThread = new UDSThread();
            newThread.initTask(r);
            // insert new thread at correct position in thread list
            threads.add(threadPosition(), newThread);
            this.highestThreadNo++;
            newThread.setPrimary(newAsPrimary());
            return newThread;
        } finally {
            schedulerLock.unlock();
        }
    }

    /**
     * Checks whether the current scheduling round meets all conditions to end.
     */
    public void checkForEndOfRound() {
        schedulerLock.lock();
        try {
            // check all primaries whether they are finished/terminated/waiting
            logger.fine(Thread.currentThread().getName() + " is UDScheduling: Checking for end of round " + round);

            // if there are currently less primaries than there should be (as per the udsConfiguration
            // for the current round), then the round is not yet over and we have to wait for more primaries
            if(primaries.size() < n(round)) {
                logger.fine(Thread.currentThread().getName() + " is UDScheduling: Round " + round +
                        " not yet over. Not enough primaries");
                return;
            }

            logger.finest(Thread.currentThread().getName() + " is UDScheduling: " +
                    "Checking primary status for end of round");
            for(UDSThread t : primaries) {
                // if any primary is still running/has steps/is waiting for its turn, round is not over
                logger.finest(t.toString());
                if(t.getEnqueued() == null
                        && !t.isTerminated()
                        && !t.isFinished()
                        && !t.isWaitingForTurn()) {
                    logger.fine(Thread.currentThread().getName() + " is UDScheduling: Round " + round +
                            " not yet over");
                    return;
                }
            }
            // end of round detected
            logger.fine(Thread.currentThread().getName() + " is UDScheduling: END of round " +
                    round + " detected");
            for(UDSThread t : primaries) {
                t.setPrimary(false);
                t.setFinished(false);
                t.setWaitingForTurn(false);
                if(t.getEnqueued() != null) {
                    t.getEnqueued().removeFromQueue(t);
                    t.setEnqueued(null);
                }
            }
            // reset primaries
            primaries.clear();

            // signal waiting threads to re-check their changed conditions
            // don't signal primaries, since we cleared primaries and no thread could possibly be primary
            logger.finer(Thread.currentThread().getName() + " is UDScheduling: Signaling threads " +
                    "waiting on isFinished in checkForEndOfRound() to start new round");
            isFinished.signalAll();

            // ... and now start a new round
            startRound();
        } finally {
            schedulerLock.unlock();
        }
    }

    /**
     * Called whenever total order needs to be obeyed by a UDSThread.
     */
    public void waitForTurn() {
        schedulerLock.lock();
        try {
            // bootstrap
            if(round == 0) {
                setProgress(true);
                startRound();
            }

            UDSThread t = getCurrentUDSThread();
            while(true) {
                // Wait until primary
                while(!primaries.contains(t)) {
                    logger.finer(t.getIdString() + " going to sleep, awaiting isPrimary in waitForTurn()");
                    t.awaitIsPrimary();
                    logger.finer(t.getIdString() + " was woken up by signal to isPrimary in waitForTurn()");
                }

                // check whether we have any steps left in total order
                if(!udsConfiguration.getTotalOrder().contains(primaries.indexOf(t))) {
                    t.setFinished(true);
                    checkForEndOfRound();
                    // The round is not yet over, but we have no steps in the total order. So wait for
                    // the signal isFinished = false, which means the next round has started and we can try again.
                    while(t.isFinished()) {
                        logger.finer(t.getIdString() + " going to sleep, awaiting isFinished in waitForTurn()");
                        isFinished.await();
                        logger.finer(t.getIdString() + " was woken up by signal to isFinished in waitForTurn()");
                    }
                } else if(udsConfiguration.getTotalOrder().size() > 0
                        && primaries.size() > udsConfiguration.getTotalOrder().get(0)
                        && primaries.get(udsConfiguration.getTotalOrder().get(0)).equals(t)) {
                    // if current thread is first in total order, it may continue and remove the step from total order
                    logger.fine("<=||" + t.getIdString() + " is removing its step from total order");

                    udsConfiguration.getTotalOrder().remove(0);

                    String totalOrderStatus = udsConfiguration.getTotalOrder().stream().map(Object::toString).
                            collect(Collectors.joining(", ", "|||| total order: [", "]"));
                    logger.finer(totalOrderStatus);

                    // wake up next thread in total order if still some steps left and if enough primaries
                    if(udsConfiguration.getTotalOrder().size() > 0
                            && primaries.size() > udsConfiguration.getTotalOrder().get(0)) {
                        primaries.get(udsConfiguration.getTotalOrder().get(0)).setWaitingForTurn(false);
                    }
                    break;
                } else {
                    // wait for step / first in total order
                    t.setWaitingForTurn(true);
                    checkForEndOfRound();
                    while(t.isWaitingForTurn()) {
                        logger.finer(t.getIdString() + " going to sleep, awaiting isWaitingForTurn in waitForTurn()");
                        t.awaitTurn();
                        logger.finer(t.getIdString() + " was woken up by signal to isWaitingForTurn in waitForTurn()");
                    }
                }
            }
        } catch(InterruptedException e) {
            logger.info(Thread.currentThread().getName() + " has been interrupted while waiting on " +
                    "isPrimary in waitForTurn(). Re-waiting ...");
        } catch(ClassCastException e) {
            logger.severe("Tried to cast a non-UDSThread to UDSThread in waitForTurn()");
            System.exit(1);
        } finally {
            schedulerLock.unlock();
        }
    }

    private void removeFromOrder(UDSThread t) {
        // remove all steps of t in total order
        Integer i = primaries.indexOf(t);
        logger.finer(Thread.currentThread().getName() + " is removing all its steps from the total order");
        while(udsConfiguration.getTotalOrder().remove(i)) ;
        String totalOrderStatus = udsConfiguration.getTotalOrder().stream().map(Object::toString).
                collect(Collectors.joining(", ", Thread.currentThread().getName() +
                        " modified total order: [", "]"));
        logger.finest(totalOrderStatus);
        // signal next thread in total order if there are steps left in this round
        if(udsConfiguration.getTotalOrder().size() > 0
                && primaries.size() > udsConfiguration.getTotalOrder().get(0)) {
            schedulerLock.lock();
            try {
                primaries.get(udsConfiguration.getTotalOrder().get(0)).setWaitingForTurn(false);
            } finally {
                schedulerLock.unlock();
            }
        }
    }

    /**
     * Called inbetween rounds in order to reconfigure UDS.
     * Copies contents of requestedUDSConfiguration, which means a true reconfiguration can either be triggered from
     * the outside or by the scheduler itself, by changing requestedUDSConfiguration before this method is called
     */
    private void reconfigure(boolean progress) {
        logger.fine(Thread.currentThread().getName() + " is UDScheduling: Reconfiguring UDS");
        schedulerLock.lock();
        try {
            // reconfigure UDS by copying contents of requestedUDSConfiguration
            udsConfiguration.setN(requestedUDSConfiguration.getN());
            udsConfiguration.setTotalOrder(new ArrayList<>(this.requestedUDSConfiguration.getTotalOrder()));
            String totalOrderStatus = udsConfiguration.getTotalOrder().stream().map(Object::toString).
                    collect(Collectors.joining(", ", Thread.currentThread().getName() +
                            " is changing UDS config: |||| NEW total order: [", "] (with " + udsConfiguration.getN() +
                            " primaries)"));
            logger.finer(totalOrderStatus);

            // TODO if no progress was made, increase number of primaries for next round
        } finally {
            schedulerLock.unlock();
        }
    }

    /**
     * Called by UDSThreads when they've finished their Runnable task
     * and want to terminate
     *
     * @param t The thread who wants to terminate
     */
    protected void terminateThread(UDSThread t) {
        logger.fine(Thread.currentThread().getName() + " called for termination of thread: " +
                t.getIdString());
        // TODO check whether calling thread is terminating thread? Is this possible/necessary?

        schedulerLock.lock();
        // If we haven't had any critical ops yet, start a round so we don't wait forever on becoming primary
        if(round == 0) {
            startRound();
        }
        setProgress(true);
        try {
            while(!primaries.contains(t)) {
                logger.finer(t.getIdString() + " going to sleep, awaiting isPrimary in terminateThread()");
                t.awaitIsPrimary();
                logger.finer(t.getIdString() + " was woken up by signal to isPrimary in terminateThread()");
            }

            // Thread can be marked as terminated and retire itself
            t.setTerminated(true);
            removeFromOrder(t);
            checkForEndOfRound();
            // nothing left to do, thread should stop itself after returning
        } finally {
            schedulerLock.unlock();
        }
    }


    /**
     * Predicate function for deciding whether a thread can be added to primaries.
     * Terminated threads are obviously out.
     * Can be used for fine-tuning UDS.
     *
     * @param udsThread the thread that is trying to become primary
     * @return true if the thread may become primary, false otherwise
     */
    private boolean prim(UDSThread udsThread) {
        return !udsThread.isTerminated();
    }

    /**
     * Returns the number of primaries for the current round
     * // TODO properly implement
     *
     * @param round unused atm
     * @return number of primaries for a round
     */
    private int n(int round) {
        return udsConfiguration.getN();
    }

    /**
     * Determines where newly created threads are inserted in the thread list.
     *
     * @return index in the thread list for a new thread (usually the highestThreadNo)
     */
    private int threadPosition() {
        return this.highestThreadNo;
    }

    /**
     * Specifies whether newly created threads are to be put into primaries directly upon creation
     *
     * @return true if new threads are immediately added to primaries, false if otherwise
     * TODO maybe refactor and move this to UDSConfiguration
     */
    private boolean newAsPrimary() {
        return false;
    }

    public void setProgress(boolean progress) {
        this.progress = progress;

    }

    public ReentrantLock getSchedulerLock() {
        return schedulerLock;
    }

    /**
     * Tell UDS to reconfigure itself to use the specified number of primaries, each getting stepsPerPrimary steps in
     * new rounds, with a "round robin" total order
     *
     * @param primaries       The number of primaries that should be used in all following rounds that are started
     * @param stepsPerPrimary The number of steps each primary receives in all following rounds
     */
    public int requestReconfiguration(int primaries, int stepsPerPrimary) {
        // TODO implement different total orders (all at once, random, etc)
        // TODO sanity checks on input parameters (e.g. no prims/steps < 0, etc)
        logger.warning(Thread.currentThread().getName() + " is requesting new UDS configuration (" + primaries + " " +
                "prims, " + stepsPerPrimary + " steps per prim)");
        schedulerLock.lock();
        try {
            // set new number of primaries
            this.requestedUDSConfiguration.setN(primaries);

            // create and set new total order
            List<Integer> newTotalOrder = new ArrayList<>(primaries * stepsPerPrimary + 1);
            for(int i = 0; i < stepsPerPrimary; i++) {
                for(int j = 0; j < primaries; j++) {
                    newTotalOrder.add(j);
                }
            }
            this.requestedUDSConfiguration.setTotalOrder(newTotalOrder);
        } finally {
            schedulerLock.unlock();
        }

        // return new number of primaries
        return this.requestedUDSConfiguration.getN();
    }

    /**
     * The current configuration of the UDS Scheduler.
     * Should only be changed via reconfigure().
     */
    private static class UDSConfiguration {

        /**
         * Number of primaries in the current round
         */
        private int n;

        /**
         * The total order of the current round
         */
        private List<Integer> totalOrder;

        /**
         * Create a basic configuration with 1 primary and 1 step
         */
        private UDSConfiguration() {
            this.n = 1;
            this.totalOrder = new ArrayList<>(2);
            totalOrder.add(0);
        }

        private int getN() {
            return n;
        }

        private void setN(int n) {
            this.n = n;
        }

        private List<Integer> getTotalOrder() {
            return totalOrder;
        }

        private void setTotalOrder(List<Integer> totalOrder) {
            this.totalOrder = totalOrder;
        }
    }


    /**
     * UDS's own thread class to manage UDS threads
     * A special inner class representing  of Thread which obeys UDS specifics.
     * For example, it executes terminate() before stopping, which
     * waits until the Thread is in the set of primaries before returning.
     */
    protected class UDSThread {
        /**
         * Thread ID
         */
        private int id;

        /**
         * The task of this thread
         */
        private Runnable task;

        /**
         * True if thread was already started by UDS
         */
        private boolean started;

        /**
         * The lock the thread is currently enqueued for waiting for lock acquisition
         */
        private UDSLock enqueued;

        /**
         * True if thread is primary
         */
        private boolean primary;

        /**
         * Condition for scheduler's lock to wait until thread becomes primary
         */
        private Condition isPrimaryCondition;

        /**
         * Condition for back-pressure/thread admission
         */
        private final Condition admissionCondition;

        /**
         * True if thread has terminated its usual processing, but is not yet removed by UDS
         */
        private boolean terminated;

        /**
         * True if thread has finished its round
         */
        private boolean finished;

        /**
         * True if thread waits for its turn in the total UDS order
         */
        private boolean waitingForTurn;

        /**
         * Condition for scheduler's lock to wait until it's the thread's turn
         */
        private Condition waitingForTurnCondition;

        /**
         * Constructor of UDS threads
         */
        UDSThread() {
            this.id = threadID.getAndIncrement();
            this.started = false;
            this.enqueued = null;
            this.terminated = false;
            this.finished = false;
            this.waitingForTurn = false;

            // Create and intialise condition objects
            isPrimaryCondition = schedulerLock.newCondition();
            waitingForTurnCondition = schedulerLock.newCondition();
            admissionCondition = schedulerLock.newCondition();
            
            logger.finest( "Created UDSThread " + getIdString() );
        }

        public String getIdString() {
            return "{" + id + "}";
        }

        boolean isStarted() {
            return started;
        }

        public UDSLock getEnqueued() {
            return this.enqueued;
        }

        public void setEnqueued(UDSLock l) {
            this.enqueued = l;
        }

        boolean isPrimary() {
            return this.primary;
        }

        void setPrimary(boolean primary) {
            if(!this.primary && primary)
                isPrimaryCondition.signal();
            this.primary = primary;
        }

        public void setIsPrimaryCondition(Condition con) {
            this.isPrimaryCondition = con;
        }

        /**
         * This method can only be called when holding the scheduler lock
         */
        void awaitIsPrimary() {
            while(!primary) {
                try {
                    isPrimaryCondition.await();
                } catch(InterruptedException e) {
                    logger.info(Thread.currentThread().getName() + getIdString() +
                            " has been interrupted while waiting to become primary. Re-waiting...");
                }
            }
        }

        void signalAdmission() {
            admissionCondition.signal();
        }

        void awaitAdmission() {
           	logger.finer(Thread.currentThread().getName() + " thread " + getIdString() + " awaits admission");
           	try {
				admissionCondition.await();
			} catch (InterruptedException e) {
				logger.finest(Thread.currentThread().getName() + 
						" thread " + getIdString() + " was interrupted in awaiting admissionCondition: re-waiting ...");
			}
           	logger.finer(Thread.currentThread().getName() + " thread " + getIdString() + 
           			" got admission");
        }

        boolean isTerminated() {
            return this.terminated;
        }

        void setTerminated(boolean terminated) {
            this.terminated = terminated;
        }

        public boolean isFinished() {
            return this.finished;
        }

        public void setFinished(boolean finished) {
            this.finished = finished;
        }

        boolean isWaitingForTurn() {
            return this.waitingForTurn;
        }

        /**
         * This method can only be called when holding the scheduler lock
         */
        public void setWaitingForTurn(boolean waitingForTurn) {
            if(this.waitingForTurn && !waitingForTurn)
                waitingForTurnCondition.signal();
            this.waitingForTurn = waitingForTurn;
        }

        public void setWaitingForTurnCondition(Condition con) {
            this.waitingForTurnCondition = con;
        }

        /**
         * This method can only be called when holding the scheduler lock
         */
        void awaitTurn() {
            while(waitingForTurn) {
                try {
                    waitingForTurnCondition.await();
                } catch(InterruptedException e) {
                    logger.info(Thread.currentThread().getName() + "{" + id +
                            "} has been interrupted while waiting for turn. Re-waiting...");
                }
            }
        }

        @Override
        public String toString() {
            return "{" + this.id + "} " +
                    " status: isPrimary=" + isPrimary() +
                    ", isTerminated=" + isTerminated() +
                    ", isFinished=" + isFinished() +
                    ", isWaitingForTurn=" + isWaitingForTurn() +
                    ", isEnqueued=" + (enqueued != null);
        }

        /**
         * Initialise UDS thread's task
         *
         * @param r the task to run
         */
        void initTask(Runnable r) {
            task = () -> {
                try {
                    currentUDSThread.set(this);
                    r.run();
                } finally {
                    UDScheduler.getInstance().terminateThread(this);
                }
            };
        }

        /**
         * Start the UDS thread
         */
        public void start() {
            udsThreadPool.submit(task);
            started = true;
        }
    }
}
