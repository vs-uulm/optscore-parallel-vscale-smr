package de.uniulm.vs.art.uds;

import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.client.EvalClientRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

/**
 * Represents a UDS-aware Lock. If a thread requests/takes this lock,
 * a UDS-imposed total order will be obeyed.
 */
public class UDSLock implements Lock {

    /**
     * Reference to the UDS Scheduler instance of this JVM, so we can wait on its Conditions, etc
     */
    private final UDScheduler udScheduler = UDScheduler.getInstance();

    /**
     * Identifier of this Lock
     */
    private final int lockID;

    /**
     * The thread currently holding this UDSLock, null if the lock is currently free.
     */
    private UDScheduler.UDSThread owner = null;

    /**
     * The per-mutex wait queue for threads which requested this Lock but have not yet acquired it.
     * May get cleared, e.g. at the end of a round.
     */
    private List<UDScheduler.UDSThread> enqueuedThreads = new LinkedList<>();

    /*
     * Wait Conditions for UDS
     */

    private final Condition isEnqueuedOrLockOwnedByThread;

    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(UDSLock.class.getName());


    /**
     * Creates a basic UDSLock
     */
    public UDSLock() {
        this(0);
    }

    public UDSLock(int id) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);
        this.lockID = id;
        this.isEnqueuedOrLockOwnedByThread = UDScheduler.getInstance().getSchedulerLock().newCondition();
    }

    /**
     * Obeys total order before granting a thread the lock
     */
    @Override
    public void lock() {
        UDScheduler.getInstance().getSchedulerLock().lock();
        try {
            UDScheduler.UDSThread t = UDScheduler.getCurrentUDSThread();
            logger.finer(t.getIdString() + " entering UDSLock.lock() in UDSLock " + this.lockID);

            // obey total order. Thread might get parked in UDScheduler conditions, but will eventually continue here..
            UDScheduler.getInstance().waitForTurn();

            logger.finest("UDSLock " + this.lockID + " has owner: " +
                    (this.owner != null ? owner.getIdString() : "No owner, is free"));
            if(this.owner != null) {
                // If UDSLock is occupied, enqueue current thread and let it wait
                logger.finer(t.getIdString() + " enqueuing itself at lock() in UDSLock " + this.lockID);
                this.enqueuedThreads.add(t);
                t.setEnqueued(this);

                // check if round is over, else continue by waiting until thread is first in mutex wait queue
                UDScheduler.getInstance().checkForEndOfRound();
                while(this.owner != t && t.getEnqueued() != null) {
                    logger.finer(t.getIdString() + " going to sleep, awaiting isEnqueuedOrLockedOwnedByThread" +
                            " in lock() in UDSLock " + this.lockID);
                    isEnqueuedOrLockOwnedByThread.await();
                    logger.finer(t.getIdString() + " was woken up by signal to isEnqueuedOrLockedOwnedByThread" +
                            " in lock() in UDSLock " + this.lockID);
                }
                if(this.owner != t) {
                    // repeat (see UDS v1.2.1 spec line 28)
                    // unlock first, then
                    logger.finest(t.getIdString() + " tried locking UDSLock in UDSLock " + this.lockID +
                            ", but wasn't right owner; trying again");
                    lock();
                }
            } else {
                // take this UDSLock
                logger.finer(t.getIdString() + " taking UDSLock.lock()");
                this.owner = t;
            }
            UDScheduler.getInstance().setProgress(true);
        } catch(Throwable e) {
            e.printStackTrace();
            logger.info(Thread.currentThread().getName() + " has been interrupted while waiting on " +
                    "isEnqueuedOrLockOwnedByThread ...");
        } finally {
            UDScheduler.getInstance().getSchedulerLock().unlock();
        }
    }

    /**
     * Unlocks this UDSLock and wakes up enqueued threads if applicable,
     * so they can try to acquire the lock while obeying the total order.
     */
    @Override
    public void unlock() {
        logger.finest(Thread.currentThread().getName() + " is entering unlock() in UDSLock " + this.lockID);
        UDScheduler.getInstance().getSchedulerLock().lock();
        try {
            UDScheduler.UDSThread t = UDScheduler.getCurrentUDSThread();
            if(t != this.owner) {
                // a thread not currently owning the mutex tried to unlock it. Return and ignore the request.
                // TODO proper error handling and propagation of this case. Is a RuntimeException right?
                throw new IllegalMonitorStateException(t.getIdString() + " tried to release UDSLock " + this.lockID +
                        " even though it wasn't the lock's owner");
            }
            logger.finest(t.getIdString() + " is releasing UDSLock " + this.lockID);
            this.owner = null;

            // see if  other threads are waiting for this UDSLock and grant it to the first thread in queue
            if(enqueuedThreads.size() > 0) {
                logger.finest(Thread.currentThread().getName() + " checked UDSLock.enqueuedThreads" +
                        " and is waking up threads in UDSLock " + this.lockID + "'s queue");
                UDScheduler.UDSThread next = enqueuedThreads.remove(0);
                this.owner = next;

                // signal all waiting threads, the one thread that's next will run due to changed condition
                next.setEnqueued(null);
                logger.fine(Thread.currentThread().getName() + " is UDScheduling:" +
                        " signaling threads waiting on UDSLock.isEnqueuedOrLockOwnedByThread");
                isEnqueuedOrLockOwnedByThread.signalAll();
            }

            logger.finest("UDSLock " + this.lockID + " was freed by " + Thread.currentThread().getName() + ". Status: "
                     + (this.owner != null ? "Now held by " + owner.getIdString() : "No owner, UDSLock is free"));
        } finally {
            UDScheduler.getInstance().getSchedulerLock().unlock();
        }
    }

    /**
     * Not implemented for UDSLock. Do not use.
     * Use lock() instead.
     * @return
     * @throws InterruptedException
     */
    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new NoSuchMethodError();
    }

    /**
     * Not implemented for UDSLock. Do not use.
     * Use lock() instead.
     * @return
     * @throws InterruptedException
     */
    @Override
    public boolean tryLock() {
        throw new NoSuchMethodError();
    }

    /**
     * Not implemented for UDSLock. Do not use.
     * Use lock() instead.
     * @param time
     * @param unit
     * @return
     * @throws InterruptedException
     */
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        throw new NoSuchMethodError();
    }


    /**
     * Not implemented for UDSLock. Do not use.
     */
    @Override
    public Condition newCondition() {
        throw new NoSuchMethodError();
    }

    /**********************************************************
     ***************** UDS/Scheduling Methods *****************
     **********************************************************/

    public void removeFromQueue(UDScheduler.UDSThread t) {
        // TODO check whether a thread can be in per-mutex queue more than one time. If yes, fix this code.
        // Shouldn't be possible, however.

        enqueuedThreads.remove(t);
    }

}