package de.uniulm.vs.art.uds;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DummySharedState {

    private List<String> sharedState;

    private final Lock stateLock;

    private final int id;

    private long firstModified;
    private long lastModified;

    public DummySharedState(int id, boolean withUDS) {
        this.id = id;
        this.sharedState = new ArrayList<>(1000);

        if(withUDS) {
            this.stateLock = new UDSLock();
        } else {
            this.stateLock = new ReentrantLock();
        }
    }

    public Lock getStateLock() {
        return stateLock;
    }

    public List<String> getSharedState() {
        return sharedState;
    }

    public void addToSharedState(String threadName) {
        this.stateLock.lock();
        try {
            if(sharedState.size() == 0) {
                this.firstModified = System.currentTimeMillis();
            }
            this.lastModified = System.currentTimeMillis();
            this.sharedState.add(threadName);
        } finally {
            this.stateLock.unlock();
        }
    }

    public long getFirstModified() {
        return firstModified;
    }

    public long getLastModified() {
        return lastModified;
    }

    public int getId() {
        return id;
    }
}
