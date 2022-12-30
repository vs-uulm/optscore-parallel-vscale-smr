/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.demo.parallel.ts;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.util.Storage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import bftsmart.parallelism.ParallelServiceReplica;

/**
 *
 * @author alchieri
 */
//public class TupleSpaceServer implements SingleExecutable, Recoverable {
public class TupleSpaceServer implements SingleExecutable{

    private int interval;
    private float maxTp = -1;
    private boolean context;

    private byte[] state;

    private int iterations = 0;
    private long throughputMeasurementStartTime = System.currentTimeMillis();

    private Storage totalLatency = null;
    private Storage consensusLatency = null;
    private Storage preConsLatency = null;
    private Storage posConsLatency = null;
    private Storage proposeLatency = null;
    private Storage writeLatency = null;
    private Storage acceptLatency = null;
    protected ServiceReplica replica;
    //private ReplicaContext replicaContext;

    //private StateManager stateManager;

    private Map<Integer, List<Tuple>> tuplesBag = new TreeMap<Integer, List<Tuple>>();

    public TupleSpaceServer(int id, int interval, int numThreads, int entries, int stateSize, boolean context) {

        initReplica(numThreads, id);
        
        this.interval = interval;
        this.context = context;

        this.state = new byte[stateSize];

        for (int i = 0; i < stateSize; i++) {
            state[i] = (byte) i;
        }

        totalLatency = new Storage(interval);
        consensusLatency = new Storage(interval);
        preConsLatency = new Storage(interval);
        posConsLatency = new Storage(interval);
        proposeLatency = new Storage(interval);
        writeLatency = new Storage(interval);
        acceptLatency = new Storage(interval);

        
        for (int i = 0; i < entries; i++) {
            for (int j = 1; j < 11; j++) {
                Object[] f = new Object[j];
                for (int x = 0; x < f.length; x++) {
                    f[x] = new String("Este Campo Possui Os Dados Do Campo iiiiiiiiiiii:"+i);
                    //System.out.println("Tamanho "+x+" e i "+j+" igual a " +f[x].toString().length());
                }
                out(Tuple.createTuple(f));
            }
            
        }
        for (int j = 1; j < 11; j++) {
             System.out.println("tuples"+ j+ "fields: "+getTuplesBag(j).size());
        }
        
        System.out.println("Server initialization complete!");
    }

    protected void initReplica(int numThreads, int id) {

        if (numThreads == 0) {
            System.out.println("Replica in sequential execution model.");
            replica = new ServiceReplica(id, this, null);
        } else {
            System.out.println("Replica in parallel execution model.");
           
            replica = new ParallelServiceReplica(id, this, null, numThreads);
        }

    }

    

    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    public byte[] execute(byte[] command, MessageContext msgCtx) {

        computeStatistics(msgCtx);

        try {
            ByteArrayInputStream in = new ByteArrayInputStream(command);
            ByteArrayOutputStream out = null;
            byte[] reply = null;
            int cmd = new DataInputStream(in).readInt();
            switch (cmd) {
                //operations on the list
                case BFTTupleSpace.OUT:
                    Tuple t = (Tuple) new ObjectInputStream(in).readObject();
                    //System.out.println("add received: " + t);
                    out(t);
                    boolean ret = true;
                    out = new ByteArrayOutputStream();
                    ObjectOutputStream out1 = new ObjectOutputStream(out);
                    out1.writeBoolean(ret);
                    
                    out1.flush();
                    out.flush();
                    reply = out.toByteArray();
                    break;
                case BFTTupleSpace.RDP:
                    t = (Tuple) new ObjectInputStream(in).readObject();
                    Tuple read = rdp(t);
                    out = new ByteArrayOutputStream();
                    out1 = new ObjectOutputStream(out);
                    out1.writeObject(read);
                    out1.flush();
                    out.flush();
                    reply = out.toByteArray();
                    break;
                case BFTTupleSpace.INP:
                    t = (Tuple) new ObjectInputStream(in).readObject();
                    Tuple removed = inp(t);
                    out = new ByteArrayOutputStream();
                    out1 = new ObjectOutputStream(out);
                    out1.writeObject(removed);
                    out1.flush();
                    out.flush();
                    reply = out.toByteArray();
                    break;
            }
            return reply;
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(TupleSpaceServer.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    public void computeStatistics(MessageContext msgCtx) {

        boolean readOnly = false;

        iterations++;

        if (msgCtx != null && msgCtx.getFirstInBatch() != null) {

            readOnly = msgCtx.readOnly;

            msgCtx.getFirstInBatch().executedTime = System.nanoTime();

            totalLatency.store(msgCtx.getFirstInBatch().executedTime - msgCtx.getFirstInBatch().receptionTime);

            if (readOnly == false) {

                consensusLatency.store(msgCtx.getFirstInBatch().decisionTime - msgCtx.getFirstInBatch().consensusStartTime);
                long temp = msgCtx.getFirstInBatch().consensusStartTime - msgCtx.getFirstInBatch().receptionTime;
                preConsLatency.store(temp > 0 ? temp : 0);
                posConsLatency.store(msgCtx.getFirstInBatch().executedTime - msgCtx.getFirstInBatch().decisionTime);
                proposeLatency.store(msgCtx.getFirstInBatch().writeSentTime - msgCtx.getFirstInBatch().consensusStartTime);
                writeLatency.store(msgCtx.getFirstInBatch().acceptSentTime - msgCtx.getFirstInBatch().writeSentTime);
                acceptLatency.store(msgCtx.getFirstInBatch().decisionTime - msgCtx.getFirstInBatch().acceptSentTime);

            } else {

                consensusLatency.store(0);
                preConsLatency.store(0);
                posConsLatency.store(0);
                proposeLatency.store(0);
                writeLatency.store(0);
                acceptLatency.store(0);

            }

        } else {

            consensusLatency.store(0);
            preConsLatency.store(0);
            posConsLatency.store(0);
            proposeLatency.store(0);
            writeLatency.store(0);
            acceptLatency.store(0);

        }

        float tp = -1;
        if (iterations % interval == 0) {
            if (context) {
                System.out.println("--- (Context)  iterations: " + iterations + " // regency: " + msgCtx.getRegency() + " // consensus: " + msgCtx.getConsensusId() + " ---");
            }

            System.out.println("--- Measurements after " + iterations + " ops (" + interval + " samples) ---");

            tp = (float) (interval * 1000 / (float) (System.currentTimeMillis() - throughputMeasurementStartTime));

            if (tp > maxTp) {
                maxTp = tp;
            }

            System.out.println("Throughput = " + tp + " operations/sec (Maximum observed: " + maxTp + " ops/sec)");

            System.out.println("Total latency = " + totalLatency.getAverage(false) / 1000 + " (+/- " + (long) totalLatency.getDP(false) / 1000 + ") us ");
            totalLatency.reset();
            System.out.println("Consensus latency = " + consensusLatency.getAverage(false) / 1000 + " (+/- " + (long) consensusLatency.getDP(false) / 1000 + ") us ");
            consensusLatency.reset();
            System.out.println("Pre-consensus latency = " + preConsLatency.getAverage(false) / 1000 + " (+/- " + (long) preConsLatency.getDP(false) / 1000 + ") us ");
            preConsLatency.reset();
            System.out.println("Pos-consensus latency = " + posConsLatency.getAverage(false) / 1000 + " (+/- " + (long) posConsLatency.getDP(false) / 1000 + ") us ");
            posConsLatency.reset();
            System.out.println("Propose latency = " + proposeLatency.getAverage(false) / 1000 + " (+/- " + (long) proposeLatency.getDP(false) / 1000 + ") us ");
            proposeLatency.reset();
            System.out.println("Write latency = " + writeLatency.getAverage(false) / 1000 + " (+/- " + (long) writeLatency.getDP(false) / 1000 + ") us ");
            writeLatency.reset();
            System.out.println("Accept latency = " + acceptLatency.getAverage(false) / 1000 + " (+/- " + (long) acceptLatency.getDP(false) / 1000 + ") us ");
            acceptLatency.reset();

            throughputMeasurementStartTime = System.currentTimeMillis();
        }

    }

    

    private void out(Tuple tuple) {
        getTuplesBag(tuple.getFields().length).add(tuple);
    }

    private Tuple rdp(Tuple template) {
        return findMatching(template, false);
    }

    private Tuple inp(Tuple template) {
        return findMatching(template, true);
    }

    private Tuple findMatching(Tuple template, boolean remove) {
        List<Tuple> bag = getTuplesBag(template.getFields().length);
        for (ListIterator<Tuple> i = bag.listIterator(); i.hasNext();) {
            Tuple tuple = i.next();
            if (match(tuple, template)) {
                if (remove) {
                    i.remove();
                }
                return tuple;
            }
        }

        return null;
    }

    protected boolean match(Tuple tuple, Tuple template) {
        Object[] tupleFields = tuple.getFields();
        Object[] templateFields = template.getFields();

        int n = tupleFields.length;

        if (n != templateFields.length) {
            return false;
        }

        for (int i = 0; i < n; i++) {
            if (templateFields[i] == tupleFields[i]) {
                return true;
            }

            if (templateFields[i] == null || tupleFields[i] == null) {
                return false;
            }

            if (!templateFields[i].equals(BFTTupleSpace.WILDCARD) && !templateFields[i].equals(tupleFields[i])) {
                return false;
            }
        }
        return true;
    }

    private List getTuplesBag(int i) {
        List ret = tuplesBag.get(i);
        if (ret == null) {
            synchronized (this) {
                ret = tuplesBag.get(i);
                if (ret == null) {
                    ret = new LinkedList<Tuple>();
                    tuplesBag.put(i, ret);
                }
            }
        }
        return ret;
    }

    public static void main(String[] args) {
           
        
        
        
        
        
        
        if (args.length < 6) {
            System.out.println("Usage: ... TupleSpaceServer <processId> <measurement interval> <num threads> <initial entries> <state size> <context?>");
            System.exit(-1);
        }

        int processId = Integer.parseInt(args[0]);
        int interval = Integer.parseInt(args[1]);
        int nt = Integer.parseInt(args[2]);
        int entries = Integer.parseInt(args[3]);
        int stateSize = Integer.parseInt(args[4]);
        boolean context = Boolean.parseBoolean(args[5]);

        new TupleSpaceServer(processId, interval, nt, entries, stateSize, context);
    }

    

}
