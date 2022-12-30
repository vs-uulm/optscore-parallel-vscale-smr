/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and
 * the authors indicated in the @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package bftsmart.demo.parallel.list;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.SingleExecutable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import bftsmart.parallelism.ParallelServiceReplica;
import bftsmart.parallelism.reconfiguration.LazyPolicy;


public final class ListServer implements SingleExecutable {

    private int interval;
    private float maxTp = -1;
    private boolean context;

    private int iterations = 0;
    private long throughputMeasurementStartTime = System.currentTimeMillis();

    private long start = 0;

    private ServiceReplica replica;
    //private StateManager stateManager;
    //private ReplicaContext replicaContext;

    private List<Integer> l = new LinkedList<Integer>();

    private int myId;
    private PrintWriter pw;

    private boolean closed = false;

    public ListServer(int id, int interval, int maxThreads, int minThreads, int initThreads, int entries, boolean context) {

        
        //controle dos limites de num de threads
        //alex I
        /*if (minThreads > maxThreads) {
            System.out.println("Min cannot be more than Max number of threads, Min will be equal maxNum threads");
            minThreads = maxThreads;
            if (initThreads > minThreads || initThreads < minThreads) {
                initThreads = minThreads;
                System.out.println("In this case inital will be equal min and max Num of threads");
            }
        } else if (initThreads < minThreads) {
            System.out.println("Initial cannot be less than minNum threads, so will be equal");
            initThreads = minThreads;
        } else if (initThreads > maxThreads) {
            System.out.println("Initial cannot be more than maxNum threads, so will be equal");
            initThreads = maxThreads;
        }*/
        //alex F

        if (initThreads <= 0) {
            System.out.println("Replica in sequential execution model.");
            
            replica = new ServiceReplica(id, this, null);
        } else {
            System.out.println("Replica in parallel execution model.");
            
            replica = new ParallelServiceReplica(id, this, null, minThreads, initThreads, maxThreads, new LazyPolicy());
            //replica = new ParallelServiceReplica(id, this,this, minThreads, initThreads, maxThreads, new AgressivePolicy());

        }
        
        this.interval = interval;
        this.context = context;
        this.myId = id;
        
        //this.state = new byte[stateSize];

        // for (int i = 0; i < stateSize; i++) {
        //     state[i] = (byte) i;
        //  }
        //totalLatency = new Storage(interval);
        //consensusLatency = new Storage(interval);
        //preConsLatency = new Storage(interval);
        // posConsLatency = new Storage(interval);
        // proposeLatency = new Storage(interval);
        // writeLatency = new Storage(interval);
        //  acceptLatency = new Storage(interval);
        //initialEntries = entries;
        for (int i = 0; i < entries; i++) {
            l.add(i);
            //System.out.println("adicionando key: "+i);
        }

        /*  try {
         KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
         keyPairGenerator.initialize(1024);
         java.security.KeyPair keyPair = keyPairGenerator.genKeyPair();

         pubK = keyPair.getPublic();
         privK = keyPair.getPrivate();

         } catch (Exception e) {
         e.printStackTrace();
         }
         sign();*/
        try {
            File f = new File("resultado_" + id + ".txt");
            FileWriter fw = new FileWriter(f);
            pw = new PrintWriter(fw);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        System.out.println("Server initialization complete!");
    }

    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    public byte[] execute(byte[] command, MessageContext msgCtx) {

        
        
        //System.out.println("Vai executar uma operação");
        if (start == 0) {
            start = System.currentTimeMillis();
            throughputMeasurementStartTime = start;
        }

        computeStatistics(msgCtx);
        
        
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(command);
            ByteArrayOutputStream out = null;
            byte[] reply = null;
            int cmd = new DataInputStream(in).readInt();
            
            switch (cmd) {
                case BFTList.ADD:
                    Integer value = (Integer) new ObjectInputStream(in).readObject();
                    boolean ret = false;
                    if (!l.contains(value)) {
                        ret = l.add(value);
                    }
                    out = new ByteArrayOutputStream();
                    ObjectOutputStream out1 = new ObjectOutputStream(out);
                    out1.writeBoolean(ret);
                    out.flush();
                    out1.flush();
                    reply = out.toByteArray();
                    break;
                case BFTList.REMOVE:
                    value = (Integer) new ObjectInputStream(in).readObject();
                    ret = l.remove(value);
                    out = new ByteArrayOutputStream();
                    out1 = new ObjectOutputStream(out);
                    out1.writeBoolean(ret);
                    out.flush();
                    out1.flush();

                    reply = out.toByteArray();
                    break;
                case BFTList.SIZE:
                    out = new ByteArrayOutputStream();
                    new DataOutputStream(out).writeInt(l.size());
                    reply = out.toByteArray();
                    break;
                case BFTList.CONTAINS:
                    value = (Integer) new ObjectInputStream(in).readObject();
                    out = new ByteArrayOutputStream();
                    out1 = new ObjectOutputStream(out);
                    out1.writeBoolean(l.contains(value));

                    out.flush();
                    out1.flush();

                    reply = out.toByteArray();
                    break;
                case BFTList.GET:
                    int index = new DataInputStream(in).readInt();
                    Integer r = null;
                    if (index > l.size()) {
                        r = new Integer(-1);
                    } else {
                        r = l.get(index);
                    }
                    out = new ByteArrayOutputStream();
                    out1 = new ObjectOutputStream(out);
                    out1.writeObject(r);

                    reply = out.toByteArray();

                    break;
            }
            return reply;
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(ListServer.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
            
    }

    public void computeStatistics(MessageContext msgCtx) {
        /*for(int i = 0; i < 10; i++){
         int x = (int)(Math.random()*10000000);
         t.get(String.valueOf("TESTE"+(x)));
         }*/

        // boolean readOnly = false;
        iterations++;

        /* if (msgCtx != null && msgCtx.getFirstInBatch() != null) {

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

        }*/
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

            int now = (int) ((System.currentTimeMillis() - start) / 1000);

            if (now < 300) {

                //System.out.println("****************THROUGHPUT: "+now+" "+tp);
                if (replica instanceof ParallelServiceReplica) {

                    pw.println(now + " " + tp + " " + ((ParallelServiceReplica) replica).getNumActiveThreads());
                    //System.out.println("*******************THREADS: "+now+" "+((ParallelServiceReplica)replica).getNumActiveThreads());
                } else {
                    pw.println(now + " " + tp);
                }
                pw.flush();

            } else if (!closed) {

                pw.flush();

                pw.close();

                closed = true;
            }

            /*if((now >= 180 && myId == 2) || (now >= 240 && myId == 1)){
                System.out.println("Replica "+myId+" vai executar leave");
                replica.leave();
                pw.flush();
                pw.close();
                closed = true;
                try {
                    Thread.currentThread().sleep(1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(ListServer.class.getName()).log(Level.SEVERE, null, ex);
                }
            }*/
            System.out.println("Throughput = " + tp + " operations/sec in sec: " + now);
            if (replica instanceof ParallelServiceReplica) {
                System.out.println("Active Threads = " + ((ParallelServiceReplica) replica).getNumActiveThreads() + " in sec: " + now);
            }

            System.out.println("Throughput = " + tp + " operations/sec (Maximum observed: " + maxTp + " ops/sec)");

            //System.out.println("Total latency = " + totalLatency.getAverage(false) / 1000 + " (+/- " + (long) totalLatency.getDP(false) / 1000 + ") us ");
            //totalLatency.reset();
            //System.out.println("Consensus latency = " + consensusLatency.getAverage(false) / 1000 + " (+/- " + (long) consensusLatency.getDP(false) / 1000 + ") us ");
            //consensusLatency.reset();
            //System.out.println("Pre-consensus latency = " + preConsLatency.getAverage(false) / 1000 + " (+/- " + (long) preConsLatency.getDP(false) / 1000 + ") us ");
            // preConsLatency.reset();
            // System.out.println("Pos-consensus latency = " + posConsLatency.getAverage(false) / 1000 + " (+/- " + (long) posConsLatency.getDP(false) / 1000 + ") us ");
            // posConsLatency.reset();
            //System.out.println("Propose latency = " + proposeLatency.getAverage(false) / 1000 + " (+/- " + (long) proposeLatency.getDP(false) / 1000 + ") us ");
            //proposeLatency.reset();
            //System.out.println("Write latency = " + writeLatency.getAverage(false) / 1000 + " (+/- " + (long) writeLatency.getDP(false) / 1000 + ") us ");
            //writeLatency.reset();
            //System.out.println("Accept latency = " + acceptLatency.getAverage(false) / 1000 + " (+/- " + (long) acceptLatency.getDP(false) / 1000 + ") us ");
            //acceptLatency.reset();
            throughputMeasurementStartTime = System.currentTimeMillis();
        }
        /* if (iterations == 110000) {
            //resetar
            System.out.println("Reiniciando o servidor para novo experimento");
            try {
                File f = new File("resultado_" + nT + ".txt");
                FileWriter fw = new FileWriter(f);
                PrintWriter pw = new PrintWriter(fw);

                pw.println("Throughput = " + tp + " operations/sec (Maximum observed: " + maxTp + " ops/sec)");

                pw.flush();
                fw.flush();

                fw.close();
                pw.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(0);
            }

            maxTp = 0;
            iterations = 0;
            l.clear();
            for (int i = 0; i < initialEntries; i++) {
                l.add(i);
                //System.out.println("adicionando key: "+i);
            }

            System.out.println("Servidor reiniciado");
        }*/

    }

    public static void main(String[] args) {
        if (args.length < 7) {
            System.out.println("Usage: ... ListServer <processId> <measurement interval> <maxNum threads> <minNum threads> <initialNum threads> <initial entries> <context?>");
            System.exit(-1);
        }

        int processId = Integer.parseInt(args[0]);
        int interval = Integer.parseInt(args[1]);
        int maxNT = Integer.parseInt(args[2]);
        int minNT = Integer.parseInt(args[3]);
        int initialNT = Integer.parseInt(args[4]);
        int entries = Integer.parseInt(args[5]);

        boolean context = Boolean.parseBoolean(args[6]);
        

        new ListServer(processId, interval, maxNT, minNT, initialNT, entries, context);
    }


 /*
    public void installSnapshot(byte[] state) {
         try {

                       
            // serialize to byte array and return
            ByteArrayInputStream bis = new ByteArrayInputStream(state);
            ObjectInputStream in = new ObjectInputStream(bis);
            l = (List<Integer>) in.readObject();
            in.close();
            bis.close();
            System.out.println("recebeu estado: "+l.size());
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(ListServer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ListServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    
    public byte[] getSnapshot() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(l);
            out.flush();
            bos.flush();
            out.close();
            bos.close();
            return bos.toByteArray();
        } catch (IOException ex) {
            Logger.getLogger(ListServer.class.getName()).log(Level.SEVERE, null, ex);
            return new byte[0];
        } 
    }*/
/*    @Override
    public ApplicationState getState(int eid, boolean sendState) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int setState(ApplicationState state) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public StateManager getStateManager() {
       /* if (stateManager == null) {
            stateManager = new StandardStateManager();
        }
        return stateManager;*/
      // return null;
   // }

   /* @Override
    public void setReplicaContext(ReplicaContext replicaContext) {
        this.replicaContext = replicaContext;
    }

    @Override
    public void Op(int i, byte[] bytes, MessageContext mc) {
        System.out.println("*******CHAMOU oP");
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void noOp(int i, byte[][] bytes, MessageContext[] mcs) {
        System.out.println("*******CHAMOU noOP");
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
*/
}
