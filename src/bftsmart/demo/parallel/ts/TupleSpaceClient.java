/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bftsmart.demo.parallel.ts;

import bftsmart.tom.util.Storage;
import java.io.IOException;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author alchieri
 */
public class TupleSpaceClient {

    private static int VALUE_SIZE = 1024;
    public static int initId = 0;

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws IOException {
        if (args.length < 8) {
            System.out.println("Usage: ... TupleSpaceClient <num. threads> <process id> <number of operations> <interval> <max fields> <verbose?> <parallel?> <percent>");
            System.exit(-1);
        }

        int numThreads = Integer.parseInt(args[0]);
        initId = Integer.parseInt(args[1]);

        int numberOfOps = Integer.parseInt(args[2]);
        //int requestSize = Integer.parseInt(args[3]);
        int interval = Integer.parseInt(args[3]);
        int max = Integer.parseInt(args[4]);
        boolean verbose = Boolean.parseBoolean(args[5]);
        boolean parallel = Boolean.parseBoolean(args[6]);
        int perc = Integer.parseInt(args[7]);

        boolean v2 = false;
        if (args.length >= 9) {
            v2 = Boolean.parseBoolean(args[8]);
        }

        Client[] c = new Client[numThreads];

        for (int i = 0; i < numThreads; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                Logger.getLogger(TupleSpaceClient.class.getName()).log(Level.SEVERE, null, ex);
            }

            System.out.println("Launching client " + (initId + i));
            c[i] = new TupleSpaceClient.Client(initId + i, numberOfOps, interval, max, verbose, parallel, v2, perc);
            //c[i].start();
        }

        for (int i = 0; i < numThreads; i++) {

            c[i].start();
        }

        for (int i = 0; i < numThreads; i++) {

            try {
                c[i].join();
            } catch (InterruptedException ex) {
                ex.printStackTrace(System.err);
            }
        }

        System.exit(0);
    }

    static class Client extends Thread {

        int id;
        int numberOfOps;

        int interval;

        boolean verbose;
        //boolean dos;
        //ServiceProxy proxy;
        //byte[] request;
        BFTTupleSpace space;

        int maxF;

        int percent;

        public Client(int id, int numberOfOps, int interval, int max, boolean verbose, boolean parallel, boolean version2, int percent) {
            super("Client " + id);

            this.id = id;
            this.numberOfOps = numberOfOps;

            this.percent = percent;
            this.interval = interval;

            this.verbose = verbose;
            //this.proxy = new ServiceProxy(id);
            //this.request = new byte[this.requestSize];
            this.maxF = max;

            if (version2) {
                space = new BFTTupleSpacePlus(id, parallel);
                //System.out.println("PLUS PLUS");

            } else {
                space = new BFTTupleSpace(id, parallel);
               // System.out.println("NORMAL");
            }
            //this.dos = dos;
        }

        private void out(Random rand) {

            int nf = rand.nextInt(10) + 1;
            Object[] f = new Object[nf];
            for (int j = 0; j < f.length; j++) {
                f[j] = new String("Este Campo Possui Os Dados Do Campo iiiiiiiiiiii:" + rand.nextInt(maxF));
            }
            space.out(Tuple.createTuple(f));

        }

        private Tuple inp(Random rand) {
             int nf = rand.nextInt(10) + 1;
            Object[] f = new Object[nf];
            int id = rand.nextInt(maxF);
            for (int j = 0; j < f.length; j++) {
                f[j] = new String("Este Campo Possui Os Dados Do Campo iiiiiiiiiiii:" + id);
            }
            
            return space.inp(Tuple.createTuple(f));
        }

        private Tuple rdp(Random rand) {
            int nf = rand.nextInt(10) + 1;
            Object[] f = new Object[nf];
            int id = rand.nextInt(maxF);
            for (int j = 0; j < f.length; j++) {
                f[j] = new String("Este Campo Possui Os Dados Do Campo iiiiiiiiiiii:" + id);
            }
            
            return space.rdp(Tuple.createTuple(f));
        }

        public void run() {

            System.out.println("Warm up...");

            int req = 0;
            Random rand = new Random();
            WorkloadGeneratorTS work = new WorkloadGeneratorTS(this.percent, numberOfOps / 2);

            for (int i = 0; i < numberOfOps / 20; i++, req++) {
                if (verbose) {
                    System.out.print("Sending req " + req + "...");
                }

                int op = work.getOperations()[i];

                if (op == BFTTupleSpace.OUT) {

                    out(rand);
                } else if (op == BFTTupleSpace.RDP) {

                   
                    Tuple ret = rdp(rand);

                    //int index = rand.nextInt(maxIndex);
                    //boolean ret = store.contains(new Integer(index));
                    //System.out.println("Contém: "+ret);
                } else if (op == BFTTupleSpace.INP) {

                    
                    Tuple ret = inp(rand);
                }

                if (verbose) {
                    System.out.println(" sent!");
                }

                if (verbose && (req % 1000 == 0)) {
                    System.out.println(this.id + " // " + req + " operations sent!");
                }

            }

            Storage st = new Storage(numberOfOps / 2);

            // for (int j = 0; j < 5; j++) {
            System.out.println("Executing experiment for " + numberOfOps / 2 + " ops");

            //work = new WorkloadGenerator(j * 25, numberOfOps / 2);
            for (int i = 0; i < numberOfOps / 2; i++, req++) {

                if (verbose) {
                    System.out.print(this.id + " // Sending req " + req + "...");
                }

                int op = work.getOperations()[i];

                if (op == BFTTupleSpace.OUT) {
                    //int index = rand.nextInt(maxIndex * 2);

                    long last_send_instant = System.nanoTime();
                   
                    out(rand);
                    st.store(System.nanoTime() - last_send_instant);
                } else if (op == BFTTupleSpace.RDP) {

            
                    long last_send_instant = System.nanoTime();
  
                     Tuple ret = rdp(rand);
                    st.store(System.nanoTime() - last_send_instant);
                    //System.out.println("RDP: "+ret);

                } else if (op == BFTTupleSpace.INP) {

               
                    long last_send_instant = System.nanoTime();
                 
                     Tuple ret = inp(rand);
                    st.store(System.nanoTime() - last_send_instant);
                    //System.out.println("INP: "+ret);
                }

                if (verbose) {
                    System.out.println(this.id + " // sent!");
                }

                // System.out.println("resultado lido= "+ ret.toString());
                if (interval > 0) {
                    try {
                        //sleeps interval ms before sending next request
                        Thread.sleep(interval);
                    } catch (InterruptedException ex) {
                    }
                }

                if (verbose && (req % 1000 == 0)) {
                    System.out.println(this.id + " // " + req + " operations sent!");
                }

            }

            if (id == initId) {
                System.out.println(this.id + " // Average time for " + numberOfOps / 2 + " executions (-10%) = " + st.getAverage(true) / 1000 + " us ");
                System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2 + " executions (-10%) = " + st.getDP(true) / 1000 + " us ");
                System.out.println(this.id + " // Average time for " + numberOfOps / 2 + " executions (all samples) = " + st.getAverage(false) / 1000 + " us ");
                System.out.println(this.id + " // Standard desviation for " + numberOfOps / 2 + " executions (all samples) = " + st.getDP(false) / 1000 + " us ");
                System.out.println(this.id + " // Maximum time for " + numberOfOps / 2 + " executions (all samples) = " + st.getMax(false) / 1000 + " us ");
            }

        }

        
    }
}
