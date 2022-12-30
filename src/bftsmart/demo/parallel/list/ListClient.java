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

//import bftsmart.tom.bftsmart.parallelism.ParallelMapping;
import java.io.IOException;

import java.util.logging.Level;
import java.util.logging.Logger;

//import bftsmart.tom.ServiceProxy;
//import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.Storage;

/**
 * Example client that updates a BFT replicated service (a counter).
 *
 */
public class ListClient {

    private static int VALUE_SIZE = 1024;
    public static int initId = 0;

    @SuppressWarnings("static-access")
    public static void main(String[] args) throws IOException {
        if (args.length < 7) {
            System.out.println("Usage: ... ListClient <num. threads> <process id> <number of operations> <interval> <maxIndex> <verbose?> <parallel?> <conflict percent>");
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
        int percent = Integer.parseInt(args[7]);

        Client[] c = new Client[numThreads];

        for (int i = 0; i < numThreads; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                Logger.getLogger(ListClient.class.getName()).log(Level.SEVERE, null, ex);
            }

            System.out.println("Launching client " + (initId + i));
            c[i] = new ListClient.Client(initId + i, numberOfOps, interval, max, verbose, parallel, percent);
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

        int countNumOp = 0;

        boolean verbose;
        //boolean dos;
        //ServiceProxy proxy;
        //byte[] request;
        BFTList<Integer> store;

        int maxIndex;
        int percent;

        public Client(int id, int numberOfOps, int interval, int maxIndex, boolean verbose, boolean parallel, int percent) {
            super("Client " + id);

            this.id = id;
            this.numberOfOps = numberOfOps;
            this.percent = percent;

            this.interval = interval;

            this.verbose = verbose;
            //this.proxy = new ServiceProxy(id);
            //this.request = new byte[this.requestSize];
            this.maxIndex = maxIndex;

            store = new BFTList<Integer>(id, parallel);
            //this.dos = dos;
        }

        /*  private boolean insertValue(int index) {

            return store.add(index);

        }*/
        public void run() {

            //System.out.println("Warm up...");
            int req = 0;

            Storage st = new Storage(numberOfOps);

            System.out.println("Executing experiment for " + numberOfOps + " ops");

//            WorkloadGenerator work = new WorkloadGenerator(numberOfOps);
            for (int i = 0; i < numberOfOps; i++, req++) {
                              
                //de acordo com o numero de clientes no caso 10 * 1000 = 10000 intervalo para politica
                // 50 * 200 = 10000
                // 100 * 100 = 10000
                int op = BFTList.CONTAINS;
                if (countNumOp < 1000) {
                    //0 % conflitantes
                    op = BFTList.CONTAINS;
                } else if (countNumOp < 2000){
                    //100 % conflitantes
                    op = BFTList.ADD;
                }else if (countNumOp < 3000){
                    //50 % conflitantes
                    if (countNumOp < 2500) {
                        op = BFTList.ADD;
                    }else{
                        op = BFTList.CONTAINS;
                    }                                                  
                }else if (countNumOp < 4000){
                    //20 % conflitantes
                    if (countNumOp < 3200) {
                        op = BFTList.ADD;
                    }else{
                        op = BFTList.CONTAINS;
                    }                                
                }else if (countNumOp < 5000){
                    //80 % conflitantes
                    if (countNumOp < 4800) {
                        op = BFTList.ADD;
                    }else{
                        op = BFTList.CONTAINS;
                    }
                }else{
                    countNumOp = 0;
                }

                countNumOp++;
//              int op = BFTList.ADD;  
//                if (percent == 0) {
//                    op = BFTList.CONTAINS;
//                }

                
                if (op == BFTList.ADD) {

                    //int index = rand.nextInt(maxIndex);
                    int index = maxIndex - 1;
                    long last_send_instant = System.nanoTime();
                    store.add(new Integer(index));
                    st.store(System.nanoTime() - last_send_instant);
                } else if (op == BFTList.CONTAINS) {

                    //int index = rand.nextInt(maxIndex);
                    int index = maxIndex - 1;
                    long last_send_instant = System.nanoTime();
                    store.contains(new Integer(index));
                    st.store(System.nanoTime() - last_send_instant);

                } else if (op == BFTList.GET) {

                    //int index = rand.nextInt(maxIndex);
                    int index = maxIndex - 1;
                    long last_send_instant = System.nanoTime();
                    store.get(index);
                    st.store(System.nanoTime() - last_send_instant);

                } else if (op == BFTList.REMOVE) {

                    //int index = rand.nextInt(maxIndex);
                    int index = maxIndex - 1;
                    long last_send_instant = System.nanoTime();
                    store.remove(new Integer(index));
                    st.store(System.nanoTime() - last_send_instant);

                } else {//SIZE
                    long last_send_instant = System.nanoTime();
                    store.size();
                    st.store(System.nanoTime() - last_send_instant);
                }

                if (verbose) {
                    System.out.println(this.id + " // sent!");
                }

                if (interval > 0) {
                    try {
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
