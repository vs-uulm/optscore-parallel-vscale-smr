package de.optscore.vscale.util;

import de.optscore.vscale.client.ClientWorker;
import de.optscore.vscale.client.EvalClientRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * @author Dominik Meißner, Gerhard Habiger
 */
public class TestcaseSyncClient implements Runnable {
    private Socket socket;
    private BufferedReader reader;
    private PrintWriter writer;
    private HashMap<String, CountDownLatch> map;

    private boolean closed;

    /**
     * Logging
     */
    private static final Logger logger = Logger.getLogger(TestcaseSyncClient.class.getName());


    public TestcaseSyncClient(String coordinatorHost, int coordinatorPort) {
        logger.setLevel(ClientWorker.GLOBAL_LOGGING_LEVEL);

        closed = false;
        map = new HashMap<>();
        try {
            socket = new Socket(coordinatorHost, coordinatorPort);
            socket.setSoTimeout(0);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer = new PrintWriter(socket.getOutputStream(), true);
        } catch(IOException e) {
            e.printStackTrace();
            logger.severe("Could not connect to TestCoordinator! Exiting...");
            System.exit(9);
        }
    }

    public void run() {
        while(!closed) {
            try {
                String line = reader.readLine();
                String[] lineArr = null;
                if(line != null) {
                    lineArr = line.split("-", 2);
                }
                if(lineArr != null && lineArr.length == 2 && lineArr[0].equals("go")) {
                    if(map.containsKey(lineArr[1])) {
                        map.get(lineArr[1]).countDown();
                    } else {
                        logger.warning("Warning: received go for unknown waitForAllClientsReady (" + lineArr[1] + ")");
                    }
                } else if(lineArr != null && lineArr.length == 2 && lineArr[0].equals("reset")) {
                    // resend ready in case the waitForAllClientsReady broke
                    writer.println("ready-" + lineArr[1]);
                } else {
                    logger.warning("Warning: received unknown message type");
                }
            } catch(IOException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    public void waitForAllClientsReady() {
        UUID uuid = UUID.randomUUID();
        CountDownLatch latch = new CountDownLatch(1);
        map.put(uuid.toString(), latch);

        // inform coordinator that we are ready
        writer.println("ready-" + uuid.toString());

        // await the response from coordinator
        try {
            latch.await();
        } catch(InterruptedException e) {
            logger.warning("SyncClient was interrupted while waiting for other clients, reset test?");
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        logger.fine("Closing client");
        this.closed = true;
        writer.println("close-x");
        reader.close();
        writer.close();
        socket.close();
    }
}
