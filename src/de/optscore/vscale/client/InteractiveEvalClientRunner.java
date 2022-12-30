package de.optscore.vscale.client;

import de.optscore.vscale.EvalActionType;
import de.optscore.vscale.EvalRequest;

public class InteractiveEvalClientRunner extends EvalClientRunner {

    private boolean playing;
    private int rateModifier;
    private boolean shutdownRequested;
    private int rateLimitSleepDuration;

    /**
     * Creates a new interactive EvalClientRunner, which allows req/s rate adjustment during runtime
     *
     * @param id                     Id of this runner for gathering statistics per runner
     * @param rateLimitSleepDuration The runner will sleep for this duration in ms between sending each request
     */
    public InteractiveEvalClientRunner(int id, int rateLimitSleepDuration) {
        super(id);
        this.client = new InteractiveAsyncEvalClient(id);
        this.rateLimitSleepDuration = 100;
        this.rateModifier = 10;
        this.playing = false;
        this.shutdownRequested = false;
    }

    @Override
    public void run() {
        // main loop for listening to user input (e.g. also when paused and not sending requests)
        while(!shutdownRequested) {
            // fire off requests until user requests pause or shutdown
            while(playing) {
                this.client.sendRequest(this.requests.get(0));
                try {
                    Thread.sleep(this.rateLimitSleepDuration);
                } catch(InterruptedException e) {
                    System.out.println("Interactive EvalClientRunner Thread was interrupted while sleeping for rate limit.");
                }
            }

            // if we're paused, just spin while waiting for further user input
            try {
                Thread.sleep(2);
            } catch(InterruptedException e) {

            }
        }
        System.out.println("Shutdown of interactive EvalClientRunner has been requested. Shutting down...");
        shutdownRunner();
    }

    public void deactivateCPUCore(int coreIndex) {
        EvalRequest req = new EvalRequest.EvalRequestBuilder((EvalActionType.REMOVE_CPU_CORE), coreIndex).build();
        this.client.sendRequest(req);
    }

    public void activateCPUCore(int coreIndex) {
        EvalRequest req = new EvalRequest.EvalRequestBuilder((EvalActionType.ADD_CPU_CORE), coreIndex).build();
        this.client.sendRequest(req);
    }

    private void shutdownRunner() {
        this.client.shutDownClient();
    }

    public void setShutdownRequested(boolean shutdownRequested) {
        this.shutdownRequested = shutdownRequested;
    }

    public int getRateLimitSleepDuration() {
        return this.rateLimitSleepDuration;
    }

    public void setRateLimitSleepDuration(int newSleepDuration) {
        if(newSleepDuration < 1) {
            System.out.println("Invalid value for newSleepDuration (" + newSleepDuration + ") ==> Ignored.");
            return;
        }
        System.out.println("Setting new rate in interactive EvalClientRunner: " + newSleepDuration + "ms.");
        this.rateLimitSleepDuration = newSleepDuration;
    }

    public int getRateModifier() {
        return rateModifier;
    }

    public void setRateModifier(int rateModifier) {
        if(rateModifier < 1) {
            System.out.println("Wrong value for rateModifier (" + rateModifier + ") ==> Ignored.");
            return;
        }
        this.rateModifier = rateModifier;
    }

    public boolean isPlaying() {
        return playing;
    }

    public void togglePlaying() {
        this.playing = !this.playing;
    }
}
