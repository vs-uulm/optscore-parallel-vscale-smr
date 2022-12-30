package de.optscore.vscale.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper object for conveniently storing configuration data for a test case. Since its only purpose is to ease the
 * passing of lots of parameters in a convenient way, no getter/setters for fields.
 */
public class TestcaseConfiguration {

    public String name;
    public boolean withUDS;
    public int numOfActiveCores;
    public int numOfMaxCores;
    public int udsConfPrim;
    public int udsConfSteps;
    public int numOfReplicas;
    public int numOfClientWorkers;
    public int workloadId;
    public int warmupPhaseSecs;
    public int measurePhaseSecs;
    public int clientIncrement;
    public int timesToIncrement;
    public int baseLoadInterval;
    public String[] replicaIPs;
    public String[] clientWorkerIPs;

    public boolean asyncMode;
    public boolean interactiveMode;

    public String getTestCaseName() {
        return name + "-" + (withUDS ? "mt-cr" : "st-cr") + numOfActiveCores +
                "-up" + udsConfPrim + "-us" + udsConfSteps +
                "-wl" + workloadId +
                "-re" + numOfReplicas + "-cw" + numOfClientWorkers +
                "-wu" + warmupPhaseSecs + "-me" + measurePhaseSecs +
                "-ci" + clientIncrement + "-ti" + timesToIncrement +
                "-bl" + baseLoadInterval;
    }

    public int getTestCaseIdFromName() {
        Pattern extractNumberPattern = Pattern.compile("test_case_(\\d+)");
        Matcher matcher = extractNumberPattern.matcher(this.name);
        matcher.find();
        return Integer.parseInt(matcher.group(1));
    }

    public String getCsvFilePath() {
        return getTestCaseName() + ".csv";
    }
}
