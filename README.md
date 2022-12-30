# OptSCORE Multithreaded State Machine Replication with vertical scaling capabilities


This repository contains the source code used for obtaining and demonstrating the results presented in the 2018 EDCC paper [Resource-efficient state-machine replication with multithreading and vertical scaling](https://doi.org/10.1109/EDCC.2018.00024).

The project is based on BFT-SMaRt, and all evaluations were performed on an older version (v1.1-beta). Therefore, this version is included in this code base. Basic instructions on how to set up and run BFT-SMaRt clusters can be found in [README-BFT-SMaRt](README-BFT-SMaRt.txt).


### Running testcases

The published measurements were created by running the included ```de.optscore.vscale.server.EvalServer``` as a BFT-SMaRt ServiceReplica. This class replaces the standard BFT-SMaRt ServiceReplica-implementations with a ```de.optscore.vscale.server.UDSServiceReplica```, which enables multithreaded request execution based on UDS (cf. [UDS: A Novel and Flexible Scheduling Algorithm for Deterministic Multithreading, 2016, Hauck et al.](https://doi.org/10.1109/SRDS.2016.030)).

To run testcases in this project similar to the ones used in the paper, first distribute this repository (or a build) to your desired number of host machines, and configure the [config/hosts.config](config/hosts.config) accordingly.
A (set of) client machine(s) should be available, as well as a SSH user with access to all of these machines, with admin-privileges on at least the replica machines (to automatically add and remove CPU cores during runtime).


The process of setting up and preparing replicas, setting up and preparing clients, running a testcase, and logging results has been automated with the help of ```de.optscore.vscale.util.TestcaseCoordinator```.
To use this coordinator, create a .csv file with testcase specifications according to the usage instructions found in ```de.optscore.vscale.util.TestcaseCoordinator.java```, line 90ff. Further explanations can also be found below.
Additionally, the method of testing (what workloads are, etc.) has been described in the paper.

A TestcaseCoordinator will try to connect to each replica machine, start a BFT-SMaRt instance on each machine, connect to ClientWorker-machines, start ClientWorkers there, then synchronize all machines and start a testcase run.
Each machine will log results to disk. After the testcase has finished, logged results will be collected and copied to the coordinator host.



### Manually starting servers and clients


Run the aforementioned EvalServer on each host, with parameters specifying its corresponding ID and whether UDS-based multithreading should be active: ```java -cp <classpath> de.optscore.vscale.server.EvalServer <serverId> <withUDS>``` (e.g.: ```java -cp "build/classes/java/main/:build/libs/* de.optscore.vscale.server.EvalServer 1 true"```).

The clients used for generating load and logging results to disk can be found in the package ```de.optscore.vscale.client```.
```ClientWorker```s can be instantiated on multiple client machines to load the SMR cluster. Each ```ClientWorker``` will instantiate multiple actual BFT-SMaRt-clients as individual threads, and take several parameters to specify the duration and type of load that is generated on the servers:
```ClientWorker <startingProcessId> <testcaseName> <withUDS> <numOfActiveCores> <UDSprims> <UDSsteps> <numOfReplicas> <numOfClientWorkers> <workloadId> <warmupPhaseSecs> <measurementPhaseSecs> <clientIncrement> <timestoIncrement> <baseLoadInterval> <testcaseCoordinatorIP> <testcaseCoordinatorPort>```.

Clients will send requests based on these parameters. Requests contain a list of actions that are to be performed by replicas, and each action is of a certain type (see ```de.optscore.vscale.EvalActionType```). Actions are parsed and understood by ```EvalServer```. Some can reconfigure the replicas, e.g., to set up a testcase (add or remove CPU cores to simulate vertical scaling), others load the replicas with certain primitive tasks to simulate load.


The parameters can be explained as follows, if they are not explained in the paper.

* ```startingProcessId``` The starting ID the actual client threads instantiated by this Worker should use. The first client thread will use this ID, and subsequent threads will use auto-incremented higher IDs. Witd "ID" we refer to the BFT-SMaRt-internal client ID that each client connecting to a BFT-SMaRt cluster requires for establishing a connection and sending requests. If you instantiate multiple ClientWorkers (e.g., on different machines), make sure that the number of instantiated client threads (and thereby the incremented IDs) don't overlap. In other words, reserve enough space between startingProcessIds between multiple Workers.
* ```testcaseName``` A unique name given to this testcase, which will be used to name the logged results on disk and provide commandline output.
* ```withUDS``` Whether UDS was active when starting the servers (used for logging purposes, so the logged results are complete with respect to the chosen test case configuration)
* ```numOfActiveCores``` How many CPU cores should be active on each replica for this testcase. The desired number is automatically configured by sending a corresponding request with the correct actions in the beginning of the testcase, during the automatic setup phase.
* ```UDSprims``` and ```UDSsteps``` configures the primaries and steps for UDS for this testcase, if the servers were started with UDS. For details on this, please consult the paper.
* ```numOfReplicas``` The number of replicas that were started. Used for logging and when using the TestcaseCoordinator.
* ```numOfClientWorkers``` The number of client workers; used for logging and when using the TestcaseCoordinator.
* ```workloadId``` Which pre-defined workload to run. Workloads are explained in the paper and specified in the code.
* ```warmupPhaseSecs``` How long to load the cluster without measuring results after each client count increment.
* ```measurementPhaseSecs``` How long each measurement phase should take inbetween incrementing client count.
* ```clientIncrement``` How many clients should be added for each increment step.
* ```timestoIncrement``` How many increments should be performed (i.e., how often should clients be added, where each increment adds the previously configured number of clients to the pool of clients loading the cluster).
* ```baseLoadInterval``` Explained in the paper -- avoids starvation of single requests in case multithreading is active.
* ```testcaseCoordinatorIP``` Used when using a TestcaseCoordinator to automate testing.
* ```testcaseCoordinatorPort``` Used when using a TestcaseCoordinator to automate testing.