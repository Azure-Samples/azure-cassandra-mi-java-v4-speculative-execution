---
page_type: sample
languages:
- java
products:
- azure
description: "How to implement speculative execution policy in Azure Managed Instance for Apache Cassandra"
urlFragment: azure-cassandra-mi-java-v4-speculative-execution
---

# How to implement speculative execution policy in Azure Managed Instance for Apache Cassandra.

The sample loads data into a Cassandra table and artificially degrades the performance of a single node in the cluster to demonstrate the benefits of using `speculative-execution-policy` in Cassandra V4 Java driver.

## Prerequisites
* Before you can run this sample, you must have the following :
    * An Apache Cassandra cluster and networking access to it. Check out portal quickstart for [Azure Managed Instance for Apache Cassandra](https://docs.microsoft.com/azure/managed-instance-apache-cassandra/create-cluster-portal).
    * [Java Development Kit (JDK) 1.8+](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
        * On Ubuntu, run `apt-get install default-jdk` to install the JDK.
    * Be sure to set the JAVA_HOME environment variable to point to the folder where the JDK is installed.
    * [Download](http://maven.apache.org/download.cgi) and [install](http://maven.apache.org/install.html) a [Maven](http://maven.apache.org/) binary archive
        * On Ubuntu, you can run `apt-get install maven` to install Maven.
    * [Git](https://www.git-scm.com/)
        * On Ubuntu, you can run `sudo apt-get install git` to install Git.

## Running this sample

1. Clone this repository using `git clone https://github.com/Azure-Samples/azure-cassandra-mi-java-v4-speculative-execution`

1. Update parameters in `java-examples/src/main/resources/application.conf`: 
    1. Enter the datacenter name in the `DC` field.
    1. Enter `username` and `password` in `datastax-java-driver.advanced.auth-provider` section, and the IP addresses of your cluster seed nodes in `datastax-java-driver.basic.contact-points`. 
    1. Choose one node for which performance will be artifically degraded by the app, and enter the I.P. address of that node in `nodeToDegrade`.

1. Run `mvn clean package` from java-examples folder to build the project. This will generate `cassandra-mi-load-tester-1.0.0-SNAPSHOT.jar` under target folder.

1. Run `java -jar target/cassandra-mi-load-tester-1.0.0-SNAPSHOT.jar` in a terminal to start your java application. Initially this will run **without** using speculative query execution policy. It will create a keyspace and user table, load 50 records, and then read those records, measuring the p50, p99, and min/max latencies. You should see quite high latencies for P99 and max (along with messages that the selected node is degraded):

    ![Run 1](/media/run1.png?raw=true "run 1")

1. Next, review the content of the `speculative-execution-policy` section in `java-exmple/src/main/resources/application.conf`. Notice the line `class = ConstantSpeculativeExecutionPolicy` which is commented out. When this line is commented out, a default class of `NoSpeculativeExecutionPolicy` is used. Uncomment `class = ConstantSpeculativeExecutionPolicy` to implement speculative execution.

1. Compile/build and run the application again. You should see significantly reduced p99 and max latency, as other nodes are speculatively queried while waiting for the response from the initial node that was queried if it exceeds a certain delay - see below. The number of nodes that are tried, and the amount of time to wait for a response from each node, is based on the values set for `max-executions` and `delay` respectively.

    ![Run 2](/media/run2.png?raw=true "run 2")

> **IMPORTANT** -
> In this sample all requests are explicitly flagged as `idempotent` using `setIdempotent(true)` (see [read](https://github.com/Azure-Samples/azure-cassandra-mi-java-v4-speculative-execution/blob/b76b326d0ba227ea34cdcb3e44ac8db3ec4a43ea/java-examples/src/main/java/com/azure/cassandrami/repository/UserRepository.java#L102) and [write](https://github.com/Azure-Samples/azure-cassandra-mi-java-v4-speculative-execution/blob/b76b326d0ba227ea34cdcb3e44ac8db3ec4a43ea/java-examples/src/main/java/com/azure/cassandrami/repository/UserRepository.java#L69) methods in `UserRepository.java`). If a query is not idempotent, the driver will never schedule speculative executions for it, even if the policy is configured, because there is no way to guarantee that only one node will apply the mutation. Consider carefully whether a query should be idempotent or not, and ensure the setting is applied where needed.
> 
> In a real application that implements speculative execution policy, you should of course not have a `CustomLoadBalancingPolicy.java` as shown in this sample (this is just used to artifically degrade the performance on one node from the client side). If using this sample as a basis for building an app, remove `CustomLoadBalancingPolicy.java` from the project, and the reference to it in `java-exmple/src/main/resources/application.conf`.
