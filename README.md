# cassandra load test sample - speculative execution.

The sample creates a simple keyspace and table with a multi-threaded app, throws data in based on threads and no. of records, and then reads the data back.

TODO - more to be added here. 

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

1.  Update cassandra-related parameters, and number of threads/records to load, in `java-exmple/src/main/resources/application.conf`.
1.  Run `mvn clean package` from java-examples folder to build the project. This will generate cassandra-mi-load-tester-1.0.0-SNAPSHOT.jar under target folder.
1. Run java -jar target/cassandra-mi-load-tester-1.0.0-SNAPSHOT.jar in a terminal to start your java application. This will create a keyspace and user table, and then run a load test with many concurrent threads.