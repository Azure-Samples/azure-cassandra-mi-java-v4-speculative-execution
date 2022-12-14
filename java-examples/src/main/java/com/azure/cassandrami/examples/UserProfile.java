// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cassandrami.examples;

import com.azure.cassandrami.repository.UserRepository;
import com.azure.cassandrami.util.Configurations;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.github.javafaker.Faker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Load data into Cassandra
 */
public class UserProfile {

    public int iterations = 1;
    public int NUMBER_OF_THREADS = 1;
    public int NUMBER_OF_WRITES_PER_THREAD = 2;

    Queue<String> docIDs = new ConcurrentLinkedQueue<String>();
    AtomicInteger exceptionCount = new AtomicInteger(0);
    AtomicLong insertCount = new AtomicLong(0);
    AtomicInteger recordCount = new AtomicInteger(0);
    AtomicInteger readCount = new AtomicInteger(0);
    AtomicInteger verifyCount = new AtomicInteger(0);
    AtomicLong totalLatency = new AtomicLong(0);
    Queue<Long> latencies = new ConcurrentLinkedQueue<Long>();
    private static Configurations config = new Configurations();
    private static final Logger LOGGER = LoggerFactory.getLogger(UserProfile.class);

    public void loadData(final String keyspace, final String table, final UserRepository repository,
            final UserProfile u, final String preparedStatement, final int noOfThreads,
            final int noOfWritesPerThread) throws InterruptedException, NumberFormatException, IOException {

        final Faker faker = new Faker();
        final ExecutorService es = Executors.newCachedThreadPool();
        int timeout = Integer.parseInt(config.getProperty("loadTimeout"));
        LOGGER.info("Loading data (will timeout after "+timeout+" minutes)....");
        for (int i = 1; i <= noOfThreads; i++) {
            final Runnable task = () -> {
                for (int j = 1; j <= noOfWritesPerThread; j++) {
                    final UUID guid = java.util.UUID.randomUUID();
                    final String strGuid = guid.toString();
                    this.docIDs.add(strGuid);
                    try {
                        final String name = faker.name().lastName();
                        final String city = faker.address().city();
                        u.recordCount.incrementAndGet();
                        repository.insertUser(preparedStatement, guid.toString(), name, city);
                        u.insertCount.incrementAndGet();
                    } catch (final Exception e) {
                        u.exceptionCount.incrementAndGet();
                        LOGGER.info("Exception: " + e);
                    }
                }
            };
            es.execute(task);
        }
        es.shutdown();
        
        final boolean finished = es.awaitTermination(timeout, TimeUnit.MINUTES);
        if (finished) {
            LOGGER.info("number of records loaded: "+this.insertCount.get());
            LOGGER.info("Finished executing all threads for loading data.");
            Thread.sleep(3000);
        }
    }

    public void readTest(final String keyspace, final String table, final UserRepository repository,
            final UserProfile u, final int noOfThreads, final int noOfWritesPerThread) throws InterruptedException, NumberFormatException, IOException {

        Configurations config = new Configurations();
        List<String> list = new ArrayList<String>();
        for (final String id : this.docIDs) {
            list.add(id);
        }

        final ExecutorService es = Executors.newCachedThreadPool();
        List<List<String>> lists = Lists.partition(list, NUMBER_OF_WRITES_PER_THREAD);
        LOGGER.info("executing reads..." + "\n");
        int iterations = Integer.parseInt(config.getProperty("iterations"));
        for (int i=0; i<iterations; i++){
            for (List<String> splitList : lists) {
                final Runnable task = () -> {
                    for (final String id : splitList) {                 
                        try{
                            final long startTime = System.currentTimeMillis();
                            repository.selectUser(id, keyspace, table);
                            final long endTime = System.currentTimeMillis();
                            final long duration = (endTime - startTime);
                            latencies.add(duration);
                            u.readCount.incrementAndGet();
                            this.totalLatency.getAndAdd(duration); 
                        }
                        catch(Exception e)
                        {
                            //Swallow any exceptions
                            LOGGER.info("Exception: "+e);
                        }                      
                    }
                };
                es.execute(task);
            }
        }
 
        es.shutdown();
        final boolean finished = es.awaitTermination(5, TimeUnit.MINUTES);
        if (finished) {
            List<Long> latencies = new ArrayList<Long>();
            for (final Long id : this.latencies) {
                latencies.add(id);
            }
            LOGGER.info("Total number of reads executed: "+u.readCount.get());
            LOGGER.info("p50 latency: "+percentile(latencies, 50));
            LOGGER.info("p99 latency: "+percentile(latencies, 99));
            try{
                Long min = Collections.min(latencies);
                Long max = Collections.max(latencies);
                LOGGER.info("Max read duration: "+max);
                LOGGER.info("Min read duration: "+min);
            }
            catch(Exception e){
                LOGGER.info("Min/Max exception: "+e);
            }


            Thread.sleep(1000);
        }
        LOGGER.info("load test done." + "\n");
    }

    public static long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index-1);
    }

    public static void main(final String[] s) throws Exception {

        final UserProfile u = new UserProfile();
        final String keyspace = "uprofile";
        final String table = "user";
        String DC = config.getProperty("DC");
        LOGGER.info("Creating Cassandra session...");
        CqlSession cassandraSource = CqlSession.builder().withLocalDatacenter(DC).build();
        int NUMBER_OF_WRITES_PER_THREAD = Integer.parseInt(config.getProperty("threads"));
        int NUMBER_OF_THREADS = Integer.parseInt(config.getProperty("records"));        
        final UserRepository sourcerepository = new UserRepository(cassandraSource);

        try {

            // Create keyspace and table in cassandra database
            LOGGER.info("Dropping source keyspace " + keyspace + " (if exists)... ");
            sourcerepository.deleteTable("DROP KEYSPACE IF EXISTS " + keyspace + "");
            LOGGER.info("Done dropping source keyspace " + keyspace + ".");
            LOGGER.info("Creating keyspace: " + keyspace + "... ");
            sourcerepository.createKeyspace("CREATE KEYSPACE " + keyspace
                    + " WITH REPLICATION = {'class':'NetworkTopologyStrategy', '"+DC+"' :3}");
                    LOGGER.info("Done creating keyspace: " + keyspace + ".");
                    LOGGER.info("Creating table: " + table + "...");            
            sourcerepository.createTable("CREATE TABLE " + keyspace + "." + table
                    + " (user_id text PRIMARY KEY, user_name text, user_bcity text)");
                    LOGGER.info("Done creating table: " + table + "."); 
            Thread.sleep(3000);


            // Setup load test 
            final String loadTestPreparedStatement = "insert into " + keyspace + "." + table + " (user_bcity,user_id,"
                    + "user_name) VALUES (?,?,?)";                   

            // Run Load Test - Insert rows into user table
            u.loadData(keyspace, table, sourcerepository, u, loadTestPreparedStatement, 
                    NUMBER_OF_THREADS, NUMBER_OF_WRITES_PER_THREAD);
                    Thread.sleep(3000);
            u.readTest(keyspace, table, sourcerepository, u, NUMBER_OF_THREADS, NUMBER_OF_WRITES_PER_THREAD);
        } catch (final Exception e) {
            LOGGER.info("Main Exception " + e);
        }
        System.exit(0);
    }
}
