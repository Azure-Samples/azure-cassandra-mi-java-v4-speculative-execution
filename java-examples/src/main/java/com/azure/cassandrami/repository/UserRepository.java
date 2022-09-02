// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cassandrami.repository;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

import static com.datastax.oss.driver.api.core.ConsistencyLevel.QUORUM;

/**
 * This class gives implementations of create, delete table on Cassandra database Insert & select data from the table
 */
@SuppressWarnings("UnnecessaryLocalVariable")
public class UserRepository {
    private static final ConsistencyLevel CONSISTENCY_LEVEL = QUORUM;
    private static final Logger LOGGER = LoggerFactory.getLogger(UserRepository.class);
    private final CqlSession session;

    public UserRepository(final CqlSession session) {
        this.session = session;
    }

    /**
     * Create keyspace uprofile in cassandra DB
     */
    public void createKeyspace(final String queryString) {
        this.session.execute(queryString);
        LOGGER.info("Executed query: " + queryString);
    }

    /**
     * Create user table in cassandra DB
     */
    public void createTable(final String queryString) {
        final String query = queryString;
        this.session.execute(query);
        LOGGER.info("Executed query: " + queryString);
    }

    /**
     * Delete user table.
     */
    public void deleteTable(final String queryString) {
        final String query = queryString;
        this.session.execute(query);
    }

    /**
     * Insert a row into user table
     *
     * @param id   user_id
     * @param name user_name
     * @param city user_bcity
     */
    public void insertUser(final String preparedStatement, final String id, final String name, final String city) {
        final PreparedStatement prepared = this.session.prepare(preparedStatement);
        final BoundStatement bound = prepared.bind(city, id, name).setIdempotent(true);
        this.session.execute(bound);
    }

    /**
     * Create a PrepareStatement to insert a row to user table
     *
     * @return PreparedStatement
     */
    public PreparedStatement prepareInsertStatement(final String queryString) {
        final String insertStatement = queryString;
        return this.session.prepare(insertStatement);
    }

    /**
     * Select all rows from user table
     */
    public void selectAllUsers(final String keyspace) {

        final String query = "SELECT * FROM " + keyspace + ".user";
        final List<Row> rows = this.session.execute(query).all();

        for (final Row row : rows) {
            LOGGER.info("Obtained row: {} | {} | {} ", row.getInt("user_id"), row.getString("user_name"),
                row.getString("user_bcity"));
        }
    }

    /**
     * Select a row from user table
     *
     * @param id user_id
     */
    // public void selectUser(final String id, final String keyspace, final String table) {
    //     final String query = "SELECT * FROM " + keyspace + "." + table + " where user_id ='" + id + "'";
    //     final Row row = this.session.execute(query).one();
    //     LOGGER.info("Obtained row: {} | {} | {} ",
    //         Objects.requireNonNull(row).getString("user_id"), row.getString("user_name"),
    //         row.getString("user_bcity"));
    // }

    public String selectUser(final String id, final String keyspace, final String table) {
        SimpleStatement statement = SimpleStatement.newInstance("SELECT * FROM " + keyspace + "." + table + " where user_id ='" + id + "'")
        .setIdempotent(true).setConsistencyLevel(ConsistencyLevel.ONE);
        //final String query = "SELECT * FROM " + keyspace + "." + table + " where user_id ='" + id + "'";
        final Row row = this.session.execute(statement).one();
        return row.getString("user_name");
    }    

    public long selectUserCount(final String queryString) {
        final String query = queryString;
        final Row row = this.session.execute(query).one();
        return row.getLong(0);
    }

    public void simpleInsertUser(final Statement<?> statement) {
        final BatchStatement batch = BatchStatement.builder(BatchType.UNLOGGED).build();
        batch.setConsistencyLevel(CONSISTENCY_LEVEL);
        this.session.execute(batch);
    }
}