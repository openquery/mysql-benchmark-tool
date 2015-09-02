/*
 * Copyright (C) 2014 QAware GmbH (http://www.qaware.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.qaware.mysqlbenchmark.sql;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.qaware.mysqlbenchmark.logfile.Query;
import de.qaware.mysqlbenchmark.func.SQLType;
import de.qaware.mysqlbenchmark.console.Parameters;
import de.qaware.mysqlbenchmark.QueryBenchmark;

import etm.core.monitor.EtmMonitor;
import etm.core.monitor.EtmPoint;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;


class ServerConnection extends Thread {

    static ThreadGroup threadgroup = new ThreadGroup("client connections");
    private static final Logger LOG = LoggerFactory.getLogger(ServerConnection.class);

    private String connectionString;
    private String username;
    private String password;
    private Connection connection;
    private Collection<ServerConnection> previousConnections;
    private EtmMonitor etmMonitor;

    private boolean quitNow = false;
    private LinkedBlockingDeque<Query> queries = new LinkedBlockingDeque<Query>(); // a capacity would eventally block the reader

    public ServerConnection(String id, String connectionString, String user, String password,
        EtmMonitor etmMonitor, Collection<ServerConnection> connections) {
        super(threadgroup, id);
        this.username = user;
        this.password =  password;
        this.etmMonitor = etmMonitor;
        this.previousConnections = connections;
        //this.previousConnections = new HashSet<ServerConnection>(connections);
    }

    public void initdb(String database) {
        initConnection(database);
    }

    public void query(Query q) {
        try {
            queries.put(q);
        } catch (InterruptedException e) {
        }
        try {
            interrupt();
        } catch (SecurityException e) {
        }
    }

    public void quit() {
        quitNow = true;
        try {
           interrupt();
        } catch (SecurityException e) {
        }
    }

    public void run() {
        Iterator<ServerConnection> i;
        Thread t;
        // Join previous connections
        while (!previousConnections.isEmpty()) {
            i = previousConnections.iterator();
            while ((t = i.next()) != null) {
                try {
                    t.join();
                    i.remove();
                } catch (InterruptedException e) {
                    if (quitNow) return;
                    // adding a query could interrupt but
                    // we'll depend on the outer loop to ensure
                    // the previousConnections have been joined
                    // before continuing.
                }
            }
        }

        Query q;
        while (!quitNow) {
            try {
                while ((q = queries.take()) != null) {
                    processQuery(q);
                }
            } catch (EmptyStackException e) {
            } catch (InterruptedException e) {
            }
            if (quitNow) break;
            try {
               wait(500); // ms
            } catch (InterruptedException e) {
               if (quitNow) break;
            }
        }
        SQLStatementExecutor.backlogThread.interrupt();
    }

    private void processQuery(Query query) {
        EtmPoint qpoint = this.etmMonitor.createPoint("Query: " + query.getSql());
        try {
            executeStatement(query);
        } finally {
            qpoint.collect();
        }
    }

    private void executeStatement(Query name) {
        try {
            // prepare statement for execution
            PreparedStatement ps = connection.prepareStatement(name.getSql());
            // execute the statement and return the result
            if (name.getType() == SQLType.read) {
                ps.executeQuery();
            } else {
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            LOG.error("Execution of statement {} failed.", name.getSql(), e);
        }
    }

    private void initConnection(String dbname) {
        LOG.info("-------- Opening MySQL JDBC Connection ------------");

        try {
            // open new connection
            connection = DriverManager.getConnection(connectionString + dbname, username, password);
        } catch (SQLException e) {
            LOG.error("SQL connection failed!", e);
            //throw e;
        }

        LOG.info("Connection established.");
    }
}


class BacklogThread extends Thread {
    /**
     * When a connection thread count is reached, this is the waiting thread
     */
    private LinkedBlockingQueue<ServerConnection> connectionBacklog = new LinkedBlockingQueue<ServerConnection>(15);

    private boolean quitNow = false;
    int parallel;

    public void add(ServerConnection c) {
        connectionBacklog.add(c);
    }

    public void quit() {
        quitNow = true;
        try {
           interrupt();
        } catch (SecurityException e) {
        }
    }

    public void run() {
        ServerConnection s;

        while (!quitNow) {
            if (ServerConnection.threadgroup.activeCount() > parallel) {
                try {
                    s = connectionBacklog.remove();
                    s.start();
                } catch (NoSuchElementException e) {
                }
            }
            if (quitNow) break;
            try {
                wait(500); // ms
            } catch (InterruptedException e) {
               if (quitNow) break;
            }
        }
    }
}

/**
 * Wraps the SQL connection and enabled statement execution.
 * Can be passed to {@link de.qaware.mysqlbenchmark.QueryBenchmark} to execute statements.
 *
 * @author Felix Kelm felix.kelm@qaware.de
 * @author Daniel Black daniel.black@openquery.com.au
 */
public class SQLStatementExecutor extends QueryBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(SQLStatementExecutor.class);
    static BacklogThread backlogThread = new BacklogThread();

    private String server;
    private String default_database;
    private String default_username;
    private String default_password;
    private int parallel;

    /**
     *  a map of the current connection ID to a connection object
     * removed fromt this list when a Quit happens
     */
    private Hashtable<String, ServerConnection> connections =
        new Hashtable<String, ServerConnection>();


    /**
     * A new connection will wait for all those active before processing its queue of queries
     * This represents all the currently active connections (those that haven't quit) as the time
     * of a new query received. This is copied to ServerConnectin.previousConnections on creation
     * of a new connection.
     */
    private HashMap<String, ServerConnection> activeConnections = new HashMap<String, ServerConnection>();

    public SQLStatementExecutor(Parameters params) {
        server = params.getServer();
        default_database = params.getDatabase();
        default_username = params.getUsername();
        default_password = params.getPassword();
        parallel = params.getParallel();
        backlogThread.parallel = params.getParallel();
        backlogThread.start();
        // TODO params.getPasswordMap();

        try {
            // load jdbc driver
            Class.forName(params.getDriver());
        } catch (ClassNotFoundException e) {
            LOG.error("Where is your JDBC Driver?", e);
            return;
        }

        LOG.info("JDBC Driver found!");
    }

    /**
     * Creates a connection.
     *
     * @param name connectionID
     * @param name dbuser
     */
    public void connect(String connectionID, String dbuser) {
       // TODO password lookup of db user to obtain different credentials if
       // required.
       ServerConnection s = new ServerConnection(connectionID, server,
           default_username, default_password, etmMonitor, activeConnections.values());
       connections.put(connectionID, s);
       activeConnections.put(connectionID, s);
    }

    /**
     * Specified DB name on connection
     *
     * @param name connectionID
     * @param name dbname
     */
    public void initDb(String connectionID, String dbname) {
       ServerConnection s = activeConnections.get(connectionID);
       if (s == null) {
           // Always should get connect method called before however
           // TODO LOG.
           // lets fake a new connection for now.
           // s = connection(connectionID, "__defaultUser");
           s = new ServerConnection(connectionID, server,
                 default_username, default_password, etmMonitor, activeConnections.values());
       connections.put(connectionID, s);
           return;
       }
       s.initdb(dbname);
       if (ServerConnection.threadgroup.activeCount() > parallel) {
           backlogThread.add(s); 
       } else {
           s.start();
       }
    }

    /**
     * Executes a sql statement. Make sure the connection is initialized first.
     *
     * @param name connectionID
     * @param name statement string
     */
    public void query(String connectionID, Query name) {
       ServerConnection s = activeConnections.get(connectionID);
       if (s != null) {
           s.query(name);
       }
       // ignore if the connection doesn't exist. we've no idea what DB it is on.
    }

    /**
     * Close the connection if no longer needed
     *
     * @param name connectionID
     */
    public void quit(String connectionID) {
       ServerConnection s = activeConnections.remove(connectionID);
       if (s != null) {
           s.quit();
       }
    }

    /**
     * Wait until all ServerConnections have been finished
     */
    public void join() {
        backlogThread.quit();
        try {
            backlogThread.join();
        } catch (InterruptedException e) {
        }
    }

}
