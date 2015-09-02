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

package de.qaware.mysqlbenchmark;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Strings;
import de.qaware.mysqlbenchmark.console.Parameters;
import de.qaware.mysqlbenchmark.logfile.QueryParser;
import de.qaware.mysqlbenchmark.sql.SQLStatementExecutor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Entry point for starting the benchmark tool
 * Parses the command line parameters, executes sql statements and writes results.
 *
 * @author Felix Kelm felix.kelm@qaware.de
 */
final class Main {
    private static QueryParser parser;
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Main.class);

    private Main() {
        // Prevent instantiation
    }

    /**
     * Main entry point.
     *
     * @param args args are described in the class {@link de.qaware.mysqlbenchmark.console.Parameters}
     */
    public static void main(String[] args) {

        /**
         * Read command line parameters using jcommander
         */
        final Parameters params = new Parameters();
        final JCommander commander = new JCommander(params);
        commander.setProgramName("MySQL Benchmark Tool");
        try {
            commander.parse(args);
        } catch (ParameterException e) {
            commander.usage();
            return;
        }
        if (params.isHelp()) {
            commander.usage();
            return;
        }

        SQLStatementExecutor benchmark = new SQLStatementExecutor(params);
        FileWriter writer = null;
        try {
            parser = new QueryParser(benchmark, params.getInputFile(), params.getConnectionID(), params.getIgnorePrefixes());
        } catch (IOException e) {
            LOG.error("IO Exception.", e);
            return;
        }

        /**
         * parse the logfile and run queries
         */
        try {

            writer = new FileWriter(params.getResultfilename());

            while (parser.parseLogFile(params.getBatch())) {

                LOG.info("Read " + parser.size() + " queries from file '" + params.getInputFile() + "'.");

                // process queries
                try {
                    benchmark.join();
                    LOG.info("Benchmark completed");
                } catch (Exception e) {
                    LOG.error("Error processing queries.", e);
                }
                // get time measurements
                String result = benchmark.getResult(QueryBenchmark.Format.get(params.getFormat()));

                if (params.isVerbose()) {
                    LOG.info(result);
                }

                // write result to file if needed
                if (!Strings.isStringEmpty(params.getResultfilename())) {
                    LOG.info("Writing result to " + params.getResultfilename());
                    writer.write(result);
                }
            }

        } catch (FileNotFoundException e) {
            LOG.error("File not found.", e);
        } catch (IOException e) {
            LOG.error("IO Exception.", e);
        } finally {
            parser.close();
            try {
                writer.close();
            } catch (Exception e) {
                /* Intentionally Swallow  Exception */
                LOG.error("Could not close sql connection.");
            }
        }
    }
}
