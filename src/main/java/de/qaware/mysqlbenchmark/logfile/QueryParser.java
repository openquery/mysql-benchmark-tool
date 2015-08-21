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

package de.qaware.mysqlbenchmark.logfile;

import com.google.common.base.Strings;
import de.qaware.mysqlbenchmark.func.SQLFunc;
import de.qaware.mysqlbenchmark.sql.SQLStatementExecutor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Simple query parser for mysql log files based on query-ids and prefixes. Uses regex matching.
 *
 * @author Felix Kelm felix.kelm@qaware.de
 * @author Daniel Black daniel.black@openquery.com.au
 */
public class QueryParser {

    private SQLStatementExecutor executor;
    private int sessionCount = 0;
    private BufferedReader br;
    List<String> ignorePrefixes;
    String restrictedID;

    public QueryParser(SQLStatementExecutor executor, String inputFilename,
        String restrictedID, List<String> ignorePrefixes) throws IOException {

        this.executor = executor;
        this.ignorePrefixes = ignorePrefixes;
        this.restrictedID = restrictedID;
        BufferedReader br = new BufferedReader(new FileReader(inputFilename));
    }

    /**
     * Read ONE query from a string
     *
     * @param line           String to parse for ONE query.
     * @param restrictedID   only parse the query if this connection id matches
     * @param ignorePrefixes do not accept queries which start with these prefixes. May be null if not needed.
     */
    public void parseLine(String line, String restrictedID, List<String> ignorePrefixes) {


        // Break out statement into connectionID, type (Query/Connection/Quit/Init DB), query
        Pattern pattern = Pattern.compile("(?:\\d+\\s+[\\d:]*)?\\s+(\\d+)\\s+(\\w+(?:\\s\\w+)?)\\s+(.*)$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(line);

        // add all matches to the query store
        if (matcher.find()) {
            Integer id = new Integer(matcher.group(1));
            // if restricted to one connection id, create a prefix to match all queries
            if (!Strings.isNullOrEmpty(restrictedID) && id.equals(restrictedID)) {
                return;
            }

            switch (matcher.group(2).toLowerCase()) {
                // include/mysql.h.pp enum_server_command has all the options
                case "connect":
                    // matcher.group(3).left(" ")
                    executor.connect(id, matcher.group(3));
                    break;
                case "query":
                    String query = matcher.group(3);
                    // ignore queries which start with special words
                    for (String prefix : ignorePrefixes) {
                        if (query.toLowerCase().startsWith(prefix.toLowerCase())) {
                            return;
                        }
                    }

                    for(SQLFunc funcPrefix : SQLFunc.values()) {
                        if (!query.toLowerCase().startsWith(funcPrefix.name())) {
                            return;
                        }
                    }
                    executor.query(new Query(parseSQLFunc(query),query));
                    break;
                case "init db":
                    executor.initDb(id, matcher.group(3));
                    break;
                case "quit":
                    executor.quit(id);
                    break;
                default:
                    return;
            }

        }
    }

    private SQLFunc parseSQLFunc(String sql) {
         String[] tokens = sql.split(" ");
        return SQLFunc.valueOf(tokens[0]);
    }

    /**
     * Read sql queries from the given logfile
     *
     * @param offset         number of queries to read in this batch
     * @throws IOException
     */
    public boolean parseLogFile(int offset) throws IOException {
        String line;
        sessionCount = 0;
        // parse the file line by line
        while ((line = br.readLine()) != null && sessionCount<offset) {
            sessionCount++;
            parseLine(line, restrictedID, ignorePrefixes);
        }
        br.close();
        return (offset-sessionCount) == 0;
    }

    public int size() {
        return sessionCount;
    }
}
