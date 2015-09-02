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

    // Break out statement into datetime(not captured), connectionID, type (Query/Connection/Quit/Init DB), query
    // 150811  6:47:54 940899 Connect  user@192.230.152.49 as anonymous on
    //                 940899 Query    call  mysql.setdomain(current_user())
    //                 940899 Init DB  wiki
    //                 940899 Query    SET /* Database::open  */ sql_mode = ''
    //                 940899 Query    BEGIN
    //                 940899 Query    SELECT /* checkLastModified  */  MAX(rc_timestamp)  FROM `recentchanges`   LIMIT 1
    static Pattern pattern = Pattern.compile("(?:\\d+\\s+[\\d:]*)?\\s+(\\d+)\\s+(\\w+(?:\\s\\w+)?)\\s+(.*)$", Pattern.CASE_INSENSITIVE);

    public QueryParser(SQLStatementExecutor executor, String inputFilename,
        String restrictedID, List<String> ignorePrefixes) throws IOException {

        this.executor = executor;
        this.ignorePrefixes = ignorePrefixes;
        this.restrictedID = restrictedID;
        br = new BufferedReader(new FileReader(inputFilename));
    }

    public void close() throws IOException {
        br.close();
    }
    /**
     * Read ONE query from a string
     *
     * @param restrictedID   only parse the query if this connection id matches
     * @param ignorePrefixes do not accept queries which start with these prefixes. May be null if not needed.
     * @return false if no more lines in br
     */
    public boolean parseLine(String restrictedID, List<String> ignorePrefixes) throws IOException {

        String line = br.readLine();
        if (line == null) {
            return false;
        }
        Matcher matcher = pattern.matcher(line);

        // add all matches to the query store
        if (matcher.find()) {
            String id = matcher.group(1);
            // if restricted to one connection id, create a prefix to match all queries
            if (!Strings.isNullOrEmpty(restrictedID) && id.equals(restrictedID)) {
                return true;
            }

            String type =matcher.group(2).toLowerCase();
                // include/mysql.h.pp enum_server_command has all the options
            if (type.equals("connect")) {
                // matcher.group(3).left(" ")
                executor.connect(id, matcher.group(3));
            } else if (type.equals("query")) {
                String query = matcher.group(3);
                boolean ignore = false;
                // ignore queries which start with special words
                for (String prefix : ignorePrefixes) {
                    if (query.toLowerCase().startsWith(prefix.toLowerCase())) {
                        ignore = true;
                        break;
                    }
                }

                for(SQLFunc funcPrefix : SQLFunc.values()) {
                    if (!query.toLowerCase().startsWith(funcPrefix.name())) {
                        ignore = true;
                        break;
                    }
                }
                // queries can be multiple lines.
                while (true) {
                    // TODO should be max packet size (from where the query log was taken) as this is the max query length
                    br.mark(200000);
                    line = br.readLine();
                    if (line == null) {
                        return false;
                    }
                    matcher = pattern.matcher(line);
                    if (matcher.matches()) {
                        br.reset(); // revert to previous mark so next query can begin there.
                        break;
                    } else {
                       query += line;
                    }
                }
                if (!ignore) {
                    executor.query(id, new Query(parseSQLFunc(query), query));
                }
            } else if (type.equals("init db")) {
                executor.initDb(id, matcher.group(3));
            } else if (type.equals("quit")) {
                executor.quit(id);
            }

        }
        return true;
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
        sessionCount = 0;
        // parse the file line by line
        while (sessionCount<offset) {
            sessionCount++;
            if (!parseLine(restrictedID, ignorePrefixes))
                break;
        }
        return (offset-sessionCount) == 0;
    }

    public int size() {
        return sessionCount;
    }

}
