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
 */
public class QueryParser {
    /**
     * The parsed index.
     */
    private int index = 0;

    private List<Query> queries = new ArrayList<Query>(1000);

    /**
     * All parsed queries.
     *
     * @return a list of queries
     */
    public List<Query> getQueries() {
        return queries;
    }

    /**
     * Read ONE query from a string
     *
     * @param line           String to parse for ONE query.
     * @param restrictedID   only parse the query if this connection id matches
     * @param ignorePrefixes do not accept queries which start with these prefixes. May be null if not needed.
     */
    public void parseLine(String line, String restrictedID, List<String> ignorePrefixes) {

        // if restricted to one connection id, create a prefix to match all queries
        String prefixPattern = Strings.isNullOrEmpty(restrictedID) ? "\\d+" : restrictedID.toLowerCase();

        // match all statements beginning with 'query' and the prefixPattern
        Pattern pattern = Pattern.compile("[\\s\\d:]*\\s+" + prefixPattern + "\\s+query\\s+(.*)$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(line);

        // add all matches to the query store
        if (matcher.find()) {

            // ignore queries which start with special words
            for (String prefix : ignorePrefixes) {
                if (matcher.group(1).toLowerCase().startsWith(prefix.toLowerCase())) {
                    return;
                }
            }

            for(SQLFunc funcPrefix : SQLFunc.values()) {
                if (!matcher.group(1).toLowerCase().startsWith(funcPrefix.name())) {
                    return;
                }
            }
            Query query = new Query(parseSQLFunc(matcher.group(1)),matcher.group(1));
            queries.add(query);
        }
    }

    private SQLFunc parseSQLFunc(String sql) {
         String[] tokens = sql.split(" ");
        return SQLFunc.valueOf(tokens[0]);
    }

    /**
     * Read sql queries from the given logfile
     *
     * @param inputFilename  input file
     * @param restrictedID   query ids to ignore
     * @param ignorePrefixes do not accept queries which start with these prefixes. May be null if not needed.
     * @throws IOException
     */
    public boolean parseLogFile(String inputFilename, String restrictedID, List<String> ignorePrefixes, int offset) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(inputFilename));
        String line;
        this.queries.clear();
        int sessionCount = 0;
        // parse the file line by line
        while ((line = br.readLine()) != null && sessionCount<offset) {
            index++;
            sessionCount++;
            parseLine(line, restrictedID, ignorePrefixes);
        }
        br.close();
        return (offset-sessionCount) == 0;
    }
}
