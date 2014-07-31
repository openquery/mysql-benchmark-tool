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

import de.qaware.mysqlbenchmark.func.SQLFunc;
import de.qaware.mysqlbenchmark.func.SQLType;


/**
 * SQL Query Operation. Wrap Sql functions and the raw SQL.
 * @author home3k (home3k@gmail.com)
 */public class Query {

    private SQLFunc func;
    private String sql;

    public Query(SQLFunc func, String sql) {
        this.func = func;
        this.sql = sql;
    }

    public SQLType getType () {
        if (this.func == null)
            throw new IllegalStateException("Bad State!");
        if (this.func==SQLFunc.select) {
            return  SQLType.read;
        } else {
            return SQLType.write;
        }
    }

    public SQLFunc getFunc() {
        return func;
    }

    public void setFunc(SQLFunc func) {
        this.func = func;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
