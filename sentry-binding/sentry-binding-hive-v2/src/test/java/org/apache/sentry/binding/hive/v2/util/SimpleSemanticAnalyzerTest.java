/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.sentry.binding.hive.v2.util;


import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.hive.ql.plan.HiveOperation.*;

@RunWith(value = Parameterized.class)
public class SimpleSemanticAnalyzerTest {

  private String command;
  private HiveOperation hiveOperation;
  private String expectedDb;
  private String expectedTb;


  public SimpleSemanticAnalyzerTest(HiveOperation hiveOperation, String command, String expectedDb, String expectedTb) {
    this.command = command;
    this.hiveOperation = hiveOperation;
    this.expectedDb = expectedDb;
    this.expectedTb = expectedTb;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {CREATETABLE, "CREATE TABLE test_db.test_table(id INT)", "test_db", "test_table"},
        {CREATETABLE, "CREATE TABLE `test_db`.`test_table`(id INT)", "test_db", "test_table"},
        {CREATETABLE, "CREATE TABLE test_db.`test_table`(id INT)", "test_db", "test_table"},
        {CREATETABLE, "CREATE TABLE `test_db`.test_table(id INT)", "test_db", "test_table"},
        {CREATETABLE, "CREATE TABLE `test_db.test_table`(id INT)", "test_db", "test_table"},
        {CREATETABLE, "CREATE TABLE test_table(id INT)", "test_db", "test_table"},
        {CREATETABLE, "CREATE TABLE `test_table`(id INT)", "test_db", "test_table"},

        {DROPDATABASE, "DROP DATABASE test_db", "test_db", null},
        {DROPDATABASE, "DROP DATABASE `test_db`", "test_db", null},

        {DESCDATABASE, "DESCRIBE DATABASE test_db", "test_db", null},
        {DESCDATABASE, "DESCRIBE DATABASE `test_db`", "test_db", null},

        {LOCKDB, "LOCK DATABASE test_db", "test_db", null},
        {LOCKDB, "LOCK DATABASE `test_db`", "test_db", null},

        {UNLOCKDB, "UNLOCK DATABASE test_db", "test_db", null},
        {UNLOCKDB, "UNLOCK DATABASE `test_db`", "test_db", null},

        {DROPTABLE, "DROP TABLE test_db.test_table", "test_db", "test_table"},
        {DROPTABLE, "DROP TABLE `test_db`.`test_table`", "test_db", "test_table"},
        {DROPTABLE, "DROP TABLE test_db.`test_table`", "test_db", "test_table"},
        {DROPTABLE, "DROP TABLE `test_db`.test_table", "test_db", "test_table"},
        {DROPTABLE, "DROP TABLE `test_db.test_table`", "test_db", "test_table"},
        {DROPTABLE, "DROP TABLE `test_table`", "test_db", "test_table"},
        {DROPTABLE, "DROP TABLE test_table", "test_db", "test_table"},

        {TRUNCATETABLE, "TRUNCATE TABLE test_db.test_table", "test_db", "test_table"},
        {TRUNCATETABLE, "TRUNCATE TABLE `test_db`.`test_table`", "test_db", "test_table"},
        {TRUNCATETABLE, "TRUNCATE TABLE `test_db`.test_table", "test_db", "test_table"},
        {TRUNCATETABLE, "TRUNCATE TABLE test_db.`test_table`", "test_db", "test_table"},
        {TRUNCATETABLE, "TRUNCATE TABLE `test_db.test_table`", "test_db", "test_table"},
        {TRUNCATETABLE, "TRUNCATE TABLE `test_table`", "test_db", "test_table"},
        {TRUNCATETABLE, "TRUNCATE TABLE test_table", "test_db", "test_table"},

        {LOCKTABLE, "LOCK TABLE test_db.test_table", "test_db", "test_table"},
        {LOCKTABLE, "LOCK TABLE `test_db`.`test_table`", "test_db", "test_table"},
        {LOCKTABLE, "LOCK TABLE `test_db`.test_table", "test_db", "test_table"},
        {LOCKTABLE, "LOCK TABLE test_db.`test_table`", "test_db", "test_table"},
        {LOCKTABLE, "LOCK TABLE `test_db.test_table`", "test_db", "test_table"},
        {LOCKTABLE, "LOCK TABLE `test_table`", "test_db", "test_table"},
        {LOCKTABLE, "LOCK TABLE test_table", "test_db", "test_table"},

        {UNLOCKTABLE, "UNLOCK TABLE test_db.test_table", "test_db", "test_table"},
        {UNLOCKTABLE, "UNLOCK TABLE `test_db`.`test_table`", "test_db", "test_table"},
        {UNLOCKTABLE, "UNLOCK TABLE `test_db`.test_table", "test_db", "test_table"},
        {UNLOCKTABLE, "UNLOCK TABLE test_db.`test_table`", "test_db", "test_table"},
        {UNLOCKTABLE, "UNLOCK TABLE `test_db.test_table`", "test_db", "test_table"},
        {UNLOCKTABLE, "UNLOCK TABLE `test_table`", "test_db", "test_table"},
        {UNLOCKTABLE, "UNLOCK TABLE test_table", "test_db", "test_table"},

        {LOAD, "LOAD DATA INPATH '/path/to/file' INTO TABLE test_db.test_table", "test_db", "test_table"},
        {LOAD, "LOAD DATA INPATH '/path/to/file' INTO TABLE `test_db`.`test_table`", "test_db", "test_table"},
        {LOAD, "LOAD DATA INPATH '/path/to/file' INTO TABLE `test_db`.test_table", "test_db", "test_table"},
        {LOAD, "LOAD DATA INPATH '/path/to/file' INTO TABLE test_db.`test_table`", "test_db", "test_table"},
        {LOAD, "LOAD DATA INPATH '/path/to/file' INTO TABLE `test_db.test_table`", "test_db", "test_table"},
        {LOAD, "LOAD DATA INPATH '/path/to/file' INTO TABLE `test_table`", "test_db", "test_table"},
        {LOAD, "LOAD DATA INPATH '/path/to/file' INTO TABLE test_table", "test_db", "test_table"},

        {SHOWCOLUMNS, "SHOW COLUMNS FROM test_db.test_table", "test_db", "test_table"},
        {SHOWCOLUMNS, "SHOW COLUMNS FROM `test_db`.`test_table`", "test_db", "test_table"},
        {SHOWCOLUMNS, "SHOW COLUMNS FROM `test_db`.test_table", "test_db", "test_table"},
        {SHOWCOLUMNS, "SHOW COLUMNS FROM test_db.`test_table`", "test_db", "test_table"},
        {SHOWCOLUMNS, "SHOW COLUMNS FROM `test_db.test_table`", "test_db", "test_table"},
        {SHOWCOLUMNS, "SHOW COLUMNS FROM `test_table`", "test_db", "test_table"},
        {SHOWCOLUMNS, "SHOW COLUMNS FROM test_table", "test_db", "test_table"},

        {SHOW_TABLESTATUS, "SHOW TABLE EXTENDED IN test_db LIKE test_table", "test_db", "test_table"},
        {SHOW_TABLESTATUS, "SHOW TABLE EXTENDED IN `test_db` LIKE `test_table`", "test_db", "test_table"},
        {SHOW_TABLESTATUS, "SHOW TABLE EXTENDED IN test_db LIKE `test_table`", "test_db", "test_table"},

        {MSCK, "MSCK REPAIR TABLE test_db.test_table", "test_db", "test_table"},
        {MSCK, "MSCK REPAIR TABLE `test_db`.`test_table`", "test_db", "test_table"},
        {MSCK, "MSCK REPAIR TABLE `test_db`.test_table", "test_db", "test_table"},
        {MSCK, "MSCK REPAIR TABLE test_db.`test_table`", "test_db", "test_table"},
        {MSCK, "MSCK REPAIR TABLE `test_db.test_table`", "test_db", "test_table"},
        {MSCK, "MSCK REPAIR TABLE `test_table`", "test_db", "test_table"},
        {MSCK, "MSCK REPAIR TABLE test_table", "test_db", "test_table"},

        {ALTERTABLE_ADDCOLS, "ALTER TABLE test_db.test_table", "test_db", "test_table"},
        {ALTERTABLE_ADDCOLS, "ALTER TABLE `test_db`.`test_table`", "test_db", "test_table"},
        {ALTERTABLE_ADDCOLS, "ALTER TABLE `test_db`.test_table", "test_db", "test_table"},
        {ALTERTABLE_ADDCOLS, "ALTER TABLE test_db.`test_table`", "test_db", "test_table"},
        {ALTERTABLE_ADDCOLS, "ALTER TABLE `test_db.test_table`", "test_db", "test_table"},
        {ALTERTABLE_ADDCOLS, "ALTER TABLE `test_table`", "test_db", "test_table"},
        {ALTERTABLE_ADDCOLS, "ALTER TABLE test_table", "test_db", "test_table"},

        {DESCTABLE, "DESCRIBE test_db.test_table", "test_db", "test_table"},
        {DESCTABLE, "DESCRIBE `test_db`.`test_table`", "test_db", "test_table"},
        {DESCTABLE, "DESCRIBE `test_db`.test_table", "test_db", "test_table"},
        {DESCTABLE, "DESCRIBE test_db.`test_table`", "test_db", "test_table"},
        {DESCTABLE, "DESCRIBE `test_db.test_table`", "test_db", "test_table"},
        {DESCTABLE, "DESCRIBE test_table", "test_db", "test_table"},
        {DESCTABLE, "DESCRIBE `test_table`", "test_db", "test_table"},

        {DROPVIEW, "DROP VIEW test_db.test_table", "test_db", "test_table"},
        {DROPVIEW, "DROP VIEW `test_db`.`test_table`", "test_db", "test_table"},
        {DROPVIEW, "DROP VIEW `test_db`.test_table", "test_db", "test_table"},
        {DROPVIEW, "DROP VIEW test_db.`test_table`", "test_db", "test_table"},
        {DROPVIEW, "DROP VIEW `test_db.test_table`", "test_db", "test_table"},
        {DROPVIEW, "DROP VIEW test_table", "test_db", "test_table"},
        {DROPVIEW, "DROP VIEW `test_table`", "test_db", "test_table"},

        {ALTERVIEW_PROPERTIES, "ALTER VIEW test_db.test_table", "test_db", "test_table"},
        {ALTERVIEW_PROPERTIES, "ALTER VIEW `test_db`.`test_table`", "test_db", "test_table"},
        {ALTERVIEW_PROPERTIES, "ALTER VIEW `test_db`.test_table", "test_db", "test_table"},
        {ALTERVIEW_PROPERTIES, "ALTER VIEW test_db.`test_table`", "test_db", "test_table"},
        {ALTERVIEW_PROPERTIES, "ALTER VIEW `test_db.test_table`", "test_db", "test_table"},
        {ALTERVIEW_PROPERTIES, "ALTER VIEW test_table", "test_db", "test_table"},
        {ALTERVIEW_PROPERTIES, "ALTER VIEW `test_table`", "test_db", "test_table"},

        {SHOWINDEXES, "SHOW INDEX ON test_db.test_table", "test_db", "test_table"},
        {SHOWINDEXES, "SHOW INDEX ON `test_db`.`test_table`", "test_db", "test_table"},
        {SHOWINDEXES, "SHOW INDEX ON `test_db`.test_table", "test_db", "test_table"},
        {SHOWINDEXES, "SHOW INDEX ON test_db.`test_table`", "test_db", "test_table"},
        {SHOWINDEXES, "SHOW INDEX ON `test_db.test_table`", "test_db", "test_table"},
        {SHOWINDEXES, "SHOW INDEX ON `test_table`", "test_db", "test_table"},
        {SHOWINDEXES, "SHOW INDEX ON test_table", "test_db", "test_table"},

        {SHOW_TBLPROPERTIES, "SHOW TBLPROPERTIES test_db.test_table", "test_db", "test_table"},
        {SHOW_TBLPROPERTIES, "SHOW TBLPROPERTIES `test_db`.`test_table`", "test_db", "test_table"},
        {SHOW_TBLPROPERTIES, "SHOW TBLPROPERTIES `test_db`.test_table", "test_db", "test_table"},
        {SHOW_TBLPROPERTIES, "SHOW TBLPROPERTIES test_db.`test_table`", "test_db", "test_table"},
        {SHOW_TBLPROPERTIES, "SHOW TBLPROPERTIES `test_db.test_table`", "test_db", "test_table"},
        {SHOW_TBLPROPERTIES, "SHOW TBLPROPERTIES `test_table`", "test_db", "test_table"},
        {SHOW_TBLPROPERTIES, "SHOW TBLPROPERTIES test_table", "test_db", "test_table"},

        {ALTERINDEX_PROPS, "ALTER INDEX test_index ON test_db.test_table", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX test_index ON `test_db`.`test_table`", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX test_index ON `test_db`.test_table", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX test_index ON test_db.`test_table`", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX test_index ON `test_db.test_table`", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX test_index ON `test_table`", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX test_index ON test_table", "test_db", "test_table"},

        {ALTERINDEX_PROPS, "ALTER INDEX `test_index` ON test_db.test_table", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX `test_index` ON `test_db`.`test_table`", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX `test_index` ON `test_db`.test_table", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX `test_index` ON test_db.`test_table`", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX `test_index` ON `test_db.test_table`", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX `test_index` ON `test_table`", "test_db", "test_table"},
        {ALTERINDEX_PROPS, "ALTER INDEX `test_index` ON test_table", "test_db", "test_table"},

        {CREATEFUNCTION, "CREATE FUNCTION test_db.test_function AS 'test_db.test_function USING 'com.my.package'", "test_db", "test_db.test_function"},
        {CREATEFUNCTION, "CREATE FUNCTION `test_db`.test_function AS 'test_db.test_function USING 'com.my.package'", "test_db", "test_db.test_function"},
        {CREATEFUNCTION, "CREATE FUNCTION `test_db`.`test_function` AS 'test_db.test_function USING 'com.my.package'", "test_db", "test_db.test_function"},
        {CREATEFUNCTION, "CREATE FUNCTION test_db.`test_function` AS 'test_db.test_function USING 'com.my.package'", "test_db", "test_db.test_function"},
        {CREATEFUNCTION, "CREATE FUNCTION `test_db.test_function` AS 'test_db.test_function USING 'com.my.package'", "test_db", "test_db.test_function"},
        {CREATEFUNCTION, "CREATE FUNCTION test_function AS 'test_db.test_function USING 'com.my.package'", "test_db", "test_db.test_function"},
        {CREATEFUNCTION, "CREATE FUNCTION `test_function` AS 'test_db.test_function USING 'com.my.package'", "test_db", "test_db.test_function"},
    });
  }

  @Test
  public void parserTest() throws HiveAuthzPluginException {
    SimpleSemanticAnalyzer ssa = new SimpleSemanticAnalyzerMock(hiveOperation, command);
    String currentDbActual = ssa.getCurrentDb();
    String currentTableActual = ssa.getCurrentTb();
    Assert.assertEquals(expectedDb, currentDbActual);
    Assert.assertEquals(expectedTb, currentTableActual);
  }
}
