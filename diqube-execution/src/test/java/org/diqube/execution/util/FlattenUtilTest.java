/**
 * diqube: Distributed Query Base.
 *
 * Copyright (C) 2015 Bastian Gloeckle
 *
 * This file is part of diqube.
 *
 * diqube is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.diqube.execution.util;

import java.nio.charset.Charset;
import java.util.Arrays;

import org.diqube.context.Profiles;
import org.diqube.data.column.ColumnType;
import org.diqube.data.flatten.FlattenedTable;
import org.diqube.data.table.Table;
import org.diqube.data.table.TableFactory;
import org.diqube.data.table.TableShard;
import org.diqube.loader.JsonLoader;
import org.diqube.loader.LoadException;
import org.diqube.loader.LoaderColumnInfo;
import org.diqube.util.BigByteBuffer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests {@link FlattenUtil}.
 *
 * @author Bastian Gloeckle
 */
public class FlattenUtilTest {
  private static final String TABLE = "table";

  private AnnotationConfigApplicationContext dataContext;
  private JsonLoader loader;
  private TableFactory tableFactory;

  private FlattenUtil flattenUtil;

  @BeforeMethod
  public void before() {
    dataContext = new AnnotationConfigApplicationContext();
    dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
    dataContext.scan("org.diqube");
    dataContext.refresh();

    loader = dataContext.getBean(JsonLoader.class);
    tableFactory = dataContext.getBean(TableFactory.class);
    flattenUtil = dataContext.getBean(FlattenUtil.class);
  }

  @AfterMethod
  public void after() {
    dataContext.close();
  }

  @Test(enabled = false)
  public void simpleTest() throws LoadException {
    // GIVEN
    Table t = loadFromJson("[ { " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": 1, \"d\":[99, 100] }, "//
    /* */ + "{ \"b\": 2 }"//
        + "]" + //
        ",\"c\" : [ 9, 10 ] }," //
    //
        + "{ " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": 3, \"d\":[300,301,302] } "//
        + "]" + ",\"c\" : [ 0 ]}" + " ]");

    // WHEN
    FlattenedTable flattenedTable = flattenUtil.flattenTable(t, "a[*]");

    System.out.println(flattenedTable);
    // THEN
  }

  @Test(enabled = false)
  public void repeatedMiddleMissingTest() throws LoadException {
    // GIVEN
    Table t = loadFromJson("[ { " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": [ " //
    /*   */ + "{ \"c\": 1 }," //
    /*   */ + "{ \"d\": 1 }," //
    /*   */ + "{ \"c\": 1 }" //
    /* */ + "] }, "//
    /* */ + "{ \"b\": [ " //
    /*   */ + "{ \"c\": 2 }," //
    /*   */ + "{ \"d\": 2 }," //
    /*   */ + "{ \"c\": 2 }" //
    /* */ + "] } "//
        + "]" + //
        "},"
        //
        + "{ " //
        + "\"a\": [ "//
    /* */ + "{ \"b\": [ " //
    /*   */ + "{ \"c\": 1 }," //
    /*   */ + "{ \"d\": 1 }," //
    /*   */ + "{ \"c\": 1 }" //
    /* */ + "] }, "//
    /* */ + "{ \"b\": [ " //
    /*   */ + "{ \"c\": 2 }," //
    /*   */ + "{ \"d\": 2 }," //
    /*   */ + "{ \"c\": 2 }" //
    /* */ + "] } "//
        + "]" + //
        "}" + " ]");

    // WHEN
    FlattenedTable flattenedTable = flattenUtil.flattenTable(t, "a[*].b[*]");

    System.out.println(flattenedTable);
    // THEN
  }

  private Table loadFromJson(String json) throws LoadException {
    BigByteBuffer jsonBuffer = new BigByteBuffer(json.getBytes(Charset.forName("UTF-8")));
    TableShard shard = loader.load(0, jsonBuffer, TABLE, new LoaderColumnInfo(ColumnType.LONG)).iterator().next();
    return tableFactory.createDefaultTable(TABLE, Arrays.asList(shard));
  }
}
