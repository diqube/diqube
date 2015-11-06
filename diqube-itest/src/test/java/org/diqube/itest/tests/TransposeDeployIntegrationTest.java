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
package org.diqube.itest.tests;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.diqube.function.aggregate.ConcatGroupFunction;
import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.control.ToolControl;
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.ServiceTestUtil;
import org.diqube.itest.util.Waiter;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.base.util.RValueUtil;
import org.diqube.server.NewDataWatcher;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Integration test which first transposes some data and then imports that data into a server.
 *
 * @author Bastian Gloeckle
 */
public class TransposeDeployIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(TransposeDeployIntegrationTest.class);

  private static final String LOREM_JSON_TABLE = "loremJson";
  private static final String LOREM_DIQUBE_TABLE = "loremDiqube";
  private static final String LOREM_JSON_FILE =
      "/" + TransposeDeployIntegrationTest.class.getSimpleName() + "/lorem.json";
  private static final String LOREM_JSON_CONTROL_FILE =
      "/" + TransposeDeployIntegrationTest.class.getSimpleName() + "/loremJson" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String LOREM_DIQUBE_CONTROL_FILE = "/" + TransposeDeployIntegrationTest.class.getSimpleName()
      + "/loremDiqube" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String LOREM_DIQUBE_FILE = "lorem.diqube";

  private static final String DOUBLEVAL_JSON_TABLE = "doubleValJson";
  private static final String DOUBLEVAL_DIQUBE_TABLE = "doubleValDiqube";
  private static final String DOUBLEVAL_JSON_FILE =
      "/" + TransposeDeployIntegrationTest.class.getSimpleName() + "/doubleVal.json";
  private static final String DOUBLEVAL_JSON_CONTROL_FILE = "/" + TransposeDeployIntegrationTest.class.getSimpleName()
      + "/doubleValJson" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String DOUBLEVAL_DIQUBE_CONTROL_FILE = "/" + TransposeDeployIntegrationTest.class.getSimpleName()
      + "/doubleValDiqube" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String DOUBLEVAL_DIQUBE_FILE = "doubleVal.diqube";

  private static final String AGE_JSON_TABLE = "ageJson";
  private static final String AGE_DIQUBE_TABLE = "ageDiqube";
  private static final String AGE_JSON_FILE = "/" + TransposeDeployIntegrationTest.class.getSimpleName() + "/age.json";
  private static final String AGE_JSON_CONTROL_FILE =
      "/" + TransposeDeployIntegrationTest.class.getSimpleName() + "/ageJson" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String AGE_DIQUBE_CONTROL_FILE =
      "/" + TransposeDeployIntegrationTest.class.getSimpleName() + "/ageDiqube" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String AGE_DIQUBE_FILE = "age.diqube";

  @Test
  @NeedsServer(servers = 1)
  public void stringTransposeDeploy() throws IOException {
    // GIVEN
    // transposed file
    toolControl.transpose(cp(LOREM_JSON_FILE), ToolControl.TYPE_JSON, work(LOREM_DIQUBE_FILE));

    // deploy both non-transposed and transposed data
    serverControl.get(0).deploy(cp(LOREM_JSON_CONTROL_FILE), cp(LOREM_JSON_FILE));
    serverControl.get(0).deploy(cp(LOREM_DIQUBE_CONTROL_FILE), work(LOREM_DIQUBE_FILE));

    Set<Pair<String, Set<String>>> expected = new HashSet<>();
    expected.add(new Pair<>("aute", new HashSet<>(Arrays.asList("eiusmod", "aliquip", "ex"))));
    expected.add(new Pair<>("dolor", new HashSet<>(Arrays.asList("cupidatat", "esse", "proident"))));
    expected.add(new Pair<>("voluptate", new HashSet<>(Arrays.asList("elit", "in", "magna"))));
    expected.add(new Pair<>("sunt", new HashSet<>(Arrays.asList("anim", "non", "cillum"))));
    expected.add(new Pair<>("elit", new HashSet<>(Arrays.asList("Lorem", "elit", "deserunt"))));
    expected.add(new Pair<>("do", new HashSet<>(Arrays.asList("aliquip", "do", "commodo"))));
    expected.add(new Pair<>("mollit", new HashSet<>(Arrays.asList("pariatur", "incididunt", "cillum"))));

    Set<Pair<String, Set<String>>> jsonResult = queryLoremResults(LOREM_JSON_TABLE);
    Assert.assertEquals(jsonResult, expected);

    Set<Pair<String, Set<String>>> diqubeResult = queryLoremResults(LOREM_DIQUBE_TABLE);
    Assert.assertEquals(diqubeResult, expected);
  }

  private Set<Pair<String, Set<String>>> queryLoremResults(String tableName) throws IOException {
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0), (queryService) -> queryService.asyncExecuteQuery(queryUuid,
          "select a, concatgroup(b[*].c) from " + tableName, true, queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      Set<Pair<String, Set<String>>> result = new HashSet<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        result.add(new Pair<>((String) RValueUtil.createValue(row.get(0)), new HashSet<>(Arrays
            .asList(((String) RValueUtil.createValue(row.get(1))).split(ConcatGroupFunction.DEFAULT_DELIMITER)))));
      }
      return result;
    }
  }

  @Test
  @NeedsServer(servers = 1)
  public void doubleTransposeDeploy() throws IOException {
    // GIVEN
    // transposed file
    toolControl.transpose(cp(DOUBLEVAL_JSON_FILE), ToolControl.TYPE_JSON, work(DOUBLEVAL_DIQUBE_FILE));

    // deploy both non-transposed and transposed data
    serverControl.get(0).deploy(cp(DOUBLEVAL_JSON_CONTROL_FILE), cp(DOUBLEVAL_JSON_FILE));
    serverControl.get(0).deploy(cp(DOUBLEVAL_DIQUBE_CONTROL_FILE), work(DOUBLEVAL_DIQUBE_FILE));

    Set<Double> expected = new HashSet<>();
    expected.add(3915.8329);
    expected.add(3797.0344);
    expected.add(3686.7414);
    expected.add(3464.2484);
    expected.add(2863.5864);
    expected.add(2230.9298);
    expected.add(1746.0391);
    expected.add(1697.8916);
    expected.add(1333.4152);
    expected.add(1010.2351);

    Set<Double> jsonResult = queryDoubleResults(DOUBLEVAL_JSON_TABLE);
    Assert.assertEquals(jsonResult, expected);

    Set<Double> diqubeResult = queryDoubleResults(DOUBLEVAL_DIQUBE_TABLE);
    Assert.assertEquals(diqubeResult, expected);
  }

  private Set<Double> queryDoubleResults(String tableName) throws IOException {
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0), (queryService) -> queryService.asyncExecuteQuery(queryUuid,
          "select v from " + tableName, true, queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      Set<Double> result = new HashSet<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 1, "Expected to get correct number of cols returned.");
        result.add((Double) RValueUtil.createValue(row.get(0)));
      }
      return result;
    }
  }

  @Test
  @NeedsServer(servers = 1)
  public void longTransposeDeploy() throws IOException {
    // GIVEN
    // transposed file
    toolControl.transpose(cp(AGE_JSON_FILE), ToolControl.TYPE_JSON, work(AGE_DIQUBE_FILE));

    // deploy both non-transposed and transposed data
    serverControl.get(0).deploy(cp(AGE_JSON_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(0).deploy(cp(AGE_DIQUBE_CONTROL_FILE), work(AGE_DIQUBE_FILE));

    Set<Pair<Long, Long>> expected = new HashSet<>();
    expected.add(new Pair<>(5L, 5L));
    expected.add(new Pair<>(3L, 3L));
    expected.add(new Pair<>(2L, 2L));
    expected.add(new Pair<>(1L, 1L));

    Set<Pair<Long, Long>> jsonResult = queryLongResults(AGE_JSON_TABLE);
    Assert.assertEquals(jsonResult, expected);

    Set<Pair<Long, Long>> diqubeResult = queryLongResults(AGE_DIQUBE_TABLE);
    Assert.assertEquals(diqubeResult, expected);
  }

  private Set<Pair<Long, Long>> queryLongResults(String tableName) throws IOException {
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select age, count() from " + tableName + " group by age", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      Set<Pair<Long, Long>> result = new HashSet<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        result.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }
      return result;
    }
  }
}
