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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsServer;
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
 * A integration test where a server of the cluster dies.
 *
 * @author Bastian Gloeckle
 */
public class DeadServerIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(DeadServerIntegrationTest.class);

  private static final String DOUBLEVAL_TABLE = "doubleVal";
  private static final String DOUBLEVAL_JSON_FILE =
      "/" + DeadServerIntegrationTest.class.getSimpleName() + "/doubleVal.json";
  private static final String DOUBLEVAL_0_CONTROL_FILE = "/" + DeadServerIntegrationTest.class.getSimpleName()
      + "/doubleVal_rowId0" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String DOUBLEVAL_10_CONTROL_FILE = "/" + DeadServerIntegrationTest.class.getSimpleName()
      + "/doubleVal_rowId10" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String DOUBLEVAL_20_CONTROL_FILE = "/" + DeadServerIntegrationTest.class.getSimpleName()
      + "/doubleVal_rowId20" + NewDataWatcher.CONTROL_FILE_EXTENSION;

  @Test
  @NeedsServer(servers = 3)
  public void queryDiesQueryResurrectsQuery() {
    serverControl.get(0).deploy(cp(DOUBLEVAL_0_CONTROL_FILE), cp(DOUBLEVAL_JSON_FILE));
    serverControl.get(1).deploy(cp(DOUBLEVAL_10_CONTROL_FILE), cp(DOUBLEVAL_JSON_FILE));
    serverControl.get(2).deploy(cp(DOUBLEVAL_20_CONTROL_FILE), cp(DOUBLEVAL_JSON_FILE));

    // THEN receive results from all three remotes
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select v, count() from " + DOUBLEVAL_TABLE + " group by v order by count() desc, v desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Double, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(3915.8329, 3L));
      expected.add(new Pair<>(3797.0344, 3L));
      expected.add(new Pair<>(3686.7414, 3L));
      expected.add(new Pair<>(3464.2484, 3L));
      expected.add(new Pair<>(2863.5864, 3L));
      expected.add(new Pair<>(2230.9298, 3L));
      expected.add(new Pair<>(1746.0391, 3L));
      expected.add(new Pair<>(1697.8916, 3L));
      expected.add(new Pair<>(1333.4152, 3L));
      expected.add(new Pair<>(1010.2351, 3L));

      List<Pair<Double, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Double) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }

    // WHEN one server stops
    serverControl.get(1).stop();

    // THEN receive results from remaining two remotes
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select v, count() from " + DOUBLEVAL_TABLE + " group by v order by count() desc, v desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 60, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Double, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(3915.8329, 2L));
      expected.add(new Pair<>(3797.0344, 2L));
      expected.add(new Pair<>(3686.7414, 2L));
      expected.add(new Pair<>(3464.2484, 2L));
      expected.add(new Pair<>(2863.5864, 2L));
      expected.add(new Pair<>(2230.9298, 2L));
      expected.add(new Pair<>(1746.0391, 2L));
      expected.add(new Pair<>(1697.8916, 2L));
      expected.add(new Pair<>(1333.4152, 2L));
      expected.add(new Pair<>(1010.2351, 2L));

      List<Pair<Double, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Double) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }

    // WHEN the server re-starts
    serverControl.get(1).start();
    serverControl.get(1).waitUntilDeployed(cp(DOUBLEVAL_10_CONTROL_FILE));

    // THEN receive results from all three remotes again
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select v, count() from " + DOUBLEVAL_TABLE + " group by v order by count() desc, v desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Double, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(3915.8329, 3L));
      expected.add(new Pair<>(3797.0344, 3L));
      expected.add(new Pair<>(3686.7414, 3L));
      expected.add(new Pair<>(3464.2484, 3L));
      expected.add(new Pair<>(2863.5864, 3L));
      expected.add(new Pair<>(2230.9298, 3L));
      expected.add(new Pair<>(1746.0391, 3L));
      expected.add(new Pair<>(1697.8916, 3L));
      expected.add(new Pair<>(1333.4152, 3L));
      expected.add(new Pair<>(1010.2351, 3L));

      List<Pair<Double, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Double) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }
}
