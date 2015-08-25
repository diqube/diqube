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
package org.diqube.itest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.control.ServerControl.ServerAddr;
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.ServiceTestUtil;
import org.diqube.itest.util.Waiter;
import org.diqube.remote.base.thrift.RNodeAddress;
import org.diqube.remote.base.thrift.RUUID;
import org.diqube.remote.base.thrift.RValue;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.base.util.RValueUtil;
import org.diqube.server.NewDataWatcher;
import org.diqube.util.Holder;
import org.diqube.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

/**
 * Tests simple deployments of data to a cluster.
 *
 * @author Bastian Gloeckle
 */
public class SimpleServerDeployIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(SimpleServerDeployIntegrationTest.class);

  private static final String AGE_TABLE = "age";
  private static final String AGE_JSON_FILE =
      "/" + SimpleServerDeployIntegrationTest.class.getSimpleName() + "/age.json";
  private static final String AGE_0_CONTROL_FILE = "/" + SimpleServerDeployIntegrationTest.class.getSimpleName()
      + "/age_rowId0" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String AGE_12_CONTROL_FILE = "/" + SimpleServerDeployIntegrationTest.class.getSimpleName()
      + "/age_rowId12" + NewDataWatcher.CONTROL_FILE_EXTENSION;

  @Test
  @NeedsServer(servers = 1)
  public void singleServerTablesServedAndClusterLayout() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));

    // THEN
    // server 0 serves table "age"
    ServiceTestUtil.clusterMgmtService(serverControl.get(0), (cms) -> {
      List<String> tablesServed = Iterables.getOnlyElement(cms.fetchCurrentTablesServed().values());
      Assert.assertEquals(tablesServed, Arrays.asList(AGE_TABLE));
    });

    Holder<Map<ServerAddr, List<String>>> clusterLayoutHolder = new Holder<>();
    ServiceTestUtil.clusterMgmtService(serverControl.get(0),
        (cms) -> clusterLayoutHolder.setValue(toServerAddrCurrentMap(cms.clusterLayout())));

    Assert.assertTrue(clusterLayoutHolder.getValue().containsKey(serverControl.get(0).getAddr()),
        "Expected node to be contained in cluster layout");
    Assert.assertEquals(clusterLayoutHolder.getValue().get(serverControl.get(0).getAddr()), Arrays.asList(AGE_TABLE));
  }

  @Test
  @NeedsServer(servers = 2)
  public void twoServersTablesServedAndClusterLayout() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(1).deploy(cp(AGE_12_CONTROL_FILE), cp(AGE_JSON_FILE));

    // THEN
    // server 0 serves table "age"
    ServiceTestUtil.clusterMgmtService(serverControl.get(0), (cms) -> {
      List<String> tablesServed = Iterables.getOnlyElement(cms.fetchCurrentTablesServed().values());
      Assert.assertEquals(tablesServed, Arrays.asList(AGE_TABLE));
    });

    // get cluster layout
    Holder<Map<ServerAddr, List<String>>> clusterLayoutHolder = new Holder<>();
    ServiceTestUtil.clusterMgmtService(serverControl.get(0),
        (cms) -> clusterLayoutHolder.setValue(toServerAddrCurrentMap(cms.clusterLayout())));

    // server 1 serves table "age"
    ServiceTestUtil.clusterMgmtService(serverControl.get(1), (cms) -> {
      List<String> tablesServed = Iterables.getOnlyElement(cms.fetchCurrentTablesServed().values());
      Assert.assertEquals(tablesServed, Arrays.asList(AGE_TABLE));
    });
    // server 1 has same clusterlayout as server 0
    ServiceTestUtil.clusterMgmtService(serverControl.get(1),
        (cms) -> Assert.assertEquals(toServerAddrCurrentMap(cms.clusterLayout()), clusterLayoutHolder.getValue(),
            "Expected both cluster nodes to have the same cluster layout."));

    // clusterlayout contains both nodes with table "age".
    Assert.assertTrue(clusterLayoutHolder.getValue().containsKey(serverControl.get(0).getAddr()),
        "Expected node to be contained in cluster layout");
    Assert.assertEquals(clusterLayoutHolder.getValue().get(serverControl.get(0).getAddr()), Arrays.asList(AGE_TABLE));
    Assert.assertTrue(clusterLayoutHolder.getValue().containsKey(serverControl.get(1).getAddr()),
        "Expected node to be contained in cluster layout");
    Assert.assertEquals(clusterLayoutHolder.getValue().get(serverControl.get(1).getAddr()), Arrays.asList(AGE_TABLE));
  }

  @Test
  @NeedsServer(servers = 1)
  public void singleServerQuery() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 5L));
      expected.add(new Pair<>(3L, 3L));
      expected.add(new Pair<>(2L, 2L));
      expected.add(new Pair<>(1L, 1L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  @Test
  @NeedsServer(servers = 2)
  public void twoServerQuery() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(1).deploy(cp(AGE_12_CONTROL_FILE), cp(AGE_JSON_FILE));

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 10L));
      expected.add(new Pair<>(3L, 6L));
      expected.add(new Pair<>(2L, 4L));
      expected.add(new Pair<>(1L, 2L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  @Test
  @NeedsServer(servers = 1, manualStart = true)
  public void deployThenStart() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(0).start();
    serverControl.get(0).waitUntilDeployed(cp(AGE_0_CONTROL_FILE));

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 5L));
      expected.add(new Pair<>(3L, 3L));
      expected.add(new Pair<>(2L, 2L));
      expected.add(new Pair<>(1L, 1L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  @Test
  @NeedsServer(servers = 2)
  public void undeployInCluster() throws InterruptedException {
    // WHEN
    serverControl.get(0).deploy(cp(AGE_0_CONTROL_FILE), cp(AGE_JSON_FILE));
    serverControl.get(1).deploy(cp(AGE_12_CONTROL_FILE), cp(AGE_JSON_FILE));

    // THEN results from both cluster nodes
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 10L));
      expected.add(new Pair<>(3L, 6L));
      expected.add(new Pair<>(2L, 4L));
      expected.add(new Pair<>(1L, 2L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }

    // WHEN
    serverControl.get(1).undeploy(cp(AGE_12_CONTROL_FILE));

    // THEN result from one cluster node only
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      RUUID queryUuid = RUuidUtil.toRUuid(UUID.randomUUID());
      logger.info("Executing query {}", RUuidUtil.toUuid(queryUuid));
      ServiceTestUtil.queryService(serverControl.get(0),
          (queryService) -> queryService.asyncExecuteQuery(queryUuid,
              "select age, count() from age group by age order by count() desc", true,
              queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Final result of query received", 10, 500,
          () -> queryRes.check() && queryRes.getFinalUpdate() != null);

      List<Pair<Long, Long>> expected = new ArrayList<>();
      expected.add(new Pair<>(5L, 5L));
      expected.add(new Pair<>(3L, 3L));
      expected.add(new Pair<>(2L, 2L));
      expected.add(new Pair<>(1L, 1L));

      List<Pair<Long, Long>> actual = new ArrayList<>();
      for (List<RValue> row : queryRes.getFinalUpdate().getRows()) {
        Assert.assertEquals(row.size(), 2, "Expected to get correct number of cols returned.");
        actual.add(new Pair<>((Long) RValueUtil.createValue(row.get(0)), (Long) RValueUtil.createValue(row.get(1))));
      }

      Assert.assertEquals(actual, expected, "Expected to get correct results.");
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  private Map<ServerAddr, List<String>> toServerAddrCurrentMap(
      Map<RNodeAddress, Map<Long, List<String>>> clusterLayout) {
    Map<ServerAddr, List<String>> res = new HashMap<>();

    for (Entry<RNodeAddress, Map<Long, List<String>>> e : clusterLayout.entrySet()) {
      ServerAddr addr = new ServerAddr(e.getKey().getDefaultAddr().getHost(), e.getKey().getDefaultAddr().getPort());
      res.put(addr, Iterables.getOnlyElement(e.getValue().values()));
    }

    return res;
  }

}
