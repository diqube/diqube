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
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.diqube.config.ConfigKey;
import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.util.ClusterFlattenServiceTestUtil;
import org.diqube.itest.util.ClusterFlattenServiceTestUtil.TestClusterFlattenService;
import org.diqube.itest.util.ServiceTestUtil;
import org.diqube.itest.util.TestDataGenerator;
import org.diqube.itest.util.Waiter;
import org.diqube.remote.base.util.RUuidUtil;
import org.diqube.remote.cluster.thrift.ClusterFlattenService;
import org.diqube.server.NewDataWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Integration test that interacts with {@link ClusterFlattenService}s on servers (= directly talks to "query remotes"
 * !). It tests wether requests are merged correctly and how the services act if their cluster is divided into several
 * parts, each processing a different request ID.
 *
 * @author Bastian Gloeckle
 */
public class ClusterFlattenIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(ClusterFlattenIntegrationTest.class);

  private static final String BIG_TABLE = "big";
  private static final String BIG0_CONTROL_FILE =
      "/" + ClusterFlattenIntegrationTest.class.getSimpleName() + "/big0" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String BIG10_CONTROL_FILE =
      "/" + ClusterFlattenIntegrationTest.class.getSimpleName() + "/big10" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String BIG_DATA_FILE_WORK = "big.json";

  /**
   * This test starts a first flatten process on all nodes and then before the first one is completed, a second, equal
   * one is started. Tests whether the second one receives the same result as the first and validates therefore that the
   * servers merged the two requests.
   */
  @Test
  @NeedsServer(servers = 2)
  public void mergeTest() throws IOException, InterruptedException {
    // GIVEN
    TestDataGenerator.generateJsonTestData(work(BIG_DATA_FILE_WORK), 10, 2, new String[] { "a", "b" }, 10);
    serverControl.get(0).deploy(cp(BIG0_CONTROL_FILE), work(BIG_DATA_FILE_WORK));
    serverControl.get(1).deploy(cp(BIG10_CONTROL_FILE), work(BIG_DATA_FILE_WORK));

    try (TestClusterFlattenService localCfs = ClusterFlattenServiceTestUtil.createClusterFlattenService()) {

      // first: request flattening from both servers using the first Request ID
      UUID firstRequestId = UUID.randomUUID();
      logger.info("Sending first request to flatten the table (request ID {})", firstRequestId);
      ServiceTestUtil.clusterFlattenService(serverControl.get(0), clusterFlattenService -> {
        clusterFlattenService.flattenAllLocalShards(RUuidUtil.toRUuid(firstRequestId), BIG_TABLE, "a[*].a[*]",
            Arrays.asList(serverControl.get(1).getAddr().toRNodeAddress()),
            localCfs.getThisServicesAddr().toRNodeAddress());
      });
      ServiceTestUtil.clusterFlattenService(serverControl.get(1), clusterFlattenService -> {
        clusterFlattenService.flattenAllLocalShards(RUuidUtil.toRUuid(firstRequestId), BIG_TABLE, "a[*].a[*]",
            Arrays.asList(serverControl.get(0).getAddr().toRNodeAddress()),
            localCfs.getThisServicesAddr().toRNodeAddress());
      });

      // sleep for a second, both servers should get started to process the first request
      Thread.sleep(1000);

      // second: start second flattenign request. As the first one should still be running, this request should be
      // "merged" with the first one.
      UUID secondRequestId = UUID.randomUUID();
      logger.info("Sending second request to flatten the table (request ID {})", secondRequestId);
      ServiceTestUtil.clusterFlattenService(serverControl.get(0), clusterFlattenService -> {
        clusterFlattenService.flattenAllLocalShards(RUuidUtil.toRUuid(secondRequestId), BIG_TABLE, "a[*].a[*]",
            Arrays.asList(serverControl.get(1).getAddr().toRNodeAddress()),
            localCfs.getThisServicesAddr().toRNodeAddress());
      });
      ServiceTestUtil.clusterFlattenService(serverControl.get(1), clusterFlattenService -> {
        clusterFlattenService.flattenAllLocalShards(RUuidUtil.toRUuid(secondRequestId), BIG_TABLE, "a[*].a[*]",
            Arrays.asList(serverControl.get(0).getAddr().toRNodeAddress()),
            localCfs.getThisServicesAddr().toRNodeAddress());
      });

      logger.info("Waiting for first flatten request to finish...");
      new Waiter().waitUntil("First flattening request succeeded", 60, 500,
          () -> localCfs.check() && //
              localCfs.getNodeResults().values().stream().flatMap(lst -> lst.stream())
                  .filter(p -> p.getLeft().equals(firstRequestId)).count() == 2); // two results for first req

      logger.info("Waiting for second flatten request to finish...");
      new Waiter().waitUntil("Second flattening request succeeded", 60, 500,
          () -> localCfs.check() && //
              localCfs.getNodeResults().values().stream().flatMap(lst -> lst.stream())
                  .filter(p -> p.getLeft().equals(secondRequestId)).count() == 2);

      logger.info("Flatten completed successfully, now inspecting the results.");
      List<UUID> flattenTableIdForFirstRequest =
          localCfs.getNodeResults().values().stream().flatMap(lst -> lst.stream())
              .filter(p -> p.getLeft().equals(firstRequestId)).map(p -> p.getRight()).collect(Collectors.toList());

      List<UUID> flattenTableIdForSecondRequest =
          localCfs.getNodeResults().values().stream().flatMap(lst -> lst.stream())
              .filter(p -> p.getLeft().equals(secondRequestId)).map(p -> p.getRight()).collect(Collectors.toList());

      // If the flatten succeeds in a request, the id of the flattened table is equal to the request ID. Should be the
      // case for the first flatten request.
      Assert.assertEquals(flattenTableIdForFirstRequest, Arrays.asList(firstRequestId, firstRequestId),
          "Expected first request succeeded and created a flattened table with the same ID");

      // If the flatten request gets merged (to another request, because the latter is currently running and computing
      // the same result), the result flatten ID should be the ID of that request that the new request was merged to.
      Assert.assertEquals(flattenTableIdForSecondRequest, Arrays.asList(firstRequestId, firstRequestId),
          "Expected second request succeeded and merged with first request.");
    }
  }

  /**
   * Tests what happens when two equal flatten requests are issued at the same time with different request IDs. It is
   * expected that none of the query remotes returns successfully, but all fail. The query master then usually needs to
   * retry its request - we though don't do this in the test here.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  @NeedsServer(servers = 2, manualStart = true)
  public void dividedClusterTest() throws IOException, InterruptedException {
    // GIVEN

    // override the timeout to actually receive an exception from the remotes.
    serverControl.get(0).start(prop -> prop.setProperty(ConfigKey.FLATTEN_TIMEOUT_SECONDS, "60"));
    serverControl.get(1).start(prop -> prop.setProperty(ConfigKey.FLATTEN_TIMEOUT_SECONDS, "60"));

    TestDataGenerator.generateJsonTestData(work(BIG_DATA_FILE_WORK), 10, 2, new String[] { "a", "b" }, 10);
    serverControl.get(0).deploy(cp(BIG0_CONTROL_FILE), work(BIG_DATA_FILE_WORK));
    serverControl.get(1).deploy(cp(BIG10_CONTROL_FILE), work(BIG_DATA_FILE_WORK));

    try (TestClusterFlattenService localCfs = ClusterFlattenServiceTestUtil.createClusterFlattenService()) {

      // first request is sent to first server
      UUID firstRequestId = UUID.randomUUID();
      logger.info("Sending first request to flatten the table (request ID {}) to first server", firstRequestId);
      ServiceTestUtil.clusterFlattenService(serverControl.get(0), clusterFlattenService -> {
        clusterFlattenService.flattenAllLocalShards(RUuidUtil.toRUuid(firstRequestId), BIG_TABLE, "a[*].a[*]",
            Arrays.asList(serverControl.get(1).getAddr().toRNodeAddress()),
            localCfs.getThisServicesAddr().toRNodeAddress());
      });
      // second request to second server
      UUID secondRequestId = UUID.randomUUID();
      logger.info("Sending second request to flatten the table (request ID {}) to second server", secondRequestId);
      ServiceTestUtil.clusterFlattenService(serverControl.get(1), clusterFlattenService -> {
        clusterFlattenService.flattenAllLocalShards(RUuidUtil.toRUuid(secondRequestId), BIG_TABLE, "a[*].a[*]",
            Arrays.asList(serverControl.get(0).getAddr().toRNodeAddress()),
            localCfs.getThisServicesAddr().toRNodeAddress());
      });

      // sleep for a second, both servers should get started to process their respective request
      Thread.sleep(1000);

      // now complete each request
      logger.info("Sending first request to flatten the table (request ID {}) to second server", firstRequestId);
      ServiceTestUtil.clusterFlattenService(serverControl.get(1), clusterFlattenService -> {
        clusterFlattenService.flattenAllLocalShards(RUuidUtil.toRUuid(firstRequestId), BIG_TABLE, "a[*].a[*]",
            Arrays.asList(serverControl.get(0).getAddr().toRNodeAddress()),
            localCfs.getThisServicesAddr().toRNodeAddress());
      });
      logger.info("Sending second request to flatten the table (request ID {}) to first server", secondRequestId);
      ServiceTestUtil.clusterFlattenService(serverControl.get(0), clusterFlattenService -> {
        clusterFlattenService.flattenAllLocalShards(RUuidUtil.toRUuid(secondRequestId), BIG_TABLE, "a[*].a[*]",
            Arrays.asList(serverControl.get(1).getAddr().toRNodeAddress()),
            localCfs.getThisServicesAddr().toRNodeAddress());
      });

      // wait double the time of the flattentimeout we set for the servers!
      // we will receive the exception for each remote and for each requestId on the remote -> 4 times.
      new Waiter().waitUntil("Both remotes reported an exception", 120, 500,
          () -> localCfs.getExceptions().size() == 2 * 2);
    }
  }

}
