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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.UUID;
import java.util.regex.Pattern;

import org.diqube.context.Profiles;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.steps.AbstractThreadedExecutablePlanStep;
import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStep;
import org.diqube.itest.AbstractDiqubeIntegrationTest;
import org.diqube.itest.annotations.NeedsProcessPid;
import org.diqube.itest.annotations.NeedsServer;
import org.diqube.itest.control.ServerControl;
import org.diqube.itest.util.QueryResultServiceTestUtil;
import org.diqube.itest.util.QueryResultServiceTestUtil.TestQueryResultService;
import org.diqube.itest.util.TestDataGenerator;
import org.diqube.itest.util.Waiter;
import org.diqube.plan.ExecutionPlanBuilder;
import org.diqube.plan.ExecutionPlanBuilderFactory;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.remote.query.thrift.QueryService;
import org.diqube.server.NewDataWatcher;
import org.diqube.thrift.base.thrift.RUUID;
import org.diqube.thrift.base.thrift.Ticket;
import org.diqube.thrift.base.util.RUuidUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests that queries are actually cancelled on remotes, too, when
 * {@link QueryService.Iface#cancelQueryExecution(RUUID)} is called.
 *
 * @author Bastian Gloeckle
 */
public class QueryCancelIntegrationTest extends AbstractDiqubeIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(QueryCancelIntegrationTest.class);

  private static final String BIG_TABLE = "big";
  private static final String BIG_CONTROL_FILE =
      "/" + QueryCancelIntegrationTest.class.getSimpleName() + "/big" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String BIG2_CONTROL_FILE =
      "/" + QueryCancelIntegrationTest.class.getSimpleName() + "/big2" + NewDataWatcher.CONTROL_FILE_EXTENSION;
  private static final String BIG_DATA_FILE_WORK = "big.json";

  @Test
  @NeedsServer(servers = 2)
  @NeedsProcessPid
  public void queryGetsCancelledOnAllNodes() throws InterruptedException, FileNotFoundException, IOException {
    // GIVEN
    // a table which has enough data to make execution a bit longer.
    TestDataGenerator.generateJsonTestData(work(BIG_DATA_FILE_WORK), 10, 2, new String[] { "a", "b" }, 30);

    // WHEN
    serverControl.get(0).deploy(cp(BIG_CONTROL_FILE), work(BIG_DATA_FILE_WORK));
    serverControl.get(1).deploy(cp(BIG2_CONTROL_FILE), work(BIG_DATA_FILE_WORK));

    Ticket ticket = serverControl.get(0).loginSuperuser();

    // THEN
    try (TestQueryResultService queryRes = QueryResultServiceTestUtil.createQueryResultService()) {
      String diqlQuery =
          "select avg(add(a[*].a[*], 1)), avg(add(a[*].b[*], 1)), avg(add(b[*].a[*], 1)) from " + BIG_TABLE;

      UUID queryUuid = UUID.randomUUID();
      RUUID queryRUuid = RUuidUtil.toRUuid(queryUuid);
      logger.info("Executing query {}", RUuidUtil.toUuid(queryRUuid));
      // execute a long-running query.
      serverControl.get(0).getSerivceTestUtil().queryService((queryService) -> queryService.asyncExecuteQuery(ticket,
          queryRUuid, diqlQuery, true, queryRes.getThisServicesAddr().toRNodeAddress()));

      new Waiter().waitUntil("Remote worker threads start showing up", 2, 300,
          () -> threadDumpContainsString(serverControl.get(0), "query-remote-worker-" + queryUuid.toString()) && //
              threadDumpContainsString(serverControl.get(1), "query-remote-worker-" + queryUuid.toString()));

      // now remotes are running. Cancel execution.
      logger.info("Canceling query {}", queryUuid);
      serverControl.get(0).getSerivceTestUtil()
          .queryService(queryService -> queryService.cancelQueryExecution(ticket, queryRUuid));

      // now /all/ threads should be cancelled within a short amount of time, both the ones of query master and the ones
      // of query remotes.
      new Waiter().waitUntil("There are no more query threads", 20, 500,
          () -> !threadDumpContainsString(serverControl.get(0), queryUuid.toString())
              && !threadDumpContainsString(serverControl.get(1), queryUuid.toString()));

      // now check that the remotes did not actually complete processing the plan (in which case the threads would be
      // gone, too).

      int numberOfRemoteSteps = calculateNumberOfRemoteSteps(diqlQuery);
      logger.info("Found that there are {} remote steps in the execution plan", numberOfRemoteSteps);

      String stepDoneRegex = AbstractThreadedExecutablePlanStep.STEP_IS_DONE_PROCESSING_LOG_PATTERN.replace("{}", ".*");
      Pattern workerDonePattern =
          Pattern.compile(".*query-remote-worker-" + queryUuid.toString() + ".*" + stepDoneRegex);

      // validate first server cancelled.
      int completed = countLinesMatching(serverControl.get(0).getServerLogOutput(), workerDonePattern);
      logger.info("First server completed {} remote steps", completed);
      Assert.assertTrue(completed < numberOfRemoteSteps,
          "Expected that not all remote steps did complete computation. Number of remote steps: " + numberOfRemoteSteps
              + ", completed: " + completed);

      // assert that at least one step was completed -> this is mainly to ensure that the Regex is still valid. At least
      // the RowIdSinkStep should have completed, since that should complete at under a second.
      Assert.assertTrue(completed > 0,
          "Expected for at least one remote step to be completed, the regex might not be valid any more?");

      // validate second server cancelled.
      completed = countLinesMatching(serverControl.get(1).getServerLogOutput(), workerDonePattern);
      logger.info("Second server completed {} remote steps", completed);
      Assert.assertTrue(completed < numberOfRemoteSteps,
          "Expected that not all remote steps did complete computation. Number of remote steps: " + numberOfRemoteSteps
              + ", completed: " + completed);

      logger.info("Successfully received exception and all threads of the query were shut down.",
          queryRes.getException());
    } catch (IOException e) {
      throw new RuntimeException("Could not execute query", e);
    }
  }

  private boolean threadDumpContainsString(ServerControl serverControl, String searchString) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serverControl.createThreadDump(baos);
    String s = baos.toString();

    logger.info("Gathered thread dump of server process {}: {}", serverControl.getAddr(), s);

    return s.contains(searchString);
  }

  private int countLinesMatching(String input, Pattern pattern) {
    logger.trace("Starting to match log output to a pattern...");
    try {
      BufferedReader reader = new BufferedReader(new StringReader(input));
      return (int) reader.lines().parallel().filter(s -> pattern.matcher(s).matches()).count();
    } finally {
      logger.trace("Done matching log output to a pattern.");
    }
  }

  private int calculateNumberOfRemoteSteps(String diql) {
    try (AnnotationConfigApplicationContext dataContext = new AnnotationConfigApplicationContext()) {
      dataContext.getEnvironment().setActiveProfiles(Profiles.ALL_BUT_NEW_DATA_WATCHER);
      dataContext.scan("org.diqube");
      dataContext.refresh();

      ExecutionPlanBuilder planBuilder =
          dataContext.getBean(ExecutionPlanBuilderFactory.class).createExecutionPlanBuilder();

      ExecutablePlan masterPlan = planBuilder.fromDiql(diql).build();
      ExecuteRemotePlanOnShardsStep executeRemoteStep = (ExecuteRemotePlanOnShardsStep) masterPlan.getSteps().stream()
          .filter(step -> step instanceof ExecuteRemotePlanOnShardsStep).findAny().get();
      RExecutionPlan remotePlan = executeRemoteStep.getRemoteExecutionPlan();

      return remotePlan.getSteps().size();
    }
  }

}
