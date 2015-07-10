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
package org.diqube.plan.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.diqube.data.ColumnType;
import org.diqube.data.Table;
import org.diqube.data.TableShard;
import org.diqube.execution.ExecutablePlan;
import org.diqube.execution.ExecutablePlanFactory;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.ColumnValueConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.consumers.GroupIntermediaryAggregationConsumer;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStep;
import org.diqube.execution.steps.ExecuteRemotePlanOnShardsStepTestUtil;
import org.diqube.remote.cluster.thrift.RExecutionPlan;
import org.diqube.util.Pair;
import org.diqube.util.Triple;
import org.testng.annotations.BeforeMethod;

/**
 * Abstract base class for tests that want to mock away the remote.
 * 
 * @author Bastian Gloeckle
 */
public abstract class AbstractRemoteEmulatingDiqlExecutionTest<T> extends AbstractDiqlExecutionTest<T> {

  private ExecutablePlanFactory executablePlanFactory;

  /**
   * For each TableShard there will be a {@link RemoteEmulation}.
   * <p>
   * This map maps from the first rowId in a shard to the corresponding {@link RemoteEmulation}.
   * <p>
   * This will be available after the {@link ExecuteRemotePlanOnShardsStep} triggered the creation of the corresponding
   * objects. Use {@link #waitUntilOrFail(Object, Supplier, Supplier)} and {@link #remoteEmulationsNotify} to wait until
   * these values are available.
   */
  protected Map<Long, RemoteEmulation> remoteEmulations;

  /**
   * {@link Object#notifyAll()} will be called on this object as soon as there's new data available in
   * {@link #remoteEmulations}.
   */
  protected Object remoteEmulationsNotify;

  public AbstractRemoteEmulatingDiqlExecutionTest(ColumnType colType, TestDataProvider<T> dp) {
    super(colType, dp);
  }

  @Override
  @BeforeMethod
  public void setUp() {
    super.setUp();
    executablePlanFactory = dataContext.getBean(ExecutablePlanFactory.class);
  }

  @Override
  protected ExecutablePlan buildExecutablePlan(String diql) {
    ExecutablePlan res = super.buildExecutablePlan(diql);
    remoteEmulations = new ConcurrentHashMap<>();
    remoteEmulationsNotify = new Object();

    // function creating ExecutablePlans for the ExecuteRemote step, which contain only one step, namely a
    // RemoteEmulation.
    Function<Triple<RExecutionPlan, GroupIntermediaryAggregationConsumer, ColumnValueConsumer>, List<ExecutablePlan>> providerFn =
        new Function<Triple<RExecutionPlan, GroupIntermediaryAggregationConsumer, ColumnValueConsumer>, List<ExecutablePlan>>() {
          @Override
          public List<ExecutablePlan> apply(
              Triple<RExecutionPlan, GroupIntermediaryAggregationConsumer, ColumnValueConsumer> t) {
            List<ExecutablePlan> res = new ArrayList<>();
            Table table = tableRegistry.getTable(t.getLeft().getTable());
            for (TableShard shard : table.getShards()) {
              RemoteEmulation remoteEmulation = new RemoteEmulation();
              remoteEmulation.addOutputConsumer(t.getMiddle());
              remoteEmulation.addOutputConsumer(t.getRight());
              res.add(executablePlanFactory.createExecutablePlan(null,
                  new ArrayList<>(Arrays.asList(new ExecutablePlanStep[] { remoteEmulation })), null));
              remoteEmulations.put(shard.getLowestRowId(), remoteEmulation);
            }

            synchronized (remoteEmulationsNotify) {
              remoteEmulationsNotify.notifyAll();
            }
            return res;
          }
        };

    res.getSteps().stream().filter(step -> step instanceof ExecuteRemotePlanOnShardsStep)
        .forEach(new Consumer<ExecutablePlanStep>() {
          @Override
          public void accept(ExecutablePlanStep t) {
            ExecuteRemotePlanOnShardsStepTestUtil.addCustomRemoteExecution((ExecuteRemotePlanOnShardsStep) t,
                providerFn);
          }
        });

    return res;
  }

  /**
   * Initialize a number of sample shards so the test table has any shards. Returns a list with the 'first row ID' of
   * each created shard.
   */
  protected List<Long> initializeSampleTableShards(int numberOfShards) {
    List<Pair<Object[], Object[]>> shards = new ArrayList<>();
    for (int firstRowId = 0; firstRowId < numberOfShards; firstRowId++) {
      shards.add(new Pair<Object[], Object[]>(dp.a(firstRowId), dp.a(firstRowId)));
    }
    initializeMultiShardTable(shards);
    return LongStream.range(0, numberOfShards).mapToObj(Long::valueOf).collect(Collectors.toList());
  }

  /**
   * Wait until the {@link Object#notifyAll()} method is called on a specific object /and/ a specific testFunction
   * returns true.
   * 
   * <p>
   * This will wait some time and if after that the test function still fails, it will throw a {@link RuntimeException}
   * with a given message.
   */
  protected void waitUntilOrFail(Object objToWaitFor, Supplier<String> msgSupplier, Supplier<Boolean> testFunction)
      throws RuntimeException {
    int maxRuns = 30; // 3s.
    while (maxRuns-- > 0) {
      synchronized (objToWaitFor) {
        try {
          objToWaitFor.wait(100);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while waiting for result", e);
        }
      }

      if (testFunction.get())
        return;
    }
    throw new RuntimeException(msgSupplier.get());
  }

  /**
   * Emulates the execution of a remote execution plan. This means that this {@link ExecutablePlanStep} will be used as
   * the only step in a {@link ExecutablePlan} that will be executed by {@link ExecuteRemotePlanOnShardsStep}. Use
   * {@link #getOutputGroups()} and {@link #getOutputValues()} to feed data to the {@link ExecuteRemotePlanOnShardsStep}
   * and call {@link #done()} when this step is done (= send sourceIsDone to {@link ExecuteRemotePlanOnShardsStep} and
   * shut down execution thread).
   */
  public static class RemoteEmulation implements ExecutablePlanStep {

    private ColumnValueConsumer outputValues;
    private GroupIntermediaryAggregationConsumer outputGroups;

    private Object sleepObj = new Object();

    @Override
    public void run() {
      try {
        synchronized (sleepObj) {
          sleepObj.wait();
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int getStepId() {
      return 0;
    }

    @Override
    public void setStepId(int stepId) {
      // noop.
    }

    @Override
    public void continueProcessing() {
    }

    @Override
    public void wireOneInputConsumerToOutputOf(Class<? extends GenericConsumer> type, ExecutablePlanStep sourceStep)
        throws ExecutablePlanBuildException {
      // noop.
    }

    @Override
    public void addOutputConsumer(GenericConsumer consumer) {
      if (consumer instanceof ColumnValueConsumer) {
        outputValues = (ColumnValueConsumer) consumer;
        consumer.recordOneWiring();
      } else if (consumer instanceof GroupIntermediaryAggregationConsumer) {
        outputGroups = (GroupIntermediaryAggregationConsumer) consumer;
        consumer.recordOneWiring();
      }
    }

    public void done() {
      outputValues.sourceIsDone();
      outputGroups.sourceIsDone();
      synchronized (sleepObj) {
        sleepObj.notifyAll();
      }
    }

    public ColumnValueConsumer getOutputValues() {
      return outputValues;
    }

    public GroupIntermediaryAggregationConsumer getOutputGroups() {
      return outputGroups;
    }

  }

}
