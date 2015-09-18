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
package org.diqube.execution;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.diqube.data.TableShard;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.env.VersionedExecutionEnvironment;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.diqube.threads.ExecutorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a plan that is ready to be executed, either on a query master node or a query remote node. In case it is
 * executed on a query remote, this is based on a specific {@link TableShard}.
 * 
 * <p>
 * This class basically holds the various {@link ExecutablePlanStep}s from which each will be executed in its own thread
 * which is provided by an {@link Executor} on the call to {@link #executeAsynchronously(Executor)}. An instance
 * additionally holds the _default_ {@link ExecutionEnvironment} which is used to hold temporary information while the
 * plan is being executed - information that needs to be shared between the {@link ExecutablePlanStep}s. See JavaDoc of
 * {@link ExecutionEnvironment} and {@link VersionedExecutionEnvironment} for more information.
 * 
 * <p>
 * An ExecutablePlan executes one part of the overall execution for a query (= a diql string sent by the user). Each
 * {@link ExecutablePlan} therefore belongs to a query, which has a query UUID. As the execution of each query is
 * distributed across the diqube cluster, each cluster node participating in the execution (query master and each query
 * remote) has an execution UUID for identifying the things that are being executed together. Those two UUIDs can be
 * fetched from {@link QueryUuid} class while in a thread that is currently executing some part of this
 * {@link ExecutablePlan}.
 *
 * @author Bastian Gloeckle
 */
public class ExecutablePlan {
  private static final Logger logger = LoggerFactory.getLogger(ExecutablePlan.class);

  private Collection<ExecutablePlanStep> steps;

  private ExecutionEnvironment defaultEnv;

  private ExecutablePlanInfo info;

  private ColumnVersionManager columnVersionManager;

  /* package */ ExecutablePlan(ExecutionEnvironment defaultEnv, Collection<ExecutablePlanStep> steps,
      ExecutablePlanInfo info, ColumnVersionManager columnVersionManager) {
    this.defaultEnv = defaultEnv;
    this.steps = steps;
    this.info = info;
    this.columnVersionManager = columnVersionManager;
  }

  public Collection<ExecutablePlanStep> getSteps() {
    return steps;
  }

  public ExecutionEnvironment getDefaultExecutionEnvironment() {
    return defaultEnv;
  }

  /**
   * Execute all steps asynchronously using the given new {@link ExecutorService}.
   * 
   * @param executor
   *          The {@link Executor} to be used to execute the steps. This Executor should have
   *          {@link #preferredExecutorServiceSize()} free threads, as specific threads might block while executing a
   *          specific step (as the step is e.g. waiting for additional data to be provided by other steps). If there a
   *          not enough threads available, it is not guaranteed that there will be no deadlock. Additionally, all
   *          threads of the executor should have the correct {@link QueryUuidThreadState} set (e.g. by having it
   *          created by
   *          {@link ExecutorManager#newQueryFixedThreadPoolWithTimeout(int, String, java.util.UUID, java.util.UUID)}).
   * @return A Future that can be used to query the state of the computation of all steps. Note that
   *         {@link Future#cancel(boolean)} will not work on the returned future, stop the executor passed to this
   *         method instead (one might want to use
   *         {@link ExecutorManager#shutdownEverythingOfQueryExecution(java.util.UUID)} and
   *         {@link QueryRegistry#unregisterQueryExecution(java.util.UUID)}).
   */
  public Future<Void> executeAsynchronously(Executor executor) {
    logger.trace("Executing asynchronously {}", this);
    ExecutionFuture future = new ExecutionFuture(steps.size());

    // first: initialize all steps
    AtomicInteger initializedCount = new AtomicInteger(0);
    Object initializedSync = new Object();
    for (ExecutablePlanStep step : steps) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          step.initialize();
          initializedCount.incrementAndGet();
          synchronized (initializedSync) {
            initializedSync.notifyAll();
          }
        }
      });
    }

    // wait until all are initialized.
    while (initializedCount.get() < steps.size()) {
      synchronized (initializedSync) {
        try {
          initializedSync.wait(1000);
        } catch (InterruptedException e) {
          // interrupted, exit quietly
        }
      }
    }

    // start executing the steps.
    for (ExecutablePlanStep step : steps)
      executor.execute(new Runnable() {
        @Override
        public void run() {
          boolean isException = false;
          try {
            step.run();
          } catch (RuntimeException e) {
            future.oneStepIsException();
            isException = true;
            throw e;
          } finally {
            if (!isException)
              future.oneStepIsDone();
          }
        }
      });

    return future;
  }

  /**
   * @return Number of threads that should be available to use when calling
   *         {@link #executeAsynchronously(ExecutorService)}.
   */
  public int preferredExecutorServiceSize() {
    return steps.size();
  }

  /**
   * @return Information about what this {@link ExecutablePlan} will do when executed.
   */
  public ExecutablePlanInfo getInfo() {
    return info;
  }

  /**
   * @return The {@link ColumnVersionManager} that is used when this ExecutablePlan is executed. Could be
   *         <code>null</code>.
   */
  public ColumnVersionManager getColumnVersionManager() {
    return columnVersionManager;
  }

  public static class ExecutionFuture implements Future<Void> {

    private boolean cancelled = false;
    private int numberOfStepsToWaitFor;
    private AtomicInteger stepsDone = new AtomicInteger(0);
    private Object getWait = new Object();
    private boolean isException = false;

    public ExecutionFuture(int numberOfStepsToWaitFor) {
      this.numberOfStepsToWaitFor = numberOfStepsToWaitFor;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      cancelled = !isDone();
      return false;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean isDone() {
      return numberOfStepsToWaitFor == stepsDone.intValue();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
      while (!isDone()) {
        if (isException)
          throw new ExecutionException("There was an exception executing the plan", null);
        synchronized (getWait) {
          getWait.wait(500);
        }
      }
      return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      synchronized (getWait) {
        if (isDone())
          return null;
        if (isException)
          throw new ExecutionException("There was an exception executing the plan", null);

        getWait.wait(unit.toMillis(timeout));
      }

      if (isDone())
        return null;
      if (isException)
        throw new ExecutionException("There was an exception executing the plan", null);

      throw new TimeoutException("ExecutablePlan not executed yet");
    }

    private void oneStepIsDone() {
      stepsDone.incrementAndGet();
      synchronized (getWait) {
        getWait.notifyAll();
      }
    }

    private void oneStepIsException() {
      isException = true;
      synchronized (getWait) {
        getWait.notifyAll();
      }
    }

  }

  @Override
  public String toString() {
    return "ExecutablePlan[steps=" + steps + "]";
  }

}
