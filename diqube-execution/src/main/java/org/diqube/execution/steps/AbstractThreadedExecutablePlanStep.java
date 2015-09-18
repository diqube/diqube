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
package org.diqube.execution.steps;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.consumers.AbstractPlanStepBasedGenericConsumer;
import org.diqube.execution.consumers.ContinuousConsumer;
import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.queries.QueryRegistry;
import org.diqube.queries.QueryUuid;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for {@link ExecutablePlanStep} that are executed in their own thread. Takes care of waking the
 * execution thread on calls to {@link #continueProcessing()} and manages the consumers.
 * 
 * <p>
 * The input consumers to a step can be specified by subclasses by overriding the corresponding method. Each of those
 * input consumers can then be wired to an output consumer in
 * {@link #wireOneInputConsumerToOutputOf(Class, ExecutablePlanStep)}.
 * 
 * <p>
 * Typically the input consumers will be called from other threads (=threads of other steps that provide the new data),
 * which means that the consumers should be thread safe and should add the newly provided data to a thread-safe field in
 * the step object. Then, the input consumer should call {@link #continueProcessing()} on the step to wake up the thread
 * that is processing this step (done automatically by {@link AbstractPlanStepBasedGenericConsumer}). This will in turn
 * call the {@link #execute()} method in this steps thread which can then process the new data that is available in the
 * thread-safe field of the step object.
 *
 * @author Bastian Gloeckle
 */
public abstract class AbstractThreadedExecutablePlanStep implements ExecutablePlanStep {
  private static final Logger logger = LoggerFactory.getLogger(AbstractThreadedExecutablePlanStep.class);

  protected Object sync = new Object();

  protected List<GenericConsumer> outputConsumers = new LinkedList<>();

  private int stepId;

  /**
   * we store the number of 'events' (=calls to {@link #continueProcessing()}) which have not been worked on yet. This
   * is to remove the possibility that we notify the sync object just right before we start to wait -> and sleep
   * additional 2s therefore that we would not need to wait.
   */
  private AtomicInteger numberOfEventsNotProcessed = new AtomicInteger(0);

  private AtomicBoolean doneProcessing = new AtomicBoolean(false);

  protected QueryRegistry queryRegistry;

  protected QueryUuidThreadState queryUuidThreadState = null;

  private boolean currentlyMeasuringTime = false;

  /**
   * Note that constructors of {@link ExecutablePlanStep}s should execute very quickly and must NOT execute any
   * initialization code, especially not such code that relies on correct {@link QueryUuidThreadState} (which includes
   * accessing {@link ExecutionEnvironment}).
   */
  public AbstractThreadedExecutablePlanStep(int stepId, QueryRegistry queryRegistry) {
    this.stepId = stepId;
    this.queryRegistry = queryRegistry;
  }

  /**
   * Overwrite this method to initialize anything.
   * 
   * As you must not use e.g. the {@link ExecutionEnvironment} in the constructor, you can use this method to initialize
   * anything that might rely on the {@link QueryUuidThreadState}.
   */
  @Override
  public void initialize() {

  }

  @Override
  public void run() {
    queryUuidThreadState = QueryUuid.getCurrentThreadState();
    validateWiredStatus();
    while (!doneProcessing.get()) {
      numberOfEventsNotProcessed.set(0);

      long startNanos = System.nanoTime();
      currentlyMeasuringTime = true;
      execute();
      long endNanos = System.nanoTime();
      currentlyMeasuringTime = false;

      long activeMs = (long) ((endNanos - startNanos) / 1e6);

      queryRegistry.getOrCreateCurrentStatsManager().addStepThreadActiveMs(stepId, activeMs);

      if (doneProcessing.get())
        break;
      waitForNewData();
      if (doneProcessing.get())
        break;
    }
  }

  @Override
  public void continueProcessing() {
    synchronized (sync) {
      // mark a new event arrived. This is done in the sync block, because #waitForNewData checks in in a sync block,
      // too -> We will not receive an event without noticing it right before we start to wait.
      numberOfEventsNotProcessed.incrementAndGet();
      sync.notify();
    }
  }

  protected void waitForNewData() {
    try {
      synchronized (sync) {
        if (numberOfEventsNotProcessed.get() == 0)
          sync.wait(2000);
      }
    } catch (InterruptedException e) {
      // we were interrupted, therefore we choose to end processing.
      doneProcessing.set(true);
      logger.trace("Step {} ({}) was interrupted. Stopping.", this.getStepId(), this.getClass().getSimpleName(), e);
    }
  }

  protected void doneProcessing() {
    logger.trace("Step {} ({}) is done processing", this.getStepId(), this.getClass().getSimpleName());
    doneProcessing.set(true);
    continueProcessing();
  }

  @Override
  public void addOutputConsumer(GenericConsumer consumer) {
    validateOutputConsumer(consumer);
    outputConsumers.add(consumer);
    consumer.recordOneWiring();
  }

  /**
   * Called when a new Output consumer is added, should throw an exception if this step cannot handle the consumer.
   * 
   * Please note that all steps /must/ accept {@link DoneConsumer}.
   */
  abstract protected void validateOutputConsumer(GenericConsumer consumer) throws IllegalArgumentException;

  /**
   * Do something for all Output consumers of a specific type.
   * 
   * Statistics will not cover the time spent in the consumers as "active time" for this step.
   */
  @SuppressWarnings("unchecked")
  protected <T extends GenericConsumer> void forEachOutputConsumerOfType(Class<? extends T> type,
      Consumer<T> consumer) {
    long start = System.nanoTime();
    for (GenericConsumer outputConsumer : outputConsumers) {
      if (type.isInstance(outputConsumer))
        consumer.accept((T) outputConsumer);
    }
    long end = System.nanoTime();
    if (queryUuidThreadState != null && currentlyMeasuringTime) {
      QueryUuidThreadState backupState = QueryUuid.getCurrentThreadState();
      try {
        QueryUuid.setCurrentThreadState(queryUuidThreadState);
        long nonActiveMs = (long) ((end - start) / 1e6);
        queryRegistry.getOrCreateCurrentStatsManager().addStepThreadActiveMs(stepId, -1 * nonActiveMs);
      } finally {
        QueryUuid.setCurrentThreadState(backupState);
      }
    }
  }

  protected boolean existsOutputConsumerOfType(Class<? extends GenericConsumer> type) {
    for (GenericConsumer outputConsumer : outputConsumers) {
      if (type.isInstance(outputConsumer))
        return true;
    }
    return false;
  }

  /**
   * Called when there is new data to be processed, that means when the input consumers consumed new data and the
   * {@link #continueProcessing()} method on this object was called.
   * 
   * This method will be executed in this steps single thread, whereas the input consumers will typically be executed in
   * other threads. As the class comment of {@link AbstractThreadedExecutablePlanStep} describes, the {@link #execute()}
   * method should process data that was added by the input consumers to a thread-safe field in this object. The
   * {@link #execute()} method should then process that chunk of data, and, if the input consumers were reported that
   * their {@link ContinuousConsumer#sourceIsDone()} and there is no data left then call {@link #doneProcessing()} (and
   * inform possible output consumers).
   */
  abstract protected void execute();

  @Override
  public int getStepId() {
    return stepId;
  }

  @Override
  public void setStepId(int stepId) {
    this.stepId = stepId;
  }

  /**
   * Iterable list of input consumers. Each of this consumers will be wired to output consumer(s) when the wiring
   * happens by calling {@link #wireOneInputConsumerToOutputOf(Class, ExecutablePlanStep)}.
   * 
   * <p>
   * Please note that typically the wired input consumers (and therefore the objects returned by this method) will be
   * called in a multi-threaded way.
   */
  abstract protected List<GenericConsumer> inputConsumers();

  @Override
  public void wireOneInputConsumerToOutputOf(Class<? extends GenericConsumer> type, ExecutablePlanStep sourceStep)
      throws ExecutablePlanBuildException {
    for (GenericConsumer availableInputConsumer : inputConsumers()) {
      if (type.isInstance(availableInputConsumer)) {
        sourceStep.addOutputConsumer(availableInputConsumer);
        return;
      }
    }
    throw new ExecutablePlanBuildException("Could not wire input, because there is no " + type.getSimpleName()
        + " input available on " + this.getClass().getSimpleName());
  }

  /**
   * Validate that we're wired correctly.
   */
  protected void validateWiredStatus() throws ExecutablePlanBuildException {
    if (inputConsumers().stream().anyMatch(c -> c.getNumberOfTimesWired() == 0))
      throw new ExecutablePlanBuildException("Not all inputs wired.");
  }

  protected SortedMap<Integer, List<String>> getNextStepInfo() {
    SortedMap<Integer, List<String>> res = new TreeMap<Integer, List<String>>();
    forEachOutputConsumerOfType(GenericConsumer.class, c -> {
      Integer destPlanId = c.getDestinationPlanStepId();
      if (destPlanId == null)
        destPlanId = -1;
      if (!res.containsKey(destPlanId))
        res.put(destPlanId, new ArrayList<>());

      res.get(destPlanId).add(c.getType());
    });
    return res;
  }

  /**
   * @return If interesting, an additional string which will be included in {@link #toString()} calls, or
   *         <code>null</code> if nothing interesting.
   */
  protected abstract String getAdditionalToStringDetails();

  @Override
  public String toString() {
    // for debugging.
    String additional = getAdditionalToStringDetails();
    if (additional != null)
      additional = ", " + additional;
    else
      additional = "";
    return this.getClass().getSimpleName() + "[stepId=" + stepId + ", nextSteps=" + getNextStepInfo() + additional
        + "]";
  }

  @Override
  public String getDetailsDescription() {
    return getAdditionalToStringDetails();
  }

}
