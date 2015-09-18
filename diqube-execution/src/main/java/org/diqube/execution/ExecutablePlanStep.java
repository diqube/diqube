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

import org.diqube.execution.consumers.DoneConsumer;
import org.diqube.execution.consumers.GenericConsumer;
import org.diqube.execution.exception.ExecutablePlanBuildException;
import org.diqube.queries.QueryUuid.QueryUuidThreadState;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;

/**
 * One step in an executable plan that needs to be executed.
 * 
 * <p>
 * A step has both input data and output data, both represented as {@link GenericConsumer}s. As each Step is executed
 * within its own thread, as soon as there is new data for this step available in one of the input data providers, the
 * {@link #continueProcessing()} method should be called, which should wake the thread executing the logic of this step.
 * 
 * <p>
 * For each implementation of this interface, there might be a different number of input {@link GenericConsumer}s
 * needed, for convenience they can be wired with {@link #wireOneInputConsumerToOutputOf(Class, ExecutablePlanStep)}.
 *
 * <p>
 * All {@link ExecutablePlanStep}s accept a {@link DoneConsumer} as output.
 * 
 * @author Bastian Gloeckle
 */
public interface ExecutablePlanStep extends Runnable {

  /**
   * Call this method before {@link #run()} in order to initialize this step.
   * 
   * Note that when calling this, the correct {@link QueryUuidThreadState} has to be set.
   */
  public void initialize();

  /**
   * This method processes any new data that is available through the input consumers. It is executed in a single
   * thread.
   * 
   * Note that when calling this, the correct {@link QueryUuidThreadState} has to be set.
   */
  @Override
  public void run();

  /**
   * @return ID of the step as defined by the ExecutionPlanner and provided by {@link RExecutionPlanStep}.
   */
  public int getStepId();

  /**
   * @param stepId
   *          ID of the step as defined by the ExecutionPlanner and provided by {@link RExecutionPlanStep}. Must only be
   *          called while planning!
   */
  public void setStepId(int stepId);

  /**
   * Check if there is new data available in the input consumers and process it accordingly. This method will usually
   * return right away, as it will only wake the thread that executes the logic of this step. This method is
   * thread-safe.
   */
  public void continueProcessing();

  /**
   * Wire one input data consumer to the output of a sourceStep.
   * 
   * <p>
   * Depending on the implementing class of {@link ExecutablePlanStep}, there might be zero, one or multiple inputs
   * available per step. Calling this method will wire the next input available and will throw an
   * {@link ExecutablePlanBuildException} if there are no more inputs available for wiring.
   */
  public void wireOneInputConsumerToOutputOf(Class<? extends GenericConsumer> type, ExecutablePlanStep sourceStep)
      throws ExecutablePlanBuildException;

  /**
   * Add a {@link GenericConsumer} as an output to this executable step. Be aware that implementing classes usually will
   * only accept a specific implementation of {@link GenericConsumer} here, see the class comments accordingly.
   */
  public void addOutputConsumer(GenericConsumer consumer);

  /**
   * @return A descriptional string about the details that the step works on (e.g. which column is created by the step).
   *         This is used for displaying purposes only. Can be <code>null</code>.
   */
  public String getDetailsDescription();
}
