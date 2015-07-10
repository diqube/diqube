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
package org.diqube.plan.planner;

import java.util.List;

import org.diqube.execution.env.ExecutionEnvironment;
import org.diqube.plan.request.FunctionRequest;
import org.diqube.remote.cluster.thrift.RExecutionPlanStep;
import org.diqube.remote.cluster.thrift.RExecutionPlanStepType;

/**
 * A {@link ColumnManager} manages the columns that need to be available in the {@link ExecutionEnvironment} on a
 * specific type of node while executing a query.
 *
 * @author Bastian Gloeckle
 */
public interface ColumnManager<T> {
  /**
   * Create steps that build a column by executing the function of the given {@link FunctionRequest}.
   */
  public void produceColumn(FunctionRequest functionRequest);

  /**
   * Ensures that the data of a specific column is available on the type of node.
   * 
   * <p>
   * This is usually only interesting for the Query master: Calling this method makes sure that all cluster nodes select
   * the actual values of this column (for the active row IDs) and forward it to the query master. This is needed e.g.
   * if the query master needs to evaluate an ORDER or a HAVING clause and needs the data of that column not only for
   * the TableShard the query master might have himself, but for all selected rowIDs.
   * 
   * <p>
   * Calling this method will ensure that not only the actual values are resolved by the cluster nodes and forwarded to
   * the query master, but also that the query master will build an actual temporary column out of these values (i.e.
   * make it available in the {@link ExecutionEnvironment}).
   */
  public void ensureColumnAvailable(String colName);

  /**
   * If the given column name is available and is up for being created on the type of cluster node, then wire the
   * targetStep to the step building the column: The targetStep will be informed as soon as the column is actually built
   * and it will not execute before that happens.
   */
  public void wireOutputOfColumnIfAvailable(String colName, T targetStep);

  /**
   * As soon as a source step for groupings is available, this method should be called in order to wire any
   * column-creating steps (that have been created by calling {@link #produceColumn(FunctionRequest)}) are wired to that
   * group step. Please note that the type of group step might be different for each implementing class: a cluster node
   * (=remote) implementation might need a {@link RExecutionPlanStep} with type {@link RExecutionPlanStepType#GROUP} (=
   * the step that cretaes the actual groupings), whereas a query master implementation might need a step that provides
   * already intermediate-aggregated results of any group aggregation functions.
   */
  public void wireGroupInput(T groupStep);

  /**
   * Prepare the call to {@link #build()} and execute any side effects to other data structures that need to be
   * executed. See implementing classes for descriptions and when this actually has to be called.
   */
  public void prepareBuild();

  /**
   * Execute final wiring of the steps created using this {@link ColumnManager}, build and return them.
   * 
   * <p>
   * Please note that implementing classes might need to be provided additional steps, see their JavaDoc.
   */
  public List<T> build();

  /**
   * Checks if {@link #produceColumn(FunctionRequest)} was called for the given colName - meaning that the corresponding
   * column will be created.
   */
  public boolean isColumnProduced(String colName);
}
