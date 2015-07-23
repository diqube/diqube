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
package org.diqube.execution.env;

import java.util.Map;

import org.diqube.data.ColumnType;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.ExecutablePlanStep;

/**
 * The environment of an execution, which holds for example temporary data produced by some {@link ExecutablePlanStep}s
 * so other steps can fetch it from here.
 * 
 * <p>
 * This is used both on the Query Master node and on each Cluster node when executing a specific query. Note that on the
 * query master, there is no backing {@link TableShard} for this execution, as the query master itself does not need to
 * have any actual information about the Table/TableShard the query executes on.
 *
 * <p>
 * During execution of the {@link ExecutablePlanStep}s, there is usually one "default execution environment" (or
 * "defaultEnv") which is passed on to most steps in their constructor. This defaultEnv will contain those columns and
 * values that are final, meaning which will not be changed any more. On the other hand, on the query master there are
 * multiple {@link VersionedExecutionEnvironment} in place during execution, which enables the query master to not only
 * execute steps as soon as the input (or intermediary) columns are fully built, but also to execute the steps on
 * intermediary versions of the columns. As a simple example, this happens when one remote already responded with the
 * values of a specific column, but a second remote did not yet. The query master might then decide to build an
 * intermediary column out of the results of the first remote and start executing its steps based on that intermediary
 * column, in order to produce user-facing results as soon as possible. Note that on remotes, no
 * {@link VersionedExecutionEnvironment} will be used.
 *
 * @author Bastian Gloeckle
 */
public interface ExecutionEnvironment {

  public LongColumnShard getLongColumnShard(String name);

  public StringColumnShard getStringColumnShard(String name);

  public DoubleColumnShard getDoubleColumnShard(String name);

  public ColumnType getColumnType(String colName);

  public ColumnShard getColumnShard(String name);

  public Map<String, ColumnShard> getAllColumnShards();

  public void storeTemporaryLongColumnShard(LongColumnShard column);

  public void storeTemporaryStringColumnShard(StringColumnShard column);

  public void storeTemporaryDoubleColumnShard(DoubleColumnShard column);

  /**
   * The {@link TableShard} backing this {@link DefaultExecutionEnvironment}. Can be <code>null</code> in case this
   * execution happens on the query master and there is no backing {@link TableShard} object available.
   */
  public TableShard getTableShardIfAvailable();

  public long getFirstRowIdInShard();

  /**
   * @return -1 if unknown.
   */
  public long getLastRowIdInShard();
}
