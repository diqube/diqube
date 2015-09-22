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

import java.util.List;
import java.util.Map;

import org.diqube.data.ColumnType;
import org.diqube.data.TableShard;
import org.diqube.data.colshard.ColumnShard;
import org.diqube.data.colshard.ConstantColumnShard;
import org.diqube.data.colshard.StandardColumnShard;
import org.diqube.data.dbl.DoubleColumnShard;
import org.diqube.data.lng.LongColumnShard;
import org.diqube.data.str.StringColumnShard;
import org.diqube.execution.ExecutablePlanStep;
import org.diqube.execution.cache.ColumnShardCache;
import org.diqube.execution.env.querystats.QueryableColumnShard;
import org.diqube.execution.env.querystats.QueryableDoubleColumnShard;
import org.diqube.execution.env.querystats.QueryableLongColumnShard;
import org.diqube.execution.env.querystats.QueryableStringColumnShard;

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
 * <p>
 * An {@link ExecutionEnvironment} might optionally be based on a {@link ColumnShardCache}, which will be the case on
 * query remotes. Note that such an {@link ExecutionEnvironment} will not only load existing column shards from a
 * backing {@link TableShard}, but also from the cache. As such cached {@link ColumnShard}s may be evicted from the
 * cache at any time, though, the {@link ExecutionEnvironment} will add such a cached column to the "temporary columns"
 * of the {@link ExecutionEnvironment} itself as soon as the column is fetched from the cache. With that procedure, the
 * {@link ExecutionEnvironment} can guarantee that a specific column that was once "visible" to the
 * {@link #getColumnShard(String)} methods (and similar) will be available throughout the execution of a whole query
 * (=until the {@link ExecutionEnvironment} is invalidated). At the same time, the cache will be based on the temporary
 * columns that are available in the {@link ExecutionEnvironment} of a query after its execution is complete - so cached
 * columns will be presented again to the cache if they have been loaded into a {@link ExecutionEnvironment}. This
 * allows the cache then to count the usages of specific {@link ColumnShard}s and allows to tune the cache.
 *
 * @author Bastian Gloeckle
 */
public interface ExecutionEnvironment {

  /**
   * Returns a {@link QueryableLongColumnShard} for a specific column.
   * 
   * <p>
   * That column shard can either be a temporary one or a "real" one from a {@link TableShard}.
   * 
   * <p>
   * Note that this method might actually return a different instance each time called, but when a column for a name was
   * returned once, there will be data available until this {@link ExecutionEnvironment} is at its end of life.
   * 
   * @return A {@link QueryableLongColumnShard} for the column with the given name or <code>null</code> if it does not
   *         exist.
   */
  public QueryableLongColumnShard getLongColumnShard(String name);

  /**
   * Returns a {@link QueryableStringColumnShard} for a specific column.
   * 
   * <p>
   * That column shard can either be a temporary one or a "real" one from a {@link TableShard}.
   * 
   * <p>
   * Note that this method might actually return a different instance each time called, but when a column for a name was
   * returned once, there will be data available until this {@link ExecutionEnvironment} is at its end of life.
   * 
   * @return A {@link QueryableStringColumnShard} for the column with the given name or <code>null</code> if it does not
   *         exist.
   */
  public QueryableStringColumnShard getStringColumnShard(String name);

  /**
   * Returns a {@link QueryableDoubleColumnShard} for a specific column.
   * 
   * <p>
   * That column shard can either be a temporary one or a "real" one from a {@link TableShard}.
   * 
   * <p>
   * Note that this method might actually return a different instance each time called, but when a column for a name was
   * returned once, there will be data available until this {@link ExecutionEnvironment} is at its end of life.
   * 
   * @return A {@link QueryableDoubleColumnShard} for the column with the given name or <code>null</code> if it does not
   *         exist.
   */
  public QueryableDoubleColumnShard getDoubleColumnShard(String name);

  /**
   * @return the {@link ColumnType} of a column that can be fetched with {@link #getColumnShard(String)},
   *         {@link #getLongColumnShard(String)}, {@link #getStringColumnShard(String)},
   *         {@link #getDoubleColumnShard(String)}, {@link #getPureConstantColumnShard(String)} or
   *         {@link #getPureStandardColumnShard(String)}.
   */
  public ColumnType getColumnType(String colName);

  /**
   * Returns a {@link QueryableColumnShard} for a specific column (no matter what data type the corresponding column
   * has).
   * 
   * <p>
   * That column shard can either be a temporary one or a "real" one from a {@link TableShard}.
   * 
   * <p>
   * Note that this method might actually return a different instance each time called, but when a column for a name was
   * returned once, there will be data available until this {@link ExecutionEnvironment} is at its end of life.
   * 
   * @return A {@link QueryableColumnShard} for the column with the given name or <code>null</code> if it does not
   *         exist.
   */
  public QueryableColumnShard getColumnShard(String name);

  /**
   * Get the "real" (non-facaded) {@link StandardColumnShard} of a specific column.
   * 
   * <p>
   * That column shard can either be a temporary one or a "real" one from a {@link TableShard}.
   * 
   * <p>
   * Note that this method might actually return a different instance each time called, but when a column for a name was
   * returned once, there will be data available until this {@link ExecutionEnvironment} is at its end of life.
   * 
   * @return A {@link StandardColumnShard} for the column or <code>null</code> if the column not exists or if it is no
   *         {@link StandardColumnShard}.
   */
  public StandardColumnShard getPureStandardColumnShard(String name);

  /**
   * Get the "real" (non-facaded) {@link ConstantColumnShard} of a specific column.
   *
   * <p>
   * That column shard can either be a temporary one or a "real" one from a {@link TableShard}.
   * 
   * <p>
   * Note that this method might actually return a different instance each time called, but when a column for a name was
   * returned once, there will be data available until this {@link ExecutionEnvironment} is at its end of life.
   * 
   * @return A {@link ConstantColumnShard} for the column or <code>null</code> if the column not exists or if it is no
   *         {@link ConstantColumnShard}.
   */
  public ConstantColumnShard getPureConstantColumnShard(String name);

  /**
   * @return <code>true</code> if the given column is a temporary one, <code>false</code> if it is a real column present
   *         in a {@link TableShard}.
   */
  public boolean isTemporaryColumn(String colName);

  /**
   * Returns a map from colName to a list of {@link QueryableColumnShard}s for all temporary columns.
   * 
   * <p>
   * On the query master we may have several versions of a column (see {@link VersionedExecutionEnvironment}), this
   * method returns all versions of all columns, the last entry in the list being the newest version. Note that the
   * actual column shard objects might be different instances with each call.
   * 
   * <p>
   * This method will <b>not</b> return cached columns which have not been requested at least once using
   * {@link #getColumnShard(String)} etc. It will <b>not</b> load any other cached column shards into this
   * ExecutionEnvironment in order to make sure that the column stays available.
   * 
   * <p>
   * This method <b>will</b> return all column shards that were loaded from a cache because of a call to
   * {@link #getColumnShard(String)} etc.
   */
  public Map<String, List<QueryableColumnShard>> getAllTemporaryColumnShards();

  /**
   * Returns a map from colName to a list of {@link QueryableColumnShard}s for all non-temporary columns.
   * 
   * On the query master we may have several versions of a column (see {@link VersionedExecutionEnvironment}), this
   * method returns all versions of all columns, the last entry in the list being the newest version. Note that the
   * actual column shard objects might be different instances with each call.
   */
  public Map<String, QueryableColumnShard> getAllNonTemporaryColumnShards();

  /**
   * Store a new temporary {@link LongColumnShard} in this {@link ExecutionEnvironment}.
   */
  public void storeTemporaryLongColumnShard(LongColumnShard column);

  /**
   * Store a new temporary {@link StringColumnShard} in this {@link ExecutionEnvironment}.
   */
  public void storeTemporaryStringColumnShard(StringColumnShard column);

  /**
   * Store a new temporary {@link DoubleColumnShard} in this {@link ExecutionEnvironment}.
   */
  public void storeTemporaryDoubleColumnShard(DoubleColumnShard column);

  /**
   * @return The overall lowest rowID of all columns of this {@link ExecutionEnvironment}.
   */
  public long getFirstRowIdInShard();

  /**
   * @return -1 if unknown, which typically happens on the query master, as it does not have a backing
   *         {@link TableShard}.
   */
  public long getLastRowIdInShard();

  /**
   * @return -1 if unknown, which typically happens on the query master, as it does not have a backing
   *         {@link TableShard}.
   */
  public long getNumberOfRowsInShard();
}
