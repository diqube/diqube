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
package org.diqube.function;

import org.diqube.data.column.ColumnType;

/**
 * A function that aggregates values that have been grouped.
 * 
 * <p>
 * An aggregate function object is alive during the computation of one group, which means it is intended to hold a
 * state. Additionally, it is required to calculate an {@link IntermediaryResult} from time to time (
 * {@link #calculateIntermediary()}) : The {@link AggregationFunction} is executed on each cluster node, where the
 * {@link AggregationFunction} object on each node produces an {@link IntermediaryResult} which is passed on to the
 * Query Master node. That node collects the {@link IntermediaryResult}s of all cluster nodes and needs to merge the
 * results for equal groups (i.e. a group that had elements not only on one cluster node). This means that the Query
 * Master will receive the {@link IntermediaryResult} objects and feed them into another instance of the same
 * {@link AggregationFunction} to calculate the final result ({@link #calculate()}).
 * 
 * <p>
 * Implementing classes do not need to be thread-safe.
 * 
 * <p>
 * An aggregation function can have an optional input column name of a specific {@link ColumnType}. Which input column
 * type is supported by a specific class is identified by {@link #getInputType()}. There can be multiple classes with
 * the same function name, but different input column types.
 * 
 * <p>
 * In addition to that, there might be constant parameters provided to the aggregation function. These are typically the
 * first parameters to the function in diql. That constant parameter always has to be of the same type as the input
 * col-type to the aggregation function.
 * 
 * 
 * @param <I>
 *          Input value type
 * @param <M>
 *          Type of {@link IntermediaryResult} this aggregate calculates.
 * @param <O>
 *          Result type of this function.
 *
 * @author Bastian Gloeckle
 */
public interface AggregationFunction<I, M extends IntermediaryResult<?, ?, ?>, O> {
  /**
   * @return Name of the function, lowercase.
   */
  public String getNameLowerCase();

  /**
   * Provide a specific constant parameter value to the function.
   * 
   * @param idx
   *          Index of the parameter.
   * @param value
   *          The parameter value.
   */
  public void provideConstantParameter(int idx, I value);

  /**
   * Add a specific intermediary to the internal value of this instance.
   */
  public void addIntermediary(M intermediary);

  /**
   * Remove a specific intermediary to the internal value of this instance.
   */
  public void removeIntermediary(M intermediary);

  /**
   * Add actual values to the internal state of this function.
   * 
   * <p>
   * The values are not provided directly, but by an instance of a {@link ValueProvider}. The implementing class should
   * call only that method of the ValueProvider which returns the minimum information needed by the function to proceed.
   * 
   * @see #needsActualValues()
   */
  public void addValues(ValueProvider<I> valueProvider);

  /**
   * Create and return a new instance of {@link IntermediaryResult} which represents the current internal state of this
   * {@link AggregationFunction}.
   * 
   * @throws FunctionException
   *           If the intermediary cannot be calculated.
   */
  public M calculateIntermediary() throws FunctionException;

  /**
   * Calculate and return the final result.
   * 
   * @throws FunctionException
   *           If the result cannot be calculated.
   */
  public O calculate() throws FunctionException;

  /**
   * @return data type of the output of this function.
   */
  public ColumnType getOutputType();

  /**
   * If this aggregation function needs an input column, this method returns the type of that input column. If this
   * method does not need an input parameter, <code>null</code> must be returned.
   */
  public ColumnType getInputType();

  /**
   * @return true if {@link #addValues(ValueProvider)} will call the {@link ValueProvider#getValues()} method, false if
   *         not.
   */
  public boolean needsActualValues();

  /**
   * Provides values which the function needs in the {@link AggregationFunction#addValues(ValueProvider)} call.
   */
  public static interface ValueProvider<I> {

    /**
     * Fully resolve the values being provided. This is the most expensive function.
     * 
     * <p>
     * This method must only be called by the implementation of {@link AggregationFunction} if
     * {@link AggregationFunction#getInputType()} != null, as there needs to be an actual input column to resolve the
     * column value IDs of.
     */
    public I[] getValues();

    /**
     * Returns the number of values without resovling the values themselves.
     */
    public long size();
  }
}
