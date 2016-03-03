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
import org.diqube.function.aggregate.result.IntermediaryResultValueIterator;
import org.diqube.function.aggregate.result.IntermediaryResultValueSink;

/**
 * A function that aggregates values that have been grouped.
 * 
 * <p>
 * An aggregate function object is alive during the computation of one group, which means it is intended to hold a
 * state. Additionally, it is required to calculate an {@link IntermediaryResult} from time to time (
 * {@link #populateIntermediary()}) : The {@link AggregationFunction} is executed on each cluster node, where the
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
public interface AggregationFunction<I, O> {
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
   *          The parameter value. The value can be of the following types: {@link Long}, {@link Double}, {@link String}
   *          .
   * @throws FunctionException
   *           if the value is not supported.
   */
  public void provideConstantParameter(int idx, Object value) throws FunctionException;

  /**
   * Add actual values to the internal state of this function.
   * 
   * <p>
   * The values are not provided directly, but by an instance of a {@link ValueProvider}. The implementing class should
   * call only that method of the ValueProvider which returns the minimum information needed by the function to proceed.
   * 
   * <p>
   * The provided valueProvider carries a flag if the set of values is the "last" set. If that flag is true, the
   * {@link AggregationFunction} has to be able to provide its (final) result in both, the {@link #calculate()} and the
   * {@link #populateIntermediary(IntermediaryResultValueSink)} functions. This is important for
   * {@link AggregationFunction}s that cannot internally handle
   * {@link #removeIntermediary(IntermediaryResultValueIterator)} calls nicely - be sure to populate the result data
   * when all input data is consumed!
   * 
   * @see #needsActualValues()
   */
  public void addValues(ValueProvider<I> valueProvider);

  /**
   * Add intermediary values to the internal state of this instance.
   * 
   * The values provided by the passed iterator have been created by
   * {@link #populateIntermediary(IntermediaryResultValueSink)} of a potentially different instance of this
   * {@link AggregationFunction} class before.
   */
  public void addIntermediary(IntermediaryResultValueIterator intermediary);

  /**
   * Remove intermediary values to the internal state of this instance.
   * 
   * <p>
   * Note that this function does not have to be supported in an internally meaningful way. If the implementation is not
   * capable of removing internal state, it can choose to send updates only after a single instance of the class has
   * received all its input data (flag in {@link ValueProvider} at a call to {@link #addValues(ValueProvider)}).
   * 
   * The values provided by the passed iterator have been created by
   * {@link #populateIntermediary(IntermediaryResultValueSink)} of a potentially different instance of this
   * {@link AggregationFunction} class before.
   */
  public void removeIntermediary(IntermediaryResultValueIterator intermediary);

  /**
   * Populate a given instance of {@link IntermediaryResultValueSink} with the current internal state of this
   * {@link AggregationFunction}.
   * 
   * <p>
   * Expect a {@link IntermediaryResultValueIterator} with the same value-ordering to be passed to
   * {@link #addIntermediary(IntermediaryResultValueSink)} and/or
   * {@link #removeIntermediary(IntermediaryResultValueSink)} on different instances of this {@link AggregationFunction}
   * later on.
   * 
   * @throws FunctionException
   *           If the intermediary cannot be calculated.
   */
  public void populateIntermediary(IntermediaryResultValueSink res) throws FunctionException;

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

    /**
     * @return <code>true</code> if the provided values are the last ones for this {@link AggregationFunction}, because
     *         after this set, all data has been processed. {@link AggregationFunction#addValues(ValueProvider)} will
     *         not be called again.
     */
    public boolean isFinalSetOfValues();
  }
}
